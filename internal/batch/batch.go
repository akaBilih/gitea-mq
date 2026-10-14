// Package batch implements bors-style batch testing: build a single merge
// branch from N queued PRs, fast-forward the target on green, and bisect on
// red. All state lives on one pg.Batch row so the loop is restart-safe.
package batch

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/logutil"
	"github.com/Mic92/gitea-mq/internal/merge"
	"github.com/Mic92/gitea-mq/internal/queue"
	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

// MaxFFRetries caps consecutive ErrNotFastForward rebuilds before the
// remaining current members are ejected with an actionable error.
const MaxFFRetries = 3

// BranchPrefix is the namespace for batch merge branches.
const BranchPrefix = merge.BranchPrefix + "batch/"

// BranchName returns the canonical batch branch name.
func BranchName(id int64) string {
	return fmt.Sprintf("%s%d", BranchPrefix, id)
}

// Engine drives the batch lifecycle for one repository. The pg.Batch row is
// the source of truth; the mutex serialises every public entry point so the
// webhook handler and poller cannot mutate the same batch concurrently.
type Engine struct {
	Forge       forge.Forge
	Queue       *queue.Service
	Owner       string
	Repo        string
	RepoID      int64
	ExternalURL string

	BatchMax       int
	BisectMaxSteps int
	CheckTimeout   time.Duration
	FallbackChecks []string
	MergeLabel     string
	// SkipIfUpToDate lands a single up-to-date PR by fast-forwarding to its
	// head instead of building a batch branch.
	SkipIfUpToDate bool
	// TestWhileBlocked tests the entries the poller queued as waiting (open
	// dependencies that cannot move the target) in batches of their own,
	// whenever nothing that may land is queued. A batch that passes while its
	// members are still blocked is held; waiting PRs queued later are tested
	// as layers on top of it, and the whole stack lands by fast-forward once
	// their dependencies close, without another CI run.
	TestWhileBlocked bool

	// MergedPoll controls ensureMergedOrClose. Defaults: 200ms × 50 = 10s.
	MergedPollInterval time.Duration
	MergedPollAttempts int

	// Advance is called whenever a batch finishes so the next root can be
	// formed without waiting for the poll tick. Optional.
	Advance func()

	mu    sync.Mutex // guards locks
	locks map[string]*sync.Mutex
}

// lock returns the per-target-branch unlock func. Batches for different
// branches in the same repo are independent (ux_batches_live is per branch),
// so serialising them on one mutex would head-of-line block on forge I/O.
func (e *Engine) lock(targetBranch string) func() {
	e.mu.Lock()
	if e.locks == nil {
		e.locks = make(map[string]*sync.Mutex)
	}
	l, ok := e.locks[targetBranch]
	if !ok {
		l = &sync.Mutex{}
		e.locks[targetBranch] = l
	}
	e.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// Enabled reports whether batching is active. BatchMax==1 keeps the legacy
// single-PR path byte-for-byte intact.
func (e *Engine) Enabled() bool { return e != nil && e.BatchMax != 1 }

// pendingStack is the JSONB stack of int64 slices stored on the batch row.
type pendingStack [][]int64

func loadPending(raw []byte) pendingStack {
	var p pendingStack
	if len(raw) == 0 {
		return p
	}
	_ = json.Unmarshal(raw, &p)
	return p
}

func (p pendingStack) bytes() []byte {
	if len(p) == 0 {
		return []byte("[]")
	}
	b, _ := json.Marshal(p)
	return b
}

func (p pendingStack) drop(id int64) pendingStack {
	eq := func(v int64) bool { return v == id }
	out := p[:0]
	for _, s := range p {
		if s = slices.DeleteFunc(s, eq); len(s) > 0 {
			out = append(out, s)
		}
	}
	return out
}

// FormAndBuild forms a new root batch from the queued head and builds its
// branch. Returns nil when there is nothing to do (queue empty or a live
// batch already exists). Safe to call on every poll tick.
func (e *Engine) FormAndBuild(ctx context.Context, targetBranch string) (*pg.Batch, error) {
	defer e.lock(targetBranch)()
	live, err := e.Queue.GetLiveBatch(ctx, e.RepoID, targetBranch)
	if err != nil {
		return nil, err
	}
	if live != nil {
		switch {
		case live.State == pg.BatchStateForming:
			// A previous Build errored mid-run; retry it so a transient forge
			// failure does not stall the queue until restart.
			return live, e.Build(ctx, live)
		case live.State == pg.BatchStateHeld,
			live.State == pg.BatchStateTesting && e.TestsWhileBlocked():
			if dissolved, err := e.review(ctx, live); err != nil || !dissolved {
				return nil, err
			}
		default:
			return nil, nil
		}
	}
	b, err := e.Queue.FormBatch(ctx, e.RepoID, targetBranch, e.BatchMax, false)
	if err == nil && b == nil && e.TestsWhileBlocked() {
		// Nothing may land: test what is waiting on its dependencies.
		b, err = e.Queue.FormBatch(ctx, e.RepoID, targetBranch, e.BatchMax, true)
	}
	if err != nil || b == nil {
		return nil, err
	}
	slog.Info("batch formed", "batch", b.ID, "branch", targetBranch, "members", len(b.MemberIds))
	return b, e.Build(ctx, b)
}

// TestsWhileBlocked reports whether PRs blocked by dependencies that cannot
// move the target branch are tested before they may land.
func (e *Engine) TestsWhileBlocked() bool { return e.Enabled() && e.TestWhileBlocked }

// Build constructs the batch branch by merging current_ids onto the target
// tip. Conflicting members are ejected and construction continues. On success
// the batch transitions to testing and every current entry's merge_branch_*
// is set to the batch branch so the existing SHA-routing keeps working.
func (e *Engine) Build(ctx context.Context, b *pg.Batch) error {
	if !b.BranchName.Valid {
		b.BranchName = pgtype.Text{String: BranchName(b.ID), Valid: true}
	}
	branch := b.BranchName.String
	// A layer is built on what the batch already passed, which only the batch
	// branch keeps reachable: the branch is then replaced, never deleted.
	base, layered := b.TargetBranch, len(b.HeldIds) > 0
	if layered {
		base = b.HeldSha.String
	} else {
		_ = e.Forge.DeleteBranch(ctx, e.Owner, e.Repo, branch)
	}

	entries, err := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
	if err != nil {
		return fmt.Errorf("load batch members: %w", err)
	}

	heads := make([]string, len(entries))
	for i := range entries {
		heads[i] = entries[i].PrHeadSha
	}
	var (
		tip   string
		steps []forge.MergeStep
	)
	if !layered {
		tip, steps = e.headIfUpToDate(ctx, base, heads)
	}
	if tip == "" {
		tip, steps, err = e.stack(ctx, base, heads, branch)
	}
	if err != nil {
		// Whole-operation failure (clone/push). Row stays forming;
		// FormAndBuild retries on the next tick.
		return fmt.Errorf("build batch branch: %w", err)
	}

	var (
		surv []pg.QueueEntry
		prev int64 // previous PR number for conflict attribution
	)
	for i := range entries {
		ent, step := &entries[i], steps[i]
		switch {
		case step.Conflict:
			msg := "Merge conflict with target branch"
			if prev != 0 {
				msg = fmt.Sprintf("Merge conflict in batch (after #%d)", prev)
			} else if layered {
				msg = fmt.Sprintf("Merge conflict with the PRs batch #%d already passed", b.ID)
			}
			e.eject(ctx, b, ent, pg.CheckStateFailure, msg,
				"❌ Removed from merge queue: "+msg+". Please rebase and re-schedule automerge.")
		case step.Err != nil:
			e.eject(ctx, b, ent, pg.CheckStateError, "Failed to create merge branch",
				fmt.Sprintf("❌ Removed from merge queue: failed to create merge branch.\n\n```\n%v\n```", step.Err))
		default:
			surv = append(surv, *ent)
			prev = ent.PrNumber
		}
	}

	b.CurrentIds = ids(surv)
	if len(surv) == 0 {
		return e.next(ctx, b)
	}

	b.BranchSha = pgtype.Text{String: tip, Valid: true}
	b.State = pg.BatchStateTesting
	b.TestingStartedAt = pgtype.Timestamptz{Time: time.Now(), Valid: true}
	first := b.Builds == 0
	b.Builds++
	if err := e.Queue.SaveBatch(ctx, b, b.EjectedIds...); err != nil {
		return err
	}

	// Route check events: every current entry carries the batch branch/sha.
	// Clearing the check ledger comes last so a stale SaveCheckStatus that
	// raced in before SetMergeBranch (still keyed on the old SHA) is wiped.
	desc := fmt.Sprintf("Testing batch #%d (%d PRs)", b.ID, len(b.MemberIds))
	for _, ent := range surv {
		logutil.WarnIfErr(e.Queue.SetMergeBranch(ctx, e.RepoID, ent.PrNumber, branch, tip), "set merge branch failed", "pr", ent.PrNumber)
		if first {
			logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
				State: pg.CheckStatePending, Description: desc, TargetURL: e.prURL(ent.PrNumber),
			}), "set mq status failed", "pr", ent.PrNumber)
		}
	}
	logutil.WarnIfErr(e.Queue.ClearCheckStatuses(ctx, b.CurrentIds), "clear check statuses failed", "batch", b.ID)

	slog.Info("batch built", "batch", b.ID, "build", b.Builds, "sha", tip,
		"current", len(surv), "pending", len(loadPending(b.Pending)))
	return nil
}

type landedStatusHead struct {
	sha      string
	prNumber int64
}

// landedStatusHeads expands queued stack tips before the target branch moves.
// GitHub no longer reliably resolves the stack after its members are merged.
func (e *Engine) landedStatusHeads(ctx context.Context, entries []pg.QueueEntry) ([]landedStatusHead, error) {
	var heads []landedStatusHead
	seen := make(map[string]bool)
	add := func(sha string, prNumber int64) {
		if sha == "" || seen[sha] {
			return
		}
		seen[sha] = true
		heads = append(heads, landedStatusHead{sha: sha, prNumber: prNumber})
	}

	for i := range entries {
		ent := &entries[i]
		stack, err := forge.ResolveStack(ctx, e.Forge, e.Owner, e.Repo, ent.PrNumber)
		if err != nil {
			return nil, fmt.Errorf("resolve landed stack for PR #%d: %w", ent.PrNumber, err)
		}
		if stack != nil {
			if members, ok := stack.MembersUpTo(ent.PrNumber); ok {
				for _, member := range members {
					add(member.HeadSHA, ent.PrNumber)
				}
			}
		}
		add(ent.PrHeadSha, ent.PrNumber)
	}
	return heads, nil
}

// HandlePass lands current_ids by fast-forwarding the target branch to the
// tested SHA, then pops the next pending slice.
func (e *Engine) HandlePass(ctx context.Context, b *pg.Batch) error {
	if b.State != pg.BatchStateTesting {
		return nil
	}
	return e.land(ctx, b)
}

// land fast-forwards the target to the tested SHA of a testing or held batch,
// unless open dependencies keep members back.
func (e *Engine) land(ctx context.Context, b *pg.Batch) error {
	// The tested tree holds the layers already passed as well as the one just
	// tested; from here on they are one set, landed or held together.
	if len(b.HeldIds) > 0 {
		b.CurrentIds = slices.Concat(b.HeldIds, b.CurrentIds)
		b.HeldIds, b.HeldSha = nil, pgtype.Text{}
	}
	entries, err := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
	if err != nil {
		return err
	}
	// The fast-forward below skips the forge's dependency check.
	if handled, err := e.holdBlocked(ctx, b, entries); err != nil || handled {
		return err
	}
	statusHeads, err := e.landedStatusHeads(ctx, entries)
	if err != nil {
		return err
	}

	sha := b.BranchSha.String
	if err := e.Forge.FastForward(ctx, e.Owner, e.Repo, b.TargetBranch, sha); err != nil {
		var denied *forge.PushDeniedError
		switch {
		case errors.Is(err, forge.ErrNotFastForward):
			b.FfRetries++
			if b.FfRetries < MaxFFRetries {
				slog.Info("batch fast-forward raced; rebuilding", "batch", b.ID, "retry", b.FfRetries)
				return e.rebuild(ctx, b)
			}
			e.ejectCurrent(ctx, b, pg.CheckStateError, "Target branch moved repeatedly",
				fmt.Sprintf("⚠️ Removed from merge queue: the target branch moved during fast-forward "+
					"%d times in a row — is something else pushing to `%s`?", MaxFFRetries, b.TargetBranch))
		case errors.As(err, &denied):
			e.ejectCurrent(ctx, b, pg.CheckStateError, "gitea-mq cannot push to "+b.TargetBranch,
				fmt.Sprintf("⚠️ Removed from merge queue: gitea-mq is not allowed to push to `%s`.\n\n"+
					"Add the gitea-mq user/app to the branch's push whitelist (or ruleset bypass) and re-schedule.\n\n```\n%s\n```",
					b.TargetBranch, denied.Message))
		default:
			return fmt.Errorf("fast-forward %s: %w", b.TargetBranch, err)
		}
		return e.next(ctx, b)
	}
	b.FfRetries = 0

	// Persist before the best-effort forge work: the target already moved.
	landed := b.CurrentIds
	b.LandedIds = append(b.LandedIds, landed...)
	b.CurrentIds = nil
	build := popNext(b)
	if err := e.Queue.SaveBatch(ctx, b, slices.Concat(landed, b.EjectedIds)...); err != nil {
		return err
	}
	slog.Info("batch landed", "batch", b.ID, "sha", sha, "prs", len(entries))

	desc := fmt.Sprintf("Merged via batch #%d", b.ID)
	for _, head := range statusHeads {
		logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, head.sha, forge.MQStatus{
			State: pg.CheckStateSuccess, Description: desc, TargetURL: e.prURL(head.prNumber),
		}), "set mq status failed", "pr", head.prNumber)
		merge.SkipPendingMirroredChecks(ctx, e.Forge, e.Owner, e.Repo, head.sha)
	}

	var wg sync.WaitGroup
	for i := range entries {
		ent := &entries[i]
		wg.Go(func() { e.ensureMergedOrClose(ctx, ent, sha, b.ID) })
	}
	wg.Wait()

	return e.finish(ctx, b, build)
}

// HandleFail bisects: split current, push the second half, rebuild the first.
// A failing singleton is ejected with the failed check attributed.
func (e *Engine) HandleFail(ctx context.Context, b *pg.Batch, failedCheck, targetURL string) error {
	if b.State != pg.BatchStateTesting {
		return nil
	}
	if len(b.CurrentIds) == 1 {
		entries, _ := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
		if len(entries) == 1 {
			ref := failedCheck
			if targetURL != "" {
				ref = fmt.Sprintf("[%s](%s)", failedCheck, targetURL)
			}
			e.eject(ctx, b, &entries[0], pg.CheckStateFailure,
				"Check failed: "+failedCheck,
				"❌ Removed from merge queue: Check failed: "+ref)
		}
		b.CurrentIds = nil
		return e.next(ctx, b)
	}

	if e.BisectMaxSteps > 0 && int(b.Builds) >= e.BisectMaxSteps {
		// Cap is on builds, not splits: drain pending too so next() finishes
		// instead of popping another slice and rebuilding past the cap.
		for _, s := range loadPending(b.Pending) {
			b.CurrentIds = append(b.CurrentIds, s...)
		}
		b.Pending = nil
		e.ejectCurrent(ctx, b, pg.CheckStateError,
			"Bisection limit reached",
			fmt.Sprintf("⚠️ Removed from merge queue: batch bisection reached the configured limit of %d builds.", e.BisectMaxSteps))
		return e.next(ctx, b)
	}

	mid := len(b.CurrentIds) / 2
	left := slices.Clone(b.CurrentIds[:mid])
	right := slices.Clone(b.CurrentIds[mid:])
	pending := append(loadPending(b.Pending), right)
	b.Pending = pending.bytes()
	b.CurrentIds = left
	// Right-half members leave the branch; clear their routing so late events
	// for this build's SHA cannot reach them.
	logutil.WarnIfErr(e.Queue.ClearMergeBranch(ctx, right), "clear merge branch failed", "batch", b.ID)
	slog.Info("batch bisecting", "batch", b.ID, "build", b.Builds,
		"failed_check", failedCheck, "left", len(left), "right", len(right))
	return e.rebuild(ctx, b)
}

// HandleTimeout treats a CI timeout as a batch failure. The batch is reloaded
// under the lock so a poller snapshot that raced a webhook-driven rebuild
// cannot bisect a stale view.
func (e *Engine) HandleTimeout(ctx context.Context, targetBranch string, batchID int64) error {
	defer e.lock(targetBranch)()
	b, err := e.Queue.GetBatch(ctx, batchID)
	if err != nil || b == nil || b.State != pg.BatchStateTesting || !TimedOut(b, e.CheckTimeout) {
		return err
	}
	return e.HandleFail(ctx, b, "timeout", "")
}

// OnMemberRemoved drops an entry from the live batch (push/close/retarget/
// cancel). The caller is responsible for cancelling automerge / commenting on
// the PR; this function only adjusts batch bookkeeping and rebuilds when the
// removed entry was on the branch under test.
func (e *Engine) OnMemberRemoved(ctx context.Context, targetBranch string, batchID, entryID int64) error {
	defer e.lock(targetBranch)()
	b, err := e.Queue.GetBatch(ctx, batchID)
	if err != nil || b == nil {
		return err
	}
	if b.State != pg.BatchStateForming && b.State != pg.BatchStateTesting && b.State != pg.BatchStateHeld {
		return nil
	}
	id := entryID
	if slices.Contains(b.HeldIds, id) {
		// What the batch passed no longer exists without it: start over.
		b.HeldIds = slices.DeleteFunc(b.HeldIds, func(v int64) bool { return v == id })
		b.EjectedIds = append(b.EjectedIds, id)
		return e.dissolve(ctx, b, "a PR tested with it left the queue")
	}
	wasCurrent := slices.Contains(b.CurrentIds, id)
	b.CurrentIds = slices.DeleteFunc(b.CurrentIds, func(v int64) bool { return v == id })
	b.Pending = loadPending(b.Pending).drop(id).bytes()
	b.EjectedIds = append(b.EjectedIds, id)
	if !wasCurrent {
		return e.Queue.SaveBatch(ctx, b, b.EjectedIds...)
	}
	if len(b.CurrentIds) == 0 {
		return e.next(ctx, b)
	}
	return e.rebuild(ctx, b)
}

// TimedOut reports whether the batch has been testing longer than timeout.
func TimedOut(b *pg.Batch, timeout time.Duration) bool {
	return timeout > 0 && b.TestingStartedAt.Valid &&
		time.Since(b.TestingStartedAt.Time) > timeout
}

// LiveBranchNames returns the branch name of every live batch for the repo so
// the orphan-branch reaper can spare them.
func (e *Engine) LiveBranchNames(ctx context.Context) ([]string, error) {
	bs, err := e.Queue.ListLiveBatches(ctx, e.RepoID)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(bs))
	for _, b := range bs {
		if b.BranchName.Valid {
			out = append(out, b.BranchName.String)
		}
	}
	return out, nil
}

// ReconcileLive resumes batches found at startup: forming → rebuild;
// testing → re-sync each current entry's merge_branch_* from the batch row
// so a crash between Build's SaveBatch and SetMergeBranch cannot strand the
// batch with no entry routing the current SHA; held, or layered on held PRs,
// while testing blocked PRs is switched off → dissolved, since nothing would
// review it any more.
func (e *Engine) ReconcileLive(ctx context.Context) error {
	bs, err := e.Queue.ListLiveBatches(ctx, e.RepoID)
	if err != nil {
		return err
	}
	for i := range bs {
		b := &bs[i]
		unlock := e.lock(b.TargetBranch)
		switch {
		case !e.TestsWhileBlocked() && (b.State == pg.BatchStateHeld || len(b.HeldIds) > 0):
			logutil.WarnIfErr(e.dissolve(ctx, b, "testing blocked PRs is switched off"),
				"dissolve held batch failed", "batch", b.ID)
		case b.State == pg.BatchStateForming:
			slog.Info("batch reconcile: rebuilding forming batch", "batch", b.ID)
			if err := e.Build(ctx, b); err != nil {
				slog.Warn("batch reconcile build failed", "batch", b.ID, "err", err)
			}
		case b.State == pg.BatchStateTesting:
			entries, _ := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
			for _, ent := range entries {
				logutil.WarnIfErr(e.Queue.SetMergeBranch(ctx, e.RepoID, ent.PrNumber,
					b.BranchName.String, b.BranchSha.String), "set merge branch failed", "pr", ent.PrNumber)
			}
		}
		unlock()
	}
	return nil
}

// headIfUpToDate returns the lone head as tip when it already contains base;
// empty tip means a batch branch must be built.
func (e *Engine) headIfUpToDate(ctx context.Context, base string, heads []string) (string, []forge.MergeStep) {
	if !e.SkipIfUpToDate || len(heads) != 1 {
		return "", nil
	}
	up, err := e.Forge.IsUpToDate(ctx, e.Owner, e.Repo, base, heads[0])
	if err != nil {
		slog.Warn("up-to-date check failed, building batch branch", "sha", heads[0], "error", err)
		return "", nil
	}
	if !up {
		return "", nil
	}
	return heads[0], []forge.MergeStep{{}}
}

// stack builds the batch branch, preferring a forge-native MergeStacker when
// available so Gitea uses one clone for the whole stack.
func (e *Engine) stack(ctx context.Context, base string, heads []string, branch string) (string, []forge.MergeStep, error) {
	if s, ok := e.Forge.(forge.MergeStacker); ok {
		return s.StackMerges(ctx, e.Owner, e.Repo, base, heads, branch)
	}
	steps := make([]forge.MergeStep, len(heads))
	var tip string
	for i, head := range heads {
		var (
			sha      string
			conflict bool
			err      error
		)
		if tip == "" {
			sha, conflict, err = e.Forge.CreateMergeBranch(ctx, e.Owner, e.Repo, base, head, branch)
		} else {
			sha, conflict, err = e.Forge.MergeInto(ctx, e.Owner, e.Repo, branch, head)
		}
		steps[i] = forge.MergeStep{Conflict: conflict, Err: err}
		if !conflict && err == nil {
			tip = sha
		}
	}
	return tip, steps, nil
}

// rebuild persists the batch as forming (so a crash before Build's own save
// is picked up by ReconcileLive, and a concurrent HandleCheck drops on the
// state guard) and then runs Build. requeue goes back to the queue in the same
// save.
func (e *Engine) rebuild(ctx context.Context, b *pg.Batch, requeue ...int64) error {
	b.State = pg.BatchStateForming
	if err := e.Queue.SaveBatchRequeuing(ctx, b, requeue, b.EjectedIds...); err != nil {
		return err
	}
	return e.Build(ctx, b)
}

// popNext pops the pending stack into current or marks the batch done.
// Returns true when a build is needed.
func popNext(b *pg.Batch) bool {
	pending := loadPending(b.Pending)
	if n := len(pending); n > 0 {
		b.CurrentIds = pending[n-1]
		b.Pending = pending[:n-1].bytes()
		b.State = pg.BatchStateForming
		return true
	}
	if len(b.HeldIds) > 0 {
		// The last slice of a layer is gone; what passed before stays held.
		b.State = pg.BatchStateHeld
		b.BranchSha = b.HeldSha
		b.CurrentIds = nil
		return false
	}
	// Root failed but every member eventually landed and nobody was ejected →
	// flaky CI or a cross-PR interaction across halves.
	b.Flaky = len(b.EjectedIds) == 0 && b.Builds > 1 &&
		len(b.LandedIds) == len(b.MemberIds)
	b.State = pg.BatchStateDone
	b.CurrentIds = nil
	return false
}

// finish builds the popped slice or cleans up the finished batch; forming
// rows are retried by FormAndBuild.
func (e *Engine) finish(ctx context.Context, b *pg.Batch, build bool) error {
	if build {
		return e.Build(ctx, b)
	}
	if b.State == pg.BatchStateHeld {
		slog.Info("batch held until its dependencies close", "batch", b.ID, "sha", b.BranchSha.String, "held", len(b.HeldIds))
		return nil
	}
	logutil.WarnIfErr(e.Forge.DeleteBranch(ctx, e.Owner, e.Repo, b.BranchName.String), "delete batch branch failed", "branch", b.BranchName.String)
	slog.Info("batch done", "batch", b.ID, "landed", len(b.LandedIds),
		"ejected", len(b.EjectedIds), "builds", b.Builds, "flaky", b.Flaky)
	if e.Advance != nil {
		e.Advance()
	}
	return nil
}

// next pops the pending stack and rebuilds, or finishes the batch. requeue
// goes back to the queue in the same save.
func (e *Engine) next(ctx context.Context, b *pg.Batch, requeue ...int64) error {
	build := popNext(b)
	if err := e.Queue.SaveBatchRequeuing(ctx, b, requeue, b.EjectedIds...); err != nil {
		return err
	}
	return e.finish(ctx, b, build)
}

// eject marks a single member for removal: status, cancel automerge, comment.
func (e *Engine) eject(ctx context.Context, b *pg.Batch, ent *pg.QueueEntry, state pg.CheckState, statusDesc, comment string) {
	logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
		State: state, Description: statusDesc, TargetURL: e.prURL(ent.PrNumber),
	}), "set mq status failed", "pr", ent.PrNumber)
	merge.SkipPendingMirroredChecks(ctx, e.Forge, e.Owner, e.Repo, ent.PrHeadSha)
	logutil.WarnIfErr(forge.CancelMergeIntent(ctx, e.Forge, e.Owner, e.Repo, ent.PrNumber, e.MergeLabel), "cancel merge intent failed", "pr", ent.PrNumber)
	logutil.WarnIfErr(e.Forge.Comment(ctx, e.Owner, e.Repo, ent.PrNumber, comment), "post comment failed", "pr", ent.PrNumber)
	// The next SaveBatch deletes EjectedIds from the queue.
	b.EjectedIds = append(b.EjectedIds, ent.ID)
}

func (e *Engine) ejectCurrent(ctx context.Context, b *pg.Batch, state pg.CheckState, statusDesc, comment string) {
	entries, _ := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
	for i := range entries {
		e.eject(ctx, b, &entries[i], state, statusDesc, comment)
	}
	b.CurrentIds = nil
}

// ensureMergedOrClose waits briefly for the forge to notice the PR's head is
// reachable from the target branch. If it does not, the PR is closed with a
// comment carrying the landing SHA. The forge's merge endpoint is never
// called: that could mint a second merge commit on top of the tested SHA.
func (e *Engine) ensureMergedOrClose(ctx context.Context, ent *pg.QueueEntry, sha string, batchID int64) {
	interval := cmp.Or(e.MergedPollInterval, 200*time.Millisecond)
	for range cmp.Or(e.MergedPollAttempts, 50) {
		pr, err := e.Forge.GetPR(ctx, e.Owner, e.Repo, ent.PrNumber)
		if err == nil && pr != nil && (pr.Merged || pr.State == "closed") {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
	logutil.WarnIfErr(e.Forge.Comment(ctx, e.Owner, e.Repo, ent.PrNumber,
		fmt.Sprintf("✅ Merged as `%s` via batch #%d.", sha, batchID)), "post comment failed", "pr", ent.PrNumber)
	logutil.WarnIfErr(e.Forge.ClosePR(ctx, e.Owner, e.Repo, ent.PrNumber), "close pr failed", "pr", ent.PrNumber)
}

func (e *Engine) prURL(n int64) string {
	return forge.DashboardPRURL(e.ExternalURL, e.Forge.Kind(), e.Owner, e.Repo, n)
}

// MemberBucket is a member's position within a live batch for UI display.
type MemberBucket string

const (
	BucketCurrent MemberBucket = "current"
	BucketPending MemberBucket = "pending"
	BucketLanded  MemberBucket = "landed"
	BucketEjected MemberBucket = "ejected"
	// BucketHeld members passed but wait for dependencies; later layers are
	// tested on top of them.
	BucketHeld MemberBucket = "held"
)

// Bucket reports which slice of b the entry id is in.
func Bucket(b *pg.Batch, id int64) MemberBucket {
	switch {
	case slices.Contains(b.CurrentIds, id):
		return BucketCurrent
	case slices.Contains(b.HeldIds, id):
		return BucketHeld
	case slices.Contains(b.LandedIds, id):
		return BucketLanded
	case slices.Contains(b.EjectedIds, id):
		return BucketEjected
	default:
		for _, s := range loadPending(b.Pending) {
			if slices.Contains(s, id) {
				return BucketPending
			}
		}
	}
	return ""
}

// EntryState is what a queued PR is doing right now. The dashboard and the
// PR state labels both show it, so they cannot disagree; the stored entry
// state alone does not say, since batch members keep "queued" while tested.
type EntryState string

const (
	StateQueued EntryState = "queued"
	// StateQueuedBlocked is queued only to be tested early: an open
	// dependency keeps it from landing.
	StateQueuedBlocked EntryState = "queued-blocked"
	StateTesting       EntryState = "testing"
	// StateHeld passed its tests and lands once its dependencies close.
	StateHeld    EntryState = "held"
	StateMerging EntryState = "merging"
)

// StateOf reports e's state. b is the live batch e belongs to, nil if none.
func StateOf(e *pg.QueueEntry, b *pg.Batch) EntryState {
	if b != nil {
		switch Bucket(b, e.ID) {
		case BucketCurrent:
			return StateTesting
		case BucketHeld:
			return StateHeld
		case BucketLanded:
			return StateMerging
		}
		// A pending half waits for its turn like any queued PR.
	}
	switch e.State {
	case pg.EntryStateTesting:
		return StateTesting
	case pg.EntryStateSuccess:
		return StateMerging
	}
	if e.Waiting {
		return StateQueuedBlocked
	}
	return StateQueued
}

func ids(entries []pg.QueueEntry) []int64 {
	out := make([]int64, len(entries))
	for i, e := range entries {
		out[i] = e.ID
	}
	return out
}

// holdBlocked keeps members with an open dependency from landing. Those the
// poller queued as waiting go back to the queue; the rest leave it with their
// merge intent kept, and the poller re-enqueues them once it closes. When every
// member waits, nothing leaves: the passed tree is held, and any pending half
// is tested on top of it. All lookups happen before any change, so a forge
// error leaves the batch untouched. Reports whether it took b over.
func (e *Engine) holdBlocked(ctx context.Context, b *pg.Batch, entries []pg.QueueEntry) (bool, error) {
	blocked := make(map[int64][]forge.Dependency)
	for i := range entries {
		deps, err := forge.OpenDependencies(ctx, e.Forge, e.Owner, e.Repo, entries[i].PrNumber)
		if err != nil {
			return false, fmt.Errorf("check dependencies of PR #%d: %w", entries[i].PrNumber, err)
		}
		if len(deps) > 0 {
			blocked[entries[i].ID] = deps
		}
	}
	if len(blocked) == 0 {
		return false, nil
	}

	var waiting []int64
	for i := range entries {
		if _, ok := blocked[entries[i].ID]; ok && entries[i].Waiting && e.TestsWhileBlocked() {
			waiting = append(waiting, entries[i].ID)
		}
	}
	if len(waiting) == len(entries) {
		// Nobody may land yet, so what passed stays in the tree as if it had
		// landed: pending halves and later layers are built on top of it.
		b.HeldIds, b.HeldSha, b.CurrentIds = b.CurrentIds, b.BranchSha, nil
		for i := range entries {
			ent := &entries[i]
			logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
				State: pg.CheckStatePending, Description: forge.HeldStatus(b.ID, blocked[ent.ID], e.Owner, e.Repo), TargetURL: e.prURL(ent.PrNumber),
			}), "set mq status failed", "pr", ent.PrNumber)
		}
		slog.Info("batch passed while blocked; holding it", "batch", b.ID, "sha", b.HeldSha.String, "held", len(b.HeldIds))
		return true, e.next(ctx, b)
	}

	for i := range entries {
		ent := &entries[i]
		deps, ok := blocked[ent.ID]
		if !ok || slices.Contains(waiting, ent.ID) {
			continue
		}
		logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
			State: pg.CheckStatePending, Description: forge.BlockedStatus(deps, e.Owner, e.Repo), TargetURL: e.prURL(ent.PrNumber),
		}), "set mq status failed", "pr", ent.PrNumber)
		merge.SkipPendingMirroredChecks(ctx, e.Forge, e.Owner, e.Repo, ent.PrHeadSha)
		logutil.WarnIfErr(e.Forge.Comment(ctx, e.Owner, e.Repo, ent.PrNumber, forge.BlockedComment(deps, e.Owner, e.Repo)),
			"post comment failed", "pr", ent.PrNumber)
		// The next SaveBatch deletes EjectedIds from the queue.
		b.EjectedIds = append(b.EjectedIds, ent.ID)
	}
	e.setWaitingStatuses(ctx, entries, blocked, waiting...)
	b.CurrentIds = slices.DeleteFunc(b.CurrentIds, func(id int64) bool {
		_, ok := blocked[id]
		return ok
	})
	slog.Info("batch members held back by open dependencies", "batch", b.ID, "held", len(blocked),
		"requeued", len(waiting), "current", len(b.CurrentIds))
	if len(b.CurrentIds) == 0 {
		return true, e.next(ctx, b, waiting...)
	}
	return true, e.rebuild(ctx, b, waiting...)
}

// setWaitingStatuses marks blocked entries as queued to be tested meanwhile;
// with only given, just those.
func (e *Engine) setWaitingStatuses(ctx context.Context, entries []pg.QueueEntry, blocked map[int64][]forge.Dependency, only ...int64) {
	for i := range entries {
		ent := &entries[i]
		deps, ok := blocked[ent.ID]
		if !ok || (len(only) > 0 && !slices.Contains(only, ent.ID)) {
			continue
		}
		logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
			State: pg.CheckStatePending, Description: forge.WaitingStatus(deps, e.Owner, e.Repo), TargetURL: e.prURL(ent.PrNumber),
		}), "set mq status failed", "pr", ent.PrNumber)
	}
}

// review re-decides a held batch, and a testing one made only of waiting
// entries, against the queue as it is now. Whatever may land goes first, so
// such a batch never delays a PR that could merge; blocked PRs queued while it
// is held are tested as a new layer on top of it. Reports whether it
// dissolved b.
func (e *Engine) review(ctx context.Context, b *pg.Batch) (bool, error) {
	if b.State == pg.BatchStateHeld && !e.TestsWhileBlocked() {
		return true, e.dissolve(ctx, b, "testing blocked PRs is switched off")
	}
	entries, err := e.Queue.GetEntriesByIDs(ctx, slices.Concat(b.HeldIds, b.CurrentIds))
	if err != nil || len(entries) == 0 {
		return false, err
	}
	if slices.ContainsFunc(entries, func(ent pg.QueueEntry) bool { return !ent.Waiting }) {
		// The poller saw dependencies close. A testing batch runs to its
		// verdict; a held one lands now, and land sorts out any still blocked.
		if b.State == pg.BatchStateHeld {
			return false, e.land(ctx, b)
		}
		return false, nil
	}

	queued, err := e.Queue.List(ctx, e.RepoID, b.TargetBranch)
	if err != nil {
		return false, err
	}
	var waiting int
	for _, q := range queued {
		if q.State != pg.EntryStateQueued {
			continue
		}
		if !q.Waiting {
			return true, e.dissolve(ctx, b, fmt.Sprintf("PR #%d may land first", q.PrNumber))
		}
		waiting++
	}
	if b.State != pg.BatchStateHeld {
		return false, nil
	}
	up, err := e.Forge.IsUpToDate(ctx, e.Owner, e.Repo, b.TargetBranch, b.BranchSha.String)
	if err != nil {
		return false, fmt.Errorf("check held batch #%d against %s: %w", b.ID, b.TargetBranch, err)
	}
	if !up {
		return true, e.dissolve(ctx, b, b.TargetBranch+" moved")
	}
	if waiting > 0 {
		return false, e.layer(ctx, b)
	}
	return false, nil
}

// layer takes up to BatchMax waiting entries into b and tests them on top of
// what b already passed. A failing layer is bisected on its own: what b holds
// is never retested, and every layer that passes joins it, so all of them land
// in one fast-forward once their dependencies close.
func (e *Engine) layer(ctx context.Context, b *pg.Batch) error {
	if ok, err := e.Queue.ExtendBatch(ctx, b, e.BatchMax); err != nil || !ok {
		return err
	}
	slog.Info("batch layer formed", "batch", b.ID, "held", len(b.HeldIds), "layer", len(b.CurrentIds))
	if err := e.Build(ctx, b); err != nil || b.State != pg.BatchStateTesting {
		return err
	}
	entries, _ := e.Queue.GetEntriesByIDs(ctx, b.CurrentIds)
	desc := fmt.Sprintf("Testing on top of batch #%d (%d PRs passed)", b.ID, len(b.HeldIds))
	for i := range entries {
		ent := &entries[i]
		logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
			State: pg.CheckStatePending, Description: desc, TargetURL: e.prURL(ent.PrNumber),
		}), "set mq status failed", "pr", ent.PrNumber)
	}
	return nil
}

// dissolve hands every member still in play back to the queue, where each
// keeps its place, and cancels b; the next FormAndBuild forms a fresh batch.
func (e *Engine) dissolve(ctx context.Context, b *pg.Batch, reason string) error {
	back := slices.Concat(b.HeldIds, b.CurrentIds)
	for _, s := range loadPending(b.Pending) {
		back = append(back, s...)
	}
	entries, _ := e.Queue.GetEntriesByIDs(ctx, back)
	b.HeldIds, b.HeldSha = nil, pgtype.Text{}
	b.CurrentIds, b.Pending = nil, nil
	b.State = pg.BatchStateCancelled
	if err := e.Queue.SaveBatchRequeuing(ctx, b, back, b.EjectedIds...); err != nil {
		return err
	}
	logutil.WarnIfErr(e.Forge.DeleteBranch(ctx, e.Owner, e.Repo, b.BranchName.String), "delete batch branch failed", "branch", b.BranchName.String)
	for i := range entries {
		ent := &entries[i]
		logutil.WarnIfErr(e.Forge.SetMQStatus(ctx, e.Owner, e.Repo, ent.PrHeadSha, forge.MQStatus{
			State: pg.CheckStatePending, Description: "Back in the queue: " + reason, TargetURL: e.prURL(ent.PrNumber),
		}), "set mq status failed", "pr", ent.PrNumber)
	}
	slog.Info("batch dissolved", "batch", b.ID, "reason", reason, "requeued", len(back))
	return nil
}
