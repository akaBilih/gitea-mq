// Package merge handles merge branch creation, cleanup, and startup
// reconciliation for the merge queue.
package merge

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/logutil"
	"github.com/Mic92/gitea-mq/internal/queue"
	"github.com/Mic92/gitea-mq/internal/store/pg"
)

// BranchPrefix is the prefix for temporary merge branches created by gitea-mq.
const BranchPrefix = "gitea-mq/"

// StaleMirrorDescription marks gitea-mq/* mirrored statuses that were reset
// when a PR re-entered testing. The forge has no delete-status API, so we
// overwrite with this sentinel and later flip matching pendings to skipped.
const StaleMirrorDescription = "From a previous merge queue attempt"

func BranchName(prNumber int64) string {
	return fmt.Sprintf("%s%d", BranchPrefix, prNumber)
}

type StartTestingResult struct {
	MergeBranchName string
	MergeBranchSHA  string
	Removed         bool // true if the PR was removed from the queue instead of entering testing
}

// StartTesting creates a merge branch for the head-of-queue PR and
// transitions it to the "testing" state. If the merge conflicts, the PR
// is removed from the queue with automerge cancelled and a comment posted.
func StartTesting(ctx context.Context, f forge.Forge, svc *queue.Service, owner, repo string, repoID int64, entry *pg.QueueEntry, externalURL, mergeLabel string) (*StartTestingResult, error) {
	branchName := BranchName(entry.PrNumber)
	targetURL := forge.DashboardPRURL(externalURL, f.Kind(), owner, repo, entry.PrNumber)

	mergeSHA, conflict, err := f.CreateMergeBranch(ctx, owner, repo, entry.TargetBranch, entry.PrHeadSha, branchName)
	if conflict {
		slog.Info("merge conflict", "pr", entry.PrNumber)

		logutil.WarnIfErr(forge.CancelMergeIntent(ctx, f, owner, repo, entry.PrNumber, mergeLabel), "cancel merge intent failed", "pr", entry.PrNumber)
		logutil.WarnIfErr(f.SetMQStatus(ctx, owner, repo, entry.PrHeadSha, forge.MQStatus{
			State: pg.CheckStateFailure, Description: "Merge conflict with target branch", TargetURL: targetURL,
		}), "set mq status failed", "pr", entry.PrNumber)
		logutil.WarnIfErr(f.Comment(ctx, owner, repo, entry.PrNumber,
			"❌ Removed from merge queue: merge conflict with target branch. Please rebase and re-schedule the merge."),
			"post comment failed", "pr", entry.PrNumber)

		if err := svc.Dequeue(ctx, repoID, entry.PrNumber); err != nil {
			return nil, fmt.Errorf("dequeue conflicting PR #%d: %w", entry.PrNumber, err)
		}
		SkipPendingMirroredChecks(ctx, f, owner, repo, entry.PrHeadSha)
		return &StartTestingResult{Removed: true}, nil
	}
	if err != nil {
		// Non-conflict failure (e.g. unrelated histories) — surface to the
		// user and remove rather than retry silently.
		slog.Error("merge branch creation failed", "pr", entry.PrNumber, "error", err)

		logutil.WarnIfErr(forge.CancelMergeIntent(ctx, f, owner, repo, entry.PrNumber, mergeLabel), "cancel merge intent failed", "pr", entry.PrNumber)
		logutil.WarnIfErr(f.SetMQStatus(ctx, owner, repo, entry.PrHeadSha, forge.MQStatus{
			State: pg.CheckStateError, Description: "Failed to create merge branch", TargetURL: targetURL,
		}), "set mq status failed", "pr", entry.PrNumber)
		logutil.WarnIfErr(f.Comment(ctx, owner, repo, entry.PrNumber,
			fmt.Sprintf("❌ Removed from merge queue: failed to create merge branch.\n\n```\n%v\n```", err)),
			"post comment failed", "pr", entry.PrNumber)

		if err := svc.Dequeue(ctx, repoID, entry.PrNumber); err != nil {
			return nil, fmt.Errorf("dequeue PR #%d after merge error: %w", entry.PrNumber, err)
		}
		SkipPendingMirroredChecks(ctx, f, owner, repo, entry.PrHeadSha)
		return &StartTestingResult{Removed: true}, nil
	}

	if err := svc.SetMergeBranch(ctx, repoID, entry.PrNumber, branchName, mergeSHA); err != nil {
		return nil, fmt.Errorf("set merge branch for PR #%d: %w", entry.PrNumber, err)
	}
	if err := svc.UpdateState(ctx, repoID, entry.PrNumber, pg.EntryStateTesting); err != nil {
		return nil, fmt.Errorf("update state to testing for PR #%d: %w", entry.PrNumber, err)
	}

	// Reset stale gitea-mq/* mirrors from a previous attempt so they don't
	// show outdated results while new CI runs.
	clearStaleMirroredStatuses(ctx, f, owner, repo, entry.PrHeadSha)

	logutil.WarnIfErr(f.SetMQStatus(ctx, owner, repo, entry.PrHeadSha, forge.MQStatus{
		State: pg.CheckStatePending, Description: "Testing merge result", TargetURL: targetURL,
	}), "set mq status failed", "pr", entry.PrNumber)

	slog.Info("started testing", "pr", entry.PrNumber, "branch", branchName, "sha", mergeSHA)

	return &StartTestingResult{MergeBranchName: branchName, MergeBranchSHA: mergeSHA}, nil
}

// SkipPendingMirroredChecks closes mirrored checks after the queue stops
// waiting for their merge-branch results.
func SkipPendingMirroredChecks(ctx context.Context, f forge.Forge, owner, repo, sha string) {
	checks, err := f.GetCheckStates(ctx, owner, repo, sha)
	if err != nil {
		slog.Warn("failed to fetch commit statuses for skip cleanup", "sha", sha, "error", err)
		return
	}
	for ctxName, check := range checks {
		if !strings.HasPrefix(ctxName, forge.MirrorContextPrefix) || check.State != pg.CheckStatePending {
			continue
		}
		logutil.WarnIfErr(f.MirrorCheck(ctx, owner, repo, sha, ctxName, forge.Check{
			State:       forge.CheckState("skipped"),
			Description: "Merge queue stopped waiting for this check",
			TargetURL:   check.TargetURL,
		}), "mirror check skip failed", "sha", sha, "context", ctxName)
	}
}

func clearStaleMirroredStatuses(ctx context.Context, f forge.Forge, owner, repo, sha string) {
	checks, err := f.GetCheckStates(ctx, owner, repo, sha)
	if err != nil {
		slog.Warn("failed to fetch commit statuses for stale cleanup", "sha", sha, "error", err)
		return
	}
	for ctxName := range checks {
		if !forge.IsOwnContext(ctxName) {
			continue
		}
		logutil.WarnIfErr(f.MirrorCheck(ctx, owner, repo, sha, ctxName, forge.Check{
			State:       pg.CheckStatePending,
			Description: StaleMirrorDescription,
		}), "mirror check reset failed", "sha", sha, "context", ctxName)
	}
}

func CleanupMergeBranch(ctx context.Context, f forge.Forge, owner, repo string, entry *pg.QueueEntry) {
	if !entry.MergeBranchName.Valid || entry.MergeBranchName.String == "" {
		return
	}
	if err := f.DeleteBranch(ctx, owner, repo, entry.MergeBranchName.String); err != nil {
		slog.Warn("failed to delete merge branch", "branch", entry.MergeBranchName.String, "error", err)
	}
}

// FindStaleBranches lists gitea-mq/* branches not referenced by any active
// queue entry or live batch. spare lists additional branch names to keep. It
// also returns how many branches were kept.
func FindStaleBranches(ctx context.Context, f forge.Forge, svc *queue.Service, owner, repo string, repoID int64, spare []string) (stale []string, kept int, err error) {
	activeEntries, err := svc.ListActiveEntries(ctx, repoID)
	if err != nil {
		return nil, 0, fmt.Errorf("list active entries: %w", err)
	}

	activeBranches := make(map[string]bool, len(activeEntries)+len(spare))
	for _, e := range activeEntries {
		if e.MergeBranchName.Valid && e.MergeBranchName.String != "" {
			activeBranches[e.MergeBranchName.String] = true
		}
	}
	for _, b := range spare {
		activeBranches[b] = true
	}

	branches, err := f.ListBranches(ctx, owner, repo)
	if err != nil {
		return nil, 0, fmt.Errorf("list branches: %w", err)
	}

	for _, b := range branches {
		if strings.HasPrefix(b, BranchPrefix) && !activeBranches[b] {
			stale = append(stale, b)
		}
	}
	return stale, len(activeBranches), nil
}

// deleteBranches deletes each branch and returns the ones that are gone.
func deleteBranches(ctx context.Context, f forge.Forge, owner, repo string, branches []string) (deleted map[string]bool) {
	deleted = make(map[string]bool, len(branches))
	for _, b := range branches {
		slog.Info("deleting stale merge branch", "owner", owner, "repo", repo, "branch", b)
		if err := f.DeleteBranch(ctx, owner, repo, b); err != nil {
			slog.Warn("failed to delete stale branch", "branch", b, "error", err)
			continue
		}
		deleted[b] = true
	}
	return deleted
}

// CleanupStaleBranches deletes orphaned gitea-mq/* branches not referenced by
// any active queue entry or live batch. Called on startup to clean up after
// crashes; StaleSweeper repeats it while running. spare lists additional
// branch names to keep (live batch branches).
func CleanupStaleBranches(ctx context.Context, f forge.Forge, svc *queue.Service, owner, repo string, repoID int64, spare []string) error {
	stale, kept, err := FindStaleBranches(ctx, f, svc, owner, repo, repoID, spare)
	if err != nil {
		return err
	}
	deleted := deleteBranches(ctx, f, owner, repo, stale)

	slog.Info("startup merge branch cleanup", "owner", owner, "repo", repo,
		"active_branches", kept, "stale_deleted", len(deleted))
	return nil
}

// StaleSweeper removes orphaned gitea-mq/* branches while the process runs, so
// a failed delete or an abandoned batch does not wait for the next restart.
// A branch is only deleted once it has been stale on two consecutive sweeps:
// a merge or batch branch is created before the database row that claims it,
// and a single sighting could fall in that window. Not safe for concurrent use.
type StaleSweeper struct {
	Forge  forge.Forge
	Queue  *queue.Service
	Owner  string
	Repo   string
	RepoID int64
	// Spare returns extra branch names to keep, normally the live batch
	// branches. A failure skips the sweep: sparing nothing would delete them.
	Spare func(ctx context.Context) ([]string, error)

	seen map[string]bool
}

// Sweep runs one pass and returns how many branches it deleted.
func (s *StaleSweeper) Sweep(ctx context.Context) int {
	var spare []string
	if s.Spare != nil {
		var err error
		if spare, err = s.Spare(ctx); err != nil {
			slog.Warn("stale branch sweep skipped", "owner", s.Owner, "repo", s.Repo, "error", err)
			return 0
		}
	}
	stale, _, err := FindStaleBranches(ctx, s.Forge, s.Queue, s.Owner, s.Repo, s.RepoID, spare)
	if err != nil {
		slog.Warn("stale branch sweep failed", "owner", s.Owner, "repo", s.Repo, "error", err)
		return 0
	}

	var confirmed []string
	for _, b := range stale {
		if s.seen[b] {
			confirmed = append(confirmed, b)
		}
	}
	deleted := deleteBranches(ctx, s.Forge, s.Owner, s.Repo, confirmed)

	// Keep the ones still there (first sighting, or a delete that failed).
	s.seen = make(map[string]bool, len(stale))
	for _, b := range stale {
		if !deleted[b] {
			s.seen[b] = true
		}
	}
	return len(deleted)
}
