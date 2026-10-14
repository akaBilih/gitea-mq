package batch_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/gitea-mq/internal/batch"
	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/queue"
	"github.com/Mic92/gitea-mq/internal/store/pg"
)

// setupBlocked is setup with testing blocked PRs on and waiting queued as the
// poller would queue them: blocked by dependencies that cannot move main.
func setupBlocked(t *testing.T, waiting ...int64) (*batch.Engine, *fakeForge, *queue.Service, context.Context) {
	t.Helper()
	e, f, svc, ctx := setup(t)
	e.TestWhileBlocked = true
	for _, n := range waiting {
		if _, err := svc.EnqueueWaiting(ctx, e.RepoID, n, fmt.Sprintf("sha%d", n), "main"); err != nil {
			t.Fatal(err)
		}
	}
	f.IsUpToDateFn = func(_ context.Context, _, _, _, head string) (bool, error) {
		return strings.Contains(head, f.target), nil
	}
	return e, f, svc, ctx
}

// blockedBy makes prs depend on #7 for as long as *open holds.
func blockedBy(f *fakeForge, open *bool, prs ...int64) {
	f.OpenDependenciesFn = func(_ context.Context, _, _ string, n int64) ([]forge.Dependency, error) {
		if *open && slices.Contains(prs, n) {
			return []forge.Dependency{{Number: 7, PR: true}}, nil
		}
		return nil, nil
	}
}

func entryID(t *testing.T, svc *queue.Service, ctx context.Context, repoID, pr int64) int64 {
	t.Helper()
	ent, err := svc.GetEntry(ctx, repoID, pr)
	if err != nil || ent == nil {
		t.Fatalf("PR #%d not queued: %v", pr, err)
	}
	return ent.ID
}

// holdAll forms a batch of the waiting entries and passes it while blocked.
func holdAll(t *testing.T, e *batch.Engine, svc *queue.Service, ctx context.Context) *pg.Batch {
	t.Helper()
	b, err := e.FormAndBuild(ctx, "main")
	if err != nil || b == nil {
		t.Fatalf("no batch formed from waiting entries: %v", err)
	}
	if err := e.HandlePass(ctx, b); err != nil {
		t.Fatal(err)
	}
	if b = mustLive(t, svc, ctx, e.RepoID); b.State != pg.BatchStateHeld {
		t.Fatalf("want the passed batch held, got %+v", b)
	}
	return b
}

func lastStatus(f *fakeForge, sha string) string {
	var desc string
	for _, c := range f.CallsTo("SetMQStatus") {
		if c.Args[2] == sha {
			desc = c.Args[3].(forge.MQStatus).Description
		}
	}
	return desc
}

// The point of the feature: a batch of blocked PRs is tested while they wait,
// and lands as soon as the dependency closes, without another CI run.
func TestTestWhileBlocked_PassedBatchLandsOnceDependenciesClose(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10, 20)
	open := true
	blockedBy(f, &open, 10, 20)

	b := holdAll(t, e, svc, ctx)
	tested := b.BranchSha.String
	if want := "m(m(main0,sha10),sha20)"; tested != want || f.target != "main0" {
		t.Fatalf("held sha %q target %q, want %q held and main untouched", tested, f.target, want)
	}
	if got := lastStatus(f, "sha10"); !strings.Contains(got, "passed") || !strings.Contains(got, "#7") {
		t.Fatalf("held status on #10 = %q", got)
	}

	// Still blocked: nothing moves.
	if _, err := e.FormAndBuild(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	if live := mustLive(t, svc, ctx, e.RepoID); live.ID != b.ID || live.State != pg.BatchStateHeld {
		t.Fatalf("held batch changed while still blocked: %+v", live)
	}
	builds := len(f.CallsTo("CreateMergeBranch"))

	// The dependency closes; the poller clears the marks on its next poll.
	open = false
	for _, n := range []int64{10, 20} {
		if err := svc.SetWaiting(ctx, entryID(t, svc, ctx, e.RepoID, n), false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.FormAndBuild(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	if f.target != tested {
		t.Fatalf("target = %q, want the held sha %q", f.target, tested)
	}
	if n := len(f.CallsTo("CreateMergeBranch")); n != builds {
		t.Fatalf("rebuilt %d time(s) before landing; the held sha was already tested", n-builds)
	}
	for _, n := range []int64{10, 20} {
		if ent, _ := svc.GetEntry(ctx, e.RepoID, n); ent != nil {
			t.Fatalf("PR #%d still queued after landing", n)
		}
	}
}

// A PR that may land never waits behind blocked ones being tested: the held
// batch is dissolved and its members keep their place, still waiting.
func TestTestWhileBlocked_ReadyPRGoesFirst(t *testing.T) {
	for _, state := range []pg.BatchState{pg.BatchStateHeld, pg.BatchStateTesting} {
		t.Run(string(state), func(t *testing.T) {
			e, f, svc, ctx := setupBlocked(t, 10)
			open := true
			blockedBy(f, &open, 10)
			var old *pg.Batch
			if state == pg.BatchStateHeld {
				old = holdAll(t, e, svc, ctx)
			} else {
				var err error
				if old, err = e.FormAndBuild(ctx, "main"); err != nil || old == nil {
					t.Fatalf("no batch formed: %v", err)
				}
			}

			if _, err := svc.Enqueue(ctx, e.RepoID, 30, "sha30", "main"); err != nil {
				t.Fatal(err)
			}
			b, err := e.FormAndBuild(ctx, "main")
			if err != nil || b == nil {
				t.Fatalf("no batch for the ready PR: %v", err)
			}
			id30 := entryID(t, svc, ctx, e.RepoID, 30)
			if !slices.Equal(b.CurrentIds, []int64{id30}) || b.BranchSha.String != "m(main0,sha30)" {
				t.Fatalf("want a batch of #30 alone, got %+v", b)
			}
			if gone, _ := svc.GetBatch(ctx, old.ID); gone.State != pg.BatchStateCancelled {
				t.Fatalf("old batch state = %s, want cancelled", gone.State)
			}
			ent, _ := svc.GetEntry(ctx, e.RepoID, 10)
			if ent == nil || ent.State != pg.EntryStateQueued || !ent.Waiting || ent.ActiveBatchID.Valid {
				t.Fatalf("#10 should be back in the queue, still waiting: %+v", ent)
			}
			if n := len(f.CallsTo("Comment")) + len(f.CallsTo("CancelAutoMerge")); n != 0 {
				t.Fatalf("%d comment/cancel call(s); #10 only made way", n)
			}
		})
	}
}

// The held sha is only good while the target stays put: when it moves, the
// waiting PRs are tested again on the new tip.
func TestTestWhileBlocked_RetestsWhenTargetMoves(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10)
	open := true
	blockedBy(f, &open, 10)
	old := holdAll(t, e, svc, ctx)

	f.target = "main1" // something else landed on main
	b, err := e.FormAndBuild(ctx, "main")
	if err != nil || b == nil {
		t.Fatalf("held batch not rebuilt after main moved: %v", err)
	}
	if b.ID == old.ID || b.BranchSha.String != "m(main1,sha10)" || b.State != pg.BatchStateTesting {
		t.Fatalf("want #10 retested on main1, got %+v", b)
	}
}

// A PR blocked after the batch passed joins it: the batch is formed again with
// everyone, so all of them land at once when the dependency closes.
func TestTestWhileBlocked_NewlyBlockedPRJoinsHeldBatch(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10)
	open := true
	blockedBy(f, &open, 10, 20)
	holdAll(t, e, svc, ctx)

	if _, err := svc.EnqueueWaiting(ctx, e.RepoID, 20, "sha20", "main"); err != nil {
		t.Fatal(err)
	}
	b, err := e.FormAndBuild(ctx, "main")
	if err != nil || b == nil {
		t.Fatalf("held batch not re-formed with the newcomer: %v", err)
	}
	if b.BranchSha.String != "m(m(main0,sha10),sha20)" {
		t.Fatalf("want both tested together, got %+v", b)
	}

	// Full: a batch of BatchMax members does not reopen.
	e2, f2, svc2, ctx2 := setupBlocked(t, 10, 20)
	e2.BatchMax = 2
	blockedBy(f2, &open, 10, 20, 30)
	held := holdAll(t, e2, svc2, ctx2)
	if _, err := svc2.EnqueueWaiting(ctx2, e2.RepoID, 30, "sha30", "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.FormAndBuild(ctx2, "main"); err != nil {
		t.Fatal(err)
	}
	if live := mustLive(t, svc2, ctx2, e2.RepoID); live.ID != held.ID || live.State != pg.BatchStateHeld {
		t.Fatalf("full held batch re-formed: %+v", live)
	}
}

// Bisecting blocked PRs: nothing can land for the next half to build on, so a
// passing half goes back to the queue and the last one is held. The half sent
// back does not re-form the batch: together they failed, and would forever.
func TestTestWhileBlocked_BisectionHoldsLastHalfWithoutLooping(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10, 20)
	open := true
	blockedBy(f, &open, 10, 20)
	b, err := e.FormAndBuild(ctx, "main")
	if err != nil || b == nil {
		t.Fatalf("no batch formed: %v", err)
	}
	if err := e.HandleFail(ctx, b, "ci", ""); err != nil {
		t.Fatal(err)
	}
	if err := e.HandlePass(ctx, b); err != nil { // #10 alone
		t.Fatal(err)
	}
	id20 := entryID(t, svc, ctx, e.RepoID, 20)
	if !slices.Equal(b.CurrentIds, []int64{id20}) || b.BranchSha.String != "m(main0,sha20)" {
		t.Fatalf("want #20 tested next on main0, got %+v", b)
	}
	if ent, _ := svc.GetEntry(ctx, e.RepoID, 10); ent == nil || ent.State != pg.EntryStateQueued || !ent.Waiting {
		t.Fatalf("#10 should be back in the queue, waiting: %+v", ent)
	}
	if err := e.HandlePass(ctx, b); err != nil { // #20 alone
		t.Fatal(err)
	}

	if _, err := e.FormAndBuild(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	live := mustLive(t, svc, ctx, e.RepoID)
	if live.ID != b.ID || live.State != pg.BatchStateHeld || !slices.Equal(live.CurrentIds, []int64{id20}) {
		t.Fatalf("want #20 still held alone, got %+v", live)
	}
	if n := len(f.CallsTo("Comment")) + len(f.CallsTo("CancelAutoMerge")); n != 0 {
		t.Fatalf("%d comment/cancel call(s); a passing half was treated as failed", n)
	}
}

// A member blocked while its batch was testing, by a dependency that cannot
// move main, goes back to the queue to be tested meanwhile; the rest is tested
// again without it. Compare TestHandlePass_HoldsMemberWithNewDependency.
func TestTestWhileBlocked_MemberBlockedMidTestIsRequeued(t *testing.T) {
	e, f, svc, ctx := setup(t, 10, 20)
	e.TestWhileBlocked = true
	b, err := e.FormAndBuild(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	open := true
	blockedBy(f, &open, 20)
	if err := svc.SetWaiting(ctx, entryID(t, svc, ctx, e.RepoID, 20), true); err != nil { // as the poller would
		t.Fatal(err)
	}

	if err := e.HandlePass(ctx, b); err != nil {
		t.Fatal(err)
	}
	if want := "m(main0,sha10)"; f.target != "main0" || b.State != pg.BatchStateTesting || b.BranchSha.String != want {
		t.Fatalf("want #10 rebuilt alone as %q, target untouched; got target %q batch %+v", want, f.target, b)
	}
	ent, _ := svc.GetEntry(ctx, e.RepoID, 20)
	if ent == nil || ent.State != pg.EntryStateQueued || !ent.Waiting || ent.ActiveBatchID.Valid {
		t.Fatalf("#20 should wait in the queue: %+v", ent)
	}
	if n := len(f.CallsTo("Comment")); n != 0 {
		t.Fatalf("Comment calls = %d; #20 never left the queue", n)
	}
}

// Switched off, waiting entries are left alone, and a held batch found at
// startup is dissolved: nothing would review it any more.
func TestTestWhileBlocked_Off(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10)
	open := true
	blockedBy(f, &open, 10)
	held := holdAll(t, e, svc, ctx)

	e.TestWhileBlocked = false
	if err := e.ReconcileLive(ctx); err != nil {
		t.Fatal(err)
	}
	if b, _ := svc.GetBatch(ctx, held.ID); b.State != pg.BatchStateCancelled {
		t.Fatalf("held batch state = %s, want cancelled", b.State)
	}
	if b, err := e.FormAndBuild(ctx, "main"); err != nil || b != nil {
		t.Fatalf("formed a batch of waiting entries while switched off: %+v, %v", b, err)
	}
}
