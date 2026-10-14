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

// PRs blocked after the batch passed are tested as layers of up to BatchMax on
// top of it, never retesting what it holds; every layer that passes joins it,
// and the whole stack lands in one fast-forward when the dependency closes.
func TestTestWhileBlocked_LayersAccumulateAndLandTogether(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10)
	e.BatchMax = 2
	open := true
	blockedBy(f, &open, 10, 20, 30, 40)
	held := holdAll(t, e, svc, ctx)
	for _, n := range []int64{20, 30, 40} {
		if _, err := svc.EnqueueWaiting(ctx, e.RepoID, n, fmt.Sprintf("sha%d", n), "main"); err != nil {
			t.Fatal(err)
		}
	}

	// A layer of two on top of #10, then a layer of one on top of all three.
	for _, want := range []string{
		"m(m(m(main0,sha10),sha20),sha30)",
		"m(m(m(m(main0,sha10),sha20),sha30),sha40)",
	} {
		if _, err := e.FormAndBuild(ctx, "main"); err != nil {
			t.Fatal(err)
		}
		b := mustLive(t, svc, ctx, e.RepoID)
		if b.ID != held.ID || b.State != pg.BatchStateTesting || b.BranchSha.String != want {
			t.Fatalf("want a layer of batch #%d tested as %q, got %+v", held.ID, want, b)
		}
		if err := e.HandlePass(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	b := mustLive(t, svc, ctx, e.RepoID)
	if b.State != pg.BatchStateHeld || len(b.HeldIds) != 4 || len(b.CurrentIds) != 0 {
		t.Fatalf("want all four held, got %+v", b)
	}
	if n := len(f.CallsTo("CreateMergeBranch")) + len(f.CallsTo("MergeInto")); n != 4 {
		t.Fatalf("%d merges; each PR is merged once, nothing held is rebuilt", n)
	}

	open = false
	for _, n := range []int64{10, 20, 30, 40} {
		if err := svc.SetWaiting(ctx, entryID(t, svc, ctx, e.RepoID, n), false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.FormAndBuild(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	if want := "m(m(m(m(main0,sha10),sha20),sha30),sha40)"; f.target != want {
		t.Fatalf("target = %q, want the whole stack %q", f.target, want)
	}
	if b, _ := svc.GetBatch(ctx, held.ID); b.State != pg.BatchStateDone || len(b.LandedIds) != 4 {
		t.Fatalf("want all four landed, got %+v", b)
	}
}

// A failing layer is bisected on its own: the culprit is ejected and what the
// batch already holds stays held as it was, untouched.
func TestTestWhileBlocked_FailingLayerLeavesHeldAlone(t *testing.T) {
	e, f, svc, ctx := setupBlocked(t, 10)
	open := true
	blockedBy(f, &open, 10, 20)
	held := holdAll(t, e, svc, ctx)
	passed := held.BranchSha.String
	if _, err := svc.EnqueueWaiting(ctx, e.RepoID, 20, "sha20", "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.FormAndBuild(ctx, "main"); err != nil {
		t.Fatal(err)
	}

	if err := e.HandleFail(ctx, mustLive(t, svc, ctx, e.RepoID), "ci", ""); err != nil {
		t.Fatal(err)
	}
	b := mustLive(t, svc, ctx, e.RepoID)
	id10 := entryID(t, svc, ctx, e.RepoID, 10)
	if b.ID != held.ID || b.State != pg.BatchStateHeld || b.BranchSha.String != passed || !slices.Equal(b.HeldIds, []int64{id10}) {
		t.Fatalf("want #10 held as before at %q, got %+v", passed, b)
	}
	if ent, _ := svc.GetEntry(ctx, e.RepoID, 20); ent != nil {
		t.Fatalf("#20 failed and should have left the queue: %+v", ent)
	}
	if n := len(f.CallsTo("CancelAutoMerge")); n != 1 {
		t.Fatalf("CancelAutoMerge calls = %d, want #20's only", n)
	}
}

// Bisecting blocked PRs works as with ready ones: a passing half is kept in
// the tree as if it had landed, the next half is tested on top of it, and the
// culprit is ejected. Nothing goes back to the queue, so nothing can loop.
func TestTestWhileBlocked_BisectionStacksPassingHalf(t *testing.T) {
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
	id10, id20 := entryID(t, svc, ctx, e.RepoID, 10), entryID(t, svc, ctx, e.RepoID, 20)
	if !slices.Equal(b.HeldIds, []int64{id10}) || !slices.Equal(b.CurrentIds, []int64{id20}) ||
		b.BranchSha.String != "m(m(main0,sha10),sha20)" {
		t.Fatalf("want #20 tested on top of #10, got %+v", b)
	}
	if err := e.HandleFail(ctx, b, "ci", ""); err != nil { // #20 is the culprit
		t.Fatal(err)
	}
	if b.State != pg.BatchStateHeld || b.BranchSha.String != "m(main0,sha10)" || !slices.Equal(b.HeldIds, []int64{id10}) {
		t.Fatalf("want #10 held alone, got %+v", b)
	}
	if n := len(f.CallsTo("CancelAutoMerge")); n != 1 {
		t.Fatalf("CancelAutoMerge calls = %d, want #20's only", n)
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
