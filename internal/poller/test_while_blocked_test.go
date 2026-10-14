package poller_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Mic92/gitea-mq/internal/batch"
	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/poller"
	"github.com/Mic92/gitea-mq/internal/queue"
	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/Mic92/gitea-mq/internal/testutil"
)

// releaseRig models a release freeze: PRs into develop depend on #100, the
// promotion PR develop -> master, whose merge cannot move develop.
type releaseRig struct {
	deps     *poller.Deps
	mock     *forge.MockForge
	svc      *queue.Service
	ctx      context.Context
	repoID   int64
	develop  string            // tip of develop
	open     map[int64]bool    // open dependencies
	blockers map[int64][]int64 // PR -> the PRs it depends on
	bases    map[int64]string  // PR -> base branch
}

func newReleaseRig(t *testing.T, prs ...forge.PR) *releaseRig {
	t.Helper()
	svc, ctx, repoID := testutil.TestQueueService(t)
	mock := &forge.MockForge{}
	r := &releaseRig{
		svc: svc, ctx: ctx, repoID: repoID, mock: mock,
		develop:  "develop0",
		open:     map[int64]bool{100: true},
		blockers: map[int64][]int64{},
		bases:    map[int64]string{100: "master"},
	}
	r.deps = &poller.Deps{
		Forge: mock, Queue: svc, RepoID: repoID, Owner: "org", Repo: "app",
		Batch: &batch.Engine{
			Forge: mock, Queue: svc, Owner: "org", Repo: "app", RepoID: repoID,
			BatchMax: 0, TestWhileBlocked: true, MergedPollInterval: 1, MergedPollAttempts: 1,
		},
	}
	for i := range prs {
		prs[i].State, prs[i].AutoMergeEnabled = "open", true
		r.bases[prs[i].Number] = prs[i].BaseBranch
	}
	mock.ListOpenPRsFn = func(context.Context, string, string) ([]forge.PR, error) { return prs, nil }
	mock.GetPRFn = func(_ context.Context, _, _ string, n int64) (*forge.PR, error) {
		return &forge.PR{Number: n, State: "open", BaseBranch: r.bases[n], Merged: r.develop != "develop0"}, nil
	}
	mock.OpenDependenciesFn = func(_ context.Context, _, _ string, n int64) ([]forge.Dependency, error) {
		var deps []forge.Dependency
		for _, d := range r.blockers[n] {
			if r.open[d] {
				deps = append(deps, forge.Dependency{Number: d, PR: true})
			}
		}
		return deps, nil
	}
	mock.GetRequiredChecksFn = func(context.Context, string, string, string) ([]string, error) {
		return []string{"ci"}, nil
	}
	mock.GetCheckStatesFn = func(context.Context, string, string, string) (map[string]forge.Check, error) {
		return map[string]forge.Check{"ci": {State: pg.CheckStateSuccess}}, nil
	}
	var tip string
	mock.CreateMergeBranchFn = func(_ context.Context, _, _, base, head, _ string) (string, bool, error) {
		from := r.develop
		if base != "develop" {
			from = base // a layer on a passed SHA
		}
		tip = fmt.Sprintf("m(%s,%s)", from, head)
		return tip, false, nil
	}
	mock.MergeIntoFn = func(_ context.Context, _, _, _, head string) (string, bool, error) {
		tip = fmt.Sprintf("m(%s,%s)", tip, head)
		return tip, false, nil
	}
	mock.FastForwardFn = func(_ context.Context, _, _, _, sha string) error {
		if !strings.Contains(sha, r.develop) {
			return forge.ErrNotFastForward
		}
		r.develop = sha
		return nil
	}
	mock.IsUpToDateFn = func(_ context.Context, _, _, _, head string) (bool, error) {
		return strings.Contains(head, r.develop), nil
	}
	return r
}

func (r *releaseRig) poll(t *testing.T) *poller.PollResult {
	t.Helper()
	res, err := poller.PollOnce(r.ctx, r.deps)
	if err != nil || len(res.Errors) > 0 {
		t.Fatalf("PollOnce: %v %v", err, res.Errors)
	}
	return res
}

func (r *releaseRig) builds() int { return len(r.mock.CallsTo("CreateMergeBranch")) }

// The freeze end to end: PRs blocked by the promotion PR are queued and tested
// while it is open, and land the poll after it merges, without another CI run.
func TestTestWhileBlocked_ReleaseLandsFrozenPRsAtOnce(t *testing.T) {
	r := newReleaseRig(t,
		forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"},
		forge.PR{Number: 42, HeadSHA: "sha42", HeadBranch: "b42", BaseBranch: "develop"},
	)
	r.blockers[41], r.blockers[42] = []int64{100}, []int64{100}

	res := r.poll(t)
	if !slices.Equal(res.Enqueued, []int64{41, 42}) {
		t.Fatalf("want both frozen PRs queued to be tested, got %v", res.Enqueued)
	}
	live, err := r.svc.GetLiveBatch(r.ctx, r.repoID, "develop")
	if err != nil || live == nil || live.State != pg.BatchStateHeld {
		t.Fatalf("want their batch tested and held, got %+v, %v", live, err)
	}
	held := live.BranchSha.String
	if r.develop != "develop0" {
		t.Fatalf("develop moved to %q while #100 is open", r.develop)
	}

	r.poll(t) // still frozen
	if r.develop != "develop0" || r.builds() != 1 {
		t.Fatalf("frozen poll changed something: develop=%q builds=%d", r.develop, r.builds())
	}

	r.open[100] = false // released
	r.poll(t)
	if r.develop != held {
		t.Fatalf("develop = %q, want the held sha %q", r.develop, held)
	}
	if r.builds() != 1 {
		t.Fatalf("builds = %d; the held sha was tested already", r.builds())
	}
	for _, n := range []int64{41, 42} {
		if ent, _ := r.svc.GetEntry(r.ctx, r.repoID, n); ent != nil {
			t.Fatalf("PR #%d still queued after the release", n)
		}
	}
	if n := len(r.mock.CallsTo("Comment")) + len(r.mock.CallsTo("CancelAutoMerge")); n != 0 {
		t.Fatalf("%d comment/cancel call(s) along the way", n)
	}
}

// A dependency that merges into the PR's own target would change what was
// tested, so that PR stays outside the queue exactly as without the feature.
func TestTestWhileBlocked_DependencyIntoTheTargetStillBlocks(t *testing.T) {
	r := newReleaseRig(t, forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"})
	r.blockers[41] = []int64{99}
	r.open[99], r.bases[99] = true, "develop"

	if res := r.poll(t); len(res.Enqueued) != 0 {
		t.Fatalf("enqueued %v although #99 merges into develop", res.Enqueued)
	}
	if ent, _ := r.svc.GetEntry(r.ctx, r.repoID, 41); ent != nil {
		t.Fatalf("#41 queued: %+v", ent)
	}
	statuses := r.mock.CallsTo("SetMQStatus")
	if len(statuses) != 1 || statuses[0].Args[3].(forge.MQStatus).Description != "Blocked by open dependency #99" {
		t.Fatalf("want the plain blocked status, got %+v", statuses)
	}
}

// A queued PR that gains a dependency which cannot move its target stays in
// the queue, to be tested meanwhile, instead of leaving it with a comment.
func TestTestWhileBlocked_QueuedPRGainsDependencyStays(t *testing.T) {
	r := newReleaseRig(t, forge.PR{Number: 42, HeadSHA: "sha42", HeadBranch: "b42", BaseBranch: "develop"})
	if _, err := r.svc.Enqueue(r.ctx, r.repoID, 42, "sha42", "develop"); err != nil {
		t.Fatal(err)
	}
	r.blockers[42] = []int64{100}

	if res := r.poll(t); len(res.Dequeued) != 0 {
		t.Fatalf("dequeued %v", res.Dequeued)
	}
	ent, _ := r.svc.GetEntry(r.ctx, r.repoID, 42)
	if ent == nil || !ent.Waiting {
		t.Fatalf("want #42 still queued and waiting, got %+v", ent)
	}
	if n := len(r.mock.CallsTo("Comment")); n != 0 {
		t.Fatalf("Comment calls = %d", n)
	}

	r.open[100] = false
	r.poll(t)
	if ent, _ := r.svc.GetEntry(r.ctx, r.repoID, 42); ent != nil {
		t.Fatalf("#42 not landed once its dependency closed: %+v", ent)
	}
}
