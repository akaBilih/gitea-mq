package poller_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/store/pg"
)

// lastLabels maps each PR to the label its latest ApplyLabels call added.
func lastLabels(mock *forge.MockForge) map[int64]string {
	out := map[int64]string{}
	for _, c := range mock.CallsTo("ApplyLabels") {
		if add := c.Args[3].([]string); len(add) > 0 {
			out[c.Args[2].(int64)] = add[0]
		}
	}
	return out
}

func requireLabels(t *testing.T, mock *forge.MockForge, want map[int64]string) {
	t.Helper()
	got := lastLabels(mock)
	for n, w := range want {
		if got[n] != w {
			t.Errorf("PR #%d label = %q, want %q (all: %v)", n, got[n], w, got)
		}
	}
}

// A PR an open dependency keeps out of the queue says so, instead of looking
// like it waits for its own CI.
func TestStateLabelBlockedOutsideTheQueue(t *testing.T) {
	r := newReleaseRig(t, forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"})
	r.deps.LabelPrefix = "mq/"
	r.blockers[41] = []int64{99}
	r.open[99], r.bases[99] = true, "develop" // merges into develop: not testable early

	r.poll(t)
	requireLabels(t, r.mock, map[int64]string{41: "mq/blocked"})
}

// A frozen release: blocked PRs are tested and their passed batch is held.
// They are labelled held, not as a batch still under test.
func TestStateLabelHeldAfterPassing(t *testing.T) {
	r := newReleaseRig(t,
		forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"},
		forge.PR{Number: 42, HeadSHA: "sha42", HeadBranch: "b42", BaseBranch: "develop"},
	)
	r.deps.LabelPrefix = "mq/"
	r.blockers[41], r.blockers[42] = []int64{100}, []int64{100}

	r.poll(t)
	requireLabels(t, r.mock, map[int64]string{41: "mq/held", 42: "mq/held"})
}

// A blocked PR queued while a ready PR's batch is testing waits its turn as
// queued-blocked; the ready PR is labelled with its batch.
func TestStateLabelQueuedBlockedBesideATestingBatch(t *testing.T) {
	r := newReleaseRig(t,
		forge.PR{Number: 40, HeadSHA: "sha40", HeadBranch: "b40", BaseBranch: "develop"},
		forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"},
	)
	r.deps.LabelPrefix = "mq/"
	r.blockers[41] = []int64{100}
	pendingOnMergeTips(r)

	r.poll(t)
	live, err := r.svc.GetLiveBatch(r.ctx, r.repoID, "develop")
	if err != nil || live == nil || live.State != pg.BatchStateTesting {
		t.Fatalf("want #40's batch testing, got %+v, %v", live, err)
	}
	requireLabels(t, r.mock, map[int64]string{40: fmt.Sprintf("mq/batch-%d", live.ID), 41: "mq/queued-blocked"})
}

// A layer tested on top of a held set: what passed is held, the layer under
// test carries the batch label.
func TestStateLabelLayerOnHeldSet(t *testing.T) {
	r := newReleaseRig(t, forge.PR{Number: 41, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"})
	r.deps.LabelPrefix = "mq/"
	r.blockers[41], r.blockers[43] = []int64{100}, []int64{100}
	r.poll(t) // #41 tested and held

	prs := []forge.PR{
		{Number: 41, State: "open", AutoMergeEnabled: true, HeadSHA: "sha41", HeadBranch: "b41", BaseBranch: "develop"},
		{Number: 43, State: "open", AutoMergeEnabled: true, HeadSHA: "sha43", HeadBranch: "b43", BaseBranch: "develop"},
	}
	r.bases[43] = "develop"
	r.mock.ListOpenPRsFn = func(context.Context, string, string) ([]forge.PR, error) { return prs, nil }
	pendingOnMergeTips(r)
	r.mock.Calls = nil

	r.poll(t)
	live, err := r.svc.GetLiveBatch(r.ctx, r.repoID, "develop")
	if err != nil || live == nil || len(live.HeldIds) != 1 || len(live.CurrentIds) != 1 {
		t.Fatalf("want a layer testing on one held PR, got %+v, %v", live, err)
	}
	requireLabels(t, r.mock, map[int64]string{41: "mq/held", 43: fmt.Sprintf("mq/batch-%d", live.ID)})
}

// pendingOnMergeTips keeps CI running on every batch commit while PR heads
// stay green, so a built batch stays under test.
func pendingOnMergeTips(r *releaseRig) {
	r.mock.GetCheckStatesFn = func(_ context.Context, _, _, sha string) (map[string]forge.Check, error) {
		if strings.HasPrefix(sha, "m(") {
			return map[string]forge.Check{"ci": {State: pg.CheckStatePending}}, nil
		}
		return map[string]forge.Check{"ci": {State: pg.CheckStateSuccess}}, nil
	}
}
