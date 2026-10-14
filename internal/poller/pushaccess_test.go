package poller_test

import (
	"context"
	"testing"

	"github.com/Mic92/gitea-mq/internal/batch"
	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/poller"
	"github.com/Mic92/gitea-mq/internal/store/pg"
)

// A branch gitea-mq may not push to (a release branch only people merge into)
// cannot take the batch engine's fast-forward. An up-to-date head there is
// handed back to the forge's merge, as before the engine landed them.
func TestBatch_UpToDateHeadOnUnpushableBranchMergedByForge(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupLabelTest(t)
	deps.SkipQueueIfUpToDate = true
	deps.Batch = &batch.Engine{
		Forge: mock, Queue: svc, Owner: "org", Repo: "app", RepoID: repoID,
		BatchMax: 0, SkipIfUpToDate: true, MergedPollInterval: 1, MergedPollAttempts: 1,
	}
	mock.ListOpenPRsFn = func(context.Context, string, string) ([]forge.PR, error) {
		return []forge.PR{{Number: 9, State: "open", HeadSHA: "develop-tip", BaseBranch: "master", AutoMergeEnabled: true}}, nil
	}
	mock.GetRequiredChecksFn = func(context.Context, string, string, string) ([]string, error) {
		return []string{"ci"}, nil
	}
	mock.GetCheckStatesFn = func(context.Context, string, string, string) (map[string]forge.Check, error) {
		return map[string]forge.Check{"ci": {State: pg.CheckStateSuccess}}, nil
	}
	mock.IsUpToDateFn = func(context.Context, string, string, string, string) (bool, error) {
		return true, nil
	}
	mock.CanPushFn = func(_ context.Context, _, _, branch string) (bool, error) {
		return branch != "master", nil
	}
	mock.FastForwardFn = func(_ context.Context, _, _, branch, _ string) error {
		return &forge.PushDeniedError{Branch: branch, Message: "not allowed to push"}
	}

	res, err := poller.PollOnce(ctx, deps)
	if err != nil || len(res.Errors) > 0 {
		t.Fatalf("PollOnce: %v %v", err, res)
	}

	if n := len(mock.CallsTo("FastForward")); n != 0 {
		t.Fatalf("FastForward called %d times on a branch gitea-mq may not push to", n)
	}
	if n := len(mock.CallsTo("CancelAutoMerge")); n != 0 {
		t.Fatalf("merge intent cancelled %d times; the forge must be left to merge", n)
	}
	entry, _ := svc.GetEntry(ctx, repoID, 9)
	if entry == nil || entry.State != pg.EntryStateSuccess {
		t.Fatalf("entry = %+v, want state success so the forge's automerge merges it", entry)
	}
	var green bool
	for _, c := range mock.CallsTo("SetMQStatus") {
		if c.Args[2] == "develop-tip" && c.Args[3].(forge.MQStatus).State == pg.CheckStateSuccess {
			green = true
		}
	}
	if !green {
		t.Error("expected a success gitea-mq status on the PR head")
	}
}
