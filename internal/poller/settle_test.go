package poller_test

import (
	"context"
	"testing"
	"time"

	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/poller"
)

// Right after a push the head can carry only the checks the push event
// skipped, which a required "test (*)" pattern takes as passed. The PR must not
// be queued until its own CI has had time to register.
func TestEnqueueWaitsForNewHeadToSettle(t *testing.T) {
	deps, mock, svc, ctx, repoID := setupLabelTest(t)
	pushed := time.Date(2026, 10, 15, 7, 47, 3, 0, time.UTC)
	now := pushed.Add(2 * time.Second)
	deps.Now = func() time.Time { return now }
	mock.ListOpenPRsFn = func(context.Context, string, string) ([]forge.PR, error) {
		return []forge.PR{{Number: 9, State: "open", HeadSHA: "h9", BaseBranch: "develop", AutoMergeEnabled: true, HeadUpdatedAt: pushed}}, nil
	}
	mock.GetRequiredChecksFn = func(context.Context, string, string, string) ([]string, error) {
		return []string{"Build and Test / test (*)"}, nil
	}
	mock.GetCheckStatesFn = func(context.Context, string, string, string) (map[string]forge.Check, error) {
		return map[string]forge.Check{"Build and Test / test (push)": {State: forge.ParseCheckState("skipped")}}, nil
	}

	if _, err := poller.PollOnce(ctx, deps); err != nil {
		t.Fatal(err)
	}
	if e, _ := svc.GetEntry(ctx, repoID, 9); e != nil {
		t.Fatalf("queued %s after the push, before its CI registered: %+v", now.Sub(pushed), e)
	}

	now = pushed.Add(poller.HeadSettle)
	if _, err := poller.PollOnce(ctx, deps); err != nil {
		t.Fatal(err)
	}
	if e, _ := svc.GetEntry(ctx, repoID, 9); e == nil {
		t.Fatal("want #9 queued once its head settled")
	}
}
