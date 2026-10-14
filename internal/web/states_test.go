package web_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Mic92/gitea-mq/internal/gitea"
	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/Mic92/gitea-mq/internal/testutil"
)

// The repo page shows what each PR is doing, in the words of its state label:
// a passed batch held for its dependencies is "held", not the "queued" its
// entries still store, and a blocked PR queued to be tested early says so.
func TestRepoDetailShowsHeldAndQueuedBlocked(t *testing.T) {
	svc, ctx, repoID := testutil.TestQueueService(t)
	for _, n := range []int64{41, 42} {
		if _, err := svc.EnqueueWaiting(ctx, repoID, n, "sha", "main"); err != nil {
			t.Fatal(err)
		}
	}
	b, err := svc.FormBatch(ctx, repoID, "main", 0, true)
	if err != nil || b == nil {
		t.Fatalf("FormBatch: %v, %v", b, err)
	}
	b.HeldIds, b.CurrentIds, b.State = b.CurrentIds, nil, pg.BatchStateHeld
	if err := svc.SaveBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.EnqueueWaiting(ctx, repoID, 44, "sha44", "main"); err != nil {
		t.Fatal(err)
	}

	body := getPage(t, newDeps(svc, nil, giteaRef("org", "app")), "/repo/org/app")

	if n := strings.Count(body, `class="state state-held"`); n != 2 {
		t.Errorf("want #41 and #42 shown held, got %d:\n%s", n, body)
	}
	if !strings.Contains(body, `class="state state-queued-blocked"`) {
		t.Errorf("want #44 shown queued-blocked:\n%s", body)
	}
	if strings.Contains(body, `class="state state-queued"`) {
		t.Errorf("no PR here is plainly queued:\n%s", body)
	}
}

// A PR kept out of the queue by an open dependency says why on its page.
func TestPRDetailNotQueuedShowsBlocker(t *testing.T) {
	svc, _, _ := testutil.TestQueueService(t)
	mock := &gitea.MockClient{
		GetPRFn: func(_ context.Context, _, _ string, n int64) (*gitea.PR, error) {
			return &gitea.PR{Index: n, State: "open", Title: "Frozen fix", Base: &gitea.PRRef{Ref: "develop"}, Head: &gitea.PRRef{Sha: "h"}}, nil
		},
		ListIssueDependenciesFn: func(context.Context, string, string, int64) ([]gitea.Issue, error) {
			return []gitea.Issue{{Index: 100, State: "open", PullRequest: &gitea.IssuePullRequest{}}}, nil
		},
	}

	body := getPage(t, newDeps(svc, giteaForges(mock), giteaRef("org", "app")), "/repo/org/app/pr/41")

	if !strings.Contains(body, "depends on #100") || !strings.Contains(body, "Frozen fix") {
		t.Errorf("want the PR title and its open dependency:\n%s", body)
	}
}
