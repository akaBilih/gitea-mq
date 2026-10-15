package gitea_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Mic92/gitea-mq/internal/forge"
	"github.com/Mic92/gitea-mq/internal/gitea"
)

func TestForge_OpenDependencies_KeepsOnlyOpenOnes(t *testing.T) {
	mock := &gitea.MockClient{
		ListIssueDependenciesFn: func(_ context.Context, _, _ string, _ int64) ([]gitea.Issue, error) {
			return []gitea.Issue{
				{Index: 7, State: "open", Repository: &gitea.IssueRepo{FullName: "org/app"}},
				{Index: 8, State: "closed", Repository: &gitea.IssueRepo{FullName: "org/app"}},
				{Index: 3, State: "open", Repository: &gitea.IssueRepo{FullName: "org/lib"}},
			}, nil
		},
	}
	deps, err := forge.OpenDependencies(context.Background(), newForge(mock), "org", "app", 42)
	if err != nil {
		t.Fatal(err)
	}
	if got := forge.DependencyRefs(deps, "org", "app"); got != "#7, org/lib#3" {
		t.Fatalf("open dependencies: got %q", got)
	}
}

// Gitea sends "pull_request": null for an issue and an object for a PR; only
// a PR can move a branch when it closes.
func TestForge_OpenDependencies_TellsPullRequestsFromIssues(t *testing.T) {
	var issues []gitea.Issue
	if err := json.Unmarshal([]byte(`[
		{"number": 5, "state": "open", "pull_request": null},
		{"number": 6, "state": "open", "pull_request": {"merged": false, "draft": false}}
	]`), &issues); err != nil {
		t.Fatal(err)
	}
	mock := &gitea.MockClient{
		ListIssueDependenciesFn: func(context.Context, string, string, int64) ([]gitea.Issue, error) {
			return issues, nil
		},
	}
	deps, err := forge.OpenDependencies(context.Background(), newForge(mock), "org", "app", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 2 || deps[0].PR || !deps[1].PR {
		t.Fatalf("want #5 an issue and #6 a PR, got %+v", deps)
	}
}

// A repository with dependencies disabled answers 404: that means none, not an
// error that would stall the queue.
func TestForge_OpenDependencies_DisabledMeansNone(t *testing.T) {
	mock := &gitea.MockClient{
		ListIssueDependenciesFn: func(_ context.Context, _, _ string, _ int64) ([]gitea.Issue, error) {
			return nil, &gitea.APIError{StatusCode: 404, Body: "disabled repo dependencies"}
		},
	}
	deps, err := forge.OpenDependencies(context.Background(), newForge(mock), "org", "app", 42)
	if err != nil || len(deps) != 0 {
		t.Fatalf("want no dependencies and no error, got %v, %v", deps, err)
	}
}

// Any other failure must surface: treating it as "no dependencies" would let a
// blocked PR land.
func TestForge_OpenDependencies_OtherErrorsSurface(t *testing.T) {
	mock := &gitea.MockClient{
		ListIssueDependenciesFn: func(_ context.Context, _, _ string, _ int64) ([]gitea.Issue, error) {
			return nil, &gitea.APIError{StatusCode: 500, Body: "boom"}
		},
	}
	_, err := forge.OpenDependencies(context.Background(), newForge(mock), "org", "app", 42)
	var apiErr *gitea.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 {
		t.Fatalf("want the 500 back, got %v", err)
	}
}

// A forge without native dependencies (GitHub) reports none.
func TestOpenDependencies_UnsupportedForge(t *testing.T) {
	deps, err := forge.OpenDependencies(context.Background(), struct{ forge.Forge }{}, "org", "app", 42)
	if err != nil || deps != nil {
		t.Fatalf("want nil, nil; got %v, %v", deps, err)
	}
}

func TestForge_CanPush_ReadsGiteaVerdict(t *testing.T) {
	ctx := context.Background()
	mock := &gitea.MockClient{
		GetBranchFn: func(_ context.Context, _, _, branch string) (*gitea.Branch, error) {
			return &gitea.Branch{Name: branch, UserCanPush: branch == "develop"}, nil
		},
	}
	f := gitea.NewForge(mock, "https://gitea.example.com")

	for branch, want := range map[string]bool{"develop": true, "master": false} {
		got, err := forge.CanPush(ctx, f, "org", "app", branch)
		if err != nil || got != want {
			t.Errorf("CanPush(%s) = %v, %v; want %v", branch, got, err, want)
		}
	}

	mock.GetBranchFn = func(context.Context, string, string, string) (*gitea.Branch, error) {
		return nil, errors.New("502 Bad Gateway")
	}
	if _, err := forge.CanPush(ctx, f, "org", "app", "master"); err == nil {
		t.Error("CanPush must report a failed lookup")
	}
}

func TestHeadUpdatedAt_LatestPushOrOpening(t *testing.T) {
	opened := time.Date(2026, 10, 14, 16, 0, 0, 0, time.UTC)
	pr := &gitea.PR{Created: &opened}
	if got := gitea.HeadUpdatedAt(pr, nil); !got.Equal(opened) {
		t.Errorf("no pushes: got %v, want the opening %v", got, opened)
	}
	timeline := []gitea.TimelineComment{
		{Type: "pull_push", CreatedAt: opened.Add(time.Hour)},
		{Type: "label", CreatedAt: opened.Add(3 * time.Hour)},
		{Type: "pull_push", CreatedAt: opened.Add(2 * time.Hour)},
	}
	if got := gitea.HeadUpdatedAt(pr, timeline); !got.Equal(opened.Add(2 * time.Hour)) {
		t.Errorf("got %v, want the latest push", got)
	}
}
