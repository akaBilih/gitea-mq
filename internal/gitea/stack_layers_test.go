package gitea

import (
	"context"
	"path/filepath"
	"testing"
)

// A batch stacks a new layer on the commit it already passed, given as a SHA.
// The layer replaces whatever the batch branch held, including a layer that
// failed on top of the same commit, which a plain push would refuse.
func TestStackMerges_OntoPassedCommit(t *testing.T) {
	root := t.TempDir()
	origin := filepath.Join(root, "o", "r.git")
	gitIn(t, root, "init", "--bare", "-q", "-b", "main", origin)
	gitIn(t, origin, "config", "uploadpack.allowFilter", "true")
	work := t.TempDir()
	gitIn(t, work, "init", "-q", "-b", "main")
	gitIn(t, work, "remote", "add", "origin", origin)
	commitFile(t, work, "f", "base\n", "base")
	gitIn(t, work, "push", "-q", "origin", "main")
	head := func(branch, file string) string {
		gitIn(t, work, "checkout", "-q", "-b", branch, "main")
		sha := commitFile(t, work, file, file+"\n", branch)
		gitIn(t, work, "push", "-q", "origin", branch)
		return sha
	}
	h1, h2, h3 := head("p1", "a"), head("p2", "b"), head("p3", "c")

	c := &HTTPClient{baseURL: root, gitCache: newTestCache(t)}
	ctx := context.Background()
	const branch = "gitea-mq/batch/1"
	stack := func(base, h string) string {
		t.Helper()
		tip, steps, err := c.StackMerges(ctx, "o", "r", base, []string{h}, branch)
		if err != nil || len(steps) != 1 || steps[0].Conflict || steps[0].Err != nil || tip == "" {
			t.Fatalf("StackMerges(%s, %s): tip=%q steps=%+v err=%v", base, h, tip, steps, err)
		}
		return tip
	}

	passed := stack("main", h1) // first layer, passed and held
	stack(passed, h2)           // a layer that failed
	tip := stack(passed, h3)    // the next one, on the passed commit again

	if got := gitIn(t, origin, "rev-parse", "refs/heads/"+branch); got != tip {
		t.Fatalf("batch branch at %s, want the new layer %s", got, tip)
	}
	if got := gitIn(t, origin, "rev-parse", tip+"^1"); got != passed {
		t.Fatalf("new layer's first parent is %s, want the passed commit %s", got, passed)
	}
	if out := gitIn(t, origin, "ls-tree", "--name-only", tip); out != "a\nc\nf" {
		t.Fatalf("new layer tree = %q, want a, c and f: p1 kept, p2 dropped, p3 added", out)
	}
}
