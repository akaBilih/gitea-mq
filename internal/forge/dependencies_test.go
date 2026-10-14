package forge

import (
	"context"
	"errors"
	"testing"
)

// Only a pull request of the same repository into one of the branches can move
// it; its base is asked once per guard.
func TestBranchGuard_CanMove(t *testing.T) {
	bases := map[int64]string{100: "master", 101: "develop", 102: "feature-x"}
	f := &MockForge{GetPRFn: func(_ context.Context, _, _ string, n int64) (*PR, error) {
		if base, ok := bases[n]; ok {
			return &PR{Number: n, BaseBranch: base}, nil
		}
		return nil, nil
	}}
	cases := []struct {
		name string
		dep  Dependency
		want bool
	}{
		{"issue", Dependency{Number: 101}, false},
		{"PR into another branch", Dependency{Number: 100, PR: true}, false},
		{"PR into the target", Dependency{Number: 101, PR: true}, true},
		{"PR into the head branch", Dependency{Number: 102, PR: true}, true},
		{"PR elsewhere", Dependency{Repo: "org/lib", Number: 101, PR: true}, false},
		{"PR here, spelled out", Dependency{Repo: "Org/App", Number: 101, PR: true}, true},
		{"PR whose base is unknown", Dependency{Number: 999, PR: true}, true},
	}
	g := NewBranchGuard(f, "org", "app")
	for _, c := range cases {
		got, err := g.CanMove(context.Background(), []Dependency{c.dep}, "develop", "feature-x")
		if err != nil || got != c.want {
			t.Errorf("%s: CanMove = %v, %v; want %v", c.name, got, err, c.want)
		}
	}
	if n := len(f.CallsTo("GetPR")); n != 4 {
		t.Fatalf("GetPR calls = %d, want one per pull request asked about (100, 101, 102, 999)", n)
	}
}

// A failed lookup counts as able to move the branch, and surfaces.
func TestBranchGuard_LookupErrorSurfaces(t *testing.T) {
	f := &MockForge{GetPRFn: func(context.Context, string, string, int64) (*PR, error) {
		return nil, errors.New("forge down")
	}}
	got, err := NewBranchGuard(f, "org", "app").CanMove(context.Background(), []Dependency{{Number: 1, PR: true}}, "main")
	if err == nil || !got {
		t.Fatalf("want true and the error, got %v, %v", got, err)
	}
}
