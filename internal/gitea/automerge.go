package gitea

import "time"

// Timeline comment types emitted by Gitea when a user schedules or cancels
// auto-merge on a PR. These are the canonical signals the Gitea adapter folds
// into forge.PR.AutoMergeEnabled.
const (
	commentTypeAutomergeScheduled = "pull_scheduled_merge"
	commentTypeAutomergeCancelled = "pull_cancel_scheduled_merge"
)

// HasAutomergeScheduled reports whether the most recent automerge-related
// timeline comment indicates auto-merge is currently scheduled. An empty
// timeline (no automerge comments) returns false.
func HasAutomergeScheduled(timeline []TimelineComment) bool {
	for i := len(timeline) - 1; i >= 0; i-- {
		switch timeline[i].Type {
		case commentTypeAutomergeScheduled:
			return true
		case commentTypeAutomergeCancelled:
			return false
		}
	}
	return false
}

// commentTypePush is the timeline comment Gitea adds when commits are pushed
// to a PR's head branch.
const commentTypePush = "pull_push"

// HeadUpdatedAt is when the PR's head last changed: its latest push, or its
// opening when nothing was pushed since. Zero when Gitea reports neither.
func HeadUpdatedAt(pr *PR, timeline []TimelineComment) time.Time {
	var at time.Time
	if pr.Created != nil {
		at = *pr.Created
	}
	for _, c := range timeline {
		if c.Type == commentTypePush && c.CreatedAt.After(at) {
			at = c.CreatedAt
		}
	}
	return at
}
