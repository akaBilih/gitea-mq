package batch_test

import (
	"testing"

	"github.com/Mic92/gitea-mq/internal/batch"
	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestStateOf(t *testing.T) {
	live := &pg.Batch{ID: 7, State: pg.BatchStateTesting, CurrentIds: []int64{1}, HeldIds: []int64{2}, LandedIds: []int64{3}}
	inBatch := pgtype.Int8{Int64: 7, Valid: true}
	for _, tc := range []struct {
		name string
		e    pg.QueueEntry
		b    *pg.Batch
		want batch.EntryState
	}{
		{"queued", pg.QueueEntry{ID: 9, State: pg.EntryStateQueued}, nil, batch.StateQueued},
		{"queued while blocked", pg.QueueEntry{ID: 9, State: pg.EntryStateQueued, Waiting: true}, nil, batch.StateQueuedBlocked},
		{"single-PR testing", pg.QueueEntry{ID: 9, State: pg.EntryStateTesting}, nil, batch.StateTesting},
		{"handed to the forge", pg.QueueEntry{ID: 9, State: pg.EntryStateSuccess}, nil, batch.StateMerging},
		{"batch member under test keeps state queued", pg.QueueEntry{ID: 1, State: pg.EntryStateQueued, Waiting: true, ActiveBatchID: inBatch}, live, batch.StateTesting},
		{"held after passing", pg.QueueEntry{ID: 2, State: pg.EntryStateQueued, Waiting: true, ActiveBatchID: inBatch}, live, batch.StateHeld},
		{"landed", pg.QueueEntry{ID: 3, State: pg.EntryStateQueued, ActiveBatchID: inBatch}, live, batch.StateMerging},
		{"bisect half waiting its turn", pg.QueueEntry{ID: 4, State: pg.EntryStateQueued, Waiting: true, ActiveBatchID: inBatch}, live, batch.StateQueuedBlocked},
	} {
		if got := batch.StateOf(&tc.e, tc.b); got != tc.want {
			t.Errorf("%s: StateOf = %q, want %q", tc.name, got, tc.want)
		}
	}
}
