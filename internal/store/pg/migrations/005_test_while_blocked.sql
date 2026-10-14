-- +goose NO TRANSACTION
-- ALTER TYPE ... ADD VALUE must commit before the index below may use it.

-- +goose Up
ALTER TYPE batch_state ADD VALUE IF NOT EXISTS 'held';

-- A held batch passed CI but waits for its members' dependencies; it still
-- owns the target branch's single live slot.
DROP INDEX IF EXISTS ux_batches_live;
CREATE UNIQUE INDEX ux_batches_live ON batches(repo_id, target_branch)
    WHERE state IN ('forming', 'testing', 'held');

-- Queued only to be tested early: the PR has open dependencies that cannot
-- move the target branch, so it may not land yet.
ALTER TABLE queue_entries
    ADD COLUMN IF NOT EXISTS waiting BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
-- Postgres cannot drop an enum value; 'held' stays but nothing uses it.
UPDATE batches SET state = 'cancelled' WHERE state = 'held';
DROP INDEX IF EXISTS ux_batches_live;
CREATE UNIQUE INDEX ux_batches_live ON batches(repo_id, target_branch)
    WHERE state IN ('forming', 'testing');
ALTER TABLE queue_entries DROP COLUMN IF EXISTS waiting;
