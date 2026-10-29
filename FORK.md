# This fork

`akaBilih/gitea-mq` carries additions on top of
[Mic92/gitea-mq](https://github.com/Mic92/gitea-mq).

## Branches

```
Mic92/main ── fork main ── deployment
```

- `main` mirrors upstream `main`; it only ever fast-forwards.
- `deployment` is upstream plus our additions, rebased onto `main` when
  upstream moves. Deploy from it.
- Work meant for upstream branches from `main`; local-only work branches from
  `deployment`.

## Database migrations

Upstream's migrations live in `internal/store/pg/migrations` and are recorded in
`goose_db_version`. Ours live in `internal/store/pg/migrations_fork`, numbered
from `001`, and are recorded in `goose_db_version_fork`. Upstream's run first.
The two never share a number, so a rebase cannot change what a recorded
version means.

A fork migration must:

- be safe to re-run on a schema that already has its changes
  (`IF NOT EXISTS`, `IF EXISTS`);
- only add to the schema, never rename or drop what upstream owns.

`internal/queue/fork_migration_test.go` checks both tracks: a fresh database,
an upgrade from the previous deployment whichever track gained a migration,
and re-running fork migrations whose changes already exist.

### One-off: databases migrated before the split

Before the split, our test-while-blocked migration was upstream number 005. A
database that ran it has version 5 recorded, so it never runs upstream's
`005_priority.sql`. Check, and if `max` is 5, apply that migration by hand
before deploying:

```sql
SELECT max(version_id) FROM goose_db_version;

ALTER TABLE queue_entries ADD COLUMN IF NOT EXISTS priority BOOLEAN NOT NULL DEFAULT FALSE;
DELETE FROM queue_entries WHERE state IN ('failed', 'cancelled');
```

The fork track then re-runs our migration, which changes nothing.
