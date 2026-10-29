package queue_test

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Mic92/gitea-mq/internal/store/pg"
	"github.com/Mic92/gitea-mq/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fileVersions lists the goose versions of the migrations in dir.
func fileVersions(t *testing.T, dir string) []int64 {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var versions []int64
	for _, f := range files {
		num, _, ok := strings.Cut(f.Name(), "_")
		if !ok || !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		v, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			t.Fatalf("%s: %v", f.Name(), err)
		}
		versions = append(versions, v)
	}
	slices.Sort(versions)
	return versions
}

func upstreamVersions(t *testing.T) []int64 { return fileVersions(t, "../store/pg/migrations") }
func forkVersions(t *testing.T) []int64     { return fileVersions(t, "../store/pg/migrations_fork") }

// appliedVersions lists the versions goose recorded in table.
func appliedVersions(t *testing.T, pool *pgxpool.Pool, table string) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT version_id FROM `+table+` WHERE version_id > 0 AND is_applied ORDER BY version_id`)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return versions
}

// requireFullyMigrated checks both tracks are complete and the schema has a
// column from each.
func requireFullyMigrated(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if got, want := appliedVersions(t, pool, "goose_db_version"), upstreamVersions(t); !slices.Equal(got, want) {
		t.Errorf("upstream track applied %v, want %v", got, want)
	}
	if got, want := appliedVersions(t, pool, "goose_db_version_fork"), forkVersions(t); !slices.Equal(got, want) {
		t.Errorf("fork track applied %v, want %v", got, want)
	}
	if _, err := pool.Exec(context.Background(), `SELECT priority, waiting FROM queue_entries LIMIT 0`); err != nil {
		t.Errorf("schema lacks a column of one track: %v", err)
	}
}

// Fork migrations keep their own version table, so upstream's table only
// ever holds upstream's numbers and the two can never collide.
func TestForkMigrations_OwnVersionTable(t *testing.T) {
	requireFullyMigrated(t, testutil.NewTestDB(t, testutil.Server()))
}

// A deployment upgrade: the database was migrated by the previous deployment,
// then one track gained a migration. Whichever moved, the upgrade completes.
func TestForkMigrations_UpgradeFromPreviousDeployment(t *testing.T) {
	up, fork := upstreamVersions(t), forkVersions(t)
	for _, tc := range []struct {
		name           string
		upstream, fork int64 // versions the previous deployment had reached
	}{
		{"upstream gained a migration", up[len(up)-2], fork[len(fork)-1]},
		{"fork gained a migration", up[len(up)-1], 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testutil.NewRawTestDB(t, testutil.Server())
			ctx := context.Background()
			if err := pg.MigrateTo(pool, tc.upstream); err != nil {
				t.Fatalf("previous deployment, upstream to %d: %v", tc.upstream, err)
			}
			if err := pg.MigrateForkTo(ctx, pool, tc.fork); err != nil {
				t.Fatalf("previous deployment, fork to %d: %v", tc.fork, err)
			}

			if err := pg.Migrate(ctx, pool); err != nil {
				t.Fatalf("upgrade: %v", err)
			}
			requireFullyMigrated(t, pool)
		})
	}
}

// Fork migrations must be safe to re-run on a schema that already has their
// changes but no record of them, as when a fork migration moves between
// tracks.
func TestForkMigrations_RerunOnExistingChanges(t *testing.T) {
	pool := testutil.NewTestDB(t, testutil.Server())
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `DROP TABLE goose_db_version_fork`); err != nil {
		t.Fatal(err)
	}

	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("re-run fork migrations: %v", err)
	}
	requireFullyMigrated(t, pool)
}
