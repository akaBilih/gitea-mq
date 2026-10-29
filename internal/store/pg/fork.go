package pg

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

// Fork-only schema changes live in their own migration track: upstream's
// goose_db_version then only ever holds upstream's numbers, so a rebase can
// never make a recorded version mean a different migration. Fork migrations
// run after upstream's, must be safe to re-run, and only add to the schema.

//go:embed migrations_fork/*.sql
var embedForkMigrations embed.FS

const forkVersionTable = "goose_db_version_fork"

func forkProvider(db *sql.DB) (*goose.Provider, error) {
	fsys, err := fs.Sub(embedForkMigrations, "migrations_fork")
	if err != nil {
		return nil, err
	}
	return goose.NewProvider(goose.DialectPostgres, db, fsys, goose.WithTableName(forkVersionTable))
}

func migrateFork(ctx context.Context, db *sql.DB) error {
	p, err := forkProvider(db)
	if err != nil {
		return fmt.Errorf("fork migrations: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("fork migrations: %w", err)
	}
	return nil
}

// Migrate runs both migration tracks on an existing pool, as Connect does.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	goose.SetBaseFS(embedMigrations)
	db := stdlib.OpenDBFromPool(pool)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	if err := goose.Up(db, "migrations"); err != nil {
		return err
	}
	return migrateFork(ctx, db)
}

// MigrateForkTo runs fork migrations up to and including version; 0 runs
// none. Exposed so migration tests can stop partway, like MigrateTo.
func MigrateForkTo(ctx context.Context, pool *pgxpool.Pool, version int64) error {
	if version < 1 {
		return nil
	}
	p, err := forkProvider(stdlib.OpenDBFromPool(pool))
	if err != nil {
		return err
	}
	_, err = p.UpTo(ctx, version)
	return err
}
