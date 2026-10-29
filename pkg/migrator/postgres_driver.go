package migrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

var (
	ErrLockTimeout       = errors.New("timed out waiting for database migration advisory lock")
	DefaultAdvisoryLockID int64 = 839201948271039
)

type PostgresDriver struct {
	db     *sql.DB
	lockID int64
}

func NewPostgresDriver(databaseURL string) (*PostgresDriver, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresDriver{
		db:     db,
		lockID: DefaultAdvisoryLockID,
	}, nil
}

func (p *PostgresDriver) SetAdvisoryLockID(id int64) {
	p.lockID = id
}

func (p *PostgresDriver) EnsureMigrationsTable(ctx context.Context) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			execution_time_ms BIGINT NOT NULL,
			status TEXT NOT NULL,
			last_error TEXT
		);
	`
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	return nil
}

func (p *PostgresDriver) GetAppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	query := `
		SELECT version, name, checksum, applied_at, execution_time_ms, status, COALESCE(last_error, '')
		FROM schema_migrations
		ORDER BY version ASC
	`
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		err := rows.Scan(
			&r.Version,
			&r.Name,
			&r.Checksum,
			&r.AppliedAt,
			&r.ExecutionTimeMs,
			&r.Status,
			&r.LastError,
		)
		if err != nil {
			return nil, fmt.Errorf("scan migration record: %w", err)
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

func (p *PostgresDriver) ApplyMigration(ctx context.Context, m *Migration) (int64, error) {
	start := time.Now()

	if m.NoTransaction {
		// Non-transactional DDL (e.g., CREATE INDEX CONCURRENTLY, ALTER TYPE ADD VALUE)
		_, err := p.db.ExecContext(ctx, m.RawSQL)
		duration := time.Since(start).Milliseconds()
		if err != nil {
			return duration, fmt.Errorf("non-transactional migration failed: %w", err)
		}
		return duration, nil
	}

	// Transactional DDL execution
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin migration tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, m.RawSQL)
	duration := time.Since(start).Milliseconds()
	if err != nil {
		return duration, fmt.Errorf("execute migration SQL: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return duration, fmt.Errorf("commit migration tx: %w", err)
	}

	return duration, nil
}

func (p *PostgresDriver) RecordMigration(ctx context.Context, r MigrationRecord) error {
	query := `
		INSERT INTO schema_migrations (version, name, checksum, applied_at, execution_time_ms, status, last_error)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (version) DO UPDATE SET
			checksum = EXCLUDED.checksum,
			applied_at = EXCLUDED.applied_at,
			execution_time_ms = EXCLUDED.execution_time_ms,
			status = EXCLUDED.status,
			last_error = EXCLUDED.last_error;
	`
	_, err := p.db.ExecContext(
		ctx,
		query,
		r.Version,
		r.Name,
		r.Checksum,
		r.AppliedAt,
		r.ExecutionTimeMs,
		r.Status,
		r.LastError,
	)
	if err != nil {
		return fmt.Errorf("record migration metadata: %w", err)
	}
	return nil
}

// Lock acquires a session-level PostgreSQL advisory lock with timeout
func (p *PostgresDriver) Lock(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pollInterval := 50 * time.Millisecond

	for {
		var acquired bool
		err := p.db.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, p.lockID).Scan(&acquired)
		if err != nil {
			return fmt.Errorf("try advisory lock query: %w", err)
		}

		if acquired {
			return nil
		}

		if time.Now().After(deadline) {
			return ErrLockTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			if pollInterval < 500*time.Millisecond {
				pollInterval *= 2
			}
		}
	}
}

// Unlock releases the advisory lock
func (p *PostgresDriver) Unlock(ctx context.Context) error {
	var released bool
	err := p.db.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, p.lockID).Scan(&released)
	if err != nil {
		return fmt.Errorf("release advisory lock query: %w", err)
	}
	return nil
}

func (p *PostgresDriver) Close() error {
	return p.db.Close()
}
