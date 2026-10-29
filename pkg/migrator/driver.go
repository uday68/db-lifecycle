package migrator

import (
	"context"
	"time"
)

// MigrationRecord represents an applied migration record in the metadata store
type MigrationRecord struct {
	Version         int64     `json:"version"`
	Name            string    `json:"name"`
	Checksum        string    `json:"checksum"`
	AppliedAt       time.Time `json:"applied_at"`
	ExecutionTimeMs int64     `json:"execution_time_ms"`
	Status          string    `json:"status"` // SUCCESS or FAILED
	LastError       string    `json:"last_error,omitempty"`
}

// Driver abstracts database execution and metadata storage
type Driver interface {
	EnsureMigrationsTable(ctx context.Context) error
	GetAppliedMigrations(ctx context.Context) ([]MigrationRecord, error)
	ApplyMigration(ctx context.Context, m *Migration) (int64, error)
	RecordMigration(ctx context.Context, record MigrationRecord) error
	Close() error
}

// Locker coordinates concurrent migration attempts across multiple processes
type Locker interface {
	Lock(ctx context.Context, timeout time.Duration) error
	Unlock(ctx context.Context) error
}
