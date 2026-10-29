package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrChecksumMismatch = errors.New("migration file checksum mismatch: file was modified after application")
	ErrMigrationFailed  = errors.New("migration execution failed and was rolled back")
)

type MigrationStatus struct {
	Version         int64     `json:"version"`
	Name            string    `json:"name"`
	DiskChecksum    string    `json:"disk_checksum"`
	AppliedChecksum string    `json:"applied_checksum,omitempty"`
	Status          string    `json:"status"` // PENDING, APPLIED, FAILED, MODIFIED
	AppliedAt       time.Time `json:"applied_at,omitempty"`
	ExecutionTimeMs int64     `json:"execution_time_ms,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

type MigratorConfig struct {
	MigrationsDir string
	LockTimeout   time.Duration
}

type Migrator struct {
	cfg    MigratorConfig
	driver Driver
	locker Locker
}

func NewMigrator(cfg MigratorConfig, driver Driver, locker Locker) *Migrator {
	if cfg.LockTimeout <= 0 {
		cfg.LockTimeout = 10 * time.Second
	}
	return &Migrator{
		cfg:    cfg,
		driver: driver,
		locker: locker,
	}
}

// Plan identifies eligible pending migrations without modifying database state
func (m *Migrator) Plan(ctx context.Context) ([]*Migration, error) {
	diskMigrations, err := LoadMigrationsFromDir(m.cfg.MigrationsDir)
	if err != nil {
		return nil, fmt.Errorf("load disk migrations: %w", err)
	}

	if err := m.driver.EnsureMigrationsTable(ctx); err != nil {
		return nil, fmt.Errorf("ensure migrations table: %w", err)
	}

	appliedRecords, err := m.driver.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("get applied migrations: %w", err)
	}

	appliedMap := make(map[int64]MigrationRecord)
	for _, rec := range appliedRecords {
		appliedMap[rec.Version] = rec
	}

	// Validate integrity of already applied migrations
	for _, dm := range diskMigrations {
		if rec, exists := appliedMap[dm.Version]; exists {
			if rec.Status == "SUCCESS" && rec.Checksum != dm.Checksum {
				return nil, fmt.Errorf("%w: version %d (%s) recorded %s but file has %s",
					ErrChecksumMismatch, dm.Version, dm.Name, rec.Checksum, dm.Checksum)
			}
		}
	}

	// Filter unapplied migrations
	pending := make([]*Migration, 0)
	for _, dm := range diskMigrations {
		rec, exists := appliedMap[dm.Version]
		if !exists || rec.Status == "FAILED" {
			pending = append(pending, dm)
		}
	}

	return pending, nil
}

// Up applies all pending migrations in deterministic order with advisory locking
func (m *Migrator) Up(ctx context.Context) ([]MigrationRecord, error) {
	// 1. Acquire concurrency advisory lock
	if err := m.locker.Lock(ctx, m.cfg.LockTimeout); err != nil {
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_ = m.locker.Unlock(ctx)
	}()

	// 2. Compute execution plan
	pending, err := m.Plan(ctx)
	if err != nil {
		return nil, err
	}

	if len(pending) == 0 {
		return nil, nil
	}

	executed := make([]MigrationRecord, 0, len(pending))

	// 3. Execute migrations in order
	for _, mig := range pending {
		durationMs, err := m.driver.ApplyMigration(ctx, mig)
		if err != nil {
			// Record failure in migration history
			failRec := MigrationRecord{
				Version:         mig.Version,
				Name:            mig.Name,
				Checksum:        mig.Checksum,
				AppliedAt:       time.Now().UTC(),
				ExecutionTimeMs: durationMs,
				Status:          "FAILED",
				LastError:       err.Error(),
			}
			_ = m.driver.RecordMigration(ctx, failRec)
			return executed, fmt.Errorf("%w: version %d (%s) failed after %dms: %v",
				ErrMigrationFailed, mig.Version, mig.Name, durationMs, err)
		}

		successRec := MigrationRecord{
			Version:         mig.Version,
			Name:            mig.Name,
			Checksum:        mig.Checksum,
			AppliedAt:       time.Now().UTC(),
			ExecutionTimeMs: durationMs,
			Status:          "SUCCESS",
		}

		if err := m.driver.RecordMigration(ctx, successRec); err != nil {
			return executed, fmt.Errorf("record migration success version %d: %w", mig.Version, err)
		}

		executed = append(executed, successRec)
	}

	return executed, nil
}

// Status inspects and correlates disk migration files with recorded database state
func (m *Migrator) Status(ctx context.Context) ([]MigrationStatus, error) {
	diskMigrations, err := LoadMigrationsFromDir(m.cfg.MigrationsDir)
	if err != nil {
		return nil, fmt.Errorf("load disk migrations: %w", err)
	}

	if err := m.driver.EnsureMigrationsTable(ctx); err != nil {
		return nil, fmt.Errorf("ensure migrations table: %w", err)
	}

	appliedRecords, err := m.driver.GetAppliedMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("get applied migrations: %w", err)
	}

	appliedMap := make(map[int64]MigrationRecord)
	for _, rec := range appliedRecords {
		appliedMap[rec.Version] = rec
	}

	statuses := make([]MigrationStatus, 0, len(diskMigrations))

	for _, dm := range diskMigrations {
		st := MigrationStatus{
			Version:      dm.Version,
			Name:         dm.Name,
			DiskChecksum: dm.Checksum,
			Status:       "PENDING",
		}

		if rec, exists := appliedMap[dm.Version]; exists {
			st.AppliedChecksum = rec.Checksum
			st.AppliedAt = rec.AppliedAt
			st.ExecutionTimeMs = rec.ExecutionTimeMs
			st.LastError = rec.LastError

			if rec.Status == "SUCCESS" {
				if rec.Checksum == dm.Checksum {
					st.Status = "APPLIED"
				} else {
					st.Status = "MODIFIED"
				}
			} else {
				st.Status = "FAILED"
			}
		}

		statuses = append(statuses, st)
	}

	return statuses, nil
}

// Validate verifies file checksums against applied migration records
func (m *Migrator) Validate(ctx context.Context) error {
	statuses, err := m.Status(ctx)
	if err != nil {
		return err
	}

	for _, s := range statuses {
		if s.Status == "MODIFIED" {
			return fmt.Errorf("%w: version %d (%s) modified on disk after application",
				ErrChecksumMismatch, s.Version, s.Name)
		}
		if s.Status == "FAILED" {
			return fmt.Errorf("database contains failed migration version %d (%s): %s",
				s.Version, s.Name, s.LastError)
		}
	}

	return nil
}

// Version returns the current highest successfully applied migration version
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	if err := m.driver.EnsureMigrationsTable(ctx); err != nil {
		return 0, err
	}

	applied, err := m.driver.GetAppliedMigrations(ctx)
	if err != nil {
		return 0, err
	}

	var highest int64 = 0
	for _, rec := range applied {
		if rec.Status == "SUCCESS" && rec.Version > highest {
			highest = rec.Version
		}
	}

	return highest, nil
}
