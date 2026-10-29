package migrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

type MemoryDriver struct {
	mu           sync.Mutex
	records      map[int64]MigrationRecord
	locked       bool
	lockOwner    string
	failOnSQL    string
	applyDelay   time.Duration
	failOnLock   bool
	simulateRollback bool
}

func NewMemoryDriver() *MemoryDriver {
	return &MemoryDriver{
		records: make(map[int64]MigrationRecord),
	}
}

func (m *MemoryDriver) SetFailOnSQL(pattern string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOnSQL = pattern
}

func (m *MemoryDriver) EnsureMigrationsTable(ctx context.Context) error {
	return nil
}

func (m *MemoryDriver) GetAppliedMigrations(ctx context.Context) ([]MigrationRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]MigrationRecord, 0, len(m.records))
	for _, r := range m.records {
		result = append(result, r)
	}
	return result, nil
}

func (m *MemoryDriver) ApplyMigration(ctx context.Context, mig *Migration) (int64, error) {
	m.mu.Lock()
	delay := m.applyDelay
	failPattern := m.failOnSQL
	m.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	if failPattern != "" && strings.Contains(mig.RawSQL, failPattern) {
		return 10, errors.New("simulated migration execution failure: syntax or constraint error")
	}

	return 15, nil
}

func (m *MemoryDriver) RecordMigration(ctx context.Context, record MigrationRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.Version] = record
	return nil
}

func (m *MemoryDriver) Lock(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	poll := 10 * time.Millisecond

	for {
		m.mu.Lock()
		if m.failOnLock {
			m.mu.Unlock()
			return errors.New("simulated lock acquisition network failure")
		}
		if !m.locked {
			m.locked = true
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()

		if time.Now().After(deadline) {
			return ErrLockTimeout
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

func (m *MemoryDriver) Unlock(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locked = false
	return nil
}

func (m *MemoryDriver) Close() error {
	return nil
}
