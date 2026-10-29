package migrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func createTempMigrations(t *testing.T, files map[string]string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "migrations-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatalf("write temp migration %s: %v", name, err)
		}
	}
	return dir
}

// 1. Applying migrations in deterministic version order
func TestMigrator_AppliesInDeterministicOrder(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"002_add_index.sql": "CREATE INDEX idx_test ON test(col);",
		"001_create_table.sql": "CREATE TABLE test (id INT PRIMARY KEY, col TEXT);",
		"003_add_column.sql": "ALTER TABLE test ADD COLUMN email TEXT;",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	executed, err := migrator.Up(context.Background())
	if err != nil {
		t.Fatalf("migrator up failed: %v", err)
	}

	if len(executed) != 3 {
		t.Fatalf("expected 3 migrations applied, got %d", len(executed))
	}

	if executed[0].Version != 1 || executed[1].Version != 2 || executed[2].Version != 3 {
		t.Fatalf("migrations not in order: %v, %v, %v", executed[0].Version, executed[1].Version, executed[2].Version)
	}

	currVer, err := migrator.Version(context.Background())
	if err != nil || currVer != 3 {
		t.Fatalf("expected current version 3, got %d (err: %v)", currVer, err)
	}
}

// 2. Repeated invocation without reapplying successful migrations
func TestMigrator_IdempotentSubsequentRuns(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_init.sql": "CREATE TABLE test (id INT);",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	// First run applies migration
	executed, err := migrator.Up(context.Background())
	if err != nil || len(executed) != 1 {
		t.Fatalf("first run failed: %v, executed: %d", err, len(executed))
	}

	// Second run should apply 0 migrations
	secondRun, err := migrator.Up(context.Background())
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if len(secondRun) != 0 {
		t.Fatalf("expected 0 migrations on second run, got %d", len(secondRun))
	}
}

// 3. Checksum mismatch detection: file modified on disk after application
func TestMigrator_DetectsChecksumTamperingAfterApplication(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_init.sql": "CREATE TABLE test (id INT);",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	_, err := migrator.Up(context.Background())
	if err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Modify file content on disk
	filePath := filepath.Join(dir, "001_init.sql")
	_ = os.WriteFile(filePath, []byte("CREATE TABLE test (id INT, modified BOOLEAN);"), 0644)

	// Plan should fail with ErrChecksumMismatch
	_, err = migrator.Plan(context.Background())
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	// Validate command should also report checksum mismatch
	err = migrator.Validate(context.Background())
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected Validate to return ErrChecksumMismatch, got %v", err)
	}
}

// 4. Duplicate version detection
func TestMigrator_RejectsDuplicateVersions(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_first.sql":  "CREATE TABLE t1 (id INT);",
		"001_second.sql": "CREATE TABLE t2 (id INT);",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	_, err := migrator.Plan(context.Background())
	if err == nil {
		t.Fatal("expected error on duplicate migration version numbers")
	}
}

// 5. Concurrent migration attempts with advisory locking
func TestMigrator_ConcurrentAdvisoryLockContention(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_init.sql": "CREATE TABLE test (id INT);",
	})

	driver := NewMemoryDriver()
	// Set artificial delay to ensure overlap
	driver.applyDelay = 50 * time.Millisecond

	m1 := NewMigrator(MigratorConfig{MigrationsDir: dir, LockTimeout: 200 * time.Millisecond}, driver, driver)
	m2 := NewMigrator(MigratorConfig{MigrationsDir: dir, LockTimeout: 200 * time.Millisecond}, driver, driver)

	var wg sync.WaitGroup
	wg.Add(2)

	var err1, err2 error
	var exec1, exec2 []MigrationRecord

	go func() {
		defer wg.Done()
		exec1, err1 = m1.Up(context.Background())
	}()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Ensure m1 locks first
		exec2, err2 = m2.Up(context.Background())
	}()

	wg.Wait()

	if err1 != nil {
		t.Fatalf("m1 failed: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("m2 failed: %v", err2)
	}

	// Only one migrator should have applied the migration; the other waited and found 0 pending
	totalApplied := len(exec1) + len(exec2)
	if totalApplied != 1 {
		t.Fatalf("expected exactly 1 application across concurrent runners, got %d", totalApplied)
	}
}

// 6. Lock timeout
func TestMigrator_LockTimeout(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_init.sql": "CREATE TABLE test (id INT);",
	})

	driver := NewMemoryDriver()
	// Acquire lock manually
	_ = driver.Lock(context.Background(), time.Second)

	migrator := NewMigrator(MigratorConfig{
		MigrationsDir: dir,
		LockTimeout:   20 * time.Millisecond,
	}, driver, driver)

	_, err := migrator.Up(context.Background())
	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout, got %v", err)
	}
}

// 7. Migration failure and rollback reporting
func TestMigrator_RollbackOnFailure(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_valid.sql": "CREATE TABLE test (id INT);",
		"002_fail.sql":  "MALFORMED SYNTAX ERROR STATEMENT;",
	})

	driver := NewMemoryDriver()
	driver.SetFailOnSQL("MALFORMED")

	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	executed, err := migrator.Up(context.Background())
	if err == nil {
		t.Fatal("expected migration up to fail on malformed SQL")
	}

	if len(executed) != 1 {
		t.Fatalf("expected 1 migration applied before failure, got %d", len(executed))
	}

	// Verify failed migration status is recorded
	statuses, err := migrator.Status(context.Background())
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}

	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if statuses[0].Status != "APPLIED" || statuses[1].Status != "FAILED" {
		t.Fatalf("unexpected statuses: %s, %s", statuses[0].Status, statuses[1].Status)
	}
}

// 8. Dry-run plan mode (non-mutation guarantee)
func TestMigrator_DryRunPlanNonMutation(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_init.sql": "CREATE TABLE test (id INT);",
	})

	driver := NewMemoryDriver()
	migrator := NewMigrator(MigratorConfig{MigrationsDir: dir}, driver, driver)

	pending, err := migrator.Plan(context.Background())
	if err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending migration, got %d", len(pending))
	}

	// Verify database was NOT mutated
	applied, _ := driver.GetAppliedMigrations(context.Background())
	if len(applied) != 0 {
		t.Fatalf("expected 0 applied migrations after dry-run plan, got %d", len(applied))
	}
}

// 9. Non-transactional migration directive
func TestMigrator_NonTransactionalDirective(t *testing.T) {
	dir := createTempMigrations(t, map[string]string{
		"001_concurrent_idx.sql": "-- +migrate no-transaction\nCREATE INDEX CONCURRENTLY idx ON tbl(col);",
	})

	migrations, err := LoadMigrationsFromDir(dir)
	if err != nil {
		t.Fatalf("load migrations failed: %v", err)
	}

	if len(migrations) != 1 {
		t.Fatalf("expected 1 migration, got %d", len(migrations))
	}

	if !migrations[0].NoTransaction {
		t.Fatal("expected NoTransaction to be true for -- +migrate no-transaction directive")
	}
}

// 10. Expand-and-contract batch backfiller
func TestZeroDowntime_BatchedBackfill(t *testing.T) {
	executor := NewSimulatedBackfillExecutor(2500)
	stats, err := executor.ExecuteBackfill(context.Background(), BackfillConfig{
		BatchSize:  500,
		BatchDelay: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("backfill execution failed: %v", err)
	}

	if stats.TotalProcessed != 2500 {
		t.Fatalf("expected 2500 rows processed, got %d", stats.TotalProcessed)
	}
	if stats.TotalBatches != 5 {
		t.Fatalf("expected 5 batches for 2500 rows at batch size 500, got %d", stats.TotalBatches)
	}
}

// 11. Context cancellation during batched backfill
func TestZeroDowntime_BatchedBackfill_Interrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	executor := NewSimulatedBackfillExecutor(10000)

	// Cancel after short delay
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	stats, err := executor.ExecuteBackfill(ctx, BackfillConfig{
		BatchSize:  100,
		BatchDelay: 2 * time.Millisecond,
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if stats.TotalProcessed >= 10000 {
		t.Fatalf("expected partial processing on cancel, got %d", stats.TotalProcessed)
	}
}

// 12. PostgreSQL CTE batched update SQL generation
func TestZeroDowntime_CTEBatchedUpdateSQL(t *testing.T) {
	sql := GenerateCTEBatchedUpdateSQL("users", "id", "full_name = first_name || ' ' || last_name", "full_name IS NULL", 250)
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}

	expectedClauses := []string{
		"WITH batch AS",
		"SELECT id",
		"FROM users",
		"WHERE full_name IS NULL",
		"ORDER BY id ASC",
		"LIMIT 250",
		"FOR UPDATE SKIP LOCKED",
		"UPDATE users t",
		"SET full_name = first_name || ' ' || last_name",
		"WHERE t.id = batch.id",
	}

	for _, clause := range expectedClauses {
		if !strings.Contains(sql, clause) {
			t.Fatalf("missing expected clause '%s' in SQL:\n%s", clause, sql)
		}
	}
}
