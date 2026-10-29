package migrator

import (
	"context"
	"fmt"
	"time"
)

// BackfillConfig specifies safe parameters for batched data transformation without lock contention
type BackfillConfig struct {
	BatchSize    int
	BatchDelay   time.Duration
	TransformSQL string // e.g. "UPDATE users SET full_name = first_name || ' ' || last_name WHERE id >= $1 AND id < $2 AND full_name IS NULL"
}

type BackfillStats struct {
	TotalProcessed int64
	TotalBatches   int64
	DurationMs     int64
}

// SimulatedBackfillExecutor demonstrates expand-and-contract batched backfilling
type SimulatedBackfillExecutor struct {
	totalRows int64
}

func NewSimulatedBackfillExecutor(totalRows int64) *SimulatedBackfillExecutor {
	return &SimulatedBackfillExecutor{totalRows: totalRows}
}

// ExecuteBackfill iterates over dataset in primary key ranges to eliminate table lock starvation
func (b *SimulatedBackfillExecutor) ExecuteBackfill(ctx context.Context, cfg BackfillConfig) (BackfillStats, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}

	start := time.Now()
	var processed int64 = 0
	var batches int64 = 0

	for processed < b.totalRows {
		if ctx.Err() != nil {
			return BackfillStats{
				TotalProcessed: processed,
				TotalBatches:   batches,
				DurationMs:     time.Since(start).Milliseconds(),
			}, ctx.Err()
		}

		currentBatch := int64(cfg.BatchSize)
		if processed+currentBatch > b.totalRows {
			currentBatch = b.totalRows - processed
		}

		// Simulate batch update query execution
		processed += currentBatch
		batches++

		if cfg.BatchDelay > 0 {
			select {
			case <-ctx.Done():
				return BackfillStats{
					TotalProcessed: processed,
					TotalBatches:   batches,
					DurationMs:     time.Since(start).Milliseconds(),
				}, ctx.Err()
			case <-time.After(cfg.BatchDelay):
			}
		}
	}

	return BackfillStats{
		TotalProcessed: processed,
		TotalBatches:   batches,
		DurationMs:     time.Since(start).Milliseconds(),
	}, nil
}

// GenerateCTEBatchedUpdateSQL generates PostgreSQL CTE query for non-blocking batched updates.
// Uses FOR UPDATE SKIP LOCKED to prevent table locks and lock contention with concurrent OLTP writes.
func GenerateCTEBatchedUpdateSQL(table, primaryKey, updateAssignment, whereClause string, batchSize int) string {
	if batchSize <= 0 {
		batchSize = 500
	}
	if whereClause == "" {
		whereClause = "1=1"
	}
	return fmt.Sprintf(`WITH batch AS (
    SELECT %s
    FROM %s
    WHERE %s
    ORDER BY %s ASC
    LIMIT %d
    FOR UPDATE SKIP LOCKED
)
UPDATE %s t
SET %s
FROM batch
WHERE t.%s = batch.%s;`, primaryKey, table, whereClause, primaryKey, batchSize, table, updateAssignment, primaryKey, primaryKey)
}

// OperationalRisksGuide documents realistic locking trade-offs and non-zero-downtime edges.
func OperationalRisksGuide() map[string]string {
	return map[string]string{
		"ALTER TABLE ... VALIDATE CONSTRAINT": "Acquires SHARE UPDATE EXCLUSIVE lock. While concurrent SELECT/INSERT/UPDATE are permitted, it blocks concurrent VACUUM, CREATE INDEX CONCURRENTLY, and other DDL.",
		"CREATE INDEX CONCURRENTLY":            "Requires two table scans and waits for pending transactions to complete. If a long-running transaction is active, it blocks index completion and can leave invalid indexes.",
		"Irreversible Migrations":             "Operations like DROP COLUMN cannot be rolled back via transactional DDL; forward-recovery migrations (e.g. re-adding column as nullable) must be planned.",
		"Lock Queue Starvation":               "Even fast catalog operations (e.g. ALTER TABLE ADD COLUMN NULLABLE) require ACCESS EXCLUSIVE briefly; if long-running analytical queries hold locks, the DDL waits in queue and blocks subsequent read queries behind it unless lock_timeout is configured.",
	}
}

// ZeroDowntimeGuide provides the documented 5-step expand-and-contract operational contract
func ZeroDowntimeGuide() []string {
	return []string{
		"1. Expand: Add new column as NULLABLE (e.g. ALTER TABLE payments ADD COLUMN provider_ref TEXT)",
		"2. Dual-Write: Deploy code version N+1 that writes to both legacy and new schema, reading from legacy",
		"3. Batched Backfill: Run asynchronous chunked updates with sleep intervals to populate historic records without locking",
		"4. Validate & Constrain: Add NOT VALID constraint, validate constraint concurrently (no ACCESS EXCLUSIVE lock)",
		"5. Contract: Switch reads to new column, verify metrics, and drop obsolete column only after old code versions terminate",
	}
}
