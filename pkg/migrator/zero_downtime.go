package migrator

import (
	"context"
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
