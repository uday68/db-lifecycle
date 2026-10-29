# db-lifecycle: Enterprise PostgreSQL Schema Migration & Zero-Downtime Lifecycle Platform

[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-12%2F12%20passing-brightgreen.svg)]()
[![Concurrency](https://img.shields.io/badge/locking-pg__advisory__lock-blueviolet.svg)]()
[![Zero-Downtime](https://img.shields.io/badge/pattern-expand--and--contract-success.svg)]()

`db-lifecycle` is a concurrency-safe, transactional PostgreSQL schema migration engine and data lifecycle platform written in **Go**. Built specifically for high-concurrency, mission-critical distributed systems, it provides deterministic schema evolutions, eliminates race conditions across distributed microservices via **64-bit PostgreSQL advisory locking**, detects post-deployment tampering via **normalized SHA-256 checksums**, and enforces zero-downtime **expand-and-contract** patterns.

---

## Table of Contents

1. [The Problem: Why Traditional Migrations Fail at Scale](#-the-problem-why-traditional-migrations-fail-at-scale)
2. [Architectural Overview](#-architectural-overview)
3. [Core Systems Design](#-core-systems-design)
   - [Distributed Concurrency via PostgreSQL Advisory Locks](#1-distributed-concurrency-via-postgresql-advisory-locks)
   - [Transactional DDL vs. Non-Transactional Directives](#2-transactional-ddl-vs-non-transactional-directives)
   - [Cryptographic Tamper Detection & Normalization](#3-cryptographic-tamper-detection--normalization)
   - [The `schema_migrations` State Ledger](#4-the-schema_migrations-state-ledger)
4. [Zero-Downtime Expand-and-Contract Architecture](#-zero-downtime-expand-and-contract-architecture)
   - [Phase-by-Phase Rollout Walkthrough](#phase-by-phase-rollout-walkthrough)
   - [CTE Batched Backfilling with `FOR UPDATE SKIP LOCKED`](#cte-batched-backfilling-with-for-update-skip-locked)
   - [PostgreSQL Lock Escalation & Queue Starvation Analysis](#postgresql-lock-escalation--queue-starvation-analysis)
5. [CLI Reference & Interactive Outputs](#-cli-reference--interactive-outputs)
6. [Operational Failure & Recovery Playbook](#-operational-failure--recovery-playbook)
7. [Production Deployment Architecture](#-production-deployment-architecture)
8. [Benchmarks & Microarchitectural Performance](#-benchmarks--microarchitectural-performance)
9. [Verification & Test Suite](#-verification--test-suite)

---

## 🚨 The Problem: Why Traditional Migrations Fail at Scale

In multi-tenant SaaS environments handling billions of queries per day, running rudimentary migration scripts (`ALTER TABLE ...`) causes catastrophic cascading outages:

1. **Lock Queue Starvation:** Even operations that only take milliseconds to execute (such as `ALTER TABLE ADD COLUMN nullable_col`) require an `ACCESS EXCLUSIVE` lock. If a long-running analytical query or background worker holds an open read transaction, the `ALTER TABLE` queues behind it. Subsequent reads then queue behind the DDL request, saturating database connection pools within seconds.
2. **Concurrent Pod Race Conditions:** In autoscaling Kubernetes clusters, rolling deployments launch multiple application pods simultaneously. If multiple pods execute migrations upon boot, uncoordinated DDL runners can deadlock, execute out of order, or write corrupted metadata.
3. **Silent Script Modification:** Developers altering migration scripts after deployment to staging or canary environments create silent divergence, invalidating database schema assumptions across regions.
4. **Transactional DDL Limitations:** PostgreSQL enforces transactional DDL, which is safe for adding tables or foreign keys. However, crucial non-blocking operations like `CREATE INDEX CONCURRENTLY` cannot be executed within a transaction block (`ERROR: CREATE INDEX CONCURRENTLY cannot run inside a transaction block`). Primitive tools fail to manage this boundary safely.

`db-lifecycle` was designed from first principles to solve these distributed systems hazards.

---

## 🏛️ Architectural Overview

```
                                  +-----------------------+
                                  |   db-lifecycle CLI    |
                                  |  (or K8s initJob)     |
                                  +-----------+-----------+
                                              |
                     1. Acquire Lock          | 2. Verify Schema Table
            +---------------------------------+---------------------------------+
            |                                                                   |
            v                                                                   v
+------------------------+                                         +------------------------+
| pg_try_advisory_lock() |                                         |   schema_migrations    |
| (64-bit cluster hash)  |                                         |   (Historical Ledger)  |
+-----------+------------+                                         +-----------+------------+
            |                                                                   |
            | Lock Granted                                                      | Checksums Valid
            +---------------------------------+---------------------------------+
                                              |
                                 3. Compute Migration Plan
                                              |
                                              v
                              +-------------------------------+
                              |    Parse Migration Files      |
                              | - Checksum Hashing            |
                              | - Directive Detection:        |
                              |   "-- +migrate no-transaction"|
                              +---------------+---------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
             Standard DDL                                      Directive Present
                     |                                                 |
                     v                                                 v
        +-------------------------+                       +-------------------------+
        |   Begin Atomic Tx       |                       |   Direct DB Execution   |
        |   (tx.BeginTx)          |                       |   (No Transaction Block)|
        |   - Apply Migration DDL |                       |   - CREATE INDEX        |
        |   - Record in Ledger    |                       |     CONCURRENTLY        |
        |   - Commit / Rollback   |                       |   - Record in Ledger    |
        +-------------------------+                       +-------------------------+
                                              |
                                 4. Release Advisory Lock
                                              |
                                              v
                                  +-----------------------+
                                  |  pg_advisory_unlock() |
                                  +-----------------------+
```

---

## ⚙️ Core Systems Design

### 1. Distributed Concurrency via PostgreSQL Advisory Locks

Rather than using table-level locks that interfere with application queries, `db-lifecycle` implements distributed mutual exclusion via **PostgreSQL 64-bit session-level advisory locks**:

- **Deterministic Lock Key:** A 64-bit integer hash generated from the target namespace (`0x524950504C494E47` - `"RIPPLING"`).
- **Non-Blocking Probing:** Invokes `SELECT pg_try_advisory_lock($1)`. If acquired, execution proceeds immediately.
- **Exponential Jitter Polling:** If held by another runner, the worker sleeps with randomized exponential backoff (starting at 50ms up to 500ms max) until either acquired or the `--lock-timeout` context expires.
- **Safe Release Invariant:** Lock release is guaranteed via `SELECT pg_advisory_unlock($1)` within a `defer` block, and automatically released by PostgreSQL if the network connection terminates abruptly.

```go
// PostgresDriver Lock acquisition snippet
func (d *PostgresDriver) AcquireLock(ctx context.Context, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    pollInterval := 50 * time.Millisecond
    for {
        var acquired bool
        err := d.db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", d.lockID).Scan(&acquired)
        if err == nil && acquired {
            return nil
        }
        if time.Now().After(deadline) {
            return ErrLockTimeout
        }
        time.Sleep(pollInterval)
        pollInterval = min(pollInterval*2, 500*time.Millisecond)
    }
}
```

### 2. Transactional DDL vs. Non-Transactional Directives

PostgreSQL supports transactional schema updates, which allows atomic rollback on syntax or constraint errors. However, concurrent operations that bypass locking must run outside transaction blocks:

* **Transactional Path (Default):**
  Wraps the migration script and ledger insertion inside an explicit database transaction (`BeginTx`). If any statement fails, the transaction is rolled back, leaving the database completely clean without half-applied tables or broken constraints.
* **Non-Transactional Path (`-- +migrate no-transaction`):**
  When a migration contains the directive comment `-- +migrate no-transaction`, `db-lifecycle` detects the flag and executes statements directly against the connection pool. This is mandatory for:
  - `CREATE INDEX CONCURRENTLY`
  - `DROP INDEX CONCURRENTLY`
  - `ALTER TYPE ... ADD VALUE` (in older PostgreSQL versions)
  - `VACUUM FULL` / `REINDEX CONCURRENTLY`

### 3. Cryptographic Tamper Detection & Normalization

To guarantee that deployed schemas remain strictly immutable across developer machines, staging, and production:

1. **Line-Ending Normalization:** Normalized by trimming leading/trailing whitespace and converting CRLF (`\r\n`) to LF (`\n`). This ensures developer operating systems (Windows, macOS, Linux) produce identical checksums.
2. **SHA-256 Digest:** Computes the hexadecimal digest over the normalized SQL.
3. **Enforcement:** During `plan`, `up`, and `validate`, the computed hash is compared against the `checksum` column in `schema_migrations`. If any byte has changed, execution halts immediately with `ErrChecksumMismatch`.

### 4. The `schema_migrations` State Ledger

`db-lifecycle` maintains complete audit history in an explicit metadata table:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version         BIGINT PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    checksum        CHAR(64) NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_ms    BIGINT NOT NULL,
    no_transaction  BOOLEAN NOT NULL DEFAULT FALSE,
    status          VARCHAR(32) NOT NULL DEFAULT 'APPLIED'
);
```

---

## 🔄 Zero-Downtime Expand-and-Contract Architecture

Changing schemas in petabyte-scale databases without taking down user traffic requires an asynchronous, multi-phase lifecycle known as **Expand and Contract**:

```
+---------------------------------------------------------------------------------------------------+
|                                 Zero-Downtime Migration Phases                                    |
+---------------------------------------------------------------------------------------------------+
|  Phase 1: Expand          |  Phase 2: Concurrently Index  |  Phase 3: Backfill  |  Phase 4: Contract  |
|  Add column as NULLABLE   |  CREATE INDEX CONCURRENTLY    |  Batched CTE chunk  |  Drop legacy col    |
|  (Microsecond catalog lock)| (No read/write locks held)    | (Zero lock starvation) (After app v2 stable)  |
+---------------------------------------------------------------------------------------------------+
```

### Phase-by-Phase Rollout Walkthrough

Refer to [`examples/zero_downtime_expand_contract/`](./examples/zero_downtime_expand_contract) for ready-to-run SQL migrations:

#### Phase 1: Expand (Add Column as Nullable)
```sql
-- 001_add_column_nullable.up.sql
-- Fast metadata catalog update; requires brief ACCESS EXCLUSIVE lock.
ALTER TABLE accounts ADD COLUMN normalized_phone VARCHAR(32);
```
> [!NOTE]
> Never add a column with a non-null `DEFAULT` on older PostgreSQL versions without `DEFAULT ... NOT NULL` optimizations, as it triggers a full table rewrite.

#### Phase 2: Concurrent Indexing
```sql
-- 002_add_index_concurrently.up.sql
-- +migrate no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_normalized_phone 
ON accounts (normalized_phone) 
WHERE normalized_phone IS NOT NULL;
```
> [!IMPORTANT]
> Must use `-- +migrate no-transaction`. Concurrent indexing performs two table scans without holding write locks.

#### Phase 3: Application Dual-Writing
Deploy application version $N+1$. New code writes to both `phone` and `normalized_phone`, but continues reading from `phone`.

#### Phase 4: Batched Backfill
Populate existing records using chunked updates with sleep intervals (see next section).

#### Phase 5: Deferred Constraint Validation
```sql
-- 003_add_constraint_not_valid.up.sql
ALTER TABLE accounts ADD CONSTRAINT chk_phone_format 
CHECK (length(normalized_phone) >= 10) NOT VALID;

-- 004_validate_constraint.up.sql
ALTER TABLE accounts VALIDATE CONSTRAINT chk_phone_format;
```
> [!TIP]
> Adding a constraint as `NOT VALID` validates future writes immediately without scanning historic data. `VALIDATE CONSTRAINT` then scans existing rows with only a `SHARE UPDATE EXCLUSIVE` lock, permitting concurrent `SELECT`, `INSERT`, `UPDATE`, and `DELETE`.

#### Phase 6: Contract
Deploy application version $N+2$ to read from `normalized_phone`. Once verified, safely drop the old column:
```sql
-- 005_drop_obsolete_column.up.sql
ALTER TABLE accounts DROP COLUMN phone;
```

---

### CTE Batched Backfilling with `FOR UPDATE SKIP LOCKED`

Running a naive `UPDATE accounts SET normalized_phone = format(phone);` on a 50-million row table locks rows, saturates transaction logs (WAL), and introduces multi-second replication lag on database replicas.

`db-lifecycle` generates optimal, non-blocking CTE backfill statements:

```sql
WITH batch AS (
    SELECT id
    FROM accounts
    WHERE normalized_phone IS NULL AND phone IS NOT NULL
    ORDER BY id ASC
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
)
UPDATE accounts t
SET normalized_phone = format_e164(t.phone)
FROM batch
WHERE t.id = batch.id;
```

#### Why this pattern is production-grade:
1. **Bounded Execution:** Only locks and updates 1,000 rows per transaction.
2. **`FOR UPDATE SKIP LOCKED`:** Guarantees that if a live user transaction is currently updating row ID 42, the backfill job skips it rather than waiting and creating a deadlock.
3. **Replication Safety:** Configurable inter-batch sleep delays (e.g. 50ms) allow PostgreSQL replicas to flush WAL buffers, preventing replica lag from breaking read-your-writes guarantees.

---

### PostgreSQL Lock Escalation & Queue Starvation Analysis

`db-lifecycle` does **not** make naive claims of "lock-free" migrations. PostgreSQL locks tables during DDL. Here is the exact lock mode matrix and risk mitigation strategy:

| Operation | PostgreSQL Lock Acquired | Permitted Concurrent Operations | Blocked Concurrent Operations | Mitigation Strategy |
|---|---|---|---|---|
| `ALTER TABLE ADD COLUMN` | `ACCESS EXCLUSIVE` | *None* | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Set `SET lock_timeout = '2s';` so DDL aborts if it cannot acquire lock immediately. |
| `CREATE INDEX CONCURRENTLY` | `SHARE UPDATE EXCLUSIVE` | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Concurrent `VACUUM`, other `CREATE INDEX`, DDL | Execute with `-- +migrate no-transaction`. |
| `VALIDATE CONSTRAINT` | `SHARE UPDATE EXCLUSIVE` | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Other DDL, concurrent `VACUUM` | Deferred validation after historic backfill completes. |
| `DROP COLUMN` | `ACCESS EXCLUSIVE` | *None* | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | Contract phase only executed after older application pods terminate. |

---

## 💻 CLI Reference & Interactive Outputs

```bash
# Build the binary
go build -o bin/dblifecycle ./cmd/dblifecycle
```

### 1. `dblifecycle status`
Inspects applied migrations and displays pending files:
```text
$ dblifecycle status --dir=./migrations --db=$DB_URL

VERSION  NAME                          STATUS   APPLIED AT           DURATION  NO-TX
1        001_add_column_nullable.sql   APPLIED  2026-10-29 17:00:12  14ms      false
2        002_add_index_concurrently.sql APPLIED  2026-10-29 17:01:05  412ms     true
3        003_add_constraint_not_valid  PENDING  -                    -         false
4        004_validate_constraint      PENDING  -                    -         false
```

### 2. `dblifecycle plan` (Dry-Run Preview)
Calculates pending changes without mutating state:
```text
$ dblifecycle plan --dir=./migrations

Pending Migrations (Dry Run):
  [3] 003_add_constraint_not_valid (Transactional DDL)
  [4] 004_validate_constraint (Transactional DDL)
Database state unmodified. 2 migrations ready to execute.
```

### 3. `dblifecycle up`
Applies all pending migrations using advisory locking:
```text
$ dblifecycle up --dir=./migrations --db=$DB_URL --lock-timeout=30s

[INFO] Acquired PostgreSQL 64-bit advisory lock (Key: 0x524950504C494E47)
[INFO] Applying migration 003: 003_add_constraint_not_valid (Mode: Transactional)
[INFO] Successfully applied 003 in 8ms (Checksum: a7f8d3...)
[INFO] Applying migration 004: 004_validate_constraint (Mode: Transactional)
[INFO] Successfully applied 004 in 35ms (Checksum: 9c21e0...)
[INFO] Released PostgreSQL advisory lock. 2 migrations applied successfully.
```

### 4. `dblifecycle validate`
Verifies SHA-256 integrity against disk files to catch tampering:
```text
$ dblifecycle validate --dir=./migrations --db=$DB_URL

[SUCCESS] All 4 applied migrations match filesystem SHA-256 checksums exactly. Zero tampering detected.
```

---

## 🛡️ Operational Failure & Recovery Playbook

| Failure Scenario | Engine Behavior | Recovery Action |
|---|---|---|
| **SQL Syntax Error in Transactional Migration** | Transaction aborts; `tx.Rollback()` executed automatically. `schema_migrations` records state as `FAILED`. | Fix SQL syntax in migration file and re-run `dblifecycle up`. |
| **Connection Drop during `CREATE INDEX CONCURRENTLY`** | Index is left in PostgreSQL catalog with `INVALID` state (`indisvalid = false`). | Re-running migration detects failed index. Forward recovery drops the invalid index (`DROP INDEX CONCURRENTLY idx;`) and recreates it. |
| **Runner Pod Killed (`SIGKILL`)** | PostgreSQL session drops; session-level advisory lock is **automatically released** by PostgreSQL kernel. | New pod acquires advisory lock cleanly on reboot and resumes pending migrations. |
| **Migration File Modified After Deployment** | Engine calculates checksum mismatch on boot; aborts before acquiring lock or touching user tables. | Revert unauthorized file edit on disk. If intentional, file a forward migration (`005_...sql`). |
| **Advisory Lock Timeout (Held by Long Runner)** | Engine exits with code 1 (`ErrLockTimeout`) after backoff polling expires. | Inspect `pg_stat_activity` for active migration PID. Prevents overlapping executions. |

---

## 🚀 Production Deployment Architecture

In production Kubernetes clusters, `db-lifecycle` is executed as an **initContainer** or **pre-deploy Helm Hook Job**:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: db-lifecycle-migration-runner
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migrator
          image: ghcr.io/uday68/db-lifecycle:v1.0.0
          command: ["/bin/dblifecycle", "up", "--dir=/migrations", "--lock-timeout=60s"]
          env:
            - name: DB_URL
              valueFrom:
                secretKeyRef:
                  name: db-credentials
                  key: connection_string
          resources:
            requests:
              cpu: "100m"
              memory: "64Mi"
            limits:
              cpu: "500m"
              memory: "128Mi"
          securityContext:
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 10001
            allowPrivilegeEscalation: false
```

Even if multiple jobs spin up concurrently across regions or clusters, **the PostgreSQL advisory lock serializes execution safely**.

---

## ⚡ Benchmarks & Microarchitectural Performance

Benchmarks executed on local development testbeds using Go's standard benchmarking harness (`go test -bench . -benchmem`):

```bash
cd pkg/migrator
go test -bench "Benchmark" -benchmem
```

| Benchmark Routine | Operations | Time / Op | Memory / Op | Allocations | Engineering Interpretation |
|---|:---:|:---:|:---:|:---:|---|
| `BenchmarkMigrator_ComputeChecksum` | 762,583 | **1,591 ns/op** (1.59 μs) | 688 B/op | 7 allocs/op | Sub-2-microsecond normalized SHA-256 computation over full migration file content. |
| `BenchmarkMigrator_PlanOverhead` | 784 | **1.52 ms/op** | 48,154 B/op | 631 allocs/op | Complete filesystem traversal, lexical version sorting, and dry-run ledger comparison. |
| `BenchmarkAdvisoryLock_Contention` | - | **0.07 s** | - | - | Deterministic contention resolution across parallel goroutines without deadlocks. |

---

## 🧪 Verification & Test Suite

The test suite covers distributed race conditions, tampering, and rollback mechanics:

```bash
# Run all unit tests with full output
go test -v ./...
```

### Key Test Cases (`pkg/migrator/migrator_test.go`):
1. `TestMigrator_AppliesInDeterministicOrder`: Verifies migrations apply in strict numeric order regardless of directory ordering.
2. `TestMigrator_IdempotentSubsequentRuns`: Guarantees subsequent `up` runs apply 0 migrations and leave database state unchanged.
3. `TestMigrator_DetectsChecksumTamperingAfterApplication`: Modifies migration SQL after application; asserts `ErrChecksumMismatch`.
4. `TestMigrator_RejectsDuplicateVersions`: Detects duplicate prefix identifiers and halts.
5. `TestMigrator_ConcurrentAdvisoryLockContention`: Simulates concurrent migration processes contending on the same locker; verifies exactly one runner applies migrations without race conditions.
6. `TestMigrator_LockTimeout`: Asserts clean timeout when lock is pre-held.
7. `TestMigrator_RollbackOnFailure`: Injects malformed SQL; validates transaction rollback leaves 0 dirty rows.
8. `TestMigrator_DryRunPlanNonMutation`: Verifies `Plan()` does not modify database state.
9. `TestMigrator_NonTransactionalDirective`: Asserts `-- +migrate no-transaction` bypasses transaction blocks.
10. `TestZeroDowntime_BatchedBackfill`: Executes chunked backfill across 2,500 rows in 5 batches.
11. `TestZeroDowntime_BatchedBackfill_Interrupted`: Validates graceful cancellation and partial progress preservation on `context.Canceled`.
12. `TestZeroDowntime_CTEBatchedUpdateSQL`: Asserts valid PostgreSQL CTE query generation using `FOR UPDATE SKIP LOCKED`.

---

## 👨‍💻 Author & Engineering Standards

* **V Uday** — [GitHub: @uday68](https://github.com/uday68) • [LinkedIn](https://www.linkedin.com/in/uday-v-37515b253/)
* **Engineered for:** Systems-level database lifecycle management, zero-downtime operations, and high-concurrency distributed platforms.
