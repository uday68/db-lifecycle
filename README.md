# db-lifecycle: Database Migration & Schema Lifecycle Manager

`db-lifecycle` is a concurrency-safe, transactional PostgreSQL schema migration manager engineered in **Go**. Built for high-reliability data infrastructure, it orchestrates zero-downtime schema evolution, detects post-application file tampering via SHA-256 checksums, and coordinates distributed migration runners using PostgreSQL session-level advisory locks.

---

## 1. Key Architectural Features

* **Distributed Advisory Locking:** Acquires `pg_try_advisory_lock` with exponential polling and configurable timeout (`--lock-timeout`), ensuring that multiple concurrent pods or CI runners cannot execute overlapping migrations.
* **Cryptographic Tamper Detection:** Generates SHA-256 checksums over normalized SQL file contents. If a migration file is modified after being applied to production, `validate` and `plan` immediately flag the violation with `ErrChecksumMismatch`.
* **Transactional & Non-Transactional DDL Support:** Runs standard DDL statements inside atomic transactions (`BEGIN ... COMMIT`) with automatic rollback on failure. Explicitly supports non-transactional statements (such as `CREATE INDEX CONCURRENTLY` or `ALTER TYPE ADD VALUE`) via the `-- +migrate no-transaction` directive.
* **Deterministic Sequencing:** Strict version sorting (`<version>_<name>.sql`) with automated validation preventing duplicate version numbers or gaps.
* **Dry-Run Planning (`plan`):** Computes pending migrations and previews execution modes (transactional vs. non-transactional) without mutating database state.
* **Zero-Downtime Expand-and-Contract Architecture:** Provides documented and tested patterns for non-blocking column additions, concurrent indexing, deferred constraint validation (`NOT VALID` -> `VALIDATE CONSTRAINT`), and batched chunked backfills.

---

## 2. CLI Usage

```bash
# Display migration status and execution history
dblifecycle status --dir=./migrations --db="postgres://user:pass@localhost:5432/app?sslmode=disable"

# Dry-run execution plan without modifying database
dblifecycle plan --dir=./migrations

# Apply pending migrations in order with advisory lock
dblifecycle up --dir=./migrations --db=$DB_URL

# Validate checksums and integrity
dblifecycle validate --dir=./migrations --db=$DB_URL

# Check current applied version
dblifecycle version --db=$DB_URL
```

---

## 3. Zero-Downtime Expand-and-Contract Migration Workflow

To change a database schema under active production traffic without taking table locks:

```
[Phase 1: Expand]
  Add column as NULLABLE (ALTER TABLE accounts ADD COLUMN normalized_phone VARCHAR(32);)
       ↓
[Phase 2: Concurrently Index]
  Create index without blocking writes (-- +migrate no-transaction; CREATE INDEX CONCURRENTLY ...)
       ↓
[Phase 3: Application Dual-Write]
  Deploy application code that writes to legacy and new columns, reading from legacy
       ↓
[Phase 4: Batched Backfill]
  Run chunked primary-key updates with sleep intervals to populate historic records
       ↓
[Phase 5: Validate Constraint]
  Add constraint as NOT VALID, then VALIDATE CONSTRAINT (SHARE UPDATE EXCLUSIVE lock)
       ↓
[Phase 6: Contract]
  Switch reads to new column, verify metrics, and drop legacy column after older services stop
```

---

## 4. Benchmark Measurements (12th Gen Intel Core i5-12450H)

* **SHA-256 Checksum Calculation:** `1,591 ns/op` (~628,000 checksums/sec)
* **Plan Computation Overhead:** `1.52 ms/op` (including disk I/O, file parsing, and plan synthesis)
* **In-Memory Advisory Locking:** `0.07s` total contention resolution across concurrent worker routines
