-- Phase 2: Create index CONCURRENTLY to prevent blocking live writes
-- +migrate no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_normalized_phone
ON accounts (normalized_phone);
