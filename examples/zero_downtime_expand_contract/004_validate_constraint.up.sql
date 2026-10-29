-- Phase 4: Validate constraint after backfill completes
-- Safe for zero-downtime: Acquires only SHARE UPDATE EXCLUSIVE lock (does not block concurrent reads or writes)

ALTER TABLE accounts
VALIDATE CONSTRAINT check_normalized_phone_format;
