-- Phase 3: Add constraint as NOT VALID
-- Safe for zero-downtime: Verifies only new rows, avoiding long table lock

ALTER TABLE accounts
ADD CONSTRAINT check_normalized_phone_format
CHECK (normalized_phone ~ '^\+[1-9]\d{1,14}$')
NOT VALID;
