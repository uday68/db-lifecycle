-- Phase 1 of Expand-and-Contract: Add new column as NULLABLE
-- Safe for zero-downtime: Does not lock the table or rewrite rows

ALTER TABLE accounts
ADD COLUMN IF NOT EXISTS normalized_phone VARCHAR(32);
