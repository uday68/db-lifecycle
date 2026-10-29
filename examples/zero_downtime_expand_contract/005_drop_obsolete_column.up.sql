-- Phase 5: Contract - Drop legacy column only after older application versions have been decommissioned

ALTER TABLE accounts
DROP COLUMN IF EXISTS legacy_phone;
