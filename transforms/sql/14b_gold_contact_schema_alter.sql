-- transforms/sql/14b_gold_contact_schema_alter.sql
-- Adds optional generic_email fields (idempotent).

SET search_path = public;

-- Add generic_email
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email TEXT';
  END IF;
END$$;

-- Add generated lower for generic_email
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email_lower TEXT GENERATED ALWAYS AS (lower(generic_email)) STORED';
  END IF;
END$$;

-- Non-unique index for generic email lookup
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind='i' AND c.relname='ix_contact_generic_email_lower' AND n.nspname='gold'
  ) THEN
    EXECUTE 'CREATE INDEX ix_contact_generic_email_lower ON gold.contact (generic_email_lower)';
  END IF;
END$$;
