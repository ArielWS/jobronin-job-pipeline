-- transforms/sql/14b_gold_contact_schema_alter.sql
-- Idempotent alters for contact schema:
--  - ensure generic_email columns exist
--  - migrate name_norm to a generated column (from full_name) if it's still a plain column

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

-- Migrate name_norm to GENERATED ALWAYS if it's not yet generated
DO $$
DECLARE
  is_gen text;
BEGIN
  SELECT c.is_generated
  INTO is_gen
  FROM information_schema.columns c
  WHERE c.table_schema='gold' AND c.table_name='contact' AND c.column_name='name_norm';

  IF is_gen IS NULL THEN
    -- Column missing: add as generated
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN name_norm TEXT GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED';
  ELSIF is_gen = 'NEVER' THEN
    -- Drop dependent index if present
    IF EXISTS (
      SELECT 1 FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind='i' AND c.relname='ix_contact_name_company_noemail' AND n.nspname='gold'
    ) THEN
      EXECUTE 'DROP INDEX IF EXISTS gold.ix_contact_name_company_noemail';
    END IF;

    -- Replace plain column with generated one (value is derivable from full_name)
    EXECUTE 'ALTER TABLE gold.contact DROP COLUMN name_norm';
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN name_norm TEXT GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED';

    -- Recreate index
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail ON gold.contact (name_norm, primary_company_id) WHERE primary_email IS NULL';
  END IF;
END$$;
