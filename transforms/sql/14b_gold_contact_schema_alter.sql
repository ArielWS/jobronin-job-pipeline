-- transforms/sql/14b_gold_contact_schema_alter.sql
-- Idempotent, dependency-safe alters for contacts.
-- - Ensure generic_email + generated lower exist
-- - Keep name_norm in sync WITHOUT dropping it (avoid breaking dependent views)
-- - No attempts to rewrite name_norm as GENERATED to prevent dependency errors

SET search_path = public;

--------------------------------------------------------------------------------
-- Ensure generic_email exists
--------------------------------------------------------------------------------
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

--------------------------------------------------------------------------------
-- Ensure generic_email_lower GENERATED column exists
--------------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    -- Keep expression simple and compatible with existing views/indexes
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email_lower TEXT GENERATED ALWAYS AS (lower(generic_email)) STORED';
  END IF;
END$$;

--------------------------------------------------------------------------------
-- Ensure lookup index for generic_email_lower (simple, idempotent)
--------------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_contact_generic_email_lower
ON gold.contact (generic_email_lower);

--------------------------------------------------------------------------------
-- NAME_NORM: keep it in sync via trigger (do NOT drop; views may depend on it)
--------------------------------------------------------------------------------
-- Create or replace a small sync function
CREATE OR REPLACE FUNCTION gold.sync_contact_name_norm()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Only update when column exists and is not generated (we check below)
  NEW.name_norm := util.person_name_norm(NEW.full_name);
  RETURN NEW;
END;
$$;

-- If name_norm column is missing, add it (plain column)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='name_norm'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN name_norm TEXT';
  END IF;
END$$;

-- Backfill & trigger ONLY when name_norm is a plain column (is_generated = 'NEVER')
DO $$
DECLARE
  is_gen text;
BEGIN
  SELECT c.is_generated
    INTO is_gen
  FROM information_schema.columns c
  WHERE c.table_schema='gold'
    AND c.table_name='contact'
    AND c.column_name='name_norm';

  -- If plain column, backfill and attach trigger to keep in sync.
  IF is_gen = 'NEVER' THEN
    -- Backfill where out-of-sync
    UPDATE gold.contact
    SET name_norm = util.person_name_norm(full_name)
    WHERE name_norm IS DISTINCT FROM util.person_name_norm(full_name);

    -- Recreate trigger safely
    EXECUTE 'DROP TRIGGER IF EXISTS trg_contact_name_norm ON gold.contact';
    EXECUTE 'CREATE TRIGGER trg_contact_name_norm
             BEFORE INSERT OR UPDATE OF full_name ON gold.contact
             FOR EACH ROW EXECUTE FUNCTION gold.sync_contact_name_norm()';
  END IF;

  -- If it’s already GENERATED, do nothing (don’t attempt to drop/alter).
END$$;
