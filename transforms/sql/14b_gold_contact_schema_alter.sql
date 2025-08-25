-- transforms/sql/14b_gold_contact_schema_alter.sql
-- Idempotent alters for contact schema:
--  1) ensure generic_email columns + index exist
--  2) migrate name_norm to a generated column when safe
--     else install a maintenance trigger so the plain column stays correct
-- This file is safe to run many times.

SET search_path = public;

--------------------------------------------------------------------------------
-- 1) generic_email column + generated lower + index (idempotent)
--------------------------------------------------------------------------------
DO $$
BEGIN
  -- generic_email
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email TEXT';
  END IF;

  -- generic_email_lower (generated)
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email_lower TEXT GENERATED ALWAYS AS (lower(generic_email)) STORED';
  END IF;

  -- non-unique index on generic_email_lower (partial if column nullable)
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind='i' AND c.relname='ix_contact_generic_email_lower' AND n.nspname='gold'
  ) THEN
    EXECUTE 'CREATE INDEX ix_contact_generic_email_lower ON gold.contact (generic_email_lower) WHERE generic_email IS NOT NULL';
  END IF;
END$$;

--------------------------------------------------------------------------------
-- 2) name_norm: prefer GENERATED ALWAYS AS util.person_name_norm(full_name)
--    If we cannot safely swap (due to dependent views), keep plain column and
--    maintain it via a BEFORE INSERT/UPDATE trigger.
--------------------------------------------------------------------------------
-- Helper: create/replace maintenance trigger function (only used if needed)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname='gold' AND p.proname='maintain_contact_name_norm'
  ) THEN
    EXECUTE $fn$
      CREATE OR REPLACE FUNCTION gold.maintain_contact_name_norm()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $BODY$
      BEGIN
        NEW.name_norm := util.person_name_norm(NEW.full_name);
        RETURN NEW;
      END;
      $BODY$;
    $fn$;
  END IF;
END$$;

-- Migrate to GENERATED if safe; else install/ensure the maintenance trigger
DO $$
DECLARE
  is_gen        TEXT;
  can_swap      BOOLEAN := TRUE;
  dep_count     INT := 0;
  has_trigger   BOOLEAN := FALSE;
BEGIN
  -- Check current column state
  SELECT c.is_generated
  INTO is_gen
  FROM information_schema.columns c
  WHERE c.table_schema='gold' AND c.table_name='contact' AND c.column_name='name_norm';

  -- If column missing entirely, add as GENERATED
  IF is_gen IS NULL THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN name_norm TEXT GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED';
    RETURN;
  END IF;

  -- Already GENERATED? Nothing to do.
  IF is_gen = 'ALWAYS' THEN
    RETURN;
  END IF;

  -- is_gen = 'NEVER' (plain column). Can we swap to GENERATED safely?
  -- If any dependent VIEW exists on this column, we must NOT drop/re-add.
  SELECT COUNT(*)
  INTO dep_count
  FROM pg_depend d
  JOIN pg_class tbl ON tbl.oid = d.refobjid
  JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attname = 'name_norm'
  JOIN pg_class dep ON dep.oid = d.objid
  JOIN pg_namespace n_tbl ON n_tbl.oid = tbl.relnamespace
  JOIN pg_namespace n_dep ON n_dep.oid = dep.relnamespace
  WHERE n_tbl.nspname='gold'
    AND tbl.relname='contact'
    AND dep.relkind='v';  -- any views depending on this column?

  IF dep_count > 0 THEN
    can_swap := FALSE;
  END IF;

  IF can_swap THEN
    -- Drop just the dependent index if present (we'll recreate later)
    IF EXISTS (
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind='i' AND c.relname='ix_contact_name_company_noemail' AND n.nspname='gold'
    ) THEN
      EXECUTE 'DROP INDEX IF EXISTS gold.ix_contact_name_company_noemail';
    END IF;

    -- Replace plain column with GENERATED
    EXECUTE 'ALTER TABLE gold.contact DROP COLUMN name_norm';
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN name_norm TEXT GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED';

    -- Recreate the partial index
    EXECUTE 'CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail ON gold.contact (name_norm, primary_company_id) WHERE primary_email IS NULL';
  ELSE
    -- Keep plain column; ensure it stays correct via trigger + backfill once.
    SELECT EXISTS (
      SELECT 1
      FROM pg_trigger t
      JOIN pg_class r ON r.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = r.relnamespace
      WHERE n.nspname='gold' AND r.relname='contact' AND t.tgname='trg_contact_name_norm_maintain'
    )
    INTO has_trigger;

    IF NOT has_trigger THEN
      EXECUTE 'CREATE TRIGGER trg_contact_name_norm_maintain
               BEFORE INSERT OR UPDATE OF full_name ON gold.contact
               FOR EACH ROW
               EXECUTE FUNCTION gold.maintain_contact_name_norm()';
    END IF;

    -- Backfill any stale values (idempotent)
    EXECUTE 'UPDATE gold.contact
             SET name_norm = util.person_name_norm(full_name)
             WHERE name_norm IS DISTINCT FROM util.person_name_norm(full_name)';
  END IF;
END$$;
