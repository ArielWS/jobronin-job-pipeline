-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, triggers, legacy cleanup, and hygiene.
-- Idempotent. Safe with existing objects and dependent views.

SET search_path = public;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS gold;

--------------------------------------------------------------------------------
-- LEGACY CLEANUP: remove any global UNIQUE on gold.contact_evidence.value
--------------------------------------------------------------------------------
DO $$
DECLARE
  idx TEXT;
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='gold' AND table_name='contact_evidence'
  ) THEN
    -- Drop known legacy UNIQUE constraint, if present
    IF EXISTS (
      SELECT 1
      FROM pg_constraint con
      JOIN pg_class rel ON rel.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = rel.relnamespace
      WHERE n.nspname = 'gold'
        AND rel.relname = 'contact_evidence'
        AND con.contype = 'u'
        AND con.conname = 'ux_contact_evidence_email_global'
    ) THEN
      EXECUTE 'ALTER TABLE gold.contact_evidence DROP CONSTRAINT ux_contact_evidence_email_global';
    END IF;

    -- Drop ANY UNIQUE index enforcing global uniqueness on value/lower(value)
    FOR idx IN
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'gold'
        AND tablename  = 'contact_evidence'
        AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
        AND (
              indexdef ILIKE '%(value)%'
           OR indexdef ILIKE '%lower(value)%'
           OR indexdef ILIKE '%lower((value))%'
        )
    LOOP
      EXECUTE format('DROP INDEX IF EXISTS gold.%I', idx);
    END LOOP;
  END IF;
END$$;

--------------------------------------------------------------------------------
-- Touch-updated-at trigger helper
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

--------------------------------------------------------------------------------
-- gold.contact
--------------------------------------------------------------------------------
-- name_norm is a plain column (NOT generated) so ETL can set it explicitly.
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name               text,
  name_norm               text,
  primary_email           text,
  primary_phone           text,
  primary_linkedin_slug   text,
  title_raw               text,
  primary_company_id      bigint REFERENCES gold.company(company_id) ON DELETE SET NULL,

  generic_email           text,

  -- present in legacy installs; we will (attempt to) enforce as GENERATED later
  primary_email_lower     text,
  generic_email_lower     text,

  country_guess           text,
  region_guess            text,
  city_guess              text,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

-- If an older install had name_norm GENERATED, drop the expression
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact'
      AND column_name='name_norm' AND is_generated='ALWAYS'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ALTER COLUMN name_norm DROP EXPRESSION';
  END IF;
END$$;

-- Recreate *_lower columns as GENERATED when possible; otherwise keep as-is.
-- We catch dependency errors (SQLSTATE '2BP01') and skip the change to avoid breaking views.
DO $$
BEGIN
  -- primary_email_lower
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='primary_email_lower'
  ) THEN
    BEGIN
      EXECUTE 'ALTER TABLE gold.contact DROP COLUMN primary_email_lower';
      EXECUTE 'ALTER TABLE gold.contact
               ADD COLUMN primary_email_lower text GENERATED ALWAYS AS (lower(btrim(primary_email))) STORED';
    EXCEPTION
      WHEN SQLSTATE '2BP01' THEN  -- dependent objects exist
        RAISE NOTICE 'Keeping gold.contact.primary_email_lower as-is due to dependent objects.';
    END;
  ELSE
    EXECUTE 'ALTER TABLE gold.contact
             ADD COLUMN primary_email_lower text GENERATED ALWAYS AS (lower(btrim(primary_email))) STORED';
  END IF;

  -- generic_email_lower
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    BEGIN
      EXECUTE 'ALTER TABLE gold.contact DROP COLUMN generic_email_lower';
      EXECUTE 'ALTER TABLE gold.contact
               ADD COLUMN generic_email_lower text GENERATED ALWAYS AS (lower(btrim(generic_email))) STORED';
    EXCEPTION
      WHEN SQLSTATE '2BP01' THEN
        RAISE NOTICE 'Keeping gold.contact.generic_email_lower as-is due to dependent objects.';
    END;
  ELSE
    EXECUTE 'ALTER TABLE gold.contact
             ADD COLUMN generic_email_lower text GENERATED ALWAYS AS (lower(btrim(generic_email))) STORED';
  END IF;
END$$;

-- Updated-at trigger
DROP TRIGGER IF EXISTS trg_contact_updated_at ON gold.contact;
CREATE TRIGGER trg_contact_updated_at
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION gold.set_updated_at();

-- Email identity: functional unique index decoupled from *_lower column
DROP INDEX IF EXISTS gold.ux_contact_primary_email_lower;
CREATE UNIQUE INDEX ux_contact_primary_email_lower
ON gold.contact ((lower(btrim(primary_email))))
WHERE primary_email IS NOT NULL;

-- Helpful lookups
CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail
ON gold.contact (name_norm, primary_company_id)
WHERE primary_email IS NULL;

CREATE INDEX IF NOT EXISTS ix_contact_company_id
ON gold.contact (primary_company_id);

CREATE INDEX IF NOT EXISTS ix_contact_generic_email_lower
ON gold.contact (generic_email_lower)
WHERE generic_email IS NOT NULL;

--------------------------------------------------------------------------------
-- gold.contact_alias
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact_alias (
  contact_id  uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  alias       text NOT NULL,
  alias_norm  text GENERATED ALWAYS AS (util.person_name_norm(alias)) STORED,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, alias_norm)
);

--------------------------------------------------------------------------------
-- gold.contact_evidence
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact_evidence (
  contact_id  uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  kind        text NOT NULL,  -- email | phone | linkedin | title | location | other
  value       text NOT NULL,
  source      text,
  source_id   text,
  detail      jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, kind, value)
);

-- Non-unique helper index for lookups
CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
ON gold.contact_evidence (kind, value);

--------------------------------------------------------------------------------
-- gold.contact_affiliation
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact_affiliation (
  contact_id  uuid   NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  company_id  bigint NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  role        text,
  seniority   text,
  first_seen  timestamptz,
  last_seen   timestamptz,
  active      boolean,
  source      text,
  source_id   text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, company_id)
);

CREATE INDEX IF NOT EXISTS ix_contact_affil_company
ON gold.contact_affiliation (company_id);

--------------------------------------------------------------------------------
-- ONE-TIME HYGIENE (idempotent): normalize emails & remove empty shells
--------------------------------------------------------------------------------
-- Normalize stored emails (primary & generic)
UPDATE gold.contact
SET primary_email = lower(btrim(primary_email)), updated_at = now()
WHERE primary_email IS NOT NULL AND primary_email <> lower(btrim(primary_email));

UPDATE gold.contact
SET generic_email = lower(btrim(generic_email)), updated_at = now()
WHERE generic_email IS NOT NULL AND generic_email <> lower(btrim(generic_email));

-- Remove truly empty contacts (no identity, no evidence, no affiliation)
DELETE FROM gold.contact c
WHERE c.primary_email IS NULL
  AND c.generic_email IS NULL
  AND c.primary_phone IS NULL
  AND c.title_raw IS NULL
  AND c.full_name IS NULL
  AND NOT EXISTS (SELECT 1 FROM gold.contact_evidence e WHERE e.contact_id = c.contact_id)
  AND NOT EXISTS (SELECT 1 FROM gold.contact_affiliation a WHERE a.contact_id = c.contact_id);

-- Ensure contact_evidence has intended PK even on legacy installs
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='gold' AND table_name='contact_evidence'
  ) AND NOT EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold' AND rel.relname='contact_evidence' AND con.contype='p'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact_evidence
             ADD CONSTRAINT pk_contact_evidence
             PRIMARY KEY (contact_id, kind, value)';
  END IF;
END$$;
