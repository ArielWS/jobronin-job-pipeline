-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, triggers, and cleanup of legacy uniques.

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- Legacy cleanup: remove any global-unique on gold.contact_evidence.value
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  idx TEXT;
BEGIN
  -- Only if table already exists
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='gold' AND table_name='contact_evidence'
  ) THEN

    -- Drop a known legacy UNIQUE constraint name if present
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

    -- Drop ANY UNIQUE index enforcing global uniqueness on value / lower(value)
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

    -- Ensure the intended PRIMARY KEY exists (contact_id, kind, value)
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint con
      JOIN pg_class rel ON rel.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = rel.relnamespace
      WHERE n.nspname = 'gold'
        AND rel.relname = 'contact_evidence'
        AND con.contype = 'p'
    ) THEN
      EXECUTE 'ALTER TABLE gold.contact_evidence
               ADD CONSTRAINT pk_contact_evidence
               PRIMARY KEY (contact_id, kind, value)';
    END IF;
  END IF;
END$$;

-- -----------------------------------------------------------------------------
-- Touch-updated-at trigger
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- -----------------------------------------------------------------------------
-- gold.contact
-- -----------------------------------------------------------------------------
-- UUID PK via pgcrypto.gen_random_uuid() (enabled in 00_extensions.sql)
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name               text,
  -- kept as a regular column (not GENERATED) to allow controlled migrations if ever needed
  name_norm               text,
  primary_email           text,
  primary_phone           text,
  primary_linkedin_slug   text,
  title_raw               text,
  primary_company_id      bigint REFERENCES gold.company(company_id) ON DELETE SET NULL,

  -- generic mailbox (careers@, info@, etc.) for reference only
  generic_email           text,

  -- convenience lowers
  primary_email_lower     text GENERATED ALWAYS AS (lower(primary_email)) STORED,
  generic_email_lower     text GENERATED ALWAYS AS (lower(generic_email)) STORED,

  -- optional geo hints
  country_guess           text,
  region_guess            text,
  city_guess              text,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_contact_updated_at ON gold.contact;
CREATE TRIGGER trg_contact_updated_at
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION gold.set_updated_at();

-- Unique partial for email identity
CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_primary_email_lower
ON gold.contact ((lower(primary_email)))
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

-- -----------------------------------------------------------------------------
-- gold.contact_alias
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact_alias (
  contact_id  uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  alias       text NOT NULL,
  alias_norm  text GENERATED ALWAYS AS (util.person_name_norm(alias)) STORED,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, alias_norm)
);

-- -----------------------------------------------------------------------------
-- gold.contact_evidence
-- -----------------------------------------------------------------------------
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

-- Non-unique helper index (ok to keep)
CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
ON gold.contact_evidence (kind, value);

-- -----------------------------------------------------------------------------
-- gold.contact_affiliation
-- -----------------------------------------------------------------------------
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
