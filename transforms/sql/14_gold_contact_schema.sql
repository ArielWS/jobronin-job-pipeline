-- 14_gold_contact_schema.sql
-- Schema for GOLD contacts: core tables, FKs, indexes, triggers.

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS gold;

-- A tiny helper trigger to keep updated_at fresh
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
-- Using UUID for contact_id (gen_random_uuid() from pgcrypto; enabled in 00_extensions.sql)
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name               text,
  name_norm               text, -- util.person_name_norm in ETL (kept as a regular column for control)
  primary_email           text,
  primary_phone           text,
  primary_linkedin_slug   text,
  title_raw               text,
  primary_company_id      bigint REFERENCES gold.company(company_id) ON DELETE SET NULL,

  -- store generic mailbox email separately (e.g., careers@, info@); not for matching
  generic_email           text,

  -- convenience generated lowers (handy for QA/joins)
  primary_email_lower     text GENERATED ALWAYS AS (lower(primary_email)) STORED,
  generic_email_lower     text GENERATED ALWAYS AS (lower(generic_email)) STORED,

  -- optional geo hints (kept light for now)
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

-- Unique expression index for dedup by email (non-null)
-- NOTE: this is an INDEX (not a CONSTRAINT), and ETL uses index inference to target it.
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
