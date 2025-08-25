-- transforms/sql/14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, cleanup of legacy uniques, and safe indexes.

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS gold;

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
-- Tables
-- -----------------------------------------------------------------------------

-- gold.contact
-- NOTE: name_norm is GENERATED from full_name so ETL must NOT try to write it.
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name             text,
  name_norm             text GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED,
  primary_email         text,
  primary_phone         text,
  primary_linkedin_slug text,
  title_raw             text,
  primary_company_id    bigint REFERENCES gold.company(company_id) ON DELETE SET NULL,

  -- generic mailbox (careers@, info@, etc.)
  generic_email         text,

  -- convenience lowers (keep whatever exists; we won’t try to toggle generated here)
  primary_email_lower   text GENERATED ALWAYS AS (lower(primary_email)) STORED,
  generic_email_lower   text GENERATED ALWAYS AS (lower(generic_email)) STORED,

  -- optional geo hints
  country_guess         text,
  region_guess          text,
  city_guess            text,

  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_contact_updated_at ON gold.contact;
CREATE TRIGGER trg_contact_updated_at
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION gold.set_updated_at();

-- gold.contact_alias
CREATE TABLE IF NOT EXISTS gold.contact_alias (
  contact_id  uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  alias       text NOT NULL,
  alias_norm  text GENERATED ALWAYS AS (util.person_name_norm(alias)) STORED,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, alias_norm)
);

-- gold.contact_evidence
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

-- gold.contact_affiliation
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

-- -----------------------------------------------------------------------------
-- Indexes (non-unique helpers)
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_contact_company_id
  ON gold.contact (primary_company_id);

CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail
  ON gold.contact (name_norm, primary_company_id)
  WHERE primary_email IS NULL;

CREATE INDEX IF NOT EXISTS ix_contact_generic_email_lower
  ON gold.contact (generic_email_lower)
  WHERE generic_email IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
  ON gold.contact_evidence (kind, value);

CREATE INDEX IF NOT EXISTS ix_contact_affil_company
  ON gold.contact_affiliation (company_id);

-- -----------------------------------------------------------------------------
-- Legacy cleanup: remove any *global* UNIQUE on contact_evidence.value
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  r RECORD;
BEGIN
  -- Drop legacy UNIQUE constraints on (value) if any
  FOR r IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold'
      AND rel.relname='contact_evidence'
      AND con.contype='u'
      AND pg_get_constraintdef(con.oid) ILIKE '%(value)%'
  LOOP
    EXECUTE 'ALTER TABLE gold.contact_evidence DROP CONSTRAINT ' || quote_ident(r.conname);
  END LOOP;

  -- Drop legacy UNIQUE indexes on (value) / lower(value)
  FOR r IN
    SELECT schemaname, indexname
    FROM pg_indexes
    WHERE schemaname='gold'
      AND tablename='contact_evidence'
      AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND (indexdef ILIKE '%(value)%' OR indexdef ILIKE '%lower(value)%' OR indexdef ILIKE '%lower((value))%')
  LOOP
    EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(r.schemaname) || '.' || quote_ident(r.indexname);
  END LOOP;
END$$;

-- -----------------------------------------------------------------------------
-- Email hygiene & unique constraint on email (expression-based, whitespace-safe)
-- -----------------------------------------------------------------------------
-- 1) Trim whitespace in emails (idempotent)
UPDATE gold.contact
SET
  primary_email = NULLIF(btrim(primary_email), ''),
  generic_email = NULLIF(btrim(generic_email), ''),
  updated_at    = now()
WHERE (primary_email IS NOT NULL AND primary_email <> btrim(primary_email))
   OR (generic_email IS NOT NULL AND generic_email <> btrim(generic_email));

-- 2) Resolve duplicate primary_email values (case/space-insensitive):
--    keep earliest created_at per normalized email; null others.
WITH norm AS (
  SELECT contact_id, lower(btrim(primary_email)) AS norm_email, created_at
  FROM gold.contact
  WHERE primary_email IS NOT NULL
),
dups AS (
  SELECT norm_email
  FROM norm
  GROUP BY norm_email
  HAVING COUNT(*) > 1
),
keepers AS (
  SELECT DISTINCT ON (n.norm_email)
         n.norm_email, n.contact_id
  FROM norm n
  JOIN dups d USING (norm_email)
  ORDER BY n.norm_email, n.created_at ASC, n.contact_id ASC
),
victims AS (
  SELECT n.contact_id
  FROM norm n
  JOIN dups d USING (norm_email)
  LEFT JOIN keepers k ON k.norm_email = n.norm_email AND k.contact_id = n.contact_id
  WHERE k.contact_id IS NULL
)
UPDATE gold.contact c
SET primary_email = NULL,
    updated_at    = now()
WHERE c.contact_id IN (SELECT contact_id FROM victims);

-- 3) Unique expression index for email identity (ignore stray spaces)
--    Using expression avoids depending on the generated column definition.
DO $$
DECLARE
  idx_exists BOOLEAN := FALSE;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname='gold'
      AND tablename='contact'
      AND indexname='ux_contact_email_lower_btrim'
  ) INTO idx_exists;

  IF NOT idx_exists THEN
    EXECUTE 'CREATE UNIQUE INDEX ux_contact_email_lower_btrim
             ON gold.contact ((lower(btrim(primary_email))))
             WHERE primary_email IS NOT NULL';
  END IF;
END$$;
