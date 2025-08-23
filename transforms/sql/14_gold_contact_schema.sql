-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, triggers, legacy cleanup, hygiene & de-dup.
-- Safe to re-run. Does NOT drop columns that views depend on.

SET search_path = public;

-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS gold;

--------------------------------------------------------------------------------
-- LEGACY CLEANUP #1: remove any global UNIQUE on gold.contact_evidence.value
--------------------------------------------------------------------------------
DO $$
DECLARE
  idx TEXT;
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='gold' AND table_name='contact_evidence'
  ) THEN
    -- Drop known legacy UNIQUE constraint if it exists
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

    -- Drop ANY unique index enforcing global uniqueness on (value) or lower(value)
    FOR idx IN
      SELECT indexname
      FROM pg_indexes
      WHERE schemaname = 'gold'
        AND tablename  = 'contact_evidence'
        AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
        AND ( indexdef ILIKE '%(value)%'
              OR indexdef ILIKE '%lower(value)%'
              OR indexdef ILIKE '%lower((value))%' )
    LOOP
      EXECUTE format('DROP INDEX IF EXISTS gold.%I', idx);
    END LOOP;
  END IF;
END$$;

--------------------------------------------------------------------------------
-- TABLES
--------------------------------------------------------------------------------
-- Keep name_norm a plain column (NOT generated) so ETL can set it.
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

  -- These may already exist. We won't drop them here because views might depend.
  primary_email_lower     text,
  generic_email_lower     text,

  country_guess           text,
  region_guess            text,
  city_guess              text,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Updated-at trigger
CREATE OR REPLACE FUNCTION gold.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_contact_updated_at ON gold.contact;
CREATE TRIGGER trg_contact_updated_at
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION gold.set_updated_at();

-- Lookups
CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail
  ON gold.contact (name_norm, primary_company_id)
  WHERE primary_email IS NULL;

CREATE INDEX IF NOT EXISTS ix_contact_company_id
  ON gold.contact (primary_company_id);

-- Ensure lower columns exist (as plain or generated). If they already exist, keep.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='primary_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN primary_email_lower text';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email_lower text';
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS ix_contact_generic_email_lower
  ON gold.contact (generic_email_lower)
  WHERE generic_email IS NOT NULL;

-- Aliases
CREATE TABLE IF NOT EXISTS gold.contact_alias (
  contact_id  uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  alias       text NOT NULL,
  alias_norm  text GENERATED ALWAYS AS (util.person_name_norm(alias)) STORED,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, alias_norm)
);

-- Evidence
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

CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
  ON gold.contact_evidence (kind, value);

-- Affiliations
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
-- LEGACY CLEANUP #2 (CRITICAL): drop ANY unique constraint/index on email lower
-- We do this BEFORE normalization so updates won't violate old uniques.
--------------------------------------------------------------------------------
DO $$
DECLARE
  cname TEXT;
  iname TEXT;
BEGIN
  -- Drop UNIQUE constraints that include primary_email_lower
  FOR cname IN
    SELECT con.conname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold'
      AND rel.relname='contact'
      AND con.contype='u'
      AND EXISTS (
        SELECT 1
        FROM unnest(con.conkey) AS u(attnum)
        JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = u.attnum
        WHERE a.attname = 'primary_email_lower'
      )
  LOOP
    EXECUTE format('ALTER TABLE gold.contact DROP CONSTRAINT %I', cname);
  END LOOP;

  -- Drop UNIQUE indexes that enforce uniqueness on primary_email_lower or expression
  FOR iname IN
    SELECT i.indexname
    FROM pg_indexes i
    WHERE i.schemaname='gold'
      AND i.tablename='contact'
      AND i.indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND (
        i.indexdef ILIKE '%(primary_email_lower)%' OR
        i.indexdef ILIKE '%lower(primary_email)%'  OR
        i.indexdef ILIKE '%lower((primary_email))%' OR
        i.indexdef ILIKE '%lower(btrim(primary_email))%' OR
        i.indexdef ILIKE '%lower((btrim((primary_email))))%'
      )
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS gold.%I', iname);
  END LOOP;
END$$;

--------------------------------------------------------------------------------
-- HYGIENE: normalize emails and backfill *_lower columns (no UNIQUE in the way)
--------------------------------------------------------------------------------
-- Normalize stored emails (lower + trim)
UPDATE gold.contact
SET primary_email = lower(btrim(primary_email)),
    updated_at    = now()
WHERE primary_email IS NOT NULL
  AND primary_email <> lower(btrim(primary_email));

UPDATE gold.contact
SET generic_email = lower(btrim(generic_email)),
    updated_at    = now()
WHERE generic_email IS NOT NULL
  AND generic_email <> lower(btrim(generic_email));

-- Backfill *_lower columns if they are plain columns
UPDATE gold.contact
SET primary_email_lower = lower(btrim(primary_email))
WHERE primary_email IS NOT NULL
  AND (primary_email_lower IS DISTINCT FROM lower(btrim(primary_email)));

UPDATE gold.contact
SET generic_email_lower = lower(btrim(generic_email))
WHERE generic_email IS NOT NULL
  AND (generic_email_lower IS DISTINCT FROM lower(btrim(generic_email)));

-- Remove truly empty contacts (no identity, no evidence, no affiliation)
DELETE FROM gold.contact c
WHERE c.primary_email IS NULL
  AND c.generic_email IS NULL
  AND c.primary_phone IS NULL
  AND c.title_raw IS NULL
  AND c.full_name IS NULL
  AND NOT EXISTS (SELECT 1 FROM gold.contact_evidence e WHERE e.contact_id = c.contact_id)
  AND NOT EXISTS (SELECT 1 FROM gold.contact_affiliation a WHERE a.contact_id = c.contact_id);

--------------------------------------------------------------------------------
-- PRE-INDEX DE-DUP: collapse rows that share the same normalized primary_email
-- Keep the richest row (more non-null fields), tiebreak by oldest created_at.
-- Merge aliases/evidence/affiliations into the keeper, then delete dups.
--------------------------------------------------------------------------------
WITH ranked AS (
  SELECT
    c.contact_id,
    lower(btrim(c.primary_email)) AS email_norm,
    ((c.full_name IS NOT NULL)::int
     + (c.title_raw IS NOT NULL)::int
     + (c.primary_phone IS NOT NULL)::int
     + (c.primary_company_id IS NOT NULL)::int
     + (c.generic_email IS NOT NULL)::int
     + (c.name_norm IS NOT NULL)::int) AS score,
    c.created_at
  FROM gold.contact c
  WHERE c.primary_email IS NOT NULL
),
dupgroups AS (
  SELECT email_norm
  FROM ranked
  GROUP BY email_norm
  HAVING COUNT(*) > 1
),
packed AS (
  SELECT
    r.email_norm,
    array_agg(r.contact_id ORDER BY r.score DESC, r.created_at ASC) AS ids
  FROM ranked r
  JOIN dupgroups d USING (email_norm)
  GROUP BY r.email_norm
),
pick AS (
  SELECT
    email_norm,
    ids[1] AS keep_id,
    CASE WHEN array_length(ids,1) > 1 THEN ids[2:array_length(ids,1)]
         ELSE ARRAY[]::uuid[] END AS dup_ids
  FROM packed
),
m_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias)
  SELECT p.keep_id, ca.alias
  FROM pick p
  JOIN gold.contact_alias ca ON ca.contact_id = ANY (p.dup_ids)
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING 1
),
m_ev AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT p.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail
  FROM pick p
  JOIN gold.contact_evidence ce ON ce.contact_id = ANY (p.dup_ids)
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),
m_aff AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT p.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id
  FROM pick p
  JOIN gold.contact_affiliation a ON a.contact_id = ANY (p.dup_ids)
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role       = COALESCE(EXCLUDED.role,       gold.contact_affiliation.role),
        seniority  = COALESCE(EXCLUDED.seniority,  gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen  = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active     = EXCLUDED.active
  RETURNING 1
),
m_contact AS (
  -- Pull over useful fields from dups where keeper is NULL
  UPDATE gold.contact keep
  SET full_name          = COALESCE(keep.full_name,          dup.full_name),
      title_raw          = COALESCE(keep.title_raw,          dup.title_raw),
      primary_phone      = COALESCE(keep.primary_phone,      dup.primary_phone),
      primary_company_id = COALESCE(keep.primary_company_id, dup.primary_company_id),
      generic_email      = COALESCE(keep.generic_email,      dup.generic_email),
      name_norm          = COALESCE(keep.name_norm,          dup.name_norm),
      updated_at         = now()
  FROM pick p
  JOIN gold.contact dup ON dup.contact_id = ANY (p.dup_ids)
  WHERE keep.contact_id = p.keep_id
  RETURNING 1
)
DELETE FROM gold.contact c
USING pick p
WHERE c.contact_id = ANY (p.dup_ids);

--------------------------------------------------------------------------------
-- RE-ENFORCE CANONICAL UNIQUENESS on normalized primary email
--------------------------------------------------------------------------------
-- Drop any old expression index with known names (no-op if absent)
DROP INDEX IF EXISTS gold.ux_contact_primary_email_lower;
DROP INDEX IF EXISTS gold.ux_contact_email_lower_idx;

-- Create ONE canonical unique functional index for ON CONFLICT inference
CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_primary_email_norm
  ON gold.contact ( (lower(btrim(primary_email))) )
  WHERE primary_email IS NOT NULL;
