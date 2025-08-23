-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, triggers, legacy cleanup, hygiene & de-dup.
-- Idempotent and safe with existing dependent views.

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

  -- may already exist in legacy; we’ll attempt to (re)make GENERATED below
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

-- Try to recreate *_lower as GENERATED; if dependent objects exist, keep as-is
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
      WHEN SQLSTATE '2BP01' THEN
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

-- Helpful lookups (safe anytime)
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
-- HYGIENE: normalize emails & remove empty shells
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

--------------------------------------------------------------------------------
-- PRE-INDEX DE-DUP: collapse contacts that share the same normalized primary_email
-- Keep the "richest" row (more non-null fields), tiebreak by oldest created_at.
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
    CASE
      WHEN array_length(ids,1) > 1 THEN ids[2:array_length(ids,1)]
      ELSE ARRAY[]::uuid[]
    END AS dup_ids
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
  -- Pull over useful fields from duplicates where keeper is null
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
-- CONSTRAINTS AFTER CLEANUP
-- Enforce email identity with a functional unique index on normalized email.
--------------------------------------------------------------------------------
DROP INDEX IF EXISTS gold.ux_contact_primary_email_lower;
CREATE UNIQUE INDEX ux_contact_primary_email_lower
ON gold.contact ((lower(btrim(primary_email))))
WHERE primary_email IS NOT NULL;

--------------------------------------------------------------------------------
-- Ensure contact_evidence has intended PK even on legacy installs
--------------------------------------------------------------------------------
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
