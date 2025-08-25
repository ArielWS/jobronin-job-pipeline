-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, constraints, triggers, safe cleanup, and idempotent guards.
-- This file is safe to run standalone (e.g., during ad-hoc contact-only refreshes).
-- It does NOT require gold.company to exist at runtime; the FK is attached only if present.

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- Trigger helper: touch updated_at on UPDATE
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
-- Legacy cleanup on gold.contact_evidence: remove any global-unique on value
-- Keep only the per-contact PK (contact_id, kind, value).
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  -- Drop a known legacy UNIQUE constraint name, if present
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

  -- Drop ANY UNIQUE index that enforces global uniqueness on value/lower(value)
  PERFORM 1
  FROM pg_indexes
  WHERE schemaname = 'gold'
    AND tablename = 'contact_evidence'
    AND indexdef ILIKE 'CREATE UNIQUE INDEX%'
    AND (
         indexdef ILIKE '%(value)%'
      OR indexdef ILIKE '%lower(value)%'
      OR indexdef ILIKE '%lower((value))%'
    );

  IF FOUND THEN
    -- Collect and drop those unique indexes
    FOR
      SELECT format('DROP INDEX IF EXISTS %I.%I', schemaname, indexname) AS ddl
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
      EXECUTE (SELECT format('DROP INDEX IF EXISTS %I.%I', 'gold', indexname)
               FROM pg_indexes
               WHERE schemaname='gold' AND tablename='contact_evidence' AND indexname = indexname
               LIMIT 1);
    END LOOP;
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- nothing to clean yet
  NULL;
END
$$;

-- -----------------------------------------------------------------------------
-- gold.contact (base table) — created if missing; otherwise left intact
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name               text,
  -- Keep name_norm as a regular column (NOT GENERATED). ETL is responsible for writing it.
  name_norm               text,
  primary_email           text,
  primary_phone           text,
  primary_linkedin_slug   text,
  title_raw               text,
  -- FK to gold.company attached later only if table exists
  primary_company_id      bigint,

  -- generic mailbox (careers@, info@) for reference only (not for identity resolution)
  generic_email           text,

  -- convenience lowers (generated); if these exist already, they'll be left as-is
  primary_email_lower     text GENERATED ALWAYS AS (lower(btrim(primary_email))) STORED,
  generic_email_lower     text GENERATED ALWAYS AS (lower(btrim(generic_email))) STORED,

  -- optional geo hints
  country_guess           text,
  region_guess            text,
  city_guess              text,

  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);

-- Ensure updated_at trigger exists
DROP TRIGGER IF EXISTS trg_contact_updated_at ON gold.contact;
CREATE TRIGGER trg_contact_updated_at
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION gold.set_updated_at();

-- -----------------------------------------------------------------------------
-- Ensure generated lower columns exist if table pre-dated them (do not ALTER existing ones)
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='primary_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN primary_email_lower text GENERATED ALWAYS AS (lower(btrim(primary_email))) STORED';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='contact' AND column_name='generic_email_lower'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact ADD COLUMN generic_email_lower text GENERATED ALWAYS AS (lower(btrim(generic_email))) STORED';
  END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- Safe pre-clean + de-dup before enforcing unique email index
-- -----------------------------------------------------------------------------
DO $$
DECLARE
BEGIN
  -- Normalize whitespace on emails to avoid expression-index collisions
  UPDATE gold.contact
  SET primary_email = btrim(primary_email)
  WHERE primary_email IS NOT NULL
    AND primary_email <> btrim(primary_email);

  -- Collapse case/space duplicates for the same normalized email (keep earliest created)
  WITH norm AS (
    SELECT contact_id, created_at, lower(btrim(primary_email)) AS eml
    FROM gold.contact
    WHERE primary_email IS NOT NULL
  ),
  dups AS (
    SELECT eml, array_agg(contact_id ORDER BY created_at ASC, contact_id ASC) AS ids
    FROM norm
    GROUP BY eml
    HAVING COUNT(*) > 1
  ),
  to_merge AS (
    SELECT eml, ids[1] AS keep_id, unnest(ids[2:]) AS dup_id
    FROM dups
  )
  -- Move aliases
  ,m_alias AS (
    INSERT INTO gold.contact_alias (contact_id, alias)
    SELECT tm.keep_id, ca.alias
    FROM to_merge tm
    JOIN gold.contact_alias ca ON ca.contact_id = tm.dup_id
    ON CONFLICT (contact_id, alias_norm) DO NOTHING
    RETURNING 1
  )
  -- Move evidence
  ,m_ev AS (
    INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail, created_at)
    SELECT tm.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail, ce.created_at
    FROM to_merge tm
    JOIN gold.contact_evidence ce ON ce.contact_id = tm.dup_id
    ON CONFLICT (contact_id, kind, value) DO NOTHING
    RETURNING 1
  )
  -- Move affiliations
  ,m_aff AS (
    INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id, created_at)
    SELECT tm.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id, a.created_at
    FROM to_merge tm
    JOIN gold.contact_affiliation a ON a.contact_id = tm.dup_id
    ON CONFLICT (contact_id, company_id) DO UPDATE
      SET role      = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
          seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
          first_seen= LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
          last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
          active    = EXCLUDED.active
    RETURNING 1
  )
  -- Finally, delete the duplicate contact rows
  DELETE FROM gold.contact c
  USING to_merge tm
  WHERE c.contact_id = tm.dup_id;
END
$$;

-- Drop any old unique index name that might conflict or use a different expression
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND tablename='contact' AND indexname='ux_contact_primary_email_lower'
  ) THEN
    EXECUTE 'DROP INDEX IF EXISTS gold.ux_contact_primary_email_lower';
  END IF;
END
$$;

-- Unique functional index for email identity (case/space-insensitive)
CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_email_lower_idx
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

-- Non-unique helper index for common lookups
CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
ON gold.contact_evidence (kind, value);

-- -----------------------------------------------------------------------------
-- gold.contact_affiliation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.contact_affiliation (
  contact_id  uuid   NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  company_id  bigint NOT NULL,
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

-- -----------------------------------------------------------------------------
-- Conditionally attach FK to gold.company (NOT VALID) if table exists
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='gold' AND table_name='company'
  ) THEN
    BEGIN
      ALTER TABLE gold.contact
        ADD CONSTRAINT fk_contact_company
        FOREIGN KEY (primary_company_id)
        REFERENCES gold.company(company_id)
        ON DELETE SET NULL
        NOT VALID;
    EXCEPTION
      WHEN duplicate_object THEN
        -- FK already present; do nothing
        NULL;
    END;

    BEGIN
      ALTER TABLE gold.contact_affiliation
        ADD CONSTRAINT fk_contact_affil_company
        FOREIGN KEY (company_id)
        REFERENCES gold.company(company_id)
        ON DELETE CASCADE
        NOT VALID;
    EXCEPTION
      WHEN duplicate_object THEN
        NULL;
    END;
  END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- Optional hygiene: drop truly empty shell contacts (no identity or signals)
-- Keep rows that have any alias/evidence/affiliation.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  DELETE FROM gold.contact c
  WHERE c.primary_email IS NULL
    AND c.full_name IS NULL
    AND c.primary_phone IS NULL
    AND c.title_raw IS NULL
    AND c.generic_email IS NULL
    AND COALESCE(c.primary_linkedin_slug,'') = ''
    AND c.country_guess IS NULL
    AND c.region_guess IS NULL
    AND c.city_guess IS NULL
    AND NOT EXISTS (SELECT 1 FROM gold.contact_alias a WHERE a.contact_id = c.contact_id)
    AND NOT EXISTS (SELECT 1 FROM gold.contact_evidence e WHERE e.contact_id = c.contact_id)
    AND NOT EXISTS (SELECT 1 FROM gold.contact_affiliation af WHERE af.contact_id = c.contact_id);
END
$$;
