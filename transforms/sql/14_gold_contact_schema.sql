-- 14_gold_contact_schema.sql
-- GOLD contacts: schema, safety cleanup, and indexes.
-- Idempotent. Does NOT write to generated columns. Uses expression indexes.

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
-- Note: We don't force column generation modes here. If columns already exist
-- as GENERATED in your DB, this CREATE IF NOT EXISTS will not alter them.

-- gold.contact
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name              text,
  name_norm              text,           -- treat as derived/readonly from ETL perspective
  primary_email          text,
  primary_phone          text,
  primary_linkedin_slug  text,
  title_raw              text,
  primary_company_id     bigint REFERENCES gold.company(company_id) ON DELETE SET NULL,
  generic_email          text,
  -- optional geo hints
  country_guess          text,
  region_guess           text,
  city_guess             text,
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now()
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
-- Legacy cleanup: remove any GLOBAL uniques on contact_evidence.value
-- (we only want uniqueness per contact_id+kind+value)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  conname text;
  idxname text;
BEGIN
  -- Drop named legacy unique constraint if present
  IF EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold'
      AND rel.relname='contact_evidence'
      AND con.contype='u'
      AND con.conname='ux_contact_evidence_email_global'
  ) THEN
    EXECUTE 'ALTER TABLE gold.contact_evidence DROP CONSTRAINT ux_contact_evidence_email_global';
  END IF;

  -- Drop any UNIQUE indexes on (value) or lower(value)
  FOR idxname IN
    SELECT i.indexname
    FROM pg_indexes i
    WHERE i.schemaname='gold'
      AND i.tablename='contact_evidence'
      AND i.indexdef ILIKE 'CREATE UNIQUE INDEX%'
      AND (
           i.indexdef ILIKE '%(value)%'
        OR i.indexdef ILIKE '%lower(value)%'
        OR i.indexdef ILIKE '%lower((value))%'
      )
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS gold.%I', idxname);
  END LOOP;
END$$;

-- Helpful non-unique index for evidence lookups
CREATE INDEX IF NOT EXISTS ix_contact_evidence_kind_value
  ON gold.contact_evidence (kind, value);

-- -----------------------------------------------------------------------------
-- DATA CLEANUP (safe & idempotent)
-- -----------------------------------------------------------------------------
-- 1) Normalize stored emails (lower + trim). Never touch *_lower generated columns.
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

-- 2) Demote generic primary emails to generic_email (avoid using generics as identity)
--    Only demote if generic AND generic_email is currently NULL.
UPDATE gold.contact c
SET generic_email = COALESCE(c.generic_email, c.primary_email),
    primary_email = NULL,
    updated_at    = now()
WHERE c.primary_email IS NOT NULL
  AND (
        util.is_generic_email_domain(util.email_domain(c.primary_email))
        OR util.is_generic_mailbox(c.primary_email)
      )
  AND c.generic_email IS NULL;

-- 3) Merge duplicate contacts by NON-generic canonical email
--    (After step 2, remaining primary_email should be non-generic.)
WITH canon AS (
  SELECT
    contact_id,
    lower(btrim(primary_email)) AS email_key,
    (primary_email IS NOT NULL)::int AS has_email,
    (primary_phone IS NOT NULL)::int AS has_phone,
    (title_raw IS NOT NULL)::int    AS has_title,
    (full_name IS NOT NULL)::int    AS has_name,
    (primary_company_id IS NOT NULL)::int AS has_company,
    created_at, updated_at
  FROM gold.contact
  WHERE primary_email IS NOT NULL
),
groups AS (
  SELECT email_key, COUNT(*) AS cnt
  FROM canon
  GROUP BY email_key
  HAVING COUNT(*) > 1
),
winners AS (
  -- pick best row per email_key deterministically
  SELECT DISTINCT ON (c.email_key)
         c.email_key, c.contact_id AS keep_id
  FROM canon c
  JOIN groups g ON g.email_key = c.email_key
  ORDER BY
    c.email_key,
    -- prefer more complete records
    (c.has_name + c.has_title + c.has_phone + c.has_company + c.has_email) DESC,
    c.updated_at DESC,
    c.created_at ASC
),
losers AS (
  SELECT c.email_key, c.contact_id AS dup_id, w.keep_id
  FROM canon c
  JOIN winners w ON w.email_key = c.email_key
  WHERE c.contact_id <> w.keep_id
)
-- move aliases
, move_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias)
  SELECT l.keep_id, ca.alias
  FROM losers l
  JOIN gold.contact_alias ca ON ca.contact_id = l.dup_id
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING contact_id
)
-- move evidence
, move_ev AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT l.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail
  FROM losers l
  JOIN gold.contact_evidence ce ON ce.contact_id = l.dup_id
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
)
-- move affiliations
, move_aff AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT l.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id
  FROM losers l
  JOIN gold.contact_affiliation a ON a.contact_id = l.dup_id
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen  = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active     = EXCLUDED.active
  RETURNING contact_id
)
-- enrich winner from loser if winner fields are NULL
, enrich_winner AS (
  UPDATE gold.contact keep
  SET
    full_name          = COALESCE(keep.full_name, dup.full_name),
    primary_phone      = COALESCE(keep.primary_phone, dup.primary_phone),
    title_raw          = COALESCE(keep.title_raw, dup.title_raw),
    primary_company_id = COALESCE(keep.primary_company_id, dup.primary_company_id),
    generic_email      = COALESCE(keep.generic_email, dup.generic_email),
    updated_at         = now()
  FROM losers l
  JOIN gold.contact dup ON dup.contact_id = l.dup_id
  WHERE keep.contact_id = l.keep_id
  RETURNING keep.contact_id
)
DELETE FROM gold.contact c
USING losers l
WHERE c.contact_id = l.dup_id;

-- 4) Remove shell rows: no anchors and no evidence/affiliation
DELETE FROM gold.contact c
WHERE COALESCE(c.primary_email,'') = ''
  AND COALESCE(c.primary_phone,'') = ''
  AND COALESCE(c.primary_linkedin_slug,'') = ''
  AND COALESCE(c.name_norm,'') IS NULL
  AND COALESCE(c.full_name,'') IS NULL
  AND COALESCE(c.generic_email,'') = ''
  AND NOT EXISTS (SELECT 1 FROM gold.contact_evidence e WHERE e.contact_id = c.contact_id)
  AND NOT EXISTS (SELECT 1 FROM gold.contact_affiliation a WHERE a.contact_id = c.contact_id);

-- -----------------------------------------------------------------------------
-- Indexes (expression-based; do not rely on *_lower columns)
-- -----------------------------------------------------------------------------

-- Helpful lookups
CREATE INDEX IF NOT EXISTS ix_contact_company_id
  ON gold.contact (primary_company_id);

CREATE INDEX IF NOT EXISTS ix_contact_name_company_noemail
  ON gold.contact (name_norm, primary_company_id)
  WHERE primary_email IS NULL;

-- Generic email lookup (expression)
CREATE INDEX IF NOT EXISTS ix_contact_generic_email_expr
  ON gold.contact ((lower(btrim(generic_email))))
  WHERE generic_email IS NOT NULL;

-- Primary email uniqueness (expression)
-- (Duplicates were removed above; if a different unique already exists, this will be skipped)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname='gold' AND tablename='contact'
      AND indexname='ux_contact_email_expr'
  ) THEN
    -- Will fail ONLY if duplicates still exist
    EXECUTE 'CREATE UNIQUE INDEX ux_contact_email_expr
             ON gold.contact ((lower(btrim(primary_email))))
             WHERE primary_email IS NOT NULL';
  END IF;
END$$;

-- Affiliation helper
CREATE INDEX IF NOT EXISTS ix_contact_affil_company
  ON gold.contact_affiliation (company_id);
