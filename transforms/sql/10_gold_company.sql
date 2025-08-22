-- transforms/sql/10_gold_company.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

-- Company
CREATE TABLE IF NOT EXISTS gold.company (
  company_id       BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL,
  name_norm        TEXT GENERATED ALWAYS AS (util.company_name_norm(name)) STORED,
  website_domain   TEXT,          -- canonical org root (no www)
  brand_key        TEXT,          -- optional brand splitter (e.g., 'aws' for amazon.com)
  linkedin_slug    TEXT,          -- linkedin.com/company/<slug>
  size_raw         TEXT,
  industry_raw     TEXT,
  description      TEXT,
  logo_url         TEXT,
  created_at       TIMESTAMPTZ DEFAULT now(),
  updated_at       TIMESTAMPTZ DEFAULT now()
);

-- Touch updated_at on UPDATE
CREATE OR REPLACE FUNCTION gold.touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS company_touch_upd ON gold.company;
CREATE TRIGGER company_touch_upd
BEFORE UPDATE ON gold.company
FOR EACH ROW EXECUTE FUNCTION gold.touch_updated_at();

-- Remove older unique indexes (avoid conflicts / ambiguity)
DROP INDEX IF EXISTS company_name_norm_uidx;
DROP INDEX IF EXISTS company_website_domain_uidx;
DROP INDEX IF EXISTS company_name_norm_expr_uidx;
DROP INDEX IF EXISTS company_name_norm_uniq_idx;

-- (A) Unique per (website_domain, brand_key) when website_domain is known
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND indexname='company_domain_brand_uniq_idx'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX company_domain_brand_uniq_idx
             ON gold.company(website_domain, brand_key)
             WHERE website_domain IS NOT NULL';
  END IF;
END$$;

-- (B) Unique per website_domain when brand_key IS NULL
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND indexname='company_domain_nobrands_uniq_idx'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX company_domain_nobrands_uniq_idx
             ON gold.company(website_domain)
             WHERE brand_key IS NULL AND website_domain IS NOT NULL';
  END IF;
END$$;

-- (C) Unique LinkedIn identity if present
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND indexname='company_linkedin_slug_uidx'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX company_linkedin_slug_uidx
             ON gold.company(lower(linkedin_slug))
             WHERE linkedin_slug IS NOT NULL';
  END IF;
END$$;

-- Lookup/analytics helpers
CREATE INDEX IF NOT EXISTS company_name_norm_idx
  ON gold.company (name_norm);
CREATE INDEX IF NOT EXISTS company_name_norm_langless_idx
  ON gold.company (util.company_name_norm_langless(name));

-- Aliases
CREATE TABLE IF NOT EXISTS gold.company_alias (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  alias_norm TEXT GENERATED ALWAYS AS (util.company_name_norm(alias)) STORED,
  PRIMARY KEY (company_id, alias_norm)
);

-- Evidence (per-company PK)
CREATE TABLE IF NOT EXISTS gold.company_evidence_domain (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  kind       TEXT NOT NULL,  -- 'website' | 'email' | 'stepstone_id' | 'location'
  value      TEXT NOT NULL,  -- host/id/raw location string (store org roots for website/email)
  source     TEXT,
  source_id  TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Composite PK
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'gold.company_evidence_domain'::regclass
      AND contype = 'p'
  ) THEN
    ALTER TABLE gold.company_evidence_domain
      DROP CONSTRAINT IF EXISTS company_evidence_domain_pkey;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'gold.company_evidence_domain'::regclass
      AND contype = 'p'
  ) THEN
    ALTER TABLE gold.company_evidence_domain
      ADD CONSTRAINT company_evidence_domain_pk PRIMARY KEY (company_id, kind, value);
  END IF;
END$$;

-- Helpful index for lookups by evidence
CREATE INDEX IF NOT EXISTS company_evidence_kind_value_idx
  ON gold.company_evidence_domain (kind, lower(value));

COMMIT;
