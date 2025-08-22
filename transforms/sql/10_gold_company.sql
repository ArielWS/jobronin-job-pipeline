-- transforms/sql/10_gold_company.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

-- ====================================================================
-- gold.company
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.company (
  company_id     BIGSERIAL PRIMARY KEY,
  name           TEXT NOT NULL,
  name_norm      TEXT GENERATED ALWAYS AS (util.company_name_norm(name)) STORED,
  website_domain TEXT,          -- canonical org root (lowercase, no www)
  brand_key      TEXT,          -- optional brand splitter (e.g., 'aws' for amazon.com)
  linkedin_slug  TEXT,
  size_raw       TEXT,
  industry_raw   TEXT,
  description    TEXT,
  logo_url       TEXT,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now()
);

-- touch updated_at
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

-- ====================================================================
-- Uniqueness & hygiene
--   - Allow multiple homonyms once a domain is known
--   - Strong identity by (website_domain, brand_key)
--   - Enforce lowercase on website_domain
-- ====================================================================

-- drop legacy name unique, if present
ALTER TABLE gold.company DROP CONSTRAINT IF EXISTS company_name_norm_uniq;

-- drop legacy indexes, if present
DROP INDEX IF EXISTS company_name_norm_uidx;
DROP INDEX IF EXISTS company_website_domain_uidx;
DROP INDEX IF EXISTS company_name_norm_expr_uidx;
DROP INDEX IF EXISTS company_name_norm_uniq_idx;

-- enforce lowercase domain
ALTER TABLE gold.company DROP CONSTRAINT IF EXISTS company_website_domain_lower;
ALTER TABLE gold.company
  ADD CONSTRAINT company_website_domain_lower
  CHECK (website_domain IS NULL OR website_domain = lower(website_domain));

-- one placeholder row per normalized name while domain is unknown
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND indexname='company_name_norm_placeholder_uidx'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX company_name_norm_placeholder_uidx
             ON gold.company (name_norm)
             WHERE website_domain IS NULL';
  END IF;
END$$;

-- one row per (website_domain, brand_key) when domain is known
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

-- one row per website_domain when brand_key IS NULL
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='gold' AND indexname='company_domain_nobrands_uniq_idx'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX company_domain_nobrands_uniq_idx
             ON gold.company(website_domain)
             WHERE brand_key IS NULL';
  END IF;
END$$;

-- helpful search indexes
CREATE INDEX IF NOT EXISTS company_name_norm_langless_idx
  ON gold.company (util.company_name_norm_langless(name));
CREATE INDEX IF NOT EXISTS company_domain_idx
  ON gold.company (website_domain);
CREATE INDEX IF NOT EXISTS company_brand_key_idx
  ON gold.company (brand_key);

-- ====================================================================
-- gold.company_alias
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.company_alias (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  alias_norm TEXT GENERATED ALWAYS AS (util.company_name_norm(alias)) STORED,
  PRIMARY KEY (company_id, alias_norm)
);

-- ====================================================================
-- gold.company_evidence_domain
-- ====================================================================
CREATE TABLE IF NOT EXISTS gold.company_evidence_domain (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  kind       TEXT NOT NULL,  -- 'website' | 'email' | 'ats_handle'
  value      TEXT NOT NULL,  -- org root for website/email, lowercase
  source     TEXT,
  source_id  TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (company_id, kind, value)
);

CREATE INDEX IF NOT EXISTS company_evidence_domain_company_idx
  ON gold.company_evidence_domain (company_id);
CREATE INDEX IF NOT EXISTS company_evidence_domain_kind_value_idx
  ON gold.company_evidence_domain (kind, value);

COMMIT;
