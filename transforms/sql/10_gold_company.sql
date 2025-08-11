CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.company (
  company_id       BIGSERIAL PRIMARY KEY,
  name             TEXT NOT NULL,
  name_norm        TEXT GENERATED ALWAYS AS (util.company_name_norm(name)) STORED,
  website_domain   TEXT,          -- canonical site host (no www)
  linkedin_slug    TEXT,          -- optional later (if you add it)
  size_raw         TEXT,
  industry_raw     TEXT,
  description      TEXT,
  logo_url         TEXT,
  created_at       TIMESTAMPTZ DEFAULT now(),
  updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS company_name_norm_uidx
  ON gold.company(name_norm);

CREATE UNIQUE INDEX IF NOT EXISTS company_website_domain_uidx
  ON gold.company(website_domain) WHERE website_domain IS NOT NULL;

CREATE TABLE IF NOT EXISTS gold.company_alias (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  alias      TEXT NOT NULL,
  alias_norm TEXT GENERATED ALWAYS AS (util.company_name_norm(alias)) STORED,
  PRIMARY KEY (company_id, alias_norm)
);

-- Evidence table: map observed domains/handles to a company (for audit and future merges)
CREATE TABLE IF NOT EXISTS gold.company_evidence_domain (
  company_id BIGINT NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  kind       TEXT NOT NULL,  -- 'website' | 'email' | 'apply' | 'ats_handle'
  value      TEXT NOT NULL,  -- host or handle
  source     TEXT,           -- jobspy/stepstone/etc
  source_id  TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (kind, value)
);
