CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.company (
  company_id     BIGSERIAL PRIMARY KEY,
  name           TEXT NOT NULL,
  name_norm      TEXT GENERATED ALWAYS AS (lower(btrim(name))) STORED,
  website_domain TEXT,
  created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS company_name_norm_uidx
  ON gold.company(name_norm);
