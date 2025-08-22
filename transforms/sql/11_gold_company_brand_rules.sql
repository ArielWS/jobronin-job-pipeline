-- transforms/sql/11_gold_company_brand_rules.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

-- Families/brands that live under the same org root but should split in gold.company.
CREATE TABLE IF NOT EXISTS gold.company_brand_rule (
  domain_root  TEXT NOT NULL,
  brand_regex  TEXT NOT NULL,
  brand_key    TEXT NOT NULL,
  active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at   TIMESTAMPTZ DEFAULT now(),
  updated_at   TIMESTAMPTZ DEFAULT now()
);

-- Ensure unique key for safe upserts
CREATE UNIQUE INDEX IF NOT EXISTS company_brand_rule_domain_brand_uidx
  ON gold.company_brand_rule(domain_root, brand_key);

-- Seed / upsert DACH+V4-relevant rules (extend as needed)
INSERT INTO gold.company_brand_rule (domain_root, brand_regex, brand_key, active)
VALUES
  -- Amazon
  ('amazon.com',   '(amazon web services|^aws$|[^a-z]aws[^a-z]?)', 'aws', TRUE),

  -- Google
  ('google.com',   '(^google cloud$|google cloud|^gcp$|[^a-z]gcp[^a-z]?)', 'gcp', TRUE),

  -- Microsoft
  ('microsoft.com','(^azure$|microsoft azure|[^a-z]azure[^a-z]?)', 'azure', TRUE),

  -- IBM
  ('ibm.com',      '(^ibm ix$|[^a-z]ibm[ -]?ix[^a-z]?)',            'ibm_ix', TRUE),

  -- Siemens (DE)
  ('siemens.com',  '(^advanta$|siemens advanta|[^a-z]advanta[^a-z]?)', 'advanta', TRUE),

  -- Allianz (DE)
  ('allianz.com',  '(^allianz technology$|allianz tech)',           'allianz_technology', TRUE)
ON CONFLICT (domain_root, brand_key) DO UPDATE
SET brand_regex = EXCLUDED.brand_regex,
    active      = EXCLUDED.active,
    updated_at  = now();

COMMIT;
