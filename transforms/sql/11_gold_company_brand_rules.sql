-- transforms/sql/11_gold_company_brand_rules.sql
BEGIN;

CREATE SCHEMA IF NOT EXISTS gold;

-- Brand rules table (idempotent shape)
CREATE TABLE IF NOT EXISTS gold.company_brand_rule (
  domain_root  TEXT NOT NULL,
  brand_regex  TEXT NOT NULL,
  brand_key    TEXT NOT NULL,
  active       BOOLEAN NOT NULL DEFAULT TRUE
);

-- Add timestamps if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='company_brand_rule' AND column_name='created_at'
  ) THEN
    EXECUTE 'ALTER TABLE gold.company_brand_rule ADD COLUMN created_at TIMESTAMPTZ DEFAULT now()';
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='gold' AND table_name='company_brand_rule' AND column_name='updated_at'
  ) THEN
    EXECUTE 'ALTER TABLE gold.company_brand_rule ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now()';
  END IF;
END$$;

-- Ensure unique key for upserts
CREATE UNIQUE INDEX IF NOT EXISTS company_brand_rule_domain_brand_uidx
  ON gold.company_brand_rule(domain_root, brand_key);

-- Upsert brand families (DACH + V4 focus) + targeted adds (adesso, Neohunter)
INSERT INTO gold.company_brand_rule (domain_root, brand_regex, brand_key, active)
VALUES
  -- Amazon
  ('amazon.com',   '(amazon web services|^aws$|[^a-z]aws[^a-z]?)',              'aws',                   TRUE),

  -- Google
  ('google.com',   '(^google cloud$|google cloud|^gcp$|[^a-z]gcp[^a-z]?)',      'gcp',                   TRUE),

  -- Microsoft
  ('microsoft.com','(^azure$|microsoft azure|[^a-z]azure[^a-z]?)',              'azure',                 TRUE),

  -- IBM
  ('ibm.com',      '(^ibm ix$|[^a-z]ibm[ -]?ix[^a-z]?)',                         'ibm_ix',                TRUE),

  -- Siemens (DE)
  ('siemens.com',  '(^advanta$|siemens advanta|[^a-z]advanta[^a-z]?)',          'advanta',               TRUE),

  -- Allianz (DE)
  ('allianz.com',  '(^allianz technology$|allianz tech)',                        'allianz_technology',    TRUE),

  -- --------------------------
  -- Targeted adds (requested)
  -- --------------------------

  -- adesso group (treat variants as the same brand family "adesso")
  -- Covers "adesso", "adesso se", and "adesso business consulting"
  ('adesso.de',    '(^adesso$|^adesso se$|adesso business consulting)',          'adesso',                TRUE),
  ('adesso-bc.com','(^adesso$|^adesso se$|adesso business consulting)',          'adesso',                TRUE),

  -- Neohunter (Recruitment-as-a-Service) – canonical site is neohunter.io
  ('neohunter.io', '(^neohunter$|neohunter recruitment as a service|^neohunter raas$)', 'neohunter',     TRUE)

ON CONFLICT (domain_root, brand_key) DO UPDATE
SET brand_regex = EXCLUDED.brand_regex,
    active      = EXCLUDED.active,
    updated_at  = now();

COMMIT;
