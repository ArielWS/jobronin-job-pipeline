-- transforms/sql/12c_company_brand_rules.sql
BEGIN;

-- Create table if missing (older versions might lack timestamps)
CREATE TABLE IF NOT EXISTS gold.company_brand_rule (
  domain_root  TEXT NOT NULL,
  brand_regex  TEXT NOT NULL,
  brand_key    TEXT NOT NULL,
  active       BOOLEAN NOT NULL DEFAULT TRUE
);

-- Add timestamps if they don't exist yet
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

-- Ensure a unique key so ON CONFLICT works
CREATE UNIQUE INDEX IF NOT EXISTS company_brand_rule_domain_brand_uidx
  ON gold.company_brand_rule(domain_root, brand_key);

-- Upsert DACH/V4-relevant brand families
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
