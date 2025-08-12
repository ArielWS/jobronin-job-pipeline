-- transforms/sql/12c_company_brand_rules.sql
BEGIN;

CREATE TABLE IF NOT EXISTS gold.company_brand_rule (
  domain_root  TEXT NOT NULL,
  brand_regex  TEXT NOT NULL,
  brand_key    TEXT NOT NULL,
  active       BOOLEAN NOT NULL DEFAULT TRUE,
  created_at   TIMESTAMPTZ DEFAULT now(),
  updated_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY(domain_root, brand_key)
);

-- DACH + V4 relevant brand families (expand as needed)
INSERT INTO gold.company_brand_rule (domain_root, brand_regex, brand_key, active)
VALUES
  -- Amazon
  ('amazon.com',   '(amazon web services|^aws$|[^a-z]aws[^a-z]?)', 'aws', TRUE),

  -- Google
  ('google.com',   '(^google cloud$|google cloud|^gcp$|[^a-z]gcp[^a-z]?)', 'gcp', TRUE),

  -- Microsoft
  ('microsoft.com','(^azure$|microsoft azure|[^a-z]azure[^a-z]?)', 'azure', TRUE),

  -- IBM
  ('ibm.com',      '(^ibm ix$|[^a-z]ibm[ -]?ix[^a-z]?)', 'ibm_ix', TRUE),

  -- Siemens (DE)
  ('siemens.com',  '(^advanta$|siemens advanta|[^a-z]advanta[^a-z]?)', 'advanta', TRUE),

  -- Allianz (DE)
  ('allianz.com',  '(^allianz technology$|allianz tech)', 'allianz_technology', TRUE)

ON CONFLICT (domain_root, brand_key)
DO UPDATE SET
  brand_regex = EXCLUDED.brand_regex,
  active      = EXCLUDED.active,
  updated_at  = now();

COMMIT;
