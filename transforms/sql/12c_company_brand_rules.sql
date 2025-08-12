BEGIN;
CREATE TABLE IF NOT EXISTS gold.company_brand_rule (
  domain_root TEXT NOT NULL,
  brand_regex TEXT NOT NULL,   -- POSIX/PG regex on normalized name or aliases
  brand_key   TEXT NOT NULL,
  active      BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (domain_root, brand_key)
);

-- Seed: keep AWS distinct from Amazon.com
INSERT INTO gold.company_brand_rule(domain_root, brand_regex, brand_key, active)
VALUES ('amazon.com', '\baws\b', 'aws', TRUE)
ON CONFLICT (domain_root, brand_key) DO UPDATE
SET brand_regex = EXCLUDED.brand_regex, active = EXCLUDED.active;

COMMIT;
