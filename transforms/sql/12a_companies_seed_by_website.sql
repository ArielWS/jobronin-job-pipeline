WITH cand AS (
  SELECT DISTINCT
    company_name,
    util.company_name_norm(company_name) AS name_norm,
    NULLIF(company_domain,'') AS website_domain
  FROM silver.unified
  WHERE company_name IS NOT NULL AND btrim(company_name) <> ''
)
INSERT INTO gold.company (name, website_domain)
SELECT c.company_name, c.website_domain
FROM cand c
LEFT JOIN gold.company gc
  ON (c.website_domain IS NOT NULL AND gc.website_domain = c.website_domain)
  OR (c.website_domain IS NULL AND gc.name_norm = c.name_norm)
WHERE gc.company_id IS NULL;
