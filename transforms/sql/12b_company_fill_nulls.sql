BEGIN;

WITH c AS (SELECT company_id, website_domain FROM gold.company),
su AS (
  SELECT
    util.org_domain(NULLIF(company_domain,'')) AS org_root,
    company_description_raw, company_size_raw, company_industry_raw, company_logo_url,
    date_posted
  FROM silver.unified
),
donor AS (
  SELECT
    c.company_id, s.*
  FROM c
  JOIN su s
    ON c.website_domain IS NOT NULL
   AND s.org_root IS NOT NULL
   AND util.same_org_domain(c.website_domain, s.org_root)
),
best AS (
  SELECT DISTINCT ON (company_id)
    company_id, company_description_raw, company_size_raw, company_industry_raw, company_logo_url
  FROM donor
  ORDER BY company_id,
           (company_description_raw IS NOT NULL) DESC,
           (company_size_raw IS NOT NULL) DESC,
           (company_industry_raw IS NOT NULL) DESC,
           (company_logo_url IS NOT NULL) DESC,
           date_posted DESC NULLS LAST
)
UPDATE gold.company gc
SET description  = COALESCE(gc.description,  b.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     b.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, b.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     b.company_logo_url)
FROM best b
WHERE gc.company_id = b.company_id;

COMMIT;
