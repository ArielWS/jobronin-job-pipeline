-- transforms/sql/12b_company_fill_nulls.sql
-- Legacy backfill for profile fields; can be removed once existing rows are migrated.
BEGIN;

UPDATE gold.company gc
SET description  = COALESCE(gc.description,  su.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     su.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, su.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     su.company_logo_url)
FROM (
  SELECT
    util.org_domain(NULLIF(company_domain,'')) AS org_root,
    util.company_name_norm(company_name)       AS name_norm,
    company_description_raw,
    company_size_raw,
    company_industry_raw,
    company_logo_url
  FROM silver.unified
) su
WHERE (gc.description IS NULL OR gc.size_raw IS NULL OR gc.industry_raw IS NULL OR gc.logo_url IS NULL)
  AND (
    (gc.website_domain IS NOT NULL AND su.org_root IS NOT NULL AND util.same_org_domain(gc.website_domain, su.org_root))
    OR (gc.website_domain IS NULL AND gc.name_norm = su.name_norm)
  );

COMMIT;
