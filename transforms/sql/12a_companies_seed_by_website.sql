WITH cand AS (
  SELECT DISTINCT
    company_name,
    util.company_name_norm(company_name) AS name_norm,
    NULLIF(company_domain,'')            AS website_root
  FROM silver.unified
  WHERE company_name IS NOT NULL
    AND btrim(company_name) <> ''
    AND util.company_name_norm(company_name) IS NOT NULL
)
INSERT INTO gold.company (name, website_domain)
SELECT cnd.company_name, cnd.website_root
FROM cand cnd
WHERE NOT EXISTS (
  SELECT 1
  FROM gold.company gc
  WHERE
    (cnd.website_root IS NOT NULL AND util.same_org_domain(gc.website_domain, cnd.website_root))
    OR (util.company_name_norm(gc.name) = cnd.name_norm)
);
