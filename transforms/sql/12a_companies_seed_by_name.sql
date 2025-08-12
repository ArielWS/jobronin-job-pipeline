INSERT INTO gold.company (name)
SELECT DISTINCT s.company_name
FROM silver.unified s
WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
  AND util.company_name_norm(s.company_name) IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM gold.company gc WHERE gc.name_norm = util.company_name_norm(s.company_name)
  )
  AND NOT EXISTS (
    SELECT 1
    FROM gold.company gc
    JOIN LATERAL util.org_domain(NULLIF(s.company_domain,'')) od(root) ON TRUE
    WHERE od.root IS NOT NULL AND gc.website_domain IS NOT NULL
      AND util.same_org_domain(gc.website_domain, od.root)
  );
