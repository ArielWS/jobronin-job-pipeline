WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    util.org_domain(NULLIF(s.company_domain,'')) AS website_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
),
filtered AS (
  SELECT * FROM cand
  WHERE website_root IS NOT NULL
    AND NOT util.is_aggregator_host(website_root)
    AND NOT util.is_ats_host(website_root)
)
INSERT INTO gold.company (name, website_domain)
SELECT f.company_name, f.website_root
FROM filtered f
WHERE NOT EXISTS (
  SELECT 1 FROM gold.company gc
  WHERE gc.name_norm = f.name_norm
     OR (gc.website_domain IS NOT NULL AND util.same_org_domain(gc.website_domain, f.website_root))
);
