WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name)           AS name_norm,
    util.org_domain(NULLIF(s.company_domain,''))     AS website_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
picked AS (
  SELECT DISTINCT ON (name_norm) company_name, name_norm, website_root
  FROM cand
  ORDER BY name_norm, website_root
)
INSERT INTO gold.company (name)
SELECT company_name
FROM picked
WHERE NOT EXISTS (
  SELECT 1 FROM gold.company gc
  WHERE picked.website_root IS NOT NULL
    AND gc.website_domain IS NOT NULL
    AND util.same_org_domain(gc.website_domain, picked.website_root)
)
ON CONFLICT (name_norm) DO NOTHING;
