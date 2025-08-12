WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    util.org_domain(NULLIF(s.company_domain,'')) AS website_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
),
filtered AS (
  SELECT *
  FROM cand
  WHERE website_root IS NOT NULL
    AND NOT util.is_aggregator_host(website_root)
    AND NOT util.is_ats_host(website_root)
),
picked AS (
  -- one candidate per name_norm (choose deterministically)
  SELECT DISTINCT ON (name_norm)
         company_name, name_norm, website_root
  FROM filtered
  ORDER BY name_norm, website_root
)
INSERT INTO gold.company (name, website_domain)
SELECT company_name, website_root
FROM picked
ON CONFLICT (name_norm) DO NOTHING
ON CONFLICT ON CONSTRAINT company_website_domain_uidx DO NOTHING;
