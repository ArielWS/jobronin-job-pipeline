WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name)                 AS name_norm,
    util.org_domain(NULLIF(s.company_domain,''))           AS website_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
filtered AS (
  SELECT *
  FROM cand
  WHERE website_root IS NOT NULL
    AND NOT util.is_aggregator_host(website_root)
    AND NOT util.is_ats_host(website_root)
),
picked AS (
  SELECT DISTINCT ON (name_norm) company_name, name_norm, website_root
  FROM filtered
  ORDER BY name_norm, website_root
)
INSERT INTO gold.company (name, website_domain)
SELECT company_name, website_root
FROM picked
ON CONFLICT (website_domain) DO UPDATE
SET name = CASE
             -- if the existing row has a placeholder name, upgrade it
             WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
             ELSE gold.company.name
           END
-- also tolerate any other uniqueness collisions
RETURNING 1;
