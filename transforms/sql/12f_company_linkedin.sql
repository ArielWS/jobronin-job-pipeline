-- transforms/sql/12f_company_linkedin.sql
-- Extract LinkedIn company slugs from source tables/views and update gold.company.linkedin_slug.
-- Idempotent and safe to re-run.

BEGIN;

WITH linkedin_sources AS (
  -- JobSpy silver view
  SELECT
    js.company_name        AS company_name,
    COALESCE(js.company_linkedin_url, js.company_website) AS linkedin_url
  FROM silver.jobspy js
  WHERE COALESCE(js.company_linkedin_url, js.company_website) ILIKE '%linkedin.com/company/%'

  UNION ALL
  -- StepStone silver view
  SELECT
    ss.company_name        AS company_name,
    COALESCE(ss.company_linkedin_url, ss.company_website) AS linkedin_url
  FROM silver.stepstone ss
  WHERE COALESCE(ss.company_linkedin_url, ss.company_website) ILIKE '%linkedin.com/company/%'

  UNION ALL
  -- Profesia.sk silver view
  SELECT
    pk.company_name          AS company_name,
    COALESCE(pk.company_linkedin_url, pk.company_website) AS linkedin_url
  FROM silver.profesia_sk pk
  WHERE COALESCE(pk.company_linkedin_url, pk.company_website) ILIKE '%linkedin.com/company/%'
),
slugs AS (
  SELECT
    util.company_name_norm(company_name) AS name_norm,
    lower(
      regexp_replace(
        linkedin_url,
        E'^https?://[^/]*linkedin\\.com/company/([^/?#]+).*',
        E'\\1'
      )
    ) AS slug
  FROM linkedin_sources
  WHERE company_name IS NOT NULL
    AND linkedin_url IS NOT NULL
),
uniq_slugs AS (
  SELECT
    name_norm,
    MIN(slug) AS slug,
    ARRAY_AGG(DISTINCT slug ORDER BY slug) AS slugs
  FROM slugs
  WHERE slug IS NOT NULL
  GROUP BY name_norm
),
updated AS (
  UPDATE gold.company gc
  SET linkedin_slug = us.slug
  FROM uniq_slugs us
  WHERE gc.name_norm = us.name_norm
    AND gc.linkedin_slug IS DISTINCT FROM us.slug
  RETURNING 1
)
SELECT name_norm, slugs
FROM uniq_slugs
WHERE array_length(slugs, 1) > 1;

COMMIT;
