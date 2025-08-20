-- transforms/sql/12f_company_linkedin.sql
-- Extract LinkedIn company slugs from raw source tables and update gold.company.linkedin_slug.
-- Idempotent and safe to re-run.

BEGIN;

WITH linkedin_sources AS (
  -- JobSpy raw table
  SELECT
    js.company               AS company_name,
    COALESCE(js.company_url_direct, js.company_url) AS linkedin_url
  FROM public.jobspy_job_scrape js
  WHERE COALESCE(js.company_url_direct, js.company_url) ILIKE '%linkedin.com/company/%'

  UNION ALL
  -- StepStone raw table
  SELECT
    ss.company_name          AS company_name,
    ss.company_website_raw   AS linkedin_url
  FROM public.stepstone_job_scrape ss
  WHERE ss.company_website_raw ILIKE '%linkedin.com/company/%'

  UNION ALL
  -- Profesia.sk raw table
  SELECT
    pk.company_name          AS company_name,
    pk.company_website       AS linkedin_url
  FROM public.profesia_sk_job_scrape pk
  WHERE pk.company_website ILIKE '%linkedin.com/company/%'
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
  SELECT DISTINCT name_norm, slug FROM slugs WHERE slug IS NOT NULL
)
UPDATE gold.company gc
SET linkedin_slug = us.slug
FROM uniq_slugs us
WHERE gc.name_norm = us.name_norm
  AND gc.linkedin_slug IS DISTINCT FROM us.slug;

COMMIT;
