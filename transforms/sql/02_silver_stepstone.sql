-- transforms/sql/02_silver_stepstone.sql
-- Silver view for StepStone → normalized common shape
-- Raw table: public.stepstone_job_scrape
-- JSON is sanitized via util.json_clean(text) → jsonb (handles NaN/Infinity/None → null)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.stepstone AS
WITH raw AS (
  SELECT
    ss.id::text        AS source_id,
    ss.client_name     AS client_name,
    ss.location        AS location_raw,
    ss.job_data        AS job_data_txt,
    ss.timestamp       AS ts_raw
  FROM public.stepstone_job_scrape ss
),
parsed AS (
  SELECT
    r.*,
    util.json_clean(r.job_data_txt) AS jd
  FROM raw r
),
fields AS (
  SELECT
    p.source_id,
    p.location_raw,
    p.ts_raw,
    p.jd,

    -- Company & title from multiple likely keys (string or object)
    btrim(NULLIF(COALESCE(
      p.jd #>> '{employer,name}',
      p.jd #>> '{company,name}',
      p.jd ->> 'companyName',
      p.jd ->> 'company'         -- plain string form
    ), '')) AS company_name_json,

    btrim(COALESCE(
      p.jd #>> '{job,title}',
      p.jd #>> '{header,title}',
      p.jd ->> 'title'
    )) AS title_raw,

    -- URLs: support snake_case and camelCase
    NULLIF(COALESCE(
      p.jd ->> 'job_url_direct',
      p.jd ->> 'job_url',
      p.jd ->> 'applyUrl',
      p.jd ->> 'applicationUrl',
      p.jd ->> 'jobUrl',
      p.jd ->> 'url'
    ), '') AS job_url_direct,

    NULLIF(COALESCE(
      p.jd #>> '{company,website}',
      p.jd #>> '{company,homepage}',
      p.jd ->> 'company_website',
      p.jd ->> 'company_homepage',
      p.jd ->> 'companyWebsite',
      p.jd ->> 'homepage',
      p.jd #>> '{employer,website}'
    ), '') AS company_website_raw,

    -- Try to find *any* email in the JSON blob for contact root
    util.first_email(p.jd::text) AS email_found,

    -- Enrichment bits commonly present in StepStone payloads (best-effort)
    COALESCE(p.jd #>> '{company,size}', p.jd ->> 'companySize')         AS company_size_raw,
    COALESCE(p.jd #>> '{company,industry}', p.jd #>> '{industry,name}') AS company_industry_raw,
    COALESCE(p.jd #>> '{company,logoUrl}', p.jd ->> 'companyLogoUrl')   AS company_logo_url,
    COALESCE(p.jd ->> 'companyDescription', p.jd #>> '{company,description}') AS company_description_raw
  FROM parsed p
),
norm AS (
  SELECT
    'stepstone'  AS source,
    f.source_id,
    -- If StepStone doesn't give a stable row URL, use the apply/job URL
    f.job_url_direct AS source_row_url,
    f.job_url_direct AS job_url_direct,

    f.title_raw,
    CASE WHEN f.title_raw IS NULL THEN NULL ELSE lower(btrim(f.title_raw)) END AS title_norm,

    -- Company: prefer JSON; else guess from title suffix (“ - WalkMe”, “ | WalkMe”, “ @ WalkMe”)
    f.company_name_json AS company_raw,
    CASE
      WHEN f.company_name_json IS NOT NULL
           AND NOT util.is_placeholder_company_name(f.company_name_json)
           AND util.company_name_norm(f.company_name_json) IS NOT NULL
        THEN f.company_name_json
      ELSE
        NULLIF(
          btrim(
            regexp_replace(
              coalesce(
                (regexp_match(coalesce(f.title_raw,''), '(?:\s[-–—|@]\s)([A-Za-z0-9&.\- ]{2,})$'))[1],
                ''
              ),
              '\s*\((?:m\/w\/d|w\/m\/d|mwd|m\/f\/d|f\/m\/d)\)\s*',
              '',
              'gi'
            )
          ),
          ''
        )
    END AS company_name,

    -- Location
    f.location_raw,
    NULLIF(split_part(f.location_raw, ', ', 1), '') AS city_guess,
    NULLIF(split_part(f.location_raw, ', ', 2), '') AS region_guess,
    NULL::text                                      AS country_guess,

    -- Date
    CASE WHEN f.ts_raw IS NOT NULL THEN f.ts_raw::timestamptz ELSE NULL END AS date_posted,

    -- Flags
    (f.location_raw ILIKE '%remote%' OR f.title_raw ILIKE '%remote%') AS is_remote,
    COALESCE(f.jd ->> 'contractType', f.jd #>> '{job,contractType}')  AS contract_type_raw,

    -- Pay (guard casts in case StepStone uses strings)
    CASE WHEN (f.jd ->> 'salaryMin') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (f.jd ->> 'salaryMin')::numeric ELSE NULL END AS salary_min,
    CASE WHEN (f.jd ->> 'salaryMax') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (f.jd ->> 'salaryMax')::numeric ELSE NULL END AS salary_max,
    COALESCE(f.jd ->> 'salaryCurrency', f.jd #>> '{salary,currency}') AS currency,

    -- Emails → domains
    f.email_found                                AS emails_raw,
    util.email_domain(f.email_found)             AS contact_email_domain,
    util.org_domain(util.email_domain(f.email_found)) AS contact_email_root,

    -- Apply
    util.url_host(f.job_url_direct)                   AS apply_domain,
    util.org_domain(util.url_host(f.job_url_direct))  AS apply_root,

    -- Company site (filter aggregators/ATS for company_domain only)
    f.company_website_raw,
    CASE
      WHEN util.url_host(f.company_website_raw) = 'linkedin.com' THEN f.company_website_raw
      ELSE NULL
    END                                              AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_website_raw))        THEN NULL
      ELSE f.company_website_raw
    END                                              AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_website_raw))        THEN NULL
      ELSE util.org_domain(util.url_host(f.company_website_raw))
    END                                              AS company_domain,

    -- Keep enrichment columns so CREATE OR REPLACE doesn’t try to drop them
    f.company_size_raw,
    f.company_industry_raw,
    f.company_logo_url,
    f.company_description_raw
  FROM fields f
),
-- Stronger junk filter:
-- 1) Must have a non-placeholder title
-- 2) Must have a usable company_name (after normalization) OR at least one link/email
keep AS (
  SELECT *
  FROM norm n
  WHERE
    NOT util.is_placeholder_company_name(n.title_raw)
    AND (
      util.company_name_norm(n.company_name) IS NOT NULL
      OR COALESCE(n.job_url_direct, n.company_website, n.emails_raw) IS NOT NULL
    )
)
SELECT
  source, source_id, source_row_url, job_url_direct,
  title_raw, title_norm,
  company_raw, company_name,
  location_raw, city_guess, region_guess, country_guess,
  date_posted, is_remote, contract_type_raw,
  salary_min, salary_max, currency,
  emails_raw, contact_email_domain, contact_email_root,
  apply_domain, apply_root,
  company_website_raw, company_linkedin_url, company_website, company_domain,
  company_size_raw, company_industry_raw, company_logo_url, company_description_raw
FROM keep;
