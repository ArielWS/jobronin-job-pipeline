-- transforms/sql/01_silver_jobspy.sql
-- Silver view for JobSpy → normalized common shape
-- Source (Bronze): public.jobspy_job_scrape
-- Depends on: util.url_canonical, util.url_host, util.org_domain, util.location_parse,
--             util.first_email, util.email_domain, util.is_generic_email_domain,
--             util.is_aggregator_host, util.is_ats_host, util.is_career_host,
--             util.company_name_norm, util.company_name_norm_langless

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.jobspy AS
WITH src AS (
  SELECT
    js.id::text                       AS source_id,
    NULLIF(js.site, '')               AS source_site,

    -- URLs
    NULLIF(js.job_url, '')            AS job_url_raw,
    NULLIF(js.job_url_direct, '')     AS apply_url_raw,
    NULLIF(js.company_url_direct, '') AS company_url_direct_raw,
    NULLIF(js.company_url, '')        AS company_url_raw,

    -- Core job fields
    NULLIF(js.title, '')              AS title_raw,
    NULLIF(js.job_type, '')           AS contract_type_raw,
    NULLIF(js.company, '')            AS company_raw,

    -- Content
    NULLIF(js.description, '')        AS description_raw,

    -- Company enrich
    NULLIF(js.company_industry, '')   AS company_industry_raw,
    NULLIF(js.company_logo, '')       AS company_logo_url,
    NULLIF(js.company_description,'') AS company_description_raw,
    NULLIF(js.company_addresses,'')   AS company_location_raw,

    -- Contacts
    NULLIF(js.emails, '')             AS emails_raw,

    -- Location
    NULLIF(js.location, '')           AS location_raw,

    -- Dates
    js.date_posted                    AS date_posted,   -- keep as DATE
    js."time_stamp"                   AS scraped_at,    -- timestamptz

    -- Flags
    js.is_remote                      AS is_remote_raw, -- already boolean

    -- Compensation
    js.min_amount                     AS salary_min_raw,
    js.max_amount                     AS salary_max_raw,
    NULLIF(js.currency, '')           AS currency_raw,
    NULLIF(js."interval", '')         AS salary_interval_raw
  FROM public.jobspy_job_scrape js
),
norm AS (
  SELECT
    'jobspy'::text                                 AS source,
    s.source_site,
    s.source_id,
    -- canonical pointer back to the source listing (prefer the listing URL)
    util.url_canonical(COALESCE(s.job_url_raw, s.apply_url_raw)) AS source_row_url,
    s.scraped_at,
    s.date_posted,

    -- Titles
    s.title_raw,
    NULL::text                                     AS title_norm,

    -- Company names (normalized)
    s.company_raw,
    util.company_name_norm_langless(s.company_raw) AS company_name_norm_langless,
    util.company_name_norm(s.company_raw)          AS company_name_norm,

    -- Content
    s.description_raw,

    -- Location
    s.location_raw,
    lp.city                                        AS city_guess,
    lp.region                                      AS region_guess,
    lp.country                                     AS country_guess,

    -- Job meta
    s.contract_type_raw,
    s.is_remote_raw                                 AS is_remote,

    -- Compensation
    s.salary_min_raw::numeric                      AS salary_min,
    s.salary_max_raw::numeric                      AS salary_max,
    s.currency_raw                                 AS currency,
    CASE
      WHEN s.salary_interval_raw ILIKE 'hour%'  THEN 'hourly'
      WHEN s.salary_interval_raw ILIKE 'day%'   THEN 'daily'
      WHEN s.salary_interval_raw ILIKE 'week%'  THEN 'weekly'
      WHEN s.salary_interval_raw ILIKE 'month%' THEN 'monthly'
      WHEN s.salary_interval_raw ILIKE 'year%'  THEN 'yearly'
      ELSE NULL
    END                                            AS salary_interval,

    -- Listing URL (raw + canonical + helper id)
    s.job_url_raw                                  AS job_url_raw,
    util.url_canonical(s.job_url_raw)              AS job_url_canonical,
    (regexp_match(coalesce(util.url_canonical(s.job_url_raw),''), '/jobs/view/([0-9]+)'))[1] AS linkedin_job_id,

    -- Apply URL: prefer direct; canonical + domains
    COALESCE(s.apply_url_raw, s.job_url_raw)       AS apply_url_raw,
    util.url_canonical(COALESCE(s.apply_url_raw, s.job_url_raw)) AS apply_url_canonical,
    util.url_host(util.url_canonical(COALESCE(s.apply_url_raw, s.job_url_raw))) AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(COALESCE(s.apply_url_raw, s.job_url_raw)))) AS apply_root,

    -- Company website: prefer direct; canonicalize; filter ATS/aggregator/career hosts
    COALESCE(s.company_url_direct_raw, s.company_url_raw) AS company_website_raw,
    util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw)) AS company_website_canonical,
    CASE
      WHEN util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))) = 'linkedin.com'
        THEN util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))
      ELSE NULL
    END                                            AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw)))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))))     THEN NULL
      ELSE util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))
    END                                            AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw)))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))))     THEN NULL
      ELSE util.org_domain(util.url_host(util.url_canonical(COALESCE(s.company_url_direct_raw, s.company_url_raw))))
    END                                            AS company_domain,

    -- Email → contact evidence
    s.emails_raw,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(s.emails_raw))) THEN NULL
      ELSE util.email_domain(util.first_email(s.emails_raw))
    END                                            AS contact_email_domain,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(s.emails_raw))) THEN NULL
      ELSE util.org_domain(util.email_domain(util.first_email(s.emails_raw)))
    END                                            AS contact_email_root,

    -- Company passthroughs
    s.company_industry_raw,
    s.company_logo_url,
    s.company_description_raw,
    s.company_location_raw

  FROM src s
  LEFT JOIN LATERAL util.location_parse(s.location_raw) lp ON TRUE
)
SELECT
  source,
  source_site,
  source_id,
  source_row_url,
  scraped_at,
  date_posted,

  title_raw,
  title_norm,

  company_raw,
  company_name_norm_langless,
  company_name_norm,

  description_raw,

  location_raw,
  city_guess,
  region_guess,
  country_guess,

  contract_type_raw,
  is_remote,

  salary_min,
  salary_max,
  currency,
  salary_interval,

  job_url_raw,
  job_url_canonical,
  linkedin_job_id,

  apply_url_raw,
  apply_url_canonical,
  apply_domain,
  apply_root,

  company_website_raw,
  company_website_canonical,
  company_linkedin_url,
  company_website,
  company_domain,

  emails_raw,
  contact_email_domain,
  contact_email_root,

  company_industry_raw,
  company_logo_url,
  company_description_raw,
  company_location_raw
FROM norm;
