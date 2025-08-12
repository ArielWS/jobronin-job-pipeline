-- Silver view for JobSpy → normalized common shape
-- Raw table (assumed): public.jobspy_job_scrape
-- Known columns: id, company, company_url, company_url_direct, emails, job_url, job_url_direct, location, date_posted, (others may exist)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.jobspy AS
WITH src AS (
  SELECT
    js.id::text                                  AS source_id,
    js.company                                   AS company_raw,
    btrim(js.company)                            AS company_name,
    NULLIF(js.company_url_direct,'')             AS company_url_direct,
    NULLIF(js.company_url,'')                    AS company_url_fallback,
    NULLIF(js.job_url_direct,'')                 AS job_url_direct_raw,
    NULLIF(js.job_url,'')                        AS job_url_fallback,
    js.emails                                    AS emails_raw,
    js.location                                  AS location_raw,
    js.date_posted                               AS date_posted_raw
  FROM public.jobspy_job_scrape js
),
norm AS (
  SELECT
    'jobspy'                                     AS source,
    s.source_id,
    COALESCE(s.job_url_direct_raw, s.job_url_fallback)              AS source_row_url,
    COALESCE(s.job_url_direct_raw, s.job_url_fallback)              AS job_url_direct,

    /* Titles: many scrapes omit a stable title column; leave NULL if absent */
    NULL::text                                   AS title_raw,
    NULL::text                                   AS title_norm,

    s.company_raw,
    s.company_name,

    s.location_raw,
    /* naive location parsing (best-effort; keeps compatible types) */
    NULLIF(split_part(s.location_raw, ', ', 1), '') AS city_guess,
    NULLIF(split_part(s.location_raw, ', ', 2), '') AS region_guess,
    NULL::text                                       AS country_guess,

    /* dates */
    CASE
      WHEN s.date_posted_raw IS NOT NULL THEN s.date_posted_raw::timestamptz
      ELSE NULL
    END                                         AS date_posted,

    /* flags */
    (s.location_raw ILIKE '%remote%' OR s.company_name ILIKE '%remote%') AS is_remote,
    NULL::text                                   AS contract_type_raw,

    /* pay */
    NULL::numeric                                AS salary_min,
    NULL::numeric                                AS salary_max,
    NULL::text                                   AS currency,

    /* emails */
    s.emails_raw,
    util.email_domain(util.first_email(s.emails_raw))          AS contact_email_domain,
    util.org_domain(util.email_domain(util.first_email(s.emails_raw))) AS contact_email_root,

    /* apply */
    util.url_host(COALESCE(s.job_url_direct_raw, s.job_url_fallback))         AS apply_domain,
    util.org_domain(util.url_host(COALESCE(s.job_url_direct_raw, s.job_url_fallback))) AS apply_root,

    /* company site: prefer direct, filter out aggregators/ATS for the *company* domain */
    COALESCE(s.company_url_direct, s.company_url_fallback)     AS company_website_raw,
    CASE
      WHEN util.is_aggregator_host(util.url_host(COALESCE(s.company_url_direct, s.company_url_fallback))) THEN NULL
      WHEN util.is_ats_host(util.url_host(COALESCE(s.company_url_direct, s.company_url_fallback))) THEN NULL
      ELSE COALESCE(s.company_url_direct, s.company_url_fallback)
    END                                         AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(COALESCE(s.company_url_direct, s.company_url_fallback))) THEN NULL
      WHEN util.is_ats_host(util.url_host(COALESCE(s.company_url_direct, s.company_url_fallback))) THEN NULL
      ELSE util.org_domain(util.url_host(COALESCE(s.company_url_direct, s.company_url_fallback)))
    END                                         AS company_domain,

    /* enrichment passthroughs (JobSpy raw often lacks these; keep NULL-safe) */
    NULL::text                                   AS company_size_raw,
    NULL::text                                   AS company_industry_raw,
    NULL::text                                   AS company_logo_url,
    NULL::text                                   AS company_description_raw
  FROM src s
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
  company_website, company_domain,
  company_size_raw, company_industry_raw, company_logo_url, company_description_raw
FROM norm;
