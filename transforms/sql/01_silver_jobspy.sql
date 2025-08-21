-- Silver view for JobSpy → normalized common shape
-- Raw table (assumed): public.jobspy_job_scrape
-- Known columns: id, company, company_url, company_url_direct, emails, job_url, job_url_direct, location, job_data (json), company_location, date_posted, (others may exist)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.jobspy AS
WITH src AS (
  SELECT
    js.id::text                                   AS source_id,
    NULLIF(js.title, '')                          AS title_raw,
    NULLIF(js.job_type, '')                       AS contract_type_raw,
    js.company                                    AS company_raw,
    btrim(js.company)                             AS company_name,
    NULLIF(js.company_industry, '')               AS company_industry_raw,
    COALESCE(NULLIF(js.company_url_direct,''), NULLIF(js.company_url,'')) AS company_website_raw,
    NULLIF(js.job_url_direct,'')                  AS job_url_direct_raw,
    NULLIF(js.job_url,'')                         AS job_url_fallback,
    NULLIF(js.company_logo,'')                    AS company_logo_url,
    NULLIF(js.company_description,'')             AS company_description_raw,
    js.emails                                     AS emails_raw,
    js.location                                   AS location_raw,
    NULLIF(js.location, '')                       AS job_location_raw,
    NULLIF(js.company_addresses,'')               AS company_location_raw,
    js.date_posted                                AS date_posted_raw
  FROM public.jobspy_job_scrape js
),
norm AS (
  SELECT
    'jobspy'                                     AS source,
    s.source_id,
    COALESCE(s.job_url_direct_raw, s.job_url_fallback)              AS source_row_url,
    COALESCE(s.job_url_direct_raw, s.job_url_fallback)              AS job_url_direct,

    /* Titles: many scrapes omit a stable title column; leave NULL if absent */
    s.title_raw,
    NULL::text                                   AS title_norm,

    s.company_raw,
    s.company_name,

    s.location_raw,
    s.job_location_raw,
    /* naive location parsing (best-effort; keeps compatible types) */
    NULLIF(split_part(COALESCE(s.job_location_raw, s.location_raw), ', ', 1), '') AS city_guess,
    NULLIF(split_part(COALESCE(s.job_location_raw, s.location_raw), ', ', 2), '') AS region_guess,
    NULL::text                                       AS country_guess,

    /* dates */
    CASE
      WHEN s.date_posted_raw IS NOT NULL THEN s.date_posted_raw::timestamptz
      ELSE NULL
    END                                         AS date_posted,

    /* flags */
    (s.location_raw ILIKE '%remote%' OR s.company_name ILIKE '%remote%') AS is_remote,
    s.contract_type_raw,

    /* pay */
    NULL::numeric                                AS salary_min,
    NULL::numeric                                AS salary_max,
    NULL::text                                   AS currency,

    /* emails */
    s.emails_raw,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(s.emails_raw))) THEN NULL
      ELSE util.email_domain(util.first_email(s.emails_raw))
    END                                         AS contact_email_domain,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(s.emails_raw))) THEN NULL
      ELSE util.org_domain(util.email_domain(util.first_email(s.emails_raw)))
    END                                         AS contact_email_root,

    /* apply */
    util.url_host(COALESCE(s.job_url_direct_raw, s.job_url_fallback))         AS apply_domain,
    util.org_domain(util.url_host(COALESCE(s.job_url_direct_raw, s.job_url_fallback))) AS apply_root,

    /* company site: prefer direct, filter out aggregators/ATS for the *company* domain */
    s.company_website_raw,
    CASE
      WHEN util.url_host(s.company_website_raw) = 'linkedin.com' THEN s.company_website_raw
      ELSE NULL
    END                                         AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(s.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(s.company_website_raw)) THEN NULL
      ELSE s.company_website_raw
    END                                         AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(s.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(s.company_website_raw)) THEN NULL
      ELSE util.org_domain(util.url_host(s.company_website_raw))
    END                                         AS company_domain,

    /* enrichment passthroughs (JobSpy raw often lacks these; keep NULL-safe) */
    NULL::text                                   AS company_size_raw,
    s.company_industry_raw,
    s.company_logo_url,
    s.company_description_raw,
    s.company_location_raw
  FROM src s
)
SELECT
  source, source_id, source_row_url, job_url_direct,
  title_raw, title_norm,
  company_raw, company_name,
  location_raw, job_location_raw, city_guess, region_guess, country_guess,
  date_posted, is_remote, contract_type_raw,
  salary_min, salary_max, currency,
  emails_raw, contact_email_domain, contact_email_root,
  apply_domain, apply_root,
  company_website_raw, company_linkedin_url, company_website, company_domain,
  company_size_raw, company_industry_raw, company_logo_url, company_description_raw, company_location_raw
FROM norm;
