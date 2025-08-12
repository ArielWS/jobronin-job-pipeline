-- transforms/sql/02_silver_stepstone.sql
-- Silver view for StepStone → normalized common shape
-- Raw table: public.stepstone_job_scrape
-- Known columns: id, client_name, clientID, search_term, location, job_data (JSON text blob), timestamp
-- JSON is sanitized via util.json_clean(text) → jsonb (handles NaN/Infinity/None → null)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.stepstone AS
WITH raw AS (
  SELECT
    ss.id::text  AS source_id,
    ss.client_name,
    ss.location   AS location_raw,
    ss.job_data   AS job_data_txt,
    ss.timestamp  AS ts_raw
  FROM public.stepstone_job_scrape ss
),
parsed AS (
  SELECT
    r.*,
    util.json_clean(r.job_data_txt) AS jd  -- sanitize once here
  FROM raw r
),
fields AS (
  SELECT
    p.source_id,
    p.location_raw,
    p.ts_raw,
    p.jd,

    /* Company & title from multiple likely keys (NO fallback to client_name) */
    NULLIF(
      btrim(COALESCE(
        p.jd #>> '{employer,name}',
        p.jd #>> '{company,name}',
        p.jd ->> 'companyName'
      )),
      ''
    ) AS company_name_raw,

    btrim(COALESCE(
      p.jd #>> '{job,title}',
      p.jd #>> '{header,title}',
      p.jd ->> 'title'
    )) AS title_raw,

    /* URLs */
    NULLIF(COALESCE(
      p.jd ->> 'applyUrl',
      p.jd ->> 'applicationUrl',
      p.jd ->> 'jobUrl',
      p.jd ->> 'url'
    ), '') AS job_url_direct,

    NULLIF(COALESCE(
      p.jd #>> '{company,website}',
      p.jd #>> '{company,homepage}',
      p.jd #>> '{employer,website}',
      p.jd ->> 'companyWebsite',
      p.jd ->> 'homepage'
    ), '') AS company_website_raw,

    /* Try to find *any* email in the JSON blob for contact root */
    util.first_email(p.jd::text) AS email_found
  FROM parsed p
),
norm AS (
  SELECT
    'stepstone'                  AS source,
    f.source_id,
    /* StepStone row URL (best-effort) */
    f.job_url_direct             AS source_row_url,
    f.job_url_direct             AS job_url_direct,

    /* Title */
    f.title_raw,
    CASE WHEN f.title_raw IS NULL THEN NULL ELSE lower(btrim(f.title_raw)) END AS title_norm,

    /* Company: keep original in company_raw; NULL placeholders for company_name */
    f.company_name_raw           AS company_raw,
    CASE
      WHEN util.is_placeholder_company_name(f.company_name_raw) THEN NULL
      ELSE f.company_name_raw
    END                          AS company_name,

    /* Location */
    f.location_raw,
    NULLIF(split_part(f.location_raw, ', ', 1), '') AS city_guess,
    NULLIF(split_part(f.location_raw, ', ', 2), '') AS region_guess,
    NULL::text                                      AS country_guess,

    /* Date */
    CASE WHEN f.ts_raw IS NOT NULL THEN f.ts_raw::timestamptz ELSE NULL END AS date_posted,

    /* Flags */
    (f.location_raw ILIKE '%remote%' OR f.title_raw ILIKE '%remote%') AS is_remote,
    COALESCE(f.jd ->> 'contractType', f.jd #>> '{job,contractType}')  AS contract_type_raw,

    /* Pay (guard casts in case StepStone uses strings) */
    CASE WHEN (f.jd ->> 'salaryMin') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (f.jd ->> 'salaryMin')::numeric ELSE NULL END AS salary_min,
    CASE WHEN (f.jd ->> 'salaryMax') ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN (f.jd ->> 'salaryMax')::numeric ELSE NULL END AS salary_max,
    COALESCE(f.jd ->> 'salaryCurrency', f.jd #>> '{salary,currency}') AS currency,

    /* Emails → domains */
    f.email_found                                    AS emails_raw,
    util.email_domain(f.email_found)                 AS contact_email_domain,
    util.org_domain(util.email_domain(f.email_found)) AS contact_email_root,

    /* Apply */
    util.url_host(f.job_url_direct)                  AS apply_domain,
    util.org_domain(util.url_host(f.job_url_direct)) AS apply_root,

    /* Company site (filter aggregators/ATS for company_domain only) */
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_website_raw))        THEN NULL
      ELSE f.company_website_raw
    END AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_website_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_website_raw))        THEN NULL
      ELSE util.org_domain(util.url_host(f.company_website_raw))
    END AS company_domain,

    /* Common enrichment bits in StepStone payloads (best-effort) */
    COALESCE(f.jd #>> '{company,size}',     f.jd ->> 'companySize')         AS company_size_raw,
    COALESCE(f.jd #>> '{company,industry}', f.jd #>> '{industry,name}')     AS company_industry_raw,
    COALESCE(f.jd #>> '{company,logoUrl}',  f.jd ->> 'companyLogoUrl')      AS company_logo_url,
    COALESCE(f.jd ->> 'companyDescription', f.jd #>> '{company,description}') AS company_description_raw
  FROM fields f
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
