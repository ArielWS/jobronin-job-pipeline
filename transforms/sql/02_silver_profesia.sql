-- transforms/sql/02_silver_profesia.sql
-- Silver view for Profesia → normalized common shape
-- Raw table: public.profesia_job_scrape
-- JSON sanitized via util.json_clean(text)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.profesia AS
WITH raw AS (
  SELECT
    util.json_clean(p.job_data) AS jd
  FROM public.profesia_job_scrape p
),
fields AS (
  SELECT
    md5(jd->>'job_url')                        AS source_id,
    jd,
    jd->>'job_url'                             AS job_url_direct,
    COALESCE(jd->>'title', jd->>'job_title')   AS title_raw,
    COALESCE(jd->>'company', jd->>'company_name') AS company_raw,
    jd->>'location'                            AS location_raw,
    COALESCE(jd->>'contract_type', jd->>'employment_type') AS contract_type_raw,
    CASE
      WHEN jd ? 'is_remote' THEN
        CASE lower(jd->>'is_remote')
          WHEN 'true'  THEN TRUE
          WHEN '1'     THEN TRUE
          WHEN 'yes'   THEN TRUE
          WHEN 'false' THEN FALSE
          WHEN '0'     THEN FALSE
          WHEN 'no'    THEN FALSE
          WHEN 'hybrid' THEN NULL
          ELSE NULL
        END
      WHEN jd ? 'remote_type' THEN lower(jd->>'remote_type') = 'fully remote'
      ELSE (jd->>'location') ILIKE '%remote%' OR (COALESCE(jd->>'title', jd->>'job_title')) ILIKE '%remote%'
    END                                       AS is_remote,
    -- Salary fields
    CASE WHEN (jd->>'salary_min') ~ '^-?[0-9]+(\\.[0-9]+)?$'
         THEN (jd->>'salary_min')::numeric ELSE NULL END AS salary_min,
    CASE WHEN (jd->>'salary_max') ~ '^-?[0-9]+(\\.[0-9]+)?$'
         THEN (jd->>'salary_max')::numeric ELSE NULL END AS salary_max,
    COALESCE(jd->>'salary_currency', jd->>'currency') AS currency,
    -- Emails
    util.first_email(jd::text)                AS emails_raw,
    -- Company website
    jd->>'company_url'                        AS company_url_raw,
    -- Date
    COALESCE(jd->>'date_posted', jd->>'posted_at') AS date_posted_raw,
    -- Optional enrichment fields
    jd->>'company_size'                       AS company_size_raw,
    jd->>'industry'                           AS company_industry_raw,
    jd->>'company_logo_url'                   AS company_logo_url,
    jd->>'company_description'                AS company_description_raw
  FROM raw
),
norm AS (
  SELECT
    'profesia'                                AS source,
    f.source_id,
    f.job_url_direct                          AS source_row_url,
    f.job_url_direct,
    f.title_raw,
    CASE WHEN f.title_raw IS NULL THEN NULL ELSE lower(btrim(f.title_raw)) END AS title_norm,
    f.company_raw,
    CASE
      WHEN f.company_raw IS NOT NULL
           AND NOT util.is_placeholder_company_name(f.company_raw)
           AND util.company_name_norm(f.company_raw) IS NOT NULL
        THEN f.company_raw
      ELSE NULL
    END                                       AS company_name,
    f.location_raw,
    NULLIF(split_part(f.location_raw, ', ', 1), '') AS city_guess,
    NULLIF(split_part(f.location_raw, ', ', 2), '') AS region_guess,
    NULL::text                                AS country_guess,
    CASE
      WHEN f.date_posted_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN f.date_posted_raw::timestamptz
      ELSE NULL
    END                                       AS date_posted,
    f.is_remote,
    f.contract_type_raw,
    f.salary_min,
    f.salary_max,
    f.currency,
    f.emails_raw,
    util.email_domain(f.emails_raw)           AS contact_email_domain,
    util.org_domain(util.email_domain(f.emails_raw)) AS contact_email_root,
    util.url_host(f.job_url_direct)           AS apply_domain,
    util.org_domain(util.url_host(f.job_url_direct)) AS apply_root,
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_url_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_url_raw))        THEN NULL
      ELSE f.company_url_raw
    END                                       AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(f.company_url_raw)) THEN NULL
      WHEN util.is_ats_host(util.url_host(f.company_url_raw))        THEN NULL
      ELSE util.org_domain(util.url_host(f.company_url_raw))
    END                                       AS company_domain,
    f.company_size_raw,
    f.company_industry_raw,
    f.company_logo_url,
    f.company_description_raw
  FROM fields f
),
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
  company_website, company_domain,
  company_size_raw, company_industry_raw, company_logo_url, company_description_raw
FROM keep;
