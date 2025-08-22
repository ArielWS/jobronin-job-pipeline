-- transforms/sql/02_silver_profesia_sk.sql
-- Silver view for Profesia.sk → normalized common shape
-- Raw table: public.profesiask_job_scrape
-- JSON is sanitized via util.json_clean(text) → jsonb (handles NaN/Infinity/None → null)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.profesia_sk AS
WITH src AS (
  SELECT
    -- No numeric id column provided in the DDL; use a stable hash if needed later
    NULL::text                          AS bronze_id,
    p.client_name                       AS client_name,
    p."clientID"                        AS client_id,
    p.search_term                       AS search_term,
    p."location"                        AS bronze_location,
    p.job_data                          AS job_data_txt,
    p."timestamp"                       AS ts_raw
  FROM public.profesiask_job_scrape p
),
parsed AS (
  SELECT
    s.*,
    util.json_clean(s.job_data_txt) AS jd
  FROM src s
),
fields AS (
  SELECT
    -- Source identity
    COALESCE(NULLIF(btrim(jd ->> 'id'), ''), NULLIF(btrim(jd #>> '{job,id}'), '')) AS json_id,
    NULLIF(btrim(COALESCE(jd ->> 'site', jd #>> '{job,site}')), '')                 AS source_site,
    -- Title/description
    NULLIF(btrim(COALESCE(jd ->> 'title', jd #>> '{job,title}', jd #>> '{header,title}')), '') AS title_raw,
    COALESCE(jd ->> 'description', jd #>> '{job,description}')                      AS description_raw,
    -- Company
    NULLIF(btrim(COALESCE(jd ->> 'company', jd #>> '{company,name}', jd #>> '{employer,name}')), '') AS company_raw,
    -- Location preference: job_location → location (json) → bronze
    NULLIF(
      btrim(
        COALESCE(
          CASE
            WHEN jsonb_typeof(jd -> 'job_location') = 'array'
              THEN array_to_string(ARRAY(SELECT jsonb_array_elements_text(jd -> 'job_location')), ' | ')
            WHEN jsonb_typeof(jd -> 'job_location') = 'string'
              THEN jd ->> 'job_location'
            ELSE NULL
          END,
          jd ->> 'location',
          bronze_location
        )
      ),
      ''
    ) AS location_raw,
    -- Dates
    NULLIF(jd ->> 'date_posted','')                                           AS date_posted_text,
    ts_raw                                                                     AS scraped_at_text,
    -- Work/contract
    NULLIF(jd ->> 'contract_type','')                                         AS contract_type_raw,
    NULLIF(jd ->> 'work_type','')                                             AS work_type_raw,
    -- Salary
    NULLIF(COALESCE(jd ->> 'min_amount', jd #>> '{salary,min}', jd ->> 'salaryMin'), '') AS salary_min_raw,
    NULLIF(COALESCE(jd ->> 'max_amount', jd #>> '{salary,max}', jd ->> 'salaryMax'), '') AS salary_max_raw,
    NULLIF(COALESCE(jd ->> 'currency',   jd #>> '{salary,currency}', jd ->> 'salaryCurrency'), '') AS currency_raw,
    NULLIF(COALESCE(jd ->> 'interval',   jd #>> '{salary,interval}', jd ->> 'salaryInterval'), '') AS salary_interval_raw,
    -- URLs (Profesia often only has listing URL; keep both slots)
    NULLIF(COALESCE(jd ->> 'job_url', jd ->> 'jobUrl', jd ->> 'url'), '')     AS job_url_raw,
    NULLIF(COALESCE(jd ->> 'job_url_direct', jd ->> 'applyUrl', jd ->> 'applicationUrl',
                    jd ->> 'job_url', jd ->> 'jobUrl', jd ->> 'url'), '')     AS apply_url_raw,
    -- Company website if present in payload
    NULLIF(COALESCE(
      jd #>> '{company,website}',
      jd #>> '{company,homepage}',
      jd ->> 'company_website',
      jd ->> 'companyWebsite',
      jd ->> 'homepage'
    ), '') AS company_website_raw,
    -- Company enrichment (best-effort)
    COALESCE(jd #>> '{company,industry}', jd #>> '{company_profile,industries}') AS company_industry_raw,
    COALESCE(jd #>> '{company,logo_url}', jd #>> '{company_profile,logo_url}')   AS company_logo_url,
    COALESCE(jd #>> '{company,address}',  jd #>> '{company_profile,address}')    AS company_location_raw,
    -- Emails (string or array or contacts[].email / emailAddress)
    NULLIF(btrim(
      (
        SELECT string_agg(DISTINCT e, '; ' ORDER BY e)
        FROM (
          SELECT NULLIF(btrim(x), '') AS e
          FROM (
            SELECT unnest(
              CASE
                WHEN jsonb_typeof(jd -> 'emails') = 'array'
                  THEN ARRAY(SELECT jsonb_array_elements_text(jd -> 'emails'))
                WHEN jsonb_typeof(jd -> 'emails') = 'string'
                  THEN string_to_array(jd ->> 'emails', ',')
                ELSE ARRAY[]::text[]
              END
            )
          ) q(x)
          UNION ALL
          SELECT NULLIF(btrim(c ->> 'email'), '')
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(jd -> 'contacts') = 'array' THEN jd -> 'contacts' ELSE '[]'::jsonb END) c
          UNION ALL
          SELECT NULLIF(btrim(c2 ->> 'emailAddress'), '')
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(jd -> 'contacts') = 'array' THEN jd -> 'contacts' ELSE '[]'::jsonb END) c2
        ) z
        WHERE e IS NOT NULL
      )
    ), '') AS emails_raw
  FROM parsed
),
norm AS (
  SELECT
    'profesia_sk'::text                                         AS source,
    COALESCE(f.source_site, 'Profesia SK')                      AS source_site,
    COALESCE(f.json_id, md5(coalesce(f.job_url_raw,'') || coalesce(f.title_raw,'') || coalesce(f.company_raw,'') || coalesce(f.location_raw,''))) AS source_id,

    -- Canonical pointer back to the listing
    util.url_canonical(COALESCE(f.job_url_raw, f.apply_url_raw)) AS source_row_url,

    -- Timestamps
    CASE WHEN f.scraped_at_text IS NOT NULL THEN f.scraped_at_text::timestamptz ELSE NULL END AS scraped_at,
    CASE WHEN f.date_posted_text IS NOT NULL THEN f.date_posted_text::date ELSE NULL END       AS date_posted,

    -- Titles
    f.title_raw,
    NULL::text                                                AS title_norm,

    -- Company
    f.company_raw,
    util.company_name_norm_langless(f.company_raw)            AS company_name_norm_langless,
    util.company_name_norm(f.company_raw)                     AS company_name_norm,

    -- Content
    f.description_raw,

    -- Location
    f.location_raw,
    lp.city                                                   AS city_guess,
    lp.region                                                 AS region_guess,
    lp.country                                                AS country_guess,

    -- Job meta
    f.contract_type_raw,
    CASE
      WHEN f.work_type_raw ILIKE '%homeoffice%' OR f.work_type_raw ILIKE '%home office%' OR f.work_type_raw ILIKE '%remote%'
        OR f.title_raw ILIKE '%remote%' OR f.location_raw ILIKE '%remote%'
        OR f.work_type_raw ILIKE '%práca z domu%' OR f.work_type_raw ILIKE '%praca z domu%'
      THEN TRUE
      ELSE NULL
    END                                                       AS is_remote,

    -- Compensation
    CASE WHEN f.salary_min_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_min_raw::numeric ELSE NULL END AS salary_min,
    CASE WHEN f.salary_max_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_max_raw::numeric ELSE NULL END AS salary_max,
    NULLIF(f.currency_raw,'')                                 AS currency,
    CASE
      WHEN f.salary_interval_raw ILIKE 'hour%'  THEN 'hourly'
      WHEN f.salary_interval_raw ILIKE 'day%'   THEN 'daily'
      WHEN f.salary_interval_raw ILIKE 'week%'  THEN 'weekly'
      WHEN f.salary_interval_raw ILIKE 'month%' THEN 'monthly'
      WHEN f.salary_interval_raw ILIKE 'year%'  THEN 'yearly'
      ELSE NULL
    END                                                       AS salary_interval,

    -- Listing URL
    f.job_url_raw                                             AS job_url_raw,
    util.url_canonical(f.job_url_raw)                         AS job_url_canonical,
    (regexp_match(coalesce(util.url_canonical(f.job_url_raw),''), '/jobs/view/([0-9]+)'))[1] AS linkedin_job_id,

    -- Apply URL
    COALESCE(f.apply_url_raw, f.job_url_raw)                  AS apply_url_raw,
    util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)) AS apply_url_canonical,
    util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))                AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))) AS apply_root,

    -- Company website (filter aggregator/ATS/career)
    f.company_website_raw,
    util.url_canonical(f.company_website_raw)                 AS company_website_canonical,
    CASE
      WHEN util.url_host(util.url_canonical(f.company_website_raw)) = 'linkedin.com'
        THEN util.url_canonical(f.company_website_raw)
      ELSE NULL
    END                                                       AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.url_canonical(f.company_website_raw)
    END                                                       AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.org_domain(util.url_host(util.url_canonical(f.company_website_raw)))
    END                                                       AS company_domain,

    -- Email evidence
    f.emails_raw,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(f.emails_raw))) THEN NULL
      ELSE util.email_domain(util.first_email(f.emails_raw))
    END                                                       AS contact_email_domain,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(f.emails_raw))) THEN NULL
      ELSE util.org_domain(util.email_domain(util.first_email(f.emails_raw)))
    END                                                       AS contact_email_root

  FROM fields f
  LEFT JOIN LATERAL util.location_parse(f.location_raw) lp ON TRUE
),
-- De-duplication:
-- Keep the most recent row per canonical key.
-- Key priority: job_url_canonical → apply_url_canonical → stable hash of (title, company, location)
keys AS (
  SELECT
    n.*,
    COALESCE(
      n.job_url_canonical,
      n.apply_url_canonical,
      'hash:' || md5(coalesce(n.title_raw,'') || '|' || coalesce(n.company_raw,'') || '|' || coalesce(n.location_raw,''))
    ) AS dedup_key
  FROM norm n
),
dedup AS (
  SELECT DISTINCT ON (dedup_key)
    *
  FROM keys
  ORDER BY dedup_key, scraped_at DESC NULLS LAST, date_posted DESC NULLS LAST
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
  contact_email_root
FROM dedup;
