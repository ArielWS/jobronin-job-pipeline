-- transforms/sql/02_silver_stepstone.sql
-- Silver view for StepStone → normalized common shape
-- Raw table: public.stepstone_job_scrape
-- JSON is sanitized via util.json_clean(text) → jsonb (handles NaN/Infinity/None → null)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.stepstone AS
WITH src AS (
  SELECT
    ss.id::text        AS bronze_id,
    ss.client_name     AS client_name,
    ss."clientID"      AS client_id,
    ss.search_term     AS search_term,
    ss."location"      AS bronze_location,
    ss.job_data        AS job_data_txt,
    ss."timestamp"     AS ts_raw
  FROM public.stepstone_job_scrape ss
),
parsed AS (
  SELECT
    s.*,
    util.json_clean(s.job_data_txt) AS jd
  FROM src s
),
emails AS (
  SELECT
    p.bronze_id,
    NULLIF(
      btrim(
        (
          SELECT string_agg(DISTINCT x.e, '; ' ORDER BY x.e)
          FROM (
            -- explicit "emails" field (string or array)
            SELECT NULLIF(btrim(e1), '') AS e
            FROM (
              SELECT unnest(
                CASE
                  WHEN jsonb_typeof(p.jd -> 'emails') = 'array'
                    THEN ARRAY(SELECT jsonb_array_elements_text(p.jd -> 'emails'))
                  WHEN jsonb_typeof(p.jd -> 'emails') = 'string'
                    THEN string_to_array(p.jd ->> 'emails', ',')
                  ELSE ARRAY[]::text[]
                END
              )
            ) AS t(e1)

            UNION ALL

            -- contacts[].emailAddress
            SELECT NULLIF(btrim(c ->> 'emailAddress'), '')
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(p.jd -> 'contacts') = 'array'
                  THEN p.jd -> 'contacts'
                ELSE '[]'::jsonb
              END
            ) c

            UNION ALL

            -- contacts[].email
            SELECT NULLIF(btrim(c2 ->> 'email'), '')
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(p.jd -> 'contacts') = 'array'
                  THEN p.jd -> 'contacts'
                ELSE '[]'::jsonb
              END
            ) c2
          ) x
          WHERE x.e IS NOT NULL
        )
      ),
      ''
    ) AS emails_raw
  FROM parsed p
),
fields AS (
  SELECT
    p.bronze_id AS source_id,
    NULLIF(btrim(COALESCE(p.jd ->> 'site', p.jd #>> '{job,site}')), '') AS source_site,

    NULLIF(btrim(COALESCE(
      p.jd ->> 'title',
      p.jd #>> '{job,title}',
      p.jd #>> '{header,title}'
    )), '') AS title_raw,

    COALESCE(p.jd ->> 'description', p.jd #>> '{job,description}') AS description_raw,

    -- company: prefer top-level "company"; fallback to company_profile.name
    NULLIF(btrim(COALESCE(
      p.jd ->> 'company',
      p.jd #>> '{company_profile,name}'
    )), '') AS company_raw,

    -- location preference: job_location → location → bronze
    NULLIF(
      btrim(
        COALESCE(
          CASE
            WHEN jsonb_typeof(p.jd -> 'job_location') = 'array'
              THEN array_to_string(
                     ARRAY(SELECT jsonb_array_elements_text(p.jd -> 'job_location')), ' | ')
            WHEN jsonb_typeof(p.jd -> 'job_location') = 'string'
              THEN p.jd ->> 'job_location'
            ELSE NULL
          END,
          p.jd ->> 'location',
          p.bronze_location
        )
      ), ''
    ) AS location_raw,

    -- dates
    NULLIF(p.jd ->> 'date_posted', '') AS date_posted_text,
    p.ts_raw AS scraped_at_text,

    -- work/contract meta
    NULLIF(p.jd ->> 'work_type', '')    AS work_type_raw,
    NULLIF(p.jd ->> 'job_type', '')     AS job_type_raw,
    NULLIF(p.jd ->> 'job_function', '') AS job_function_raw,
    NULLIF(COALESCE(p.jd ->> 'contract_type', p.jd #>> '{job,contractType}'), '') AS contract_type_raw,

    -- salary
    NULLIF(COALESCE(p.jd ->> 'min_amount', p.jd #>> '{salary,min}', p.jd ->> 'salaryMin'), '') AS salary_min_raw,
    NULLIF(COALESCE(p.jd ->> 'max_amount', p.jd #>> '{salary,max}', p.jd ->> 'salaryMax'), '') AS salary_max_raw,
    NULLIF(COALESCE(p.jd ->> 'currency', p.jd #>> '{salary,currency}', p.jd ->> 'salaryCurrency'), '') AS currency_raw,
    NULLIF(COALESCE(p.jd ->> 'interval', p.jd #>> '{salary,interval}', p.jd ->> 'salaryInterval'), '') AS salary_interval_raw,

    -- URLs
    NULLIF(COALESCE(
      p.jd ->> 'job_url',
      p.jd ->> 'jobUrl',
      p.jd ->> 'url'
    ), '') AS job_url_raw,

    NULLIF(COALESCE(
      p.jd ->> 'job_url_direct',
      p.jd ->> 'applyUrl',
      p.jd ->> 'applicationUrl',
      p.jd ->> 'job_url',
      p.jd ->> 'jobUrl',
      p.jd ->> 'url'
    ), '') AS apply_url_raw,

    NULLIF(COALESCE(
      p.jd #>> '{company_profile,website}',
      p.jd #>> '{company,website}',
      p.jd #>> '{company,homepage}',
      p.jd ->> 'company_website',
      p.jd ->> 'company_homepage',
      p.jd ->> 'companyWebsite',
      p.jd ->> 'homepage'
    ), '') AS company_website_raw,

    -- company enrichment
    COALESCE(
      p.jd #>> '{company_profile,industries}',
      p.jd #>> '{industry,name}'
    ) AS company_industry_raw,
    p.jd #>> '{company_profile,logo_url}'  AS company_logo_url,
    p.jd #>> '{company_profile,address}'   AS company_location_raw

  FROM parsed p
  LEFT JOIN emails e ON e.bronze_id = p.bronze_id
),
norm AS (
  SELECT
    'stepstone'::text                                   AS source,
    f.source_site,
    f.source_id,

    -- timestamps
    CASE WHEN f.scraped_at_text IS NOT NULL THEN f.scraped_at_text::timestamptz ELSE NULL END AS scraped_at,
    CASE WHEN f.date_posted_text IS NOT NULL THEN f.date_posted_text::date ELSE NULL END       AS date_posted,

    -- titles
    f.title_raw,
    NULL::text                                          AS title_norm,

    -- company names
    f.company_raw,
    util.company_name_norm_langless(f.company_raw)       AS company_name_norm_langless,
    util.company_name_norm(f.company_raw)                AS company_name_norm,

    -- content
    f.description_raw,

    -- location
    f.location_raw,
    lp.city                                              AS city_guess,
    lp.region                                            AS region_guess,
    lp.country                                           AS country_guess,

    -- job meta
    f.contract_type_raw,
    CASE
      WHEN f.work_type_raw ILIKE '%homeoffice%' OR f.work_type_raw ILIKE '%remote%'
        OR f.title_raw ILIKE '%remote%'
      THEN TRUE
      ELSE NULL
    END                                                  AS is_remote,

    -- compensation
    CASE WHEN f.salary_min_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_min_raw::numeric ELSE NULL END AS salary_min,
    CASE WHEN f.salary_max_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_max_raw::numeric ELSE NULL END AS salary_max,
    NULLIF(f.currency_raw,'')                            AS currency,
    CASE
      WHEN f.salary_interval_raw ILIKE 'hour%'  THEN 'hourly'
      WHEN f.salary_interval_raw ILIKE 'day%'   THEN 'daily'
      WHEN f.salary_interval_raw ILIKE 'week%'  THEN 'weekly'
      WHEN f.salary_interval_raw ILIKE 'month%' THEN 'monthly'
      WHEN f.salary_interval_raw ILIKE 'year%'  THEN 'yearly'
      ELSE NULL
    END                                                  AS salary_interval,

    -- listing URL
    f.job_url_raw                                        AS job_url_raw,
    util.url_canonical(f.job_url_raw)                    AS job_url_canonical,
    (regexp_match(coalesce(util.url_canonical(f.job_url_raw),''), '/jobs/view/([0-9]+)'))[1] AS linkedin_job_id,

    -- apply URL
    COALESCE(f.apply_url_raw, f.job_url_raw)             AS apply_url_raw,
    util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)) AS apply_url_canonical,
    util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))               AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))) AS apply_root,

    -- company website (filter aggregator/ATS/career)
    f.company_website_raw,
    util.url_canonical(f.company_website_raw)            AS company_website_canonical,
    CASE
      WHEN util.url_host(util.url_canonical(f.company_website_raw)) = 'linkedin.com'
        THEN util.url_canonical(f.company_website_raw)
      ELSE NULL
    END                                                  AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.url_canonical(f.company_website_raw)
    END                                                  AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.org_domain(util.url_host(util.url_canonical(f.company_website_raw)))
    END                                                  AS company_domain,

    -- emails → contact evidence
    e.emails_raw,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(e.emails_raw))) THEN NULL
      ELSE util.email_domain(util.first_email(e.emails_raw))
    END                                                  AS contact_email_domain,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(e.emails_raw))) THEN NULL
      ELSE util.org_domain(util.email_domain(util.first_email(e.emails_raw)))
    END                                                  AS contact_email_root,

    -- enrichment passthroughs
    f.company_industry_raw,
    f.company_logo_url,
    NULL::text                                           AS company_description_raw,
    f.company_location_raw

  FROM fields f
  LEFT JOIN emails e ON e.bronze_id = f.source_id
  LEFT JOIN LATERAL util.location_parse(f.location_raw) lp ON TRUE
)
SELECT
  source,
  source_site,
  source_id,
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
