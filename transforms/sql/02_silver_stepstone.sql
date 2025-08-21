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
    COALESCE(p.jd ->> 'publicationDate', p.jd #>> '{job,publicationDate}') AS date_posted_raw,

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
    COALESCE(p.jd ->> 'description', p.jd #>> '{job,description}') AS description_raw,
    p.jd ->> 'work_type'    AS work_type_raw,
    p.jd ->> 'job_type'     AS job_type_raw,
    p.jd ->> 'job_function' AS job_function_raw,

    -- Job locations can be string or array; normalize to text
    CASE
      WHEN jsonb_typeof(p.jd -> 'job_location') = 'array' THEN
        array_to_string(
          ARRAY(
            SELECT btrim(loc)
            FROM jsonb_array_elements_text(p.jd -> 'job_location') AS loc
          ), ' | '
        )
      WHEN jsonb_typeof(p.jd -> 'job_location') = 'string' THEN
        NULLIF(btrim(p.jd ->> 'job_location'), '')
      ELSE NULL
    END AS job_location_raw,

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
      p.jd #>> '{company_profile,website}',
      p.jd ->> 'company_website',
      p.jd ->> 'company_homepage',
      p.jd ->> 'companyWebsite',
      p.jd ->> 'homepage',
      p.jd #>> '{employer,website}'
    ), '') AS company_website_raw,

    -- Collect all explicit emails and contact objects
    c.emails_all,
    c.contacts_raw,

    -- Pick a representative email for legacy fields
    COALESCE(c.emails_all[1], util.first_email(p.jd::text)) AS email_found,

    -- Enrichment bits commonly present in StepStone payloads (best-effort)
    COALESCE(p.jd #>> '{company,size}', p.jd ->> 'companySize', p.jd #>> '{company_profile,employees}') AS company_size_raw,
    COALESCE(p.jd #>> '{company,industry}', p.jd #>> '{industry,name}', p.jd #>> '{company_profile,industries}') AS company_industry_raw,
    COALESCE(p.jd #>> '{company,logoUrl}', p.jd ->> 'companyLogoUrl', p.jd #>> '{company_profile,logo_url}')   AS company_logo_url,
    COALESCE(p.jd ->> 'companyDescription', p.jd #>> '{company,description}') AS company_description_raw,
    p.jd #>> '{company_profile,address}'      AS company_address_raw,
    p.jd #>> '{company_profile,stepstone_id}' AS company_stepstone_id,
    p.jd #>> '{company_profile,active_jobs}'  AS company_active_jobs,
    p.jd #>> '{company_profile,hero_url}'     AS company_hero_url,

    -- Salary fields can appear in multiple shapes; coalesce common keys
    COALESCE(
      p.jd ->> 'salaryMin',
      p.jd #>> '{salary,min}',
      p.jd ->> 'min_amount',
      p.jd #>> '{salary,min_amount}'
    ) AS salary_min_raw,
    COALESCE(
      p.jd ->> 'salaryMax',
      p.jd #>> '{salary,max}',
      p.jd ->> 'max_amount',
      p.jd #>> '{salary,max_amount}'
    ) AS salary_max_raw,
    COALESCE(
      p.jd ->> 'salaryCurrency',
      p.jd #>> '{salary,currency}',
      p.jd ->> 'currency'
    ) AS salary_currency_raw,
    COALESCE(
      p.jd ->> 'salaryInterval',
      p.jd #>> '{salary,interval}',
      p.jd ->> 'interval'
    ) AS salary_interval_raw,
    COALESCE(
      p.jd ->> 'salarySource',
      p.jd #>> '{salary,source}',
      p.jd ->> 'salary_source'
    ) AS salary_source_raw
  FROM parsed p
  LEFT JOIN LATERAL (
    SELECT
      (
        SELECT ARRAY(
          SELECT DISTINCT e
          FROM (
            SELECT jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(p.jd -> 'emails') = 'array'
                  THEN p.jd -> 'emails'
                WHEN jsonb_typeof(p.jd -> 'emails') = 'string'
                  THEN to_jsonb(string_to_array(p.jd ->> 'emails', ','))
                ELSE '[]'::jsonb
              END
            ) AS e
            UNION
            SELECT c ->> 'email'
            FROM jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(p.jd -> 'contacts') = 'array'
                  THEN p.jd -> 'contacts'
                ELSE '[]'::jsonb
              END
            ) c
            WHERE c ->> 'email' IS NOT NULL
          ) q
          WHERE e IS NOT NULL
        )
      ) AS emails_all,
      CASE
        WHEN jsonb_typeof(p.jd -> 'contacts') IN ('array', 'object')
          THEN p.jd -> 'contacts'
        ELSE NULL
      END AS contacts_raw
  ) c ON TRUE
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
    f.description_raw,

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
    f.job_location_raw,
    NULLIF(
      split_part(
        COALESCE(split_part(f.job_location_raw, ' | ', 1), f.location_raw),
        ', ',
        1
      ),
      ''
    ) AS city_guess,
    NULLIF(
      split_part(
        COALESCE(split_part(f.job_location_raw, ' | ', 1), f.location_raw),
        ', ',
        2
      ),
      ''
    ) AS region_guess,
    NULL::text                                      AS country_guess,

    -- Date
    CASE
      WHEN f.date_posted_raw IS NOT NULL THEN f.date_posted_raw::timestamptz
      WHEN f.ts_raw IS NOT NULL THEN f.ts_raw::timestamptz
      ELSE NULL
    END AS date_posted,

    -- Flags
    (f.location_raw ILIKE '%remote%' OR f.title_raw ILIKE '%remote%') AS is_remote,
    COALESCE(f.jd ->> 'contractType', f.jd #>> '{job,contractType}')  AS contract_type_raw,
    f.work_type_raw,
    f.job_type_raw,
    f.job_function_raw,

    -- Pay (guard casts in case StepStone uses strings)
    CASE WHEN f.salary_min_raw ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN f.salary_min_raw::numeric ELSE NULL END AS salary_min,
    CASE WHEN f.salary_max_raw ~ '^-?[0-9]+(\.[0-9]+)?$'
         THEN f.salary_max_raw::numeric ELSE NULL END AS salary_max,
    f.salary_currency_raw AS currency,
    f.salary_interval_raw AS salary_interval,
    f.salary_source_raw   AS salary_source,

    -- Emails → domains
    f.emails_all,
    f.email_found                                AS emails_raw,
    util.email_domain(f.email_found)             AS contact_email_domain,
    util.org_domain(util.email_domain(f.email_found)) AS contact_email_root,
    f.contacts_raw,

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
    f.company_description_raw,
    f.company_address_raw,
    f.company_stepstone_id,
    f.company_active_jobs,
    f.company_hero_url
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
    title_raw, title_norm, description_raw,
    company_raw, company_name,
    location_raw, job_location_raw, city_guess, region_guess, country_guess,
    date_posted, is_remote, contract_type_raw,
    work_type_raw, job_type_raw, job_function_raw,
    salary_min, salary_max, currency, salary_interval, salary_source,
    emails_all, emails_raw, contact_email_domain, contact_email_root, contacts_raw,
  apply_domain, apply_root,
  company_website_raw, company_linkedin_url, company_website, company_domain,
  company_size_raw, company_industry_raw, company_logo_url, company_description_raw,
  company_address_raw, company_stepstone_id, company_active_jobs, company_hero_url
FROM keep;
