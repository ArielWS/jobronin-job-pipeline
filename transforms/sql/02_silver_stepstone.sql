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
fields AS (
  SELECT
    p.bronze_id AS source_id,

    -- Site label (e.g., "Stepstone DE")
    NULLIF(btrim(COALESCE(p.jd ->> 'site', p.jd #>> '{job,site}')), '') AS source_site,

    -- Titles / description
    NULLIF(btrim(COALESCE(
      p.jd #>> '{job,title}',
      p.jd #>> '{header,title}',
      p.jd ->> 'title'
    )), '') AS title_raw,
    COALESCE(p.jd ->> 'description', p.jd #>> '{job,description}') AS description_raw,

    -- Company name (prefer top-level; fallback to company_profile.name / company.name)
    NULLIF(btrim(COALESCE(
      p.jd ->> 'company',
      p.jd #>> '{company_profile,name}',
      p.jd #>> '{company,name}'
    )), '') AS company_raw,

    -- Location: job_location (array|string) → location → bronze
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

    -- Dates (text)
    COALESCE(p.jd ->> 'date_posted', p.jd ->> 'publicationDate', p.jd #>> '{job,publicationDate}') AS date_posted_text,
    p.ts_raw AS scraped_at_text,

    -- Work / contract meta
    NULLIF(p.jd ->> 'work_type','')    AS work_type_raw,
    NULLIF(p.jd ->> 'job_type','')     AS job_type_raw,
    NULLIF(p.jd ->> 'job_function','') AS job_function_raw,
    NULLIF(COALESCE(p.jd ->> 'contract_type', p.jd #>> '{job,contractType}'),'') AS contract_type_raw,

    -- Salary (common shapes)
    NULLIF(COALESCE(p.jd ->> 'salaryMin', p.jd #>> '{salary,min}', p.jd ->> 'min_amount', p.jd #>> '{salary,min_amount}'), '') AS salary_min_raw,
    NULLIF(COALESCE(p.jd ->> 'salaryMax', p.jd #>> '{salary,max}', p.jd ->> 'max_amount', p.jd #>> '{salary,max_amount}'), '') AS salary_max_raw,
    NULLIF(COALESCE(p.jd ->> 'salaryCurrency', p.jd #>> '{salary,currency}', p.jd ->> 'currency'), '') AS salary_currency_raw,
    NULLIF(COALESCE(p.jd ->> 'salaryInterval', p.jd #>> '{salary,interval}', p.jd ->> 'interval'), '') AS salary_interval_raw,
    NULLIF(COALESCE(p.jd ->> 'salarySource', p.jd #>> '{salary,source}', p.jd ->> 'salary_source'), '') AS salary_source_raw,

    -- URLs (listing / apply)
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

    -- Company website / enrichment
    NULLIF(COALESCE(
      p.jd #>> '{company_profile,website}',
      p.jd #>> '{company,website}',
      p.jd #>> '{company,homepage}',
      p.jd ->> 'company_website',
      p.jd ->> 'company_homepage',
      p.jd ->> 'companyWebsite',
      p.jd ->> 'homepage'
    ), '') AS company_website_raw,

    COALESCE(p.jd #>> '{company,size}', p.jd ->> 'companySize', p.jd #>> '{company_profile,employees}') AS company_size_raw,
    COALESCE(p.jd #>> '{company,industry}', p.jd #>> '{industry,name}', p.jd #>> '{company_profile,industries}') AS company_industry_raw,
    COALESCE(p.jd #>> '{company,logoUrl}', p.jd ->> 'companyLogoUrl', p.jd #>> '{company_profile,logo_url}') AS company_logo_url,
    COALESCE(p.jd ->> 'companyDescription', p.jd #>> '{company,description}') AS company_description_raw,
    p.jd #>> '{company_profile,address}'      AS company_address_raw,
    p.jd #>> '{company_profile,stepstone_id}' AS company_stepstone_id,
    p.jd #>> '{company_profile,active_jobs}'  AS company_active_jobs,
    p.jd #>> '{company_profile,hero_url}'     AS company_hero_url,
    p.jd #>> '{company_profile,founded}'      AS company_founded_year_raw,

    -- External identifiers (passthrough)
    p.jd ->> 'external_id' AS external_id_raw,
    p.jd ->> 'listing_id'  AS listing_id_raw,

    -- LATERAL-extracted contacts & emails (projected so downstream CTEs can use them)
    c.emails_all,
    c.contacts_raw,
    c.contact_person_raw,
    c.contact_phone_raw

  FROM parsed p
  LEFT JOIN LATERAL (
    SELECT
      -- All emails collected from multiple shapes
      (
        SELECT ARRAY(
          SELECT DISTINCT e
          FROM (
            -- explicit "emails" field (array or string)
            SELECT NULLIF(btrim(x), '') AS e
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
            ) q(x)
            UNION ALL
            -- contacts[].email / emailAddress
            SELECT NULLIF(btrim(c1 ->> 'email'), '')
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.jd -> 'contacts') = 'array' THEN p.jd -> 'contacts' ELSE '[]'::jsonb END) c1
            UNION ALL
            SELECT NULLIF(btrim(c2 ->> 'emailAddress'), '')
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.jd -> 'contacts') = 'array' THEN p.jd -> 'contacts' ELSE '[]'::jsonb END) c2
          ) z
          WHERE e IS NOT NULL
        )
      ) AS emails_all,

      -- Raw contacts JSON
      CASE
        WHEN jsonb_typeof(p.jd -> 'contacts') IN ('array','object') THEN p.jd -> 'contacts'
        ELSE NULL
      END AS contacts_raw,

      -- First contact person (personName/name/person)
      (
        SELECT NULLIF(btrim(val), '')
        FROM (
          SELECT COALESCE(c ->> 'personName', c ->> 'name', c ->> 'person') AS val
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.jd -> 'contacts') = 'array' THEN p.jd -> 'contacts' ELSE '[]'::jsonb END) c
          WHERE COALESCE(c ->> 'personName', c ->> 'name', c ->> 'person') IS NOT NULL
          LIMIT 1
        ) s1
      ) AS contact_person_raw,

      -- First contact phone (phone/telephone/tel/mobile/phoneNumber)
      (
        SELECT NULLIF(btrim(val), '')
        FROM (
          SELECT COALESCE(c ->> 'phone', c ->> 'telephone', c ->> 'tel', c ->> 'mobile', c ->> 'phoneNumber') AS val
          FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.jd -> 'contacts') = 'array' THEN p.jd -> 'contacts' ELSE '[]'::jsonb END) c
          WHERE COALESCE(c ->> 'phone', c ->> 'telephone', c ->> 'tel', c ->> 'mobile', c ->> 'phoneNumber') IS NOT NULL
          LIMIT 1
        ) s2
      ) AS contact_phone_raw
  ) c ON TRUE
),
norm AS (
  SELECT
    'stepstone'::text AS source,
    f.source_site,
    f.source_id,

    -- Stable pointer back to listing
    util.url_canonical(COALESCE(f.job_url_raw, f.apply_url_raw)) AS source_row_url,

    -- Timestamps
    CASE WHEN f.scraped_at_text IS NOT NULL THEN f.scraped_at_text::timestamptz ELSE NULL END AS scraped_at,
    CASE
      WHEN f.date_posted_text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN f.date_posted_text::date
      WHEN regexp_replace(f.date_posted_text, '\s+', '', 'g') ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$'
        THEN to_date(regexp_replace(f.date_posted_text, '\s+', '', 'g'), 'FMDD.FMMM.YYYY')
      ELSE NULL
    END AS date_posted,

    -- Titles
    f.title_raw,
    NULL::text AS title_norm,

    -- Company names (raw + normalized variants)
    f.company_raw,
    util.company_name_norm_langless(f.company_raw) AS company_name_norm_langless,
    util.company_name_norm(f.company_raw)          AS company_name_norm,

    -- Content
    f.description_raw,

    -- Location
    f.location_raw,
    lp.city    AS city_guess,
    lp.region  AS region_guess,
    lp.country AS country_guess,

    -- Job meta
    f.contract_type_raw,
    CASE
      WHEN f.work_type_raw ILIKE '%remote%' THEN TRUE
      WHEN f.work_type_raw ILIKE '%home office%' OR f.work_type_raw ILIKE '%homeoffice%' THEN TRUE
      WHEN f.work_type_raw ILIKE '%hybrid%' THEN TRUE
      WHEN f.title_raw ILIKE '%remote%' OR f.location_raw ILIKE '%remote%' THEN TRUE
      WHEN f.description_raw ILIKE '%homeoffice%' OR f.description_raw ILIKE '%home office%' THEN TRUE
      WHEN f.description_raw ILIKE '%hybrid%' THEN TRUE
      ELSE NULL
    END AS is_remote,

    -- Compensation
    CASE WHEN f.salary_min_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_min_raw::numeric ELSE NULL END AS salary_min,
    CASE WHEN f.salary_max_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN f.salary_max_raw::numeric ELSE NULL END AS salary_max,
    NULLIF(f.salary_currency_raw,'') AS currency,
    CASE
      WHEN f.salary_interval_raw ILIKE 'hour%'  THEN 'hourly'
      WHEN f.salary_interval_raw ILIKE 'day%'   THEN 'daily'
      WHEN f.salary_interval_raw ILIKE 'week%'  THEN 'weekly'
      WHEN f.salary_interval_raw ILIKE 'month%' THEN 'monthly'
      WHEN f.salary_interval_raw ILIKE 'year%'  THEN 'yearly'
      ELSE NULL
    END AS salary_interval,
    NULLIF(f.salary_source_raw,'') AS salary_source,

    -- Listing URL
    f.job_url_raw                                AS job_url_raw,
    util.url_canonical(f.job_url_raw)            AS job_url_canonical,
    (regexp_match(coalesce(util.url_canonical(f.job_url_raw),''), '/jobs/view/([0-9]+)'))[1] AS linkedin_job_id,

    -- Apply URL
    COALESCE(f.apply_url_raw, f.job_url_raw)     AS apply_url_raw,
    util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)) AS apply_url_canonical,
    util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))              AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(COALESCE(f.apply_url_raw, f.job_url_raw)))) AS apply_root,

    -- Company website (filter aggregator/ATS/career)
    f.company_website_raw,
    util.url_canonical(f.company_website_raw)    AS company_website_canonical,
    CASE
      WHEN util.url_host(util.url_canonical(f.company_website_raw)) = 'linkedin.com'
        THEN util.url_canonical(f.company_website_raw)
      ELSE NULL
    END AS company_linkedin_url,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.url_canonical(f.company_website_raw)
    END AS company_website,
    CASE
      WHEN util.is_aggregator_host(util.url_host(util.url_canonical(f.company_website_raw))) THEN NULL
      WHEN util.is_ats_host(util.url_host(util.url_canonical(f.company_website_raw)))        THEN NULL
      WHEN util.is_career_host(util.url_host(util.url_canonical(f.company_website_raw)))     THEN NULL
      ELSE util.org_domain(util.url_host(util.url_canonical(f.company_website_raw)))
    END AS company_domain,

    -- Emails → aggregated string + domains
    CASE
      WHEN f.emails_all IS NULL OR array_length(f.emails_all,1) IS NULL THEN NULL
      ELSE array_to_string(f.emails_all, '; ')
    END AS emails_raw,
    f.emails_all,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(
           CASE WHEN f.emails_all IS NULL OR array_length(f.emails_all,1) IS NULL
                THEN NULL
                ELSE array_to_string(f.emails_all, '; ')
           END)))
      THEN NULL
      ELSE util.email_domain(util.first_email(
             CASE WHEN f.emails_all IS NULL OR array_length(f.emails_all,1) IS NULL
                  THEN NULL
                  ELSE array_to_string(f.emails_all, '; ')
             END))
    END AS contact_email_domain,
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(
           CASE WHEN f.emails_all IS NULL OR array_length(f.emails_all,1) IS NULL
                THEN NULL
                ELSE array_to_string(f.emails_all, '; ')
           END)))
      THEN NULL
      ELSE util.org_domain(util.email_domain(util.first_email(
             CASE WHEN f.emails_all IS NULL OR array_length(f.emails_all,1) IS NULL
                  THEN NULL
                  ELSE array_to_string(f.emails_all, '; ')
             END)))
    END AS contact_email_root,

    -- Contacts
    f.contacts_raw,
    f.contact_person_raw,
    f.contact_phone_raw,

    -- Company enrichment passthroughs
    f.company_size_raw,
    f.company_industry_raw,
    f.company_logo_url,
    f.company_description_raw,
    f.company_address_raw,
    f.company_stepstone_id,
    f.company_active_jobs,
    f.company_hero_url,
    CASE
      WHEN f.company_founded_year_raw ~ '^[0-9]{4}$' THEN (f.company_founded_year_raw)::int
      ELSE NULL
    END AS company_founded_year,

    -- External ids passthrough
    f.external_id_raw,
    f.listing_id_raw

  FROM fields f
  LEFT JOIN LATERAL util.location_parse(f.location_raw) lp ON TRUE
),
-- Dedup within StepStone: prefer job_url_canonical → apply_url_canonical → content hash
keys AS (
  SELECT
    k.*,
    COALESCE(
      k.job_url_canonical,
      k.apply_url_canonical,
      'hash:' || md5(coalesce(k.title_raw,'') || '|' || coalesce(k.company_raw,'') || '|' || coalesce(k.location_raw,''))
    ) AS dedup_key
  FROM norm k
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
  salary_source,

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
  emails_all,
  contact_email_domain,
  contact_email_root,
  contacts_raw,
  contact_person_raw,
  contact_phone_raw,

  company_size_raw,
  company_industry_raw,
  company_logo_url,
  company_description_raw,
  company_address_raw,
  company_stepstone_id,
  company_active_jobs,
  company_hero_url,
  company_founded_year,

  external_id_raw,
  listing_id_raw
FROM dedup;
