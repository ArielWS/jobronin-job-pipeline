CREATE OR REPLACE VIEW silver.stepstone AS
WITH raw AS (
  SELECT
    'stepstone'::text AS source,
    ss.id             AS source_id,
    ss.location       AS search_location_raw,
    ss.job_data       AS job_data_text
  FROM public.stepstone_job_scrape ss
),
clean AS (
  SELECT
    source,
    source_id,
    search_location_raw,
    regexp_replace(
      regexp_replace(
        regexp_replace(
          regexp_replace(job_data_text, ':\s*NaN(\s*[,}])', ': null\1', 'g'),
          ':\s*Infinity(\s*[,}])',       ': null\1', 'g'),
        ':\s*-Infinity(\s*[,}])',        ': null\1', 'g'),
      ':\s*None(\s*[,}])',               ': null\1', 'g'
    ) AS job_data_clean
  FROM raw
),
base AS (
  SELECT
    source,
    source_id,
    search_location_raw,
    job_data_clean::jsonb AS j
  FROM clean
)
SELECT
  b.source,
  b.source_id,
  (b.j->>'job_url')                         AS source_row_url,
  COALESCE(NULLIF(b.j->>'job_url_direct',''), b.j->>'job_url') AS job_url_direct,

  (b.j->>'title')                           AS title_raw,
  lower(btrim(b.j->>'title'))               AS title_norm,

  (b.j->>'company')                         AS company_raw,
  btrim(b.j->>'company')                    AS company_name,

  COALESCE(b.j->>'location','')             AS location_raw,
  NULLIF(btrim(split_part(COALESCE(b.j->>'job_location', b.j->>'location', ''), ',', 1)), '') AS city_guess,
  NULL::text                                AS region_guess,
  NULL::text                                AS country_guess,

  NULLIF(b.j->>'date_posted','')::date      AS date_posted,

  CASE
    WHEN lower(COALESCE(b.j->>'work_type','')) LIKE '%homeoffice%' THEN TRUE
    WHEN lower(COALESCE(b.j->>'work_type','')) LIKE '%remote%'     THEN TRUE
    ELSE NULL
  END                                        AS is_remote,

  (b.j->>'contract_type')                    AS contract_type_raw,

  NULLIF(b.j->>'min_amount','')::numeric     AS salary_min,
  NULLIF(b.j->>'max_amount','')::numeric     AS salary_max,
  NULLIF(b.j->>'currency','')                AS currency,

  b.j->>'description'                        AS description_raw,
  b.j->>'emails'                             AS emails_raw,

  -- Company website + root domain from profile (handles /karriere etc.)
  (b.j->'company_profile'->>'website')                           AS company_website,
  util.org_domain(util.url_host(b.j->'company_profile'->>'website')) AS company_domain,

  -- Email/apply domains and their org roots
  util.email_domain(NULLIF(b.j->>'emails',''))                    AS contact_email_domain,
  util.org_domain(util.email_domain(NULLIF(b.j->>'emails','')))   AS contact_email_root,
  util.url_host(COALESCE(NULLIF(b.j->>'job_url_direct',''), b.j->>'job_url')) AS apply_domain,
  util.org_domain(util.url_host(COALESCE(NULLIF(b.j->>'job_url_direct',''), b.j->>'job_url'))) AS apply_root

FROM base b;
