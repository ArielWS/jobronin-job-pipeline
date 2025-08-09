-- transforms/sql/01_silver_jobspy.sql
CREATE OR REPLACE VIEW silver.jobspy AS
SELECT
  'jobspy'::text                          AS source,
  js.id::uuid                             AS source_id,
  js.job_url                              AS source_row_url,
  COALESCE(NULLIF(js.job_url_direct, ''), js.job_url) AS job_url_direct,

  js.title                                AS title_raw,
  lower(btrim(js.title))                  AS title_norm,

  js.company                              AS company_raw,
  btrim(js.company)                       AS company_name,

  js."location"                           AS location_raw,
  NULLIF(btrim(split_part(js."location", ',', 1)), '') AS city_guess,
  NULLIF(btrim(split_part(js."location", ',', 2)), '') AS region_guess,
  NULLIF(btrim(split_part(js."location", ',', 3)), '') AS country_guess,

  js.date_posted                          AS date_posted,
  js.is_remote                            AS is_remote,
  js.job_type                             AS contract_type_raw,

  js.min_amount::numeric                  AS salary_min,
  js.max_amount::numeric                  AS salary_max,
  js.currency                             AS currency,

  js.description                          AS description_raw,
  js.emails                               AS emails_raw
FROM public.jobspy_job_scrape js;
