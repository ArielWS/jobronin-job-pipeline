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
  js.emails                               AS emails_raw,

  -- Company website: ignore aggregator hosts; keep full URL + root domain
  CASE
    WHEN util.is_aggregator_host(util.url_host(COALESCE(NULLIF(js.company_url_direct,''), NULLIF(js.company_url,''))))
      THEN NULL
    ELSE COALESCE(NULLIF(js.company_url_direct,''), NULLIF(js.company_url,''))
  END AS company_website,
  CASE
    WHEN util.is_aggregator_host(util.url_host(COALESCE(NULLIF(js.company_url_direct,''), NULLIF(js.company_url,''))))
      THEN NULL
    ELSE util.org_domain(util.url_host(COALESCE(NULLIF(js.company_url_direct,''), NULLIF(js.company_url,''))))
  END AS company_domain,

  -- Email/apply domains and their org roots
  util.email_domain(NULLIF(js.emails,''))                                        AS contact_email_domain,
  util.org_domain(util.email_domain(NULLIF(js.emails,'')))                       AS contact_email_root,
  util.url_host(COALESCE(NULLIF(js.job_url_direct,''), js.job_url))              AS apply_domain,
  util.org_domain(util.url_host(COALESCE(NULLIF(js.job_url_direct,''), js.job_url))) AS apply_root

FROM public.jobspy_job_scrape js;
