-- placeholder for deterministic job upsert (ATS/apply URL)
WITH cand AS (
  SELECT
    s.source,
    s.source_id,
    s.source_row_url,
    s.job_url_direct,
    -- normalize: strip ?query/#fragment and trailing slash
    regexp_replace(regexp_replace(s.job_url_direct, '[?#].*$', ''), '/$', '') AS apply_url_clean,
    s.title_norm,
    s.company_name,
    s.city_guess,
    s.region_guess,
    s.country_guess,
    s.date_posted,
    NULLIF(s.description_raw, '') AS description_raw,
    s.is_remote,
    s.contract_type_raw,
    s.salary_min,
    s.salary_max,
    s.currency
  FROM silver.unified s
  WHERE s.job_url_direct IS NOT NULL AND s.job_url_direct <> ''
),
linked AS (
  SELECT c.*, comp.company_id
  FROM cand c
  JOIN gold.company comp
    ON comp.name_norm = lower(btrim(c.company_name))
),
ins_jobs AS (
  INSERT INTO gold.job_post
    (company_id, title_norm, city, region, country, date_posted,
     job_url_direct, apply_url_clean, description, is_remote,
     contract_type_raw, salary_min, salary_max, currency)
  SELECT
    l.company_id, l.title_norm, l.city_guess, l.region_guess, l.country_guess,
    l.date_posted, l.job_url_direct, l.apply_url_clean, l.description_raw,
    l.is_remote, l.contract_type_raw, l.salary_min, l.salary_max, l.currency
  FROM linked l
  LEFT JOIN gold.job_post jp
    ON jp.apply_url_clean = l.apply_url_clean
  WHERE jp.job_id IS NULL
  RETURNING job_id, apply_url_clean
)
INSERT INTO gold.job_source_link (source, source_id, job_id, source_row_url)
SELECT c.source, c.source_id, jp.job_id, c.source_row_url
FROM cand c
JOIN gold.job_post jp
  ON jp.apply_url_clean = c.apply_url_clean
ON CONFLICT (source, source_id) DO NOTHING;
