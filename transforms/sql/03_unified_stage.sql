CREATE OR REPLACE VIEW silver.unified AS
SELECT
  source,
  source_id::text                 AS source_id,
  source_row_url,
  job_url_direct,
  title_raw,
  title_norm,
  company_raw,
  company_name,
  location_raw,
  city_guess,
  region_guess,
  country_guess,
  date_posted,
  is_remote,
  contract_type_raw,
  salary_min::numeric             AS salary_min,
  salary_max::numeric             AS salary_max,
  currency,
  description_raw,
  emails_raw
FROM silver.jobspy

UNION ALL

SELECT
  source,
  source_id::text                 AS source_id,
  source_row_url,
  job_url_direct,
  title_raw,
  title_norm,
  company_raw,
  company_name,
  location_raw,
  city_guess,
  region_guess,
  country_guess,
  date_posted,
  is_remote,
  contract_type_raw,
  salary_min::numeric             AS salary_min,
  salary_max::numeric             AS salary_max,
  currency,
  description_raw,
  emails_raw
FROM silver.stepstone;
