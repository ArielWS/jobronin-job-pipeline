-- Unified Silver view: consistent column set across sources
-- NOTE: Column names align with the latest spec:
-- enrichment fields end with company_* (e.g., company_description_raw)

CREATE OR REPLACE VIEW silver.unified AS
SELECT
  source,
  source_id,
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

  salary_min,
  salary_max,
  currency,

  emails_raw,
  contact_email_domain,
  contact_email_root,

  apply_domain,
  apply_root,

  company_website,
  company_domain,

  company_size_raw,
  company_industry_raw,
  company_logo_url,
  company_description_raw
FROM silver.jobspy
UNION ALL
SELECT
  source,
  source_id,
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

  salary_min,
  salary_max,
  currency,

  emails_raw,
  contact_email_domain,
  contact_email_root,

  apply_domain,
  apply_root,

  company_website,
  company_domain,

  company_size_raw,
  company_industry_raw,
  company_logo_url,
  company_description_raw
FROM silver.stepstone;
