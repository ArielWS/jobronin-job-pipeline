-- transforms/sql/03_unified_stage.sql
-- Unified Silver "stage" (evidence-preserving, no cross-source dedupe)
-- Combines silver.jobspy, silver.profesia_sk, silver.stepstone into one schema.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE VIEW silver.unified_silver AS
WITH jobspy_rows AS (
  SELECT
    -- Identity & lineage
    j.source::text                                   AS source,
    NULL::text                                       AS source_site,
    j.source_id::text                                AS source_id,
    j.source_row_url::text                           AS source_row_url,
    j.scraped_at                                     AS scraped_at,
    j.date_posted                                    AS date_posted,

    -- URLs & hosts (JobSpy: derive from source_row_url)
    j.source_row_url::text                           AS job_url_raw,
    util.url_canonical(j.source_row_url)             AS job_url_canonical,
    (regexp_match(COALESCE(util.url_canonical(j.source_row_url), ''), '/jobs/view/([0-9]+)'))[1]
                                                     AS linkedin_job_id,
    j.source_row_url::text                           AS apply_url_raw,
    util.url_canonical(j.source_row_url)             AS apply_url_canonical,
    util.url_host(util.url_canonical(j.source_row_url))                 AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(j.source_row_url))) AS apply_root,

    -- Title & content
    j.title_raw::text                                AS title_raw,
    j.title_norm::text                               AS title_norm,
    j.description_raw::text                          AS description_raw,

    -- Company
    j.company_raw::text                              AS company_raw,
    util.company_name_norm_langless(j.company_raw)   AS company_name_norm_langless,
    util.company_name_norm(j.company_raw)            AS company_name_norm,
    j.company_website_raw::text                      AS company_website_raw,
    util.url_canonical(j.company_website_raw)        AS company_website_canonical,
    j.company_linkedin_url::text                     AS company_linkedin_url,
    j.company_website::text                          AS company_website,
    j.company_domain::text                           AS company_domain,
    j.company_description_raw::text                  AS company_description_raw,
    j.company_size_raw::text                         AS company_size_raw,
    j.company_industry_raw::text                     AS company_industry_raw,
    j.company_logo_url::text                         AS company_logo_url,
    j.company_location_raw::text                     AS company_location_raw,
    NULL::text                                       AS company_address_raw,
    NULL::text                                       AS company_stepstone_id,
    NULL::int                                        AS company_active_jobs,
    NULL::text                                       AS company_hero_url,
    NULL::int                                        AS company_founded_year,

    -- Location
    j.location_raw::text                             AS location_raw,
    j.city_guess::text                               AS city_guess,
    j.region_guess::text                             AS region_guess,
    j.country_guess::text                            AS country_guess,

    -- Job meta
    j.contract_type_raw::text                        AS contract_type_raw,
    NULL::text                                       AS work_type_raw,
    NULL::text                                       AS job_type_raw,
    NULL::text                                       AS job_function_raw,
    NULL::text                                       AS job_level_raw,
    NULL::text                                       AS remote_type_raw,
    j.is_remote                                      AS is_remote,

    -- Compensation
    j.salary_min::numeric                            AS salary_min,
    j.salary_max::numeric                            AS salary_max,
    j.currency::text                                 AS currency,
    NULL::text                                       AS salary_interval,
    NULL::text                                       AS salary_source,

    -- Contacts & socials
    j.emails_raw::text                               AS emails_raw,
    CASE
      WHEN j.emails_raw IS NULL OR btrim(j.emails_raw) = '' THEN NULL
      ELSE regexp_split_to_array(j.emails_raw, '\s*[,;]\s*')
    END                                              AS emails_all,
    j.contact_email_domain::text                     AS contact_email_domain,
    j.contact_email_root::text                       AS contact_email_root,
    NULL::jsonb                                      AS contacts_raw,
    NULL::text                                       AS contact_person_raw,
    NULL::text                                       AS contact_phone_raw,
    NULL::text                                       AS social_links_raw,

    -- External IDs
    NULL::text                                       AS external_id_raw,
    NULL::text                                       AS listing_id_raw

  FROM silver.jobspy j
),
profesia_rows AS (
  SELECT
    -- Identity & lineage
    p.source::text                                   AS source,
    p.source_site::text                              AS source_site,
    p.source_id::text                                AS source_id,
    p.source_row_url::text                           AS source_row_url,
    p.scraped_at                                     AS scraped_at,
    p.date_posted                                    AS date_posted,

    -- URLs & hosts
    p.job_url_raw::text                              AS job_url_raw,
    util.url_canonical(p.job_url_raw)                AS job_url_canonical,
    (regexp_match(
       COALESCE(util.url_canonical(p.job_url_raw),
                util.url_canonical(p.apply_url_raw),
                ''),
       '/jobs/view/([0-9]+)'))[1]                    AS linkedin_job_id,
    p.apply_url_raw::text                            AS apply_url_raw,
    util.url_canonical(p.apply_url_raw)              AS apply_url_canonical,
    util.url_host(util.url_canonical(p.apply_url_raw))               AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(p.apply_url_raw))) AS apply_root,

    -- Title & content
    p.title_raw::text                                AS title_raw,
    p.title_norm::text                               AS title_norm,
    p.description_raw::text                          AS description_raw,

    -- Company
    p.company_raw::text                              AS company_raw,
    p.company_name_norm_langless::text               AS company_name_norm_langless,
    p.company_name_norm::text                        AS company_name_norm,
    p.company_website_raw::text                      AS company_website_raw,
    util.url_canonical(p.company_website_raw)        AS company_website_canonical,
    p.company_linkedin_url::text                     AS company_linkedin_url,
    p.company_website::text                          AS company_website,
    p.company_domain::text                           AS company_domain,
    p.company_description_raw::text                  AS company_description_raw,
    p.company_size_raw::text                         AS company_size_raw,
    p.company_industry_raw::text                     AS company_industry_raw,
    p.company_logo_url::text                         AS company_logo_url,
    p.company_location_raw::text                     AS company_location_raw,
    NULL::text                                       AS company_address_raw,
    NULL::text                                       AS company_stepstone_id,
    NULL::int                                        AS company_active_jobs,
    NULL::text                                       AS company_hero_url,
    NULL::int                                        AS company_founded_year,

    -- Location
    p.location_raw::text                             AS location_raw,
    p.city_guess::text                               AS city_guess,
    p.region_guess::text                             AS region_guess,
    p.country_guess::text                            AS country_guess,

    -- Job meta
    p.contract_type_raw::text                        AS contract_type_raw,
    NULL::text                                       AS work_type_raw,
    NULL::text                                       AS job_type_raw,
    NULL::text                                       AS job_function_raw,
    p.job_level_raw::text                            AS job_level_raw,
    p.remote_type_raw::text                          AS remote_type_raw,
    p.is_remote                                      AS is_remote,

    -- Compensation
    p.salary_min::numeric                            AS salary_min,
    p.salary_max::numeric                            AS salary_max,
    p.currency::text                                 AS currency,
    p.salary_interval::text                          AS salary_interval,
    p.salary_source::text                            AS salary_source,

    -- Contacts & socials
    p.emails_raw::text                               AS emails_raw,
    CASE
      WHEN p.emails_raw IS NULL OR btrim(p.emails_raw) = '' THEN NULL
      ELSE regexp_split_to_array(p.emails_raw, '\s*[,;]\s*')
    END                                              AS emails_all,
    p.contact_email_domain::text                     AS contact_email_domain,
    p.contact_email_root::text                       AS contact_email_root,
    NULL::jsonb                                      AS contacts_raw,
    p.contact_person_raw::text                       AS contact_person_raw,
    p.contact_phone_raw::text                        AS contact_phone_raw,
    p.social_links_raw::text                         AS social_links_raw,

    -- External IDs
    NULL::text                                       AS external_id_raw,
    NULL::text                                       AS listing_id_raw

  FROM silver.profesia_sk p
),
stepstone_rows AS (
  SELECT
    -- Identity & lineage
    s.source::text                                   AS source,
    s.source_site::text                              AS source_site,
    s.source_id::text                                AS source_id,
    s.source_row_url::text                           AS source_row_url,
    s.scraped_at                                     AS scraped_at,
    s.date_posted                                    AS date_posted,

    -- URLs & hosts
    s.job_url_raw::text                              AS job_url_raw,
    util.url_canonical(s.job_url_raw)                AS job_url_canonical,
    (regexp_match(
       COALESCE(util.url_canonical(s.job_url_raw),
                util.url_canonical(s.apply_url_raw),
                ''),
       '/jobs/view/([0-9]+)'))[1]                    AS linkedin_job_id,
    s.apply_url_raw::text                            AS apply_url_raw,
    util.url_canonical(s.apply_url_raw)              AS apply_url_canonical,
    util.url_host(util.url_canonical(s.apply_url_raw))               AS apply_domain,
    util.org_domain(util.url_host(util.url_canonical(s.apply_url_raw))) AS apply_root,

    -- Title & content
    s.title_raw::text                                AS title_raw,
    s.title_norm::text                               AS title_norm,
    s.description_raw::text                          AS description_raw,

    -- Company
    s.company_raw::text                              AS company_raw,
    s.company_name_norm_langless::text               AS company_name_norm_langless,
    s.company_name_norm::text                        AS company_name_norm,
    s.company_website_raw::text                      AS company_website_raw,
    util.url_canonical(s.company_website_raw)        AS company_website_canonical,
    s.company_linkedin_url::text                     AS company_linkedin_url,
    s.company_website::text                          AS company_website,
    s.company_domain::text                           AS company_domain,
    s.company_description_raw::text                  AS company_description_raw,
    s.company_size_raw::text                         AS company_size_raw,
    s.company_industry_raw::text                     AS company_industry_raw,
    s.company_logo_url::text                         AS company_logo_url,
    NULL::text                                       AS company_location_raw,
    s.company_address_raw::text                      AS company_address_raw,
    s.company_stepstone_id::text                     AS company_stepstone_id,
    CASE
      WHEN s.company_active_jobs ~ '^[0-9]+$' THEN s.company_active_jobs::int
      ELSE NULL
    END                                              AS company_active_jobs,
    s.company_hero_url::text                         AS company_hero_url,
    s.company_founded_year                           AS company_founded_year,

    -- Location
    s.location_raw::text                             AS location_raw,
    s.city_guess::text                               AS city_guess,
    s.region_guess::text                             AS region_guess,
    s.country_guess::text                            AS country_guess,

    -- Job meta
    s.contract_type_raw::text                        AS contract_type_raw,
    s.work_type_raw::text                            AS work_type_raw,
    s.job_type_raw::text                             AS job_type_raw,
    s.job_function_raw::text                         AS job_function_raw,
    NULL::text                                       AS job_level_raw,
    NULL::text                                       AS remote_type_raw,
    s.is_remote                                      AS is_remote,

    -- Compensation
    s.salary_min::numeric                            AS salary_min,
    s.salary_max::numeric                            AS salary_max,
    s.currency::text                                 AS currency,
    s.salary_interval::text                          AS salary_interval,
    s.salary_source::text                            AS salary_source,

    -- Contacts & socials
    s.emails_raw::text                               AS emails_raw,
    s.emails_all                                     AS emails_all,
    s.contact_email_domain::text                     AS contact_email_domain,
    s.contact_email_root::text                       AS contact_email_root,
    s.contacts_raw                                   AS contacts_raw,
    s.contact_person_raw::text                       AS contact_person_raw,
    s.contact_phone_raw::text                        AS contact_phone_raw,
    NULL::text                                       AS social_links_raw,

    -- External IDs
    s.external_id_raw::text                          AS external_id_raw,
    s.listing_id_raw::text                           AS listing_id_raw

  FROM silver.stepstone s
)
SELECT * FROM jobspy_rows
UNION ALL
SELECT * FROM profesia_rows
UNION ALL
SELECT * FROM stepstone_rows;
