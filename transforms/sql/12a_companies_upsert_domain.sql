BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    util.org_domain(NULLIF(s.company_domain,'')) AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'') AS apply_root_raw,
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
canon AS (
  SELECT
    src.*,
    CASE
      WHEN site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(site_root_raw)
           AND NOT util.is_ats_host(site_root_raw)
      THEN site_root_raw
      WHEN email_root_raw IS NOT NULL THEN email_root_raw
      ELSE NULL
    END AS org_root
  FROM src
),
brand AS (
  SELECT
    c.*,
    COALESCE((
      SELECT r.brand_key
      FROM gold.company_brand_rule r
      WHERE r.active = TRUE AND r.domain_root = c.org_root AND c.name_norm ~ r.brand_regex
      LIMIT 1
    ), ''::text) AS brand_key_norm
  FROM canon c
),
domain_pool AS (
  SELECT * FROM brand WHERE org_root IS NOT NULL
),
domain_winner AS (
  SELECT DISTINCT ON (org_root, brand_key_norm) *
  FROM domain_pool
  ORDER BY
    org_root, brand_key_norm,
    ((company_description_raw IS NOT NULL)::int
     + (company_size_raw IS NOT NULL)::int
     + (company_industry_raw IS NOT NULL)::int
     + (company_logo_url IS NOT NULL)::int) DESC
)
INSERT INTO gold.company (name, website_domain, brand_key, description, size_raw, industry_raw, logo_url)
SELECT
  w.company_name, w.org_root, w.brand_key_norm,
  w.company_description_raw, w.company_size_raw, w.company_industry_raw, w.company_logo_url
FROM domain_winner w
ON CONFLICT ON CONSTRAINT company_domain_brand_uniq DO UPDATE
SET name = CASE
             WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
             ELSE gold.company.name
           END,
    description  = COALESCE(gold.company.description,  EXCLUDED.description),
    size_raw     = COALESCE(gold.company.size_raw,     EXCLUDED.size_raw),
    industry_raw = COALESCE(gold.company.industry_raw, EXCLUDED.industry_raw),
    logo_url     = COALESCE(gold.company.logo_url,     EXCLUDED.logo_url);

COMMIT;
