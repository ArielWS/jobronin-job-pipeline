BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    NULLIF(s.apply_root,'') AS apply_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
name_pool AS (
  -- only rows w/o an org_root (domain was handled already)
  SELECT s.*
  FROM src s
  LEFT JOIN LATERAL util.org_domain(NULLIF(s.company_domain,'')) d(org_root) ON TRUE
  WHERE d.org_root IS NULL
),
name_winner AS (
  SELECT DISTINCT ON (name_norm) *
  FROM name_pool
  ORDER BY
    name_norm,
    ((company_description_raw IS NOT NULL)::int
     + (company_size_raw IS NOT NULL)::int
     + (company_industry_raw IS NOT NULL)::int
     + (company_logo_url IS NOT NULL)::int) DESC
),
to_insert AS (
  -- guard: skip if a company with this name_norm already exists (insert-only)
  SELECT w.*
  FROM name_winner w
  LEFT JOIN gold.company gc ON gc.name_norm = w.name_norm
  WHERE gc.company_id IS NULL
)
INSERT INTO gold.company (name, description, size_raw, industry_raw, logo_url)
SELECT
  ti.company_name,
  ti.company_description_raw, ti.company_size_raw, ti.company_industry_raw, ti.company_logo_url
FROM to_insert ti
ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO NOTHING;

COMMIT;
