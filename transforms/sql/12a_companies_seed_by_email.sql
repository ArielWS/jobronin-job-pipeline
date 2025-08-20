WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL
         ELSE s.contact_email_root END     AS email_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
filtered AS (SELECT * FROM cand WHERE email_root IS NOT NULL),
picked AS (
  SELECT DISTINCT ON (name_norm) company_name, name_norm, email_root
  FROM filtered
  ORDER BY name_norm, email_root
)
INSERT INTO gold.company (name, website_domain, brand_key)
SELECT company_name, email_root, '' AS brand_key
FROM picked
ON CONFLICT ON CONSTRAINT company_domain_brand_uniq_idx DO UPDATE
SET name = CASE
             WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
             ELSE gold.company.name
           END
RETURNING 1;
