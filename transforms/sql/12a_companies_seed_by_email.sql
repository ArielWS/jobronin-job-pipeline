WITH no_company AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    CASE WHEN util.is_generic_email_domain(s.contact_email_domain) THEN NULL
         ELSE s.contact_email_domain END AS email_domain
  FROM silver.unified s
  LEFT JOIN gold.company gc
    ON s.company_domain IS NOT NULL AND gc.website_domain = s.company_domain
  WHERE gc.company_id IS NULL
    AND s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND s.contact_email_domain IS NOT NULL
    AND util.is_generic_email_domain(s.contact_email_domain) = FALSE
),
ins AS (
  INSERT INTO gold.company (name)
  SELECT company_name
  FROM no_company nc
  LEFT JOIN gold.company gc ON gc.name_norm = nc.name_norm
  WHERE gc.company_id IS NULL
  RETURNING company_id, name
)
-- alias for those
INSERT INTO gold.company_alias (company_id, alias)
SELECT DISTINCT gc.company_id, nc.company_name
FROM no_company nc
JOIN gold.company gc ON gc.name_norm = nc.name_norm
ON CONFLICT (company_id, alias_norm) DO NOTHING;
