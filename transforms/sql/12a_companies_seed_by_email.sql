WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root
  FROM silver.unified s
  LEFT JOIN gold.company gc
    ON (s.company_domain IS NOT NULL AND util.same_org_domain(gc.website_domain, s.company_domain))
    OR (s.company_domain IS NULL AND util.company_name_norm(gc.name) = util.company_name_norm(s.company_name))
  WHERE gc.company_id IS NULL
    AND s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
)
INSERT INTO gold.company (name)
SELECT c.company_name
FROM cand c
LEFT JOIN gold.company gc ON util.company_name_norm(gc.name) = c.name_norm
WHERE gc.company_id IS NULL AND c.email_root IS NOT NULL;
