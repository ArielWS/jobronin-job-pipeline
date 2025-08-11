WITH cand AS (
  SELECT DISTINCT
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
)
INSERT INTO gold.company (name)
SELECT cnd.company_name
FROM cand cnd
WHERE cnd.email_root IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM gold.company gc
    WHERE util.company_name_norm(gc.name) = cnd.name_norm
  );
