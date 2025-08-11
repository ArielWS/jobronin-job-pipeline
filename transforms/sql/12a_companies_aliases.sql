WITH map AS (
  SELECT
    s.company_name,
    COALESCE(gc.company_id, gc2.company_id, gc3.company_id) AS company_id
  FROM silver.unified s
  LEFT JOIN gold.company gc
    ON s.company_domain IS NOT NULL AND util.same_org_domain(gc.website_domain, s.company_domain)
  LEFT JOIN gold.company gc2
    ON gc.company_id IS NULL
   AND s.contact_email_root IS NOT NULL AND NOT util.is_generic_email_domain(s.contact_email_root)
   AND gc2.name_norm = util.company_name_norm(s.company_name)
  LEFT JOIN gold.company gc3
    ON gc.company_id IS NULL AND gc2.company_id IS NULL
   AND gc3.name_norm = util.company_name_norm(s.company_name)
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
)
INSERT INTO gold.company_alias (company_id, alias)
SELECT DISTINCT company_id, company_name
FROM map
WHERE company_id IS NOT NULL
ON CONFLICT (company_id, alias_norm) DO NOTHING;
