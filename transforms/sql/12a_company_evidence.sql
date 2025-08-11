WITH m AS (
  SELECT
    s.source, s.source_id,
    COALESCE(gc.company_id, gc2.company_id, gc3.company_id) AS company_id,
    NULLIF(s.company_domain,'')         AS website_root,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root,
    NULLIF(s.apply_root,'')             AS apply_root
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
  WHERE COALESCE(gc.company_id, gc2.company_id, gc3.company_id) IS NOT NULL
)
INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT company_id, 'website', website_root, source, source_id
FROM m WHERE website_root IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;

INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT company_id, 'email', email_root, source, source_id
FROM m WHERE email_root IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;

INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT company_id, 'apply', apply_root, source, source_id
FROM m WHERE apply_root IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;
