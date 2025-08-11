WITH m AS (
  SELECT
    s.source, s.source_id,
    COALESCE(gc.company_id, gc2.company_id) AS company_id,
    NULLIF(s.company_domain,'') AS website_domain,
    CASE
      WHEN util.is_generic_email_domain(s.contact_email_domain) THEN NULL
      ELSE s.contact_email_domain
    END AS email_domain,
    NULLIF(s.apply_domain,'') AS apply_domain
  FROM silver.unified s
  LEFT JOIN gold.company gc
    ON s.company_domain IS NOT NULL AND gc.website_domain = s.company_domain
  LEFT JOIN gold.company gc2
    ON s.company_domain IS NULL AND gc2.name_norm = util.company_name_norm(s.company_name)
)
INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT m.company_id, 'website', m.website_domain, m.source, m.source_id
FROM m WHERE m.company_id IS NOT NULL AND m.website_domain IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;

INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT m.company_id, 'email', m.email_domain, m.source, m.source_id
FROM m WHERE m.company_id IS NOT NULL AND m.email_domain IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;

INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT m.company_id, 'apply', m.apply_domain, m.source, m.source_id
FROM m WHERE m.company_id IS NOT NULL AND m.apply_domain IS NOT NULL
ON CONFLICT (kind, value) DO NOTHING;
