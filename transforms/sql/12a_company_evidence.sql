WITH m AS (
  SELECT
    s.source, s.source_id,
    (SELECT gc.company_id FROM gold.company gc
     WHERE gc.name_norm = util.company_name_norm(s.company_name)
     LIMIT 1) AS company_id,

    -- build a website_root candidate from either explicit website or domain
    COALESCE(
      util.org_domain(util.url_host(NULLIF(s.company_website,''))),
      util.org_domain(NULLIF(s.company_domain,''))
    ) AS website_root,

    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL
         ELSE s.contact_email_root END AS email_root,

    NULLIF(s.apply_root,'') AS apply_root
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
),
ev AS (
  SELECT company_id, 'website'::text AS kind, website_root AS value, source, source_id
  FROM m
  WHERE company_id IS NOT NULL
    AND website_root IS NOT NULL
    AND NOT util.is_aggregator_host(website_root)
    AND NOT util.is_ats_host(website_root)
    AND NOT util.is_career_host(website_root)
  UNION ALL
  SELECT company_id, 'email', email_root, source, source_id
  FROM m WHERE company_id IS NOT NULL AND email_root IS NOT NULL
  UNION ALL
  SELECT company_id, 'apply', apply_root, source, source_id
  FROM m
  WHERE company_id IS NOT NULL AND apply_root IS NOT NULL
    AND NOT util.is_aggregator_host(apply_root)
    AND NOT util.is_ats_host(apply_root)
)
INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT company_id, kind, value, source, source_id
FROM ev
ON CONFLICT (company_id, kind, value) DO NOTHING;
