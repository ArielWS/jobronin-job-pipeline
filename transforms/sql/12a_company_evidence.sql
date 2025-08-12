-- transforms/sql/12a_company_evidence.sql
-- Write domain/email/apply evidence per company using only cleaned silver.unified fields.
-- NO JSON parsing here.

BEGIN;

WITH src AS (
  SELECT
    s.source,
    s.source_id,
    s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    -- candidates
    util.org_domain(NULLIF(s.company_domain,'')) AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'') AS apply_root_raw
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
),

resolved AS (
  -- Find the matching company_id:
  -- 1) prefer a domain match (if site_root_raw is a real site, not aggregator/ATS)
  -- 2) else fall back to name_norm
  SELECT
    s.*,
    COALESCE(
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE s.site_root_raw IS NOT NULL
          AND NOT util.is_aggregator_host(s.site_root_raw)
          AND NOT util.is_ats_host(s.site_root_raw)
          AND gc.website_domain = s.site_root_raw
        LIMIT 1
      ),
      (
        SELECT gc2.company_id
        FROM gold.company gc2
        WHERE gc2.name_norm = s.name_norm
        LIMIT 1
      )
    ) AS company_id
  FROM src s
),

evidence_rows AS (
  SELECT r.company_id, 'website'::text AS kind, r.site_root_raw AS val, r.source, r.source_id
  FROM resolved r
  WHERE r.company_id IS NOT NULL
    AND r.site_root_raw IS NOT NULL
    AND NOT util.is_aggregator_host(r.site_root_raw)
    AND NOT util.is_ats_host(r.site_root_raw)

  UNION ALL
  SELECT r.company_id, 'email', r.email_root_raw, r.source, r.source_id
  FROM resolved r
  WHERE r.company_id IS NOT NULL
    AND r.email_root_raw IS NOT NULL
    AND NOT util.is_generic_email_domain(r.email_root_raw)

  UNION ALL
  SELECT r.company_id, 'apply', r.apply_root_raw, r.source, r.source_id
  FROM resolved r
  WHERE r.company_id IS NOT NULL
    AND r.apply_root_raw IS NOT NULL
    AND NOT util.is_aggregator_host(r.apply_root_raw)
    AND NOT util.is_ats_host(r.apply_root_raw)
)

INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
SELECT DISTINCT company_id, kind, val, source, source_id
FROM evidence_rows
ON CONFLICT (company_id, kind, value) DO NOTHING;

COMMIT;
