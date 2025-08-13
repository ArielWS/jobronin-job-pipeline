-- transforms/sql/12e_company_promote_domain.sql
-- Idempotent promotion + upgrade of website_domain from evidence.
-- Order: run AFTER 12a_company_evidence.sql (and before monitoring checks).

BEGIN;

-- A) UPGRADE to a trustworthy WEBSITE domain if we currently have null or a weaker domain.
UPDATE gold.company gc
SET website_domain = w.value
FROM gold.company_evidence_domain w
WHERE w.company_id = gc.company_id
  AND w.kind = 'website'
  AND w.value IS NOT NULL
  AND NOT util.is_aggregator_host(w.value)
  AND NOT util.is_ats_host(w.value)
  AND NOT util.is_career_host(w.value)
  AND gc.website_domain IS DISTINCT FROM w.value
  AND (
        gc.website_domain IS NULL
        OR EXISTS (
             SELECT 1
             FROM gold.company_evidence_domain e
             WHERE e.company_id = gc.company_id
               AND e.kind = 'email'
               AND e.value = gc.website_domain
        )
        OR util.is_aggregator_host(gc.website_domain)
        OR util.is_ats_host(gc.website_domain)
        OR util.is_career_host(gc.website_domain)
      )
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = w.value
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

-- B) FILL from WEBSITE evidence if still NULL.
UPDATE gold.company gc
SET website_domain = w.value
FROM gold.company_evidence_domain w
WHERE w.company_id = gc.company_id
  AND w.kind = 'website'
  AND gc.website_domain IS NULL
  AND w.value IS NOT NULL
  AND NOT util.is_aggregator_host(w.value)
  AND NOT util.is_ats_host(w.value)
  AND NOT util.is_career_host(w.value)
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = w.value
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

-- C) FINAL BACKFILL from EMAIL root if still NULL (non-generic, non-ATS/aggregator/career).
UPDATE gold.company gc
SET website_domain = e.value
FROM gold.company_evidence_domain e
WHERE e.company_id = gc.company_id
  AND e.kind = 'email'
  AND gc.website_domain IS NULL
  AND e.value IS NOT NULL
  AND NOT util.is_generic_email_domain(e.value)
  AND NOT util.is_aggregator_host(e.value)
  AND NOT util.is_ats_host(e.value)
  AND NOT util.is_career_host(e.value)
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = e.value
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

COMMIT;
