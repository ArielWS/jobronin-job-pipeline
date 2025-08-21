-- transforms/sql/12c_company_domain_from_evidence.sql
-- Backfill company.website_domain from email evidence when missing.
-- Picks the first non-ATS/non-aggregator domain for each company.

BEGIN;

WITH candidate AS (
    SELECT DISTINCT ON (ced.company_id)
        ced.company_id,
        ced.value AS domain
    FROM gold.company_evidence_domain ced
    JOIN gold.company c ON c.company_id = ced.company_id
    WHERE c.website_domain IS NULL
      AND ced.kind = 'email'
      AND ced.value IS NOT NULL
      AND NOT util.is_generic_email_domain(ced.value)
      AND NOT util.is_aggregator_host(ced.value)
      AND NOT util.is_ats_host(ced.value)
      AND NOT util.is_career_host(ced.value)
      AND ced.value NOT IN ('profesia.sk','avature.net','grnh.se')
    ORDER BY ced.company_id
)
UPDATE gold.company c
SET website_domain = candidate.domain
FROM candidate
WHERE c.company_id = candidate.company_id
  AND c.website_domain IS NULL;

COMMIT;
