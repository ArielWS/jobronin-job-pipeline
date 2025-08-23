-- transforms/sql/16_gold_contact_checks.sql
-- QA / coverage checks for gold.contact

SET search_path = public;

-- 1) Summary
SELECT
  COUNT(*)                                    AS contacts_total,
  COUNT(*) FILTER (WHERE primary_email IS NOT NULL) AS with_primary_email,
  COUNT(*) FILTER (WHERE primary_phone IS NOT NULL) AS with_primary_phone,
  COUNT(*) FILTER (WHERE title_raw IS NOT NULL)     AS with_title,
  (SELECT COUNT(*) FROM gold.contact_evidence WHERE kind='email') AS emails_total,
  (SELECT COUNT(*) FROM gold.contact_evidence WHERE kind='email'
      AND NOT coalesce((detail->>'is_generic_domain')::boolean,false)
      AND NOT coalesce((detail->>'is_generic_mailbox')::boolean,false)) AS emails_non_generic,
  (SELECT COUNT(*) FROM gold.contact_evidence WHERE kind='email'
      AND (coalesce((detail->>'is_generic_domain')::boolean,false)
           OR coalesce((detail->>'is_generic_mailbox')::boolean,false))) AS emails_generic
FROM gold.contact;

-- 2) Duplicate primary emails (should be 0 by constraint)
SELECT lower(primary_email) AS email_lower, COUNT(*) AS dup_count
FROM gold.contact
WHERE primary_email IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY dup_count DESC;

-- 3) Duplicate no-email by (name_norm, company) (should trend to 0)
SELECT name_norm, primary_company_id, COUNT(*) AS cnt
FROM gold.contact
WHERE primary_email IS NULL
  AND name_norm IS NOT NULL
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY cnt DESC, 1, 2
LIMIT 50;

-- 4) Affiliation coverage
SELECT
  COUNT(*) AS contacts_total,
  COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM gold.contact_affiliation a WHERE a.contact_id=c.contact_id)) AS with_affiliation,
  ROUND(100.0 * COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM gold.contact_affiliation a WHERE a.contact_id=c.contact_id)) / GREATEST(COUNT(*),1), 1) AS pct_with_affiliation
FROM gold.contact c;

-- 5) Emails that appear tied to >1 company via affiliations (agency signals)
WITH email_to_contact AS (
  SELECT ce.value AS email, ce.contact_id
  FROM gold.contact_evidence ce
  WHERE ce.kind='email'
),
email_company_span AS (
  SELECT etc.email, COUNT(DISTINCT a.company_id) AS company_count
  FROM email_to_contact etc
  LEFT JOIN gold.contact_affiliation a ON a.contact_id = etc.contact_id
  GROUP BY etc.email
)
SELECT email, company_count
FROM email_company_span
WHERE company_count > 1
ORDER BY company_count DESC, email
LIMIT 50;

-- 6) Generic mailbox distribution (top 30 roots)
WITH ev AS (
  SELECT
    util.email_domain(value) AS domain,
    coalesce((detail->>'is_generic_domain')::boolean,false) AS is_gdomain,
    coalesce((detail->>'is_generic_mailbox')::boolean,false) AS is_gmailbox
  FROM gold.contact_evidence
  WHERE kind='email'
)
SELECT
  domain,
  COUNT(*) FILTER (WHERE is_gdomain)  AS generic_domain_hits,
  COUNT(*) FILTER (WHERE is_gmailbox) AS generic_mailbox_hits,
  COUNT(*)                            AS total
FROM ev
GROUP BY 1
ORDER BY total DESC
LIMIT 30;

-- 7) Contacts whose only email is generic (should have primary_email NULL and generic_email set)
SELECT
  c.contact_id, c.full_name, c.primary_company_id, c.primary_email, c.generic_email
FROM gold.contact c
WHERE c.primary_email IS NULL
  AND c.generic_email IS NOT NULL
ORDER BY c.updated_at DESC
LIMIT 50;
