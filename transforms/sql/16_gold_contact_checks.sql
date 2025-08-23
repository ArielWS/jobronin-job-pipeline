-- transforms/sql/16_gold_contact_checks.sql
-- Integrity & coverage checks for gold.contact*

SET search_path = public;

-- 1) Coverage snapshot
WITH cnt AS (
  SELECT
    COUNT(*) AS contacts_total,
    COUNT(*) FILTER (WHERE primary_email IS NOT NULL) AS with_primary_email,
    COUNT(*) FILTER (WHERE primary_phone IS NOT NULL) AS with_primary_phone,
    COUNT(*) FILTER (WHERE title_raw IS NOT NULL)      AS with_title
  FROM gold.contact
),
anchors AS (
  SELECT
    COUNT(*) FILTER (WHERE kind='email') AS emails_total,
    COUNT(*) FILTER (
      WHERE kind='email'
        AND NOT coalesce((detail->>'is_generic_domain')::boolean, false)
        AND NOT coalesce((detail->>'is_generic_mailbox')::boolean, false)
    ) AS emails_non_generic,
    COUNT(*) FILTER (
      WHERE kind='email'
        AND (coalesce((detail->>'is_generic_domain')::boolean, false)
             OR coalesce((detail->>'is_generic_mailbox')::boolean, false))
    ) AS emails_generic
  FROM gold.contact_evidence
)
SELECT * FROM cnt, anchors;

-- 2) Duplicates by primary_email (should be 0)
SELECT lower(primary_email) AS email_lower, COUNT(*) AS dup_count
FROM gold.contact
WHERE primary_email IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY dup_count DESC, email_lower
LIMIT 100;

-- 3) Duplicate candidates by (name_norm, primary_company_id) where no email
SELECT name_norm, primary_company_id, COUNT(*) AS cnt
FROM gold.contact
WHERE primary_email IS NULL
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100;

-- 4) Orphan affiliations (should be 0)
SELECT *
FROM gold.contact_affiliation
WHERE company_id IS NULL;

-- 5) Conflicts: same email active at 2+ companies in last 90 days
WITH recent AS (
  SELECT
    ce.value AS email,
    ca.company_id
  FROM gold.contact_evidence ce
  JOIN gold.contact c ON c.contact_id = ce.contact_id
  JOIN gold.contact_affiliation ca ON ca.contact_id = c.contact_id
  WHERE ce.kind = 'email'
    AND ca.last_seen >= now() - interval '90 days'
    AND ca.active = TRUE
    AND NOT coalesce((ce.detail->>'is_generic_domain')::boolean, false)
    AND NOT coalesce((ce.detail->>'is_generic_mailbox')::boolean, false)
)
SELECT email, COUNT(DISTINCT company_id) AS company_count
FROM recent
GROUP BY email
HAVING COUNT(DISTINCT company_id) > 1
ORDER BY company_count DESC, email
LIMIT 100;

-- 6) Top generic domains & mailboxes (diagnostics)
WITH gen AS (
  SELECT
    ce.value,
    util.email_domain(ce.value) AS domain,
    lower(split_part(ce.value,'@',1))       AS local,
    (detail->>'is_generic_domain')::boolean AS is_generic_domain,
    (detail->>'is_generic_mailbox')::boolean AS is_generic_mailbox
  FROM gold.contact_evidence ce
  WHERE ce.kind = 'email'
)
SELECT
  domain,
  COUNT(*) FILTER (WHERE is_generic_domain)  AS generic_domain_hits,
  COUNT(*) FILTER (WHERE is_generic_mailbox) AS generic_mailbox_hits,
  COUNT(*)                                   AS total
FROM gen
GROUP BY domain
ORDER BY generic_domain_hits DESC, total DESC
LIMIT 50;
