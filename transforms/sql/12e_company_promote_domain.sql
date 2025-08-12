BEGIN;

WITH web_ev AS (
  SELECT e.company_id, lower(e.value) AS domain
  FROM gold.company_evidence_domain e
  WHERE e.kind='website'
),
ranked AS (
  SELECT
    c.company_id,
    c.name,
    c.brand_key,
    c.description,
    c.size_raw,
    c.logo_url,
    w.domain,
    lower(split_part(w.domain,'.',1)) AS dom_token,
    ROW_NUMBER() OVER (
      PARTITION BY w.domain
      ORDER BY
        -- prefer parent brand
        (c.brand_key = '') DESC,
        -- prefer if company name contains the domain token (e.g., 'amazon' in 'Amazon.com')
        ( util.company_name_norm(c.name) ~ ('(^| )' || lower(split_part(w.domain,'.',1)) || '( |$)') ) DESC,
        -- prefer names without legal suffixes
        (c.name ~* '\b(gmbh|ag|se|llc|inc|ltd|bv|kg|s\.r\.o\.|sarl|sas)\b') ASC,
        -- then richer attrs
        (c.description IS NOT NULL)::int DESC,
        (c.size_raw    IS NOT NULL)::int DESC,
        (c.logo_url    IS NOT NULL)::int DESC,
        c.company_id ASC
    ) AS rn
  FROM web_ev w
  JOIN gold.company c ON c.company_id = w.company_id
  WHERE NOT util.is_career_host(w.domain)
)
UPDATE gold.company gc
SET website_domain = r.domain
FROM ranked r
WHERE r.rn = 1
  AND gc.company_id = r.company_id
  AND (gc.website_domain IS NULL OR gc.website_domain <> r.domain);

COMMIT;
