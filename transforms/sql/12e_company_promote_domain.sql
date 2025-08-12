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
    util.company_name_norm(c.name)     AS norm_name,
    ROW_NUMBER() OVER (
      PARTITION BY w.domain
      ORDER BY
        -- parent brand first
        (c.brand_key = '') DESC,
        -- exact token match first (e.g., 'amazon')
        (util.company_name_norm(c.name) = lower(split_part(w.domain,'.',1))) DESC,
        -- then names containing the token
        ( util.company_name_norm(c.name) ~ ('(^| )' || lower(split_part(w.domain,'.',1)) || '( |$)') ) DESC,
        -- shorter name wins (amazon < amazon web services)
        char_length(util.company_name_norm(c.name)) ASC,
        -- then richer
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
