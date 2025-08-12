BEGIN;

-- build winners into a temp table so both updates can see it
CREATE TEMP TABLE tmp_domain_winners ON COMMIT DROP AS
WITH domains AS (
  SELECT DISTINCT lower(value) AS domain,
         lower(split_part(value,'.',1)) AS token
  FROM gold.company_evidence_domain
  WHERE kind='website'
    AND NOT util.is_career_host(value)
),
cands AS (
  SELECT
    d.domain, d.token,
    c.company_id, c.name,
    util.company_name_norm(c.name) AS norm,
    (c.brand_key = '')::int AS is_parent,
    (util.company_name_norm(c.name) = d.token)::int AS exact_token,
    (util.company_name_norm(c.name) ~ ('(^| )' || d.token || '( |$)'))::int AS has_token,
    char_length(util.company_name_norm(c.name)) AS norm_len
  FROM domains d
  JOIN gold.company c
    ON util.company_name_norm(c.name) ~ ('(^| )' || d.token || '( |$)')
),
ranked AS (
  SELECT
    c.*,
    ROW_NUMBER() OVER (
      PARTITION BY c.domain
      ORDER BY
        is_parent   DESC,
        exact_token DESC,
        has_token   DESC,
        norm_len    ASC,
        company_id  ASC
    ) AS rn
  FROM cands c
)
SELECT domain, company_id
FROM ranked
WHERE rn = 1;

-- clear losers
UPDATE gold.company gc
SET website_domain = NULL
FROM tmp_domain_winners w
WHERE gc.website_domain = w.domain
  AND gc.company_id <> w.company_id;

-- set winners
UPDATE gold.company gc
SET website_domain = w.domain
FROM tmp_domain_winners w
WHERE gc.company_id = w.company_id
  AND (gc.website_domain IS NULL OR gc.website_domain <> w.domain);

COMMIT;
