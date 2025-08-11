WITH map AS (
  SELECT
    s.*,
    COALESCE(gc.company_id,
             gc2.company_id) AS company_id
  FROM silver.unified s
  LEFT JOIN gold.company gc
    ON s.company_domain IS NOT NULL AND gc.website_domain = s.company_domain
  LEFT JOIN gold.company gc2
    ON s.company_domain IS NULL AND gc2.name_norm = util.company_name_norm(s.company_name)
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
),
best AS (
  SELECT DISTINCT ON (company_id)
    company_id,
    -- prefer rows with website_domain, then pick longest strings
    (SELECT s1.company_industry_raw
     FROM map s1
     WHERE s1.company_id = m.company_id
     ORDER BY (s1.company_domain IS NOT NULL) DESC, length(coalesce(s1.company_industry_raw,'')) DESC
     LIMIT 1) AS industry_best,
    (SELECT s1.company_size_raw
     FROM map s1
     WHERE s1.company_id = m.company_id
     ORDER BY (s1.company_domain IS NOT NULL) DESC, length(coalesce(s1.company_size_raw,'')) DESC
     LIMIT 1) AS size_best,
    (SELECT s1.company_description_raw
     FROM map s1
     WHERE s1.company_id = m.company_id
     ORDER BY (s1.company_domain IS NOT NULL) DESC, length(coalesce(s1.company_description_raw,'')) DESC
     LIMIT 1) AS desc_best,
    (SELECT s1.company_logo_url
     FROM map s1
     WHERE s1.company_id = m.company_id
     ORDER BY (s1.company_domain IS NOT NULL) DESC, length(coalesce(s1.company_logo_url,'')) DESC
     LIMIT 1) AS logo_best
  FROM map m
)
UPDATE gold.company gc
SET
  industry_raw = COALESCE(gc.industry_raw, NULLIF(b.industry_best,'')),
  size_raw     = COALESCE(gc.size_raw,     NULLIF(b.size_best,'')),
  description  = COALESCE(gc.description,  NULLIF(b.desc_best,'')),
  logo_url     = COALESCE(gc.logo_url,     NULLIF(b.logo_best,'')),
  updated_at   = now()
FROM best b
WHERE b.company_id = gc.company_id;
