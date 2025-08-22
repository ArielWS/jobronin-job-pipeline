-- transforms/sql/13_gold_company_checks.sql

-- 1) No dupes by normalized name among placeholders (domain unknown)
SELECT name_norm, COUNT(*) AS n
FROM gold.company
WHERE website_domain IS NULL
GROUP BY 1 HAVING COUNT(*) > 1;

-- 2) No dupes by (website_domain, brand_key)
SELECT website_domain, brand_key, COUNT(*) AS n
FROM gold.company
WHERE website_domain IS NOT NULL
GROUP BY 1,2 HAVING COUNT(*) > 1;

-- 3) Coverage snapshot
SELECT
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE website_domain IS NOT NULL) AS with_site,
  COUNT(*) FILTER (WHERE description IS NOT NULL)     AS with_desc,
  COUNT(*) FILTER (WHERE size_raw IS NOT NULL)        AS with_size,
  COUNT(*) FILTER (WHERE industry_raw IS NOT NULL)    AS with_industry,
  COUNT(*) FILTER (WHERE logo_url IS NOT NULL)        AS with_logo
FROM gold.company;

-- 4) Near duplicates (manual watchlist)
SELECT a.company_id, a.name, b.company_id AS other_id, b.name AS other_name,
       similarity(a.name_norm, b.name_norm) AS sim
FROM gold.company a
JOIN gold.company b ON a.company_id < b.company_id
WHERE similarity(a.name_norm, b.name_norm) >= 0.90
ORDER BY sim DESC, a.company_id;
