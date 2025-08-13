-- scripts/sanity.sql

-- Counts
SELECT COUNT(*) AS companies FROM gold.company;
SELECT COUNT(*) AS with_site FROM gold.company WHERE website_domain IS NOT NULL;

-- Collisions (should be empty)
SELECT website_domain, brand_key, COUNT(*) AS n
FROM gold.company
WHERE website_domain IS NOT NULL
GROUP BY 1,2
HAVING COUNT(*)>1;

-- Evidence coverage
SELECT kind, COUNT(*) FROM gold.company_evidence_domain GROUP BY 1 ORDER BY 1;

-- Spot check
SELECT company_id, name, website_domain
FROM gold.company
WHERE name ILIKE 'zendesk%' OR name ILIKE 'sellerx%' OR name ILIKE 'tourlane%' OR name ILIKE 'amazon%';
