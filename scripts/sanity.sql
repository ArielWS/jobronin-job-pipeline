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

-- StepStone/Profesia SK detail check
DO $$
DECLARE
  cnt INT;
BEGIN
  WITH sample AS (
    SELECT gc.size_raw, gc.industry_raw, gc.description, gc.logo_url
    FROM gold.company gc
    JOIN gold.company_evidence_domain ced ON ced.company_id = gc.company_id
    WHERE ced.source IN ('stepstone', 'profesia_sk')
    ORDER BY random()
    LIMIT 10
  )
  SELECT COUNT(*) INTO cnt
  FROM sample
  WHERE size_raw IS NOT NULL
     OR industry_raw IS NOT NULL
     OR description IS NOT NULL
     OR logo_url IS NOT NULL;

  IF cnt = 0 THEN
    RAISE EXCEPTION 'All sampled StepStone/Profesia SK companies missing size_raw, industry_raw, description, and logo_url';
  END IF;
END
$$;
