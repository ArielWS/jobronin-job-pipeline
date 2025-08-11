INSERT INTO gold.company (name)
SELECT DISTINCT s.company_name
FROM silver.unified s
WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM gold.company gc
    WHERE util.company_name_norm(gc.name) = util.company_name_norm(s.company_name)
  );
