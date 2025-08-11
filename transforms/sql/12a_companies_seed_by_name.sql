INSERT INTO gold.company (name)
SELECT DISTINCT s.company_name
FROM silver.unified s
LEFT JOIN gold.company gc ON util.company_name_norm(gc.name) = util.company_name_norm(s.company_name)
WHERE gc.company_id IS NULL
  AND s.company_name IS NOT NULL AND btrim(s.company_name) <> '';
