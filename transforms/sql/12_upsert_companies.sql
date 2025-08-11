INSERT INTO gold.company (name, website_domain)
SELECT DISTINCT s.company_name, NULL::text
FROM silver.unified s
WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
ON CONFLICT (name_norm) DO NOTHING;
