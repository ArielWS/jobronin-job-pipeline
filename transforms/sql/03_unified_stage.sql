CREATE OR REPLACE VIEW silver.unified AS
SELECT * FROM silver.jobspy
UNION ALL
SELECT * FROM silver.stepstone;
