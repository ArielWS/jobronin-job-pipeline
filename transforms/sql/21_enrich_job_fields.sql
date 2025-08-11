WITH unified AS (
  SELECT
    u.*,
    regexp_replace(regexp_replace(u.job_url_direct,'[?#].*$',''), '/$','') AS apply_url_clean_norm
  FROM silver.unified u
),
agg AS (
  SELECT
    gsl.job_id,
    -- max numeric present
    max(u.salary_min) FILTER (WHERE u.salary_min IS NOT NULL) AS max_salary_min,
    max(u.salary_max) FILTER (WHERE u.salary_max IS NOT NULL) AS max_salary_max,
    -- pick a currency: prefer any non-null (short string), fall back to first by length desc
    substring(string_agg(coalesce(u.currency,''), '' ORDER BY length(coalesce(u.currency,'')) DESC) from 1 for 3) AS best_currency,
    -- longest description as proxy for richest content
    (SELECT u2.description_raw
     FROM unified u2
     JOIN gold.job_source_link gsl2 ON gsl2.source = u2.source AND gsl2.source_id = u2.source_id
     WHERE gsl2.job_id = gsl.job_id
     ORDER BY length(coalesce(u2.description_raw,'')) DESC NULLS LAST
     LIMIT 1) AS best_description
  FROM gold.job_source_link gsl
  JOIN unified u
    ON gsl.source = u.source AND gsl.source_id = u.source_id
  GROUP BY gsl.job_id
)
UPDATE gold.job_post jp
SET
  salary_min  = COALESCE(jp.salary_min, a.max_salary_min),
  salary_max  = COALESCE(jp.salary_max, a.max_salary_max),
  currency    = COALESCE(jp.currency, NULLIF(a.best_currency,'')),
  description = COALESCE(jp.description, a.best_description)
FROM agg a
WHERE a.job_id = jp.job_id;
