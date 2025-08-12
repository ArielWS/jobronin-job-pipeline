-- transforms/sql/12a_companies_upsert.sql
-- JSON-free, collision-free company upsert.

BEGIN;

WITH src_jobspy AS (
  SELECT DISTINCT
    'jobspy'::text                                      AS source,
    js.id::text                                         AS source_id,
    COALESCE(js.job_url_direct, js.job_url)             AS source_row_url,
    js.company                                          AS company_name,
    util.company_name_norm_langless(js.company)         AS name_norm,
    -- site root from company URL fields
    util.org_domain(util.url_host(COALESCE(js.company_url, js.company_url_direct))) AS site_root_raw,
    -- email root from first email in the raw string
    CASE
      WHEN util.is_generic_email_domain(util.email_domain(util.first_email(js.emails))) THEN NULL
      ELSE util.email_domain(util.first_email(js.emails))
    END                                                 AS email_root_raw,
    -- apply root from job link host
    util.org_domain(util.url_host(COALESCE(js.job_url_direct, js.job_url)))          AS apply_root_raw
  FROM public.jobspy_job_scrape js
  WHERE js.company IS NOT NULL
    AND btrim(js.company) <> ''
    AND util.company_name_norm_langless(js.company) IS NOT NULL
    AND util.is_placeholder_company_name(js.company) = FALSE
),
src_stepstone AS (
  -- Name-only; do not touch job_data at all
  SELECT DISTINCT
    'stepstone'::text                      AS source,
    st.id::text                            AS source_id,
    NULL::text                             AS source_row_url,
    st.client_name                         AS company_name,
    util.company_name_norm_langless(st.client_name) AS name_norm,
    NULL::text                             AS site_root_raw,
    NULL::text                             AS email_root_raw,
    NULL::text                             AS apply_root_raw
  FROM public.stepstone_job_scrape st
  WHERE st.client_name IS NOT NULL
    AND btrim(st.client_name) <> ''
    AND util.company_name_norm_langless(st.client_name) IS NOT NULL
    AND util.is_placeholder_company_name(st.client_name) = FALSE
),
src AS (
  SELECT * FROM src_jobspy
  UNION ALL
  SELECT * FROM src_stepstone
),

-- One candidate per normalized name (prefer real site host)
best_per_name AS (
  SELECT DISTINCT ON (s.name_norm)
    s.*,
    CASE
      WHEN s.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(s.site_root_raw)
           AND NOT util.is_ats_host(s.site_root_raw)
           AND NOT util.is_career_host(s.site_root_raw)
      THEN s.site_root_raw
      WHEN s.email_root_raw IS NOT NULL THEN s.email_root_raw
      ELSE NULL
    END AS org_root
  FROM src s
  ORDER BY
    s.name_norm,
    (s.site_root_raw IS NOT NULL
     AND NOT util.is_aggregator_host(s.site_root_raw)
     AND NOT util.is_ats_host(s.site_root_raw)
     AND NOT util.is_career_host(s.site_root_raw)) DESC
),

best_per_name_brand AS (
  SELECT
    b.*,
    COALESCE((
      SELECT r.brand_key
      FROM gold.company_brand_rule r
      WHERE r.active = TRUE
        AND r.domain_root = b.org_root
        AND b.name_norm ~ r.brand_regex
      LIMIT 1
    ), ''::text) AS brand_key_norm
  FROM best_per_name b
),

-- Defensive: dedupe again on name_norm before INSERT to avoid "affect row a second time"
dedup_names AS (
  SELECT DISTINCT ON (name_norm)
    name_norm, company_name, brand_key_norm
  FROM best_per_name_brand
  ORDER BY name_norm
),

-- 1) Insert one row per name_norm (no attrs → avoid JSON entirely)
ins_names AS (
  INSERT INTO gold.company AS gc (name, brand_key)
  SELECT d.company_name, COALESCE(d.brand_key_norm,'')
  FROM dedup_names d
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name      = CASE WHEN util.is_placeholder_company_name(gc.name) THEN EXCLUDED.name ELSE gc.name END,
        brand_key = COALESCE(gc.brand_key, EXCLUDED.brand_key)
  RETURNING gc.company_id, gc.name, gc.name_norm
),

-- Domain winner: one row per (org_root, brand) gets the site set
domain_winner AS (
  SELECT DISTINCT ON (org_root, brand_key_norm)
    d.*
  FROM best_per_name_brand d
  WHERE d.org_root IS NOT NULL
  ORDER BY d.org_root, d.brand_key_norm
),

-- 2) Set website_domain/brand for winners only (no multi-update collisions)
upd_domain AS (
  UPDATE gold.company gc
  SET website_domain = COALESCE(gc.website_domain, dw.org_root),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE gc.name_norm = dw.name_norm
    AND dw.org_root IS NOT NULL
  RETURNING 1
),

-- 3) Resolve each source row to a company and record evidence
resolved AS (
  SELECT
    s.source, s.source_id, s.source_row_url,
    s.company_name, s.name_norm,
    s.apply_root_raw, s.email_root_raw,
    CASE
      WHEN s.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(s.site_root_raw)
           AND NOT util.is_ats_host(s.site_root_raw)
           AND NOT util.is_career_host(s.site_root_raw)
      THEN s.site_root_raw
      WHEN s.email_root_raw IS NOT NULL
      THEN s.email_root_raw
      ELSE NULL
    END AS org_root_candidate,
    COALESCE(
      (SELECT gc.company_id
         FROM domain_winner dw
         JOIN gold.company gc ON gc.name_norm = dw.name_norm
        WHERE dw.org_root IS NOT NULL
          AND dw.org_root = CASE
                              WHEN s.site_root_raw IS NOT NULL
                                   AND NOT util.is_aggregator_host(s.site_root_raw)
                                   AND NOT util.is_ats_host(s.site_root_raw)
                                   AND NOT util.is_career_host(s.site_root_raw)
                              THEN s.site_root_raw
                              WHEN s.email_root_raw IS NOT NULL
                              THEN s.email_root_raw
                              ELSE NULL
                            END
        LIMIT 1),
      (SELECT gc2.company_id FROM gold.company gc2 WHERE gc2.name_norm = s.name_norm LIMIT 1)
    ) AS company_id
  FROM src s
),

add_alias AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT DISTINCT r.company_id, r.company_name
  FROM resolved r
  WHERE r.company_id IS NOT NULL
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),

add_evidence AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT DISTINCT r.company_id, kv.kind, kv.val, r.source, r.source_id
  FROM resolved r
  CROSS JOIN LATERAL (
    VALUES
      ('website', r.org_root_candidate),
      ('email',   CASE WHEN r.email_root_raw IS NOT NULL
                           AND NOT util.is_generic_email_domain(r.email_root_raw)
                       THEN r.email_root_raw END),
      ('apply',   CASE WHEN r.apply_root_raw IS NOT NULL
                           AND NOT util.is_aggregator_host(r.apply_root_raw)
                           AND NOT util.is_ats_host(r.apply_root_raw)
                       THEN r.apply_root_raw END)
  ) AS kv(kind, val)
  WHERE r.company_id IS NOT NULL
    AND kv.val IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)

SELECT 1;

COMMIT;
