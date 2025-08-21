-- transforms/sql/12a_companies_upsert.sql
-- JSON-free, collision-proof upsert from raw sources.

BEGIN;

-- ---------------  Source extraction (from silver.unified) ---------------
WITH src AS (
  SELECT DISTINCT
    s.source                                           AS source,
    s.source_id::text                                  AS source_id,
    s.source_row_url                                   AS source_row_url,
    s.company_name                                     AS company_name,
    util.company_name_norm_langless(s.company_name)    AS name_norm,
    util.org_domain(s.company_domain)                  AS site_root_raw,
    s.company_description_raw                          AS company_description_raw,
    regexp_replace(regexp_substr(s.company_size_raw, '\\d+\\s*-\\s*\\d+'), '\\s', '', 'g') AS company_size_raw,
    s.company_industry_raw                             AS company_industry_raw,
    s.company_logo_url                                 AS company_logo_url,
    CASE
      WHEN s.contact_email_root IS NOT NULL
           AND NOT util.is_generic_email_domain(s.contact_email_root)
      THEN s.contact_email_root
      ELSE NULL
    END                                                AS email_root_raw
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm_langless(s.company_name) IS NOT NULL
    AND util.is_placeholder_company_name(s.company_name) = FALSE
),

-- ---------------  One best candidate per name ---------------
best_per_name AS (
  SELECT
    s.*,
    CASE
      WHEN s.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(s.site_root_raw)
           AND NOT util.is_ats_host(s.site_root_raw)
           AND NOT util.is_career_host(s.site_root_raw)
      THEN s.site_root_raw
      WHEN s.email_root_raw IS NOT NULL
      THEN s.email_root_raw
      ELSE NULL
    END AS org_root,
    ROW_NUMBER() OVER (
      PARTITION BY s.name_norm
      ORDER BY
        (s.site_root_raw IS NOT NULL
         AND NOT util.is_aggregator_host(s.site_root_raw)
         AND NOT util.is_ats_host(s.site_root_raw)
         AND NOT util.is_career_host(s.site_root_raw)) DESC,
        length(coalesce(s.company_name,'')) DESC
    ) AS rn
  FROM src s
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
  WHERE b.rn = 1
),

-- ---------------  Insert names (no updates here) ---------------
ins_names AS (
  INSERT INTO gold.company AS gc (name, brand_key, description, size_raw, industry_raw, logo_url)
  SELECT
    b.company_name,
    b.brand_key_norm,
    b.company_description_raw,
    b.company_size_raw,
    b.company_industry_raw,
    b.company_logo_url
  FROM best_per_name_brand b
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO NOTHING
  RETURNING gc.company_id, gc.name_norm
),

-- ---------------  Assign one website per (org_root, brand) ---------------
domain_winner AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (PARTITION BY d.org_root, d.brand_key_norm
                       ORDER BY length(coalesce(d.company_name,'')) DESC) AS rnk
  FROM best_per_name_brand d
  WHERE d.org_root IS NOT NULL
),
upd_domain AS (
  UPDATE gold.company gc
  SET website_domain = COALESCE(gc.website_domain, dw.org_root),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE dw.rnk = 1
    AND gc.name_norm = dw.name_norm
    AND dw.org_root IS NOT NULL
  RETURNING 1
),
upd_details AS (
  UPDATE gold.company gc
  SET description  = COALESCE(gc.description,  b.company_description_raw),
      size_raw     = COALESCE(gc.size_raw,     b.company_size_raw),
      industry_raw = COALESCE(gc.industry_raw, b.company_industry_raw),
      logo_url     = COALESCE(gc.logo_url,     b.company_logo_url)
  FROM best_per_name_brand b
  WHERE gc.name_norm = b.name_norm
  RETURNING 1
),

-- ---------------  Resolve rows → company_id for evidence/aliases ---------------
resolved AS (
  SELECT
    s.source, s.source_id, s.source_row_url,
    s.company_name, s.name_norm,
    s.email_root_raw,
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
        WHERE dw.rnk = 1
          AND dw.org_root IS NOT NULL
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

-- ---------------  Aliases & evidence ---------------
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
                       THEN r.email_root_raw END)
  ) AS kv(kind, val)
  WHERE r.company_id IS NOT NULL
    AND kv.val IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
),

-- Force evaluation of ins_names CTE so the insert executes
ins_names_ref AS (
  SELECT 1 FROM ins_names LIMIT 1
)

-- ---------------  Post-insert: safely promote placeholder names ---------------
-- Only change `name` when the normalized form would remain the same as current `name_norm`.
UPDATE gold.company gc
SET name = b.company_name
FROM best_per_name_brand b, ins_names_ref
WHERE util.is_placeholder_company_name(gc.name)
  AND gc.name_norm = b.name_norm
  AND util.company_name_norm_langless(b.company_name) = gc.name_norm;

COMMIT;
