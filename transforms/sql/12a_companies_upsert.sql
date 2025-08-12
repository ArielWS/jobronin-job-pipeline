-- transforms/sql/12a_companies_upsert.sql
-- Minimal, JSON-proof company upsert:
-- - Only uses name + domain/email/apply for identity/evidence.
-- - No description/size/industry/logo reads here (avoids NaN/Infinity JSON).
-- - Brand-aware domain claim; alias & evidence written; no regress updates.

BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source,
    s.source_id,
    s.source_row_url,
    s.company_name,
    util.company_name_norm_langless(s.company_name) AS name_norm,
    util.org_domain(NULLIF(s.company_domain,''))    AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL
         ELSE s.contact_email_root
    END                                             AS email_root_raw,
    NULLIF(s.apply_root,'')                         AS apply_root_raw
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm_langless(s.company_name) IS NOT NULL
    AND util.is_placeholder_company_name(s.company_name) = FALSE
),

best_per_name AS (
  -- ONE candidate per normalized name; prefer a real site host over email
  SELECT DISTINCT ON (name_norm)
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

domain_winner AS (
  -- ONE row per (org_root, brand_key) is allowed to set website_domain
  SELECT DISTINCT ON (org_root, brand_key_norm)
    d.*
  FROM best_per_name_brand d
  WHERE d.org_root IS NOT NULL
  ORDER BY d.org_root, d.brand_key_norm
),

-- 1) Insert exactly one row per name_norm (names only; no attrs)
ins_names AS (
  INSERT INTO gold.company AS gc (name, brand_key)
  SELECT n.company_name, COALESCE(n.brand_key_norm,'')
  FROM best_per_name_brand n
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name      = CASE WHEN util.is_placeholder_company_name(gc.name) THEN EXCLUDED.name ELSE gc.name END,
        brand_key = COALESCE(gc.brand_key, EXCLUDED.brand_key)
  RETURNING gc.company_id, gc.name, gc.name_norm
),

-- 2) Set website_domain/brand for the winners only
upd_domain AS (
  UPDATE gold.company gc
  SET website_domain = COALESCE(gc.website_domain, dw.org_root),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE gc.name_norm = dw.name_norm
    AND dw.org_root IS NOT NULL
  RETURNING 1
),

-- 3) Resolve every source row to company_id (prefer domain_winner match)
resolved AS (
  SELECT
    s.source,
    s.source_id,
    s.source_row_url,
    s.company_name,
    s.name_norm,
    s.apply_root_raw,
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

-- Aliases: every observed name to its company
add_alias AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT DISTINCT r.company_id, r.company_name
  FROM resolved r
  WHERE r.company_id IS NOT NULL
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),

-- Evidence: per-company PK (company_id, kind, value)
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

-- no attribute top-up here (that’s handled by a separate enrichment step once JSON is sanitized)
SELECT 1;

COMMIT;
