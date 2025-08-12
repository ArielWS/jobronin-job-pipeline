-- One-pass, collision-free company upsert:
-- 1) Pick one best row per name_norm (prefer real site, then richness) -> insert name-only.
-- 2) Pick one winner per (org_root, brand_key) -> update website_domain/brand on that one row only.
-- 3) Resolve all source rows to company_id; write aliases & evidence; top-up attrs (no regress).

BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    -- raw candidates (site/email)
    util.org_domain(NULLIF(s.company_domain,'')) AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'') AS apply_root_raw,
    -- fillable attrs
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    -- if you do have this util fn, keep it; else remove the line:
    AND NOT util.is_placeholder_company_name(s.company_name)
),

best_per_name AS (
  -- choose ONE best candidate per name_norm (prefer real site host over email, then richer)
  SELECT DISTINCT ON (name_norm)
    s.*,
    CASE
      WHEN s.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(s.site_root_raw)
           AND NOT util.is_ats_host(s.site_root_raw)
      THEN s.site_root_raw
      WHEN s.email_root_raw IS NOT NULL
      THEN s.email_root_raw
      ELSE NULL
    END AS org_root,
    ((s.company_description_raw IS NOT NULL)::int
     + (s.company_size_raw IS NOT NULL)::int
     + (s.company_industry_raw IS NOT NULL)::int
     + (s.company_logo_url IS NOT NULL)::int) AS richness
  FROM src s
  ORDER BY
    s.name_norm,
    (s.site_root_raw IS NOT NULL
     AND NOT util.is_aggregator_host(s.site_root_raw)
     AND NOT util.is_ats_host(s.site_root_raw)) DESC,
    ((s.company_description_raw IS NOT NULL)::int
     + (s.company_size_raw IS NOT NULL)::int
     + (s.company_industry_raw IS NOT NULL)::int
     + (s.company_logo_url IS NOT NULL)::int) DESC
),

best_per_name_brand AS (
  -- infer brand_key (coalesce to '' so uniqueness works even when there's no sub-brand)
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
  -- choose ONE row per (org_root, brand_key) that will actually get the website_domain set
  SELECT DISTINCT ON (org_root, brand_key_norm)
    d.*
  FROM best_per_name_brand d
  WHERE d.org_root IS NOT NULL
  ORDER BY
    d.org_root, d.brand_key_norm,
    d.richness DESC
),

-- 1) Insert exactly one row per name_norm (names only; no domain yet)
ins_names AS (
  INSERT INTO gold.company (name, description, size_raw, industry_raw, logo_url, brand_key)
  SELECT
    n.company_name,
    n.company_description_raw, n.company_size_raw, n.company_industry_raw, n.company_logo_url,
    COALESCE(n.brand_key_norm, '')
  FROM best_per_name_brand n
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name         = CASE WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name ELSE gold.company.name END,
        description  = COALESCE(gold.company.description,  EXCLUDED.description),
        size_raw     = COALESCE(gold.company.size_raw,     EXCLUDED.size_raw),
        industry_raw = COALESCE(gold.company.industry_raw, EXCLUDED.industry_raw),
        logo_url     = COALESCE(gold.company.logo_url,     EXCLUDED.logo_url),
        brand_key    = COALESCE(gold.company.brand_key,    EXCLUDED.brand_key)
  RETURNING company_id, name, name_norm
),

-- 2) Update website_domain/brand only for the single domain_winner rows (prevents domain collisions)
upd_domain AS (
  UPDATE gold.company gc
  SET website_domain = COALESCE(gc.website_domain, dw.org_root),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE gc.name_norm = dw.name_norm
  RETURNING 1
),

-- 3) Resolve each source row to company_id:
--    If it has an org_root that matches a chosen domain_winner, map to that winner.
--    Else, fall back to the company's own name_norm row we inserted above.
resolved AS (
  SELECT
    s.source, s.source_id, s.source_row_url,
    s.company_name, s.name_norm,
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url,
    s.apply_root_raw, s.email_root_raw,
    COALESCE(
      -- map by chosen domain winner
      (SELECT gc.company_id
         FROM domain_winner dw
         JOIN gold.company gc ON gc.name_norm = dw.name_norm
        WHERE dw.org_root IS NOT NULL
          AND dw.org_root = CASE
                              WHEN s.site_root_raw IS NOT NULL
                                   AND NOT util.is_aggregator_host(s.site_root_raw)
                                   AND NOT util.is_ats_host(s.site_root_raw)
                              THEN s.site_root_raw
                              WHEN s.email_root_raw IS NOT NULL
                              THEN s.email_root_raw
                              ELSE NULL
                            END
        LIMIT 1),
      -- else by own name_norm
      (SELECT gc2.company_id FROM gold.company gc2 WHERE gc2.name_norm = s.name_norm LIMIT 1)
    ) AS company_id
  FROM src s
),

-- Aliases for every observed name -> resolved company
add_alias AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT DISTINCT r.company_id, r.company_name
  FROM resolved r
  WHERE r.company_id IS NOT NULL
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),

-- Evidence (per-company primary key: (company_id, kind, value))
add_evidence AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT DISTINCT r.company_id, kv.kind, kv.val, r.source, r.source_id
  FROM resolved r
  CROSS JOIN LATERAL (
    VALUES
      ('website', CASE
                    WHEN r.company_id IS NOT NULL THEN
                      CASE
                        WHEN s.site_root_raw IS NOT NULL
                             AND NOT util.is_aggregator_host(s.site_root_raw)
                             AND NOT util.is_ats_host(s.site_root_raw)
                        THEN s.site_root_raw
                        WHEN r.email_root_raw IS NOT NULL
                        THEN r.email_root_raw
                        ELSE NULL
                      END
                    ELSE NULL
                  END),
      ('email',   CASE WHEN r.email_root_raw IS NOT NULL
                           AND NOT util.is_generic_email_domain(r.email_root_raw)
                       THEN r.email_root_raw END),
      ('apply',   CASE WHEN r.apply_root_raw IS NOT NULL
                           AND NOT util.is_aggregator_host(r.apply_root_raw)
                           AND NOT util.is_ats_host(r.apply_root_raw)
                       THEN r.apply_root_raw END)
  ) AS kv(kind, val)
  JOIN src s ON s.name_norm = r.name_norm  -- for site_root_raw in the VALUES above
  WHERE r.company_id IS NOT NULL AND kv.val IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)

-- Final non-regressive attribute top-up from the *best_per_name* winners only (higher quality than raw src)
UPDATE gold.company gc
SET description  = COALESCE(gc.description,  bp.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     bp.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, bp.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     bp.company_logo_url)
FROM best_per_name bp
WHERE gc.name_norm = bp.name_norm;

COMMIT;
