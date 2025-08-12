-- One-pass, collision-free company upsert (lang-suffix aware):
-- 1) Pick one best row per name_norm_langless (prefer base name, then real site, then richness) -> insert name-only.
-- 2) Pick one winner per (org_root, brand_key) -> set website_domain/brand on rows whose langless norm matches.
-- 3) Resolve all rows to company_id; write aliases & evidence; top-up attrs (no regress).

BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name)            AS name_norm,
    util.company_name_norm_langless(s.company_name)   AS name_norm_langless,
    -- raw candidates (site/email)
    util.org_domain(NULLIF(s.company_domain,''))      AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'')                           AS apply_root_raw,
    -- fillable attrs
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL
    AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
),

best_per_langless AS (
  -- choose ONE best candidate per langless norm:
  --   1) prefer the base name (no language suffix)
  --   2) prefer a real site host over email (and ignore aggregators/ATS)
  --   3) prefer richer records
  SELECT DISTINCT ON (s.name_norm_langless)
    s.*,
    (util.company_name_norm_langless(s.company_name) = util.company_name_norm(s.company_name)) AS is_langless_base,
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
    s.name_norm_langless,
    (util.company_name_norm_langless(s.company_name) = util.company_name_norm(s.company_name)) DESC,
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
  FROM best_per_langless b
),

domain_winner AS (
  -- choose ONE row per (org_root, brand_key) that will actually own website_domain
  SELECT DISTINCT ON (org_root, brand_key_norm)
    d.*
  FROM best_per_name_brand d
  WHERE d.org_root IS NOT NULL
  ORDER BY
    d.org_root, d.brand_key_norm,
    d.is_langless_base DESC,
    d.richness DESC
),

-- 1) Insert at most one row per langless norm (names only; no domain yet).
--    Skip insert if a row already exists by *either* strict norm or langless norm.
ins_names AS (
  INSERT INTO gold.company (name, description, size_raw, industry_raw, logo_url, brand_key)
  SELECT
    n.company_name,
    n.company_description_raw, n.company_size_raw, n.company_industry_raw, n.company_logo_url,
    COALESCE(n.brand_key_norm, '')
  FROM best_per_name_brand n
  WHERE NOT EXISTS (
          SELECT 1 FROM gold.company gc
          WHERE gc.name_norm = n.name_norm
             OR util.company_name_norm_langless(gc.name) = n.name_norm_langless
        )
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name         = CASE WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name ELSE gold.company.name END,
        description  = COALESCE(gold.company.description,  EXCLUDED.description),
        size_raw     = COALESCE(gold.company.size_raw,     EXCLUDED.size_raw),
        industry_raw = COALESCE(gold.company.industry_raw, EXCLUDED.industry_raw),
        logo_url     = COALESCE(gold.company.logo_url,     EXCLUDED.logo_url),
        brand_key    = COALESCE(gold.company.brand_key,    EXCLUDED.brand_key)
  RETURNING company_id, name
),

-- 2) Update website_domain/brand for rows whose langless norm matches the domain winner
upd_domain AS (
  UPDATE gold.company gc
  SET website_domain = COALESCE(gc.website_domain, dw.org_root),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE util.company_name_norm_langless(gc.name) = dw.name_norm_langless
  RETURNING 1
),

-- 3) Resolve each source row to company_id and carry its best org_root_candidate for evidence
resolved AS (
  SELECT
    s.source, s.source_id, s.source_row_url,
    s.company_name, s.name_norm, s.name_norm_langless,
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url,
    s.apply_root_raw, s.email_root_raw,
    -- org_root_candidate for evidence: prefer site if valid, else email
    CASE
      WHEN s.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(s.site_root_raw)
           AND NOT util.is_ats_host(s.site_root_raw)
      THEN s.site_root_raw
      WHEN s.email_root_raw IS NOT NULL
      THEN s.email_root_raw
      ELSE NULL
    END AS org_root_candidate,
    COALESCE(
      -- 1) exact domain owner if already claimed
      (SELECT gc.company_id
         FROM gold.company gc
        WHERE gc.website_domain = CASE
                                    WHEN s.site_root_raw IS NOT NULL
                                         AND NOT util.is_aggregator_host(s.site_root_raw)
                                         AND NOT util.is_ats_host(s.site_root_raw)
                                    THEN s.site_root_raw
                                    WHEN s.email_root_raw IS NOT NULL
                                    THEN s.email_root_raw
                                    ELSE NULL
                                  END
        LIMIT 1),
      -- 2) exact strict name match
      (SELECT gc2.company_id FROM gold.company gc2 WHERE gc2.name_norm = s.name_norm LIMIT 1),
      -- 3) NEW: langless name match
      (SELECT gc3.company_id FROM gold.company gc3 WHERE util.company_name_norm_langless(gc3.name) = s.name_norm_langless LIMIT 1)
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

-- Evidence (per-company PK: (company_id, kind, value))
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
  WHERE r.company_id IS NOT NULL AND kv.val IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)

-- Final non-regressive attribute top-up from the langless winners only
UPDATE gold.company gc
SET description  = COALESCE(gc.description,  bp.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     bp.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, bp.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     bp.company_logo_url)
FROM best_per_langless bp
WHERE util.company_name_norm_langless(gc.name) = bp.name_norm_langless;

COMMIT;
