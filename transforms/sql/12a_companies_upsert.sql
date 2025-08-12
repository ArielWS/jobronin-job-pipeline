-- Domain-first, brand-aware, fuzzy-guarded upsert into gold.company from silver.unified.
-- Phase A: one row per (org_root, brand_key) -> upsert by domain constraint (prevents domain dupes).
-- Phase B: name-only winners -> upsert by name_norm.
-- Then alias/evidence, domain promotion, and non-regressive fills.

BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm_raw,
    -- org roots
    util.org_domain(NULLIF(s.company_domain,'')) AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL
         ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'') AS apply_root_raw,
    -- fillable attrs
    s.company_description_raw,
    s.company_size_raw,
    s.company_industry_raw,
    s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
canon AS (
  SELECT
    src.*,
    CASE
      WHEN site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(site_root_raw)
           AND NOT util.is_ats_host(site_root_raw)
      THEN site_root_raw
      WHEN email_root_raw IS NOT NULL
      THEN email_root_raw
      ELSE NULL
    END AS org_root,
    util.company_name_norm(src.company_name) AS name_norm
  FROM src
),
brand AS (
  SELECT
    c.*,
    -- infer brand_key from rules if org_root matches and name hits regex; coalesce to ''
    COALESCE((
      SELECT r.brand_key
      FROM gold.company_brand_rule r
      WHERE r.active = TRUE
        AND r.domain_root = c.org_root
        AND c.name_norm ~ r.brand_regex
      LIMIT 1
    ), '') AS brand_key_norm
  FROM canon c
),
dedup_name AS (
  -- mark name-only rows that are too close to existing names/aliases
  SELECT
    b.*,
    CASE
      WHEN b.org_root IS NOT NULL THEN FALSE
      ELSE EXISTS (
        SELECT 1 FROM gold.company gc
        WHERE similarity(b.name_norm, gc.name_norm) >= 0.90
      ) OR EXISTS (
        SELECT 1 FROM gold.company_alias ga
        WHERE similarity(b.name_norm, ga.alias_norm) >= 0.90
      )
    END AS is_fuzzy_dup
  FROM brand b
),

/* ---------- PHASE A: domain winners ---------- */
domain_pool AS (
  SELECT * FROM dedup_name
  WHERE org_root IS NOT NULL
),
domain_winner AS (
  -- one winner per (org_root, brand_key_norm)
  SELECT DISTINCT ON (org_root, brand_key_norm)
    dp.*
  FROM domain_pool dp
  ORDER BY
    org_root, brand_key_norm,
    ((dp.company_description_raw IS NOT NULL)::int
     + (dp.company_size_raw IS NOT NULL)::int
     + (dp.company_industry_raw IS NOT NULL)::int
     + (dp.company_logo_url IS NOT NULL)::int) DESC
),
ins_domain AS (
  INSERT INTO gold.company (name, website_domain, brand_key,
                            description, size_raw, industry_raw, logo_url)
  SELECT
    w.company_name,
    w.org_root,
    w.brand_key_norm,  -- '' for default brand
    w.company_description_raw,
    w.company_size_raw,
    w.company_industry_raw,
    w.company_logo_url
  FROM domain_winner w
  ON CONFLICT ON CONSTRAINT company_domain_brand_uniq DO UPDATE
    SET name = CASE
                 WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
                 ELSE gold.company.name
               END,
        description  = COALESCE(gold.company.description,  EXCLUDED.description),
        size_raw     = COALESCE(gold.company.size_raw,     EXCLUDED.size_raw),
        industry_raw = COALESCE(gold.company.industry_raw, EXCLUDED.industry_raw),
        logo_url     = COALESCE(gold.company.logo_url,     EXCLUDED.logo_url)
  RETURNING company_id, website_domain, brand_key
),

/* ---------- PHASE B: name-only winners ---------- */
name_pool AS (
  SELECT * FROM dedup_name
  WHERE org_root IS NULL AND is_fuzzy_dup = FALSE
),
name_winner AS (
  SELECT DISTINCT ON (name_norm)
    np.*
  FROM name_pool np
  ORDER BY
    np.name_norm,
    ((np.company_description_raw IS NOT NULL)::int
     + (np.company_size_raw IS NOT NULL)::int
     + (np.company_industry_raw IS NOT NULL)::int
     + (np.company_logo_url IS NOT NULL)::int) DESC
),
ins_name AS (
  INSERT INTO gold.company (name, description, size_raw, industry_raw, logo_url)
  SELECT
    w.company_name,
    w.company_description_raw,
    w.company_size_raw,
    w.company_industry_raw,
    w.company_logo_url
  FROM name_winner w
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name = CASE
                 WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
                 ELSE gold.company.name
               END,
        description  = COALESCE(gold.company.description,  EXCLUDED.description),
        size_raw     = COALESCE(gold.company.size_raw,     EXCLUDED.size_raw),
        industry_raw = COALESCE(gold.company.industry_raw, EXCLUDED.industry_raw),
        logo_url     = COALESCE(gold.company.logo_url,     EXCLUDED.logo_url)
  RETURNING company_id
),

/* ---------- Resolve company_id for ALL source rows ---------- */
resolved AS (
  SELECT
    d.source, d.source_id, d.source_row_url,
    d.company_name, d.name_norm,
    d.org_root, d.brand_key_norm,
    COALESCE(
      (SELECT gc.company_id
         FROM gold.company gc
         WHERE d.org_root IS NOT NULL
           AND gc.website_domain = d.org_root
           AND gc.brand_key      = d.brand_key_norm
         LIMIT 1),
      (SELECT gc2.company_id
         FROM gold.company gc2
         WHERE gc2.name_norm = d.name_norm
         LIMIT 1)
    ) AS company_id,
    d.company_description_raw, d.company_size_raw, d.company_industry_raw, d.company_logo_url,
    d.apply_root_raw, d.email_root_raw
  FROM dedup_name d
),

/* ---------- Aliases & evidence ---------- */
add_alias AS (
  INSERT INTO gold.company_alias(company_id, alias)
  SELECT DISTINCT r.company_id, r.company_name
  FROM resolved r
  WHERE r.company_id IS NOT NULL
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),
add_evidence AS (
  INSERT INTO gold.company_evidence_domain(company_id, kind, value, source, source_id)
  SELECT DISTINCT r.company_id, kv.kind, kv.val, r.source, r.source_id
  FROM resolved r
  CROSS JOIN LATERAL (
    VALUES
      ('website', r.org_root),
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
),

/* ---------- Promote domain when learned later ---------- */
promote_domain AS (
  UPDATE gold.company gc
  SET website_domain = sub.org_root
  FROM (
    SELECT r.company_id, MIN(r.org_root) AS org_root
    FROM resolved r
    WHERE r.org_root IS NOT NULL
    GROUP BY r.company_id
  ) sub
  WHERE gc.company_id = sub.company_id
    AND gc.website_domain IS NULL
  RETURNING 1
)

/* ---------- Non-regressive attribute fill ---------- */
UPDATE gold.company gc
SET description  = COALESCE(gc.description,  r.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     r.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, r.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     r.company_logo_url)
FROM resolved r
WHERE gc.company_id = r.company_id;

COMMIT;
