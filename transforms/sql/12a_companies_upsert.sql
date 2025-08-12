-- Deterministic, brand-aware, fuzzy-guarded upsert into gold.company from silver.unified.
-- Identity: prefer name_norm (so we can merge name-only rows) and set website_domain/brand_key when we learn them.
-- Always writes alias & evidence; upgrades placeholder names; promotes domain later if needed.

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
    -- carry some fillable attrs
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
  -- 1) choose best org root: prefer site_root, else email_root (skip aggregators/ATS)
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
    -- 2) canonical normalized name (apply synonyms like (aws) -> amazon web services)
    util.company_name_norm(src.company_name) AS name_norm
  FROM src
),
brand AS (
  -- 3) infer brand_key from rules if org_root matches and name hits regex
  SELECT
    c.*,
    (
      SELECT r.brand_key
      FROM gold.company_brand_rule r
      WHERE r.active = TRUE
        AND r.domain_root = c.org_root
        AND c.name_norm ~ r.brand_regex
      LIMIT 1
    ) AS brand_key
  FROM canon c
),
dedup_name AS (
  -- 4) Fuzzy guard for name-only inserts (no org root): skip if similar to an existing name/alias
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
to_insert AS (
  -- 5) Candidates we will actually insert (domain path OR clean name-only)
  SELECT * FROM dedup_name
  WHERE (org_root IS NOT NULL) OR (is_fuzzy_dup = FALSE)
),
-- 6) NAME-FIRST upsert:
--    We upsert by name_norm so if a name-only row already exists (e.g., "SAP"),
--    we update that row to add website_domain/brand_key instead of colliding.
ins_namefirst AS (
  INSERT INTO gold.company (name, website_domain, brand_key,
                            description, size_raw, industry_raw, logo_url)
  SELECT
    ti.company_name,
    ti.org_root,           -- may be NULL
    ti.brand_key,          -- may be NULL
    ti.company_description_raw,
    ti.company_size_raw,
    ti.company_industry_raw,
    ti.company_logo_url
  FROM to_insert ti
  ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
    SET name = CASE
                 WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
                 ELSE gold.company.name
               END,
        -- Only promote domain/brand when they are missing on the existing row,
        -- or when they agree with the same org root.
        website_domain = COALESCE(gold.company.website_domain, EXCLUDED.website_domain),
        brand_key      = COALESCE(gold.company.brand_key,      EXCLUDED.brand_key),
        description    = COALESCE(gold.company.description,    EXCLUDED.description),
        size_raw       = COALESCE(gold.company.size_raw,       EXCLUDED.size_raw),
        industry_raw   = COALESCE(gold.company.industry_raw,   EXCLUDED.industry_raw),
        logo_url       = COALESCE(gold.company.logo_url,       EXCLUDED.logo_url)
  RETURNING company_id
),
resolved AS (
  -- 7) Resolve the company_id for every src row (inserted or already existing)
  SELECT
    d.source, d.source_id, d.source_row_url,
    d.company_name, d.name_norm,
    d.org_root, d.brand_key,
    (SELECT company_id FROM gold.company gc WHERE gc.name_norm = d.name_norm LIMIT 1) AS company_id,
    d.company_description_raw, d.company_size_raw, d.company_industry_raw, d.company_logo_url,
    d.apply_root_raw, d.email_root_raw
  FROM dedup_name d
)
-- 8) Always add alias & evidence; promote attrs (no regress) and domain if we learn it later
, add_alias AS (
  INSERT INTO gold.company_alias(company_id, alias)
  SELECT DISTINCT r.company_id, r.company_name
  FROM resolved r
  WHERE r.company_id IS NOT NULL
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
)
, add_evidence AS (
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
)
-- 9) Promote website_domain for name-only companies if we now have evidence
, promote_domain AS (
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
-- 10) Top-up attributes (no regress)
UPDATE gold.company gc
SET description  = COALESCE(gc.description,  r.company_description_raw),
    size_raw     = COALESCE(gc.size_raw,     r.company_size_raw),
    industry_raw = COALESCE(gc.industry_raw, r.company_industry_raw),
    logo_url     = COALESCE(gc.logo_url,     r.company_logo_url)
FROM resolved r
WHERE gc.company_id = r.company_id;

COMMIT;
