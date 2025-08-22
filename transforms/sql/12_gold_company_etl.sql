-- transforms/sql/12_gold_company_etl.sql
-- Unified ETL for gold.company:
--   1) seed-by-name
--   2) upsert (name-first, then domain/brand + profile enrichment)
--   3) evidence write (website/email)
--   4) promote/upgrade website_domain from evidence
--   5) email backfill for website_domain
--   6) aliases
--   7) linkedin slug extraction
-- All standardized to silver.unified_silver (company_raw, company_domain, etc.)

BEGIN;

--------------------------------------------------------------------------------
-- 1) Seed companies by distinct normalized name
--------------------------------------------------------------------------------
WITH cand AS (
  SELECT DISTINCT
    s.company_raw                                   AS company_name,
    util.company_name_norm(s.company_raw)           AS name_norm,
    lower(util.org_domain(NULLIF(s.company_domain,''))) AS website_root
  FROM silver.unified_silver s
  WHERE s.company_raw IS NOT NULL
    AND btrim(s.company_raw) <> ''
    AND util.company_name_norm(s.company_raw) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_raw)
),
picked AS (
  SELECT DISTINCT ON (name_norm) company_name, name_norm, website_root
  FROM cand
  ORDER BY name_norm, website_root
)
INSERT INTO gold.company (name, website_domain)
SELECT company_name, website_root
FROM picked
ON CONFLICT DO NOTHING;

--------------------------------------------------------------------------------
-- 2) Collision-proof upsert from silver.unified_silver
--------------------------------------------------------------------------------
WITH src AS (
  SELECT DISTINCT
    s.source                                           AS source,
    s.source_id::text                                  AS source_id,
    s.source_row_url                                   AS source_row_url,
    s.company_raw                                      AS company_name,
    util.company_name_norm_langless(s.company_raw)     AS name_norm,
    lower(util.org_domain(NULLIF(s.company_domain,''))) AS site_root_raw,
    s.company_description_raw                          AS company_description_raw,
    CASE WHEN s.company_size_raw ~ '\d' THEN btrim(s.company_size_raw) END AS company_size_raw,
    s.company_industry_raw                             AS company_industry_raw,
    s.company_logo_url                                 AS company_logo_url,
    CASE
      WHEN s.contact_email_root IS NOT NULL
           AND NOT util.is_generic_email_domain(s.contact_email_root)
      THEN lower(s.contact_email_root)
      ELSE NULL
    END                                                AS email_root_raw
  FROM silver.unified_silver s
  WHERE s.company_raw IS NOT NULL
    AND btrim(s.company_raw) <> ''
    AND util.company_name_norm_langless(s.company_raw) IS NOT NULL
    AND util.is_placeholder_company_name(s.company_raw) = FALSE
),
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
        (s.company_description_raw IS NOT NULL
         OR s.company_size_raw IS NOT NULL
         OR s.company_industry_raw IS NOT NULL
         OR s.company_logo_url IS NOT NULL) DESC,
        (s.site_root_raw IS NOT NULL
         AND NOT util.is_aggregator_host(s.site_root_raw)
         AND NOT util.is_ats_host(s.site_root_raw)
         AND NOT util.is_career_host(s.site_root_raw)) DESC,
        length(coalesce(s.company_name,'')) DESC
    ) AS rn
  FROM src s
),
profile_agg AS (
  SELECT DISTINCT
    s.name_norm,
    FIRST_VALUE(s.company_description_raw) OVER (
      PARTITION BY s.name_norm
      ORDER BY (s.company_description_raw IS NULL),
               (s.site_root_raw IS NULL),
               length(coalesce(s.company_description_raw, '')) DESC
    ) AS company_description_raw,
    FIRST_VALUE(s.company_size_raw) OVER (
      PARTITION BY s.name_norm
      ORDER BY (s.company_size_raw IS NULL),
               (s.site_root_raw IS NULL),
               length(coalesce(s.company_size_raw, '')) DESC
    ) AS company_size_raw,
    FIRST_VALUE(s.company_industry_raw) OVER (
      PARTITION BY s.name_norm
      ORDER BY (s.company_industry_raw IS NULL),
               (s.site_root_raw IS NULL),
               length(coalesce(s.company_industry_raw, '')) DESC
    ) AS company_industry_raw,
    FIRST_VALUE(s.company_logo_url) OVER (
      PARTITION BY s.name_norm
      ORDER BY (s.company_logo_url IS NULL),
               (s.site_root_raw IS NULL),
               length(coalesce(s.company_logo_url, '')) DESC
    ) AS company_logo_url
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
ins_names AS (
  INSERT INTO gold.company AS gc (name, brand_key, description, size_raw, industry_raw, logo_url)
  SELECT
    b.company_name,
    b.brand_key_norm,
    p.company_description_raw,
    p.company_size_raw,
    p.company_industry_raw,
    p.company_logo_url
  FROM best_per_name_brand b
  JOIN profile_agg p ON p.name_norm = b.name_norm
  ON CONFLICT DO NOTHING
  RETURNING gc.company_id, gc.name_norm
),
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
  SET website_domain = COALESCE(gc.website_domain, lower(dw.org_root)),
      brand_key      = COALESCE(gc.brand_key,      dw.brand_key_norm)
  FROM domain_winner dw
  WHERE dw.rnk = 1
    AND gc.name_norm = dw.name_norm
    AND dw.org_root IS NOT NULL
  RETURNING 1
),
upd_details AS (
  UPDATE gold.company gc
  SET description  = COALESCE(gc.description,  p.company_description_raw),
      size_raw     = COALESCE(gc.size_raw,     p.company_size_raw),
      industry_raw = COALESCE(gc.industry_raw, p.company_industry_raw),
      logo_url     = COALESCE(gc.logo_url,     p.company_logo_url)
  FROM profile_agg p
  WHERE gc.name_norm = p.name_norm
  RETURNING 1
),
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
      ('website', lower(r.org_root_candidate)),
      ('email',   CASE WHEN r.email_root_raw IS NOT NULL
                           AND NOT util.is_generic_email_domain(r.email_root_raw)
                       THEN lower(r.email_root_raw) END)
  ) AS kv(kind, val)
  WHERE r.company_id IS NOT NULL
    AND kv.val IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
),
ins_names_ref AS (SELECT 1 FROM ins_names LIMIT 1)

-- Promote placeholder names if we learned a better one
UPDATE gold.company gc
SET name = b.company_name
FROM best_per_name_brand b, ins_names_ref
WHERE util.is_placeholder_company_name(gc.name)
  AND gc.name_norm = b.name_norm
  AND util.company_name_norm_langless(b.company_name) = gc.name_norm;

--------------------------------------------------------------------------------
-- 3) Promotion of website_domain from evidence (trustworthy website > email)
--------------------------------------------------------------------------------
-- Upgrade to trustworthy WEBSITE domain (or from weaker)
UPDATE gold.company gc
SET website_domain = lower(w.value)
FROM (
    SELECT DISTINCT ON (ed.value, COALESCE(c.brand_key,''))
        ed.company_id,
        ed.value
    FROM gold.company_evidence_domain ed
    JOIN gold.company c ON c.company_id = ed.company_id
    WHERE ed.kind = 'website'
      AND ed.value IS NOT NULL
      AND NOT util.is_aggregator_host(ed.value)
      AND NOT util.is_ats_host(ed.value)
      AND NOT util.is_career_host(ed.value)
    ORDER BY ed.value, COALESCE(c.brand_key,''), ed.company_id
) w
WHERE w.company_id = gc.company_id
  AND gc.website_domain IS DISTINCT FROM lower(w.value)
  AND (
        gc.website_domain IS NULL
        OR EXISTS (
             SELECT 1
             FROM gold.company_evidence_domain e
             WHERE e.company_id = gc.company_id
               AND e.kind = 'email'
               AND e.value = gc.website_domain
        )
        OR util.is_aggregator_host(gc.website_domain)
        OR util.is_ats_host(gc.website_domain)
        OR util.is_career_host(gc.website_domain)
      )
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = lower(w.value)
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

-- Fill from WEBSITE evidence if still NULL
UPDATE gold.company gc
SET website_domain = lower(w.value)
FROM gold.company_evidence_domain w
WHERE w.company_id = gc.company_id
  AND w.kind = 'website'
  AND gc.website_domain IS NULL
  AND w.value IS NOT NULL
  AND NOT util.is_aggregator_host(w.value)
  AND NOT util.is_ats_host(w.value)
  AND NOT util.is_career_host(w.value)
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = lower(w.value)
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

-- Final backfill from EMAIL root if still NULL
UPDATE gold.company gc
SET website_domain = lower(e.value)
FROM gold.company_evidence_domain e
WHERE e.company_id = gc.company_id
  AND e.kind = 'email'
  AND gc.website_domain IS NULL
  AND e.value IS NOT NULL
  AND NOT util.is_generic_email_domain(e.value)
  AND NOT util.is_aggregator_host(e.value)
  AND NOT util.is_ats_host(e.value)
  AND NOT util.is_career_host(e.value)
  AND NOT EXISTS (
        SELECT 1
        FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = lower(e.value)
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

--------------------------------------------------------------------------------
-- 4) Aliases from unified_silver using same_org_domain for direct matches
--------------------------------------------------------------------------------
WITH map AS (
  SELECT
    s.company_raw AS company_name,
    COALESCE(gc.company_id, gc2.company_id, gc3.company_id) AS company_id
  FROM silver.unified_silver s
  LEFT JOIN gold.company gc
    ON s.company_domain IS NOT NULL
   AND util.same_org_domain(gc.website_domain, s.company_domain)
  LEFT JOIN gold.company gc2
    ON gc.company_id IS NULL
   AND s.contact_email_root IS NOT NULL
   AND NOT util.is_generic_email_domain(s.contact_email_root)
   AND util.company_name_norm(gc2.name) = util.company_name_norm(s.company_raw)
  LEFT JOIN gold.company gc3
    ON gc.company_id IS NULL
   AND gc2.company_id IS NULL
   AND util.company_name_norm(gc3.name) = util.company_name_norm(s.company_raw)
  WHERE s.company_raw IS NOT NULL AND btrim(s.company_raw) <> ''
)
INSERT INTO gold.company_alias (company_id, alias)
SELECT DISTINCT company_id, company_name
FROM map
WHERE company_id IS NOT NULL
ON CONFLICT (company_id, alias_norm) DO NOTHING;

--------------------------------------------------------------------------------
-- 5) LinkedIn company slug extraction from unified_silver
--------------------------------------------------------------------------------
WITH linkedin_sources AS (
  SELECT
    util.company_name_norm(s.company_raw) AS name_norm,
    COALESCE(s.company_linkedin_url, s.company_website) AS linkedin_url
  FROM silver.unified_silver s
  WHERE COALESCE(s.company_linkedin_url, s.company_website) ILIKE '%linkedin.com/company/%'
),
slugs AS (
  SELECT
    name_norm,
    lower(
      regexp_replace(
        linkedin_url,
        E'^https?://[^/]*linkedin\\.com/company/([^/?#]+).*',
        E'\\1'
      )
    ) AS slug
  FROM linkedin_sources
  WHERE name_norm IS NOT NULL
    AND linkedin_url IS NOT NULL
),
uniq_slugs AS (
  SELECT
    name_norm,
    MIN(slug) AS slug
  FROM slugs
  WHERE slug IS NOT NULL
  GROUP BY name_norm
)
UPDATE gold.company gc
SET linkedin_slug = us.slug
FROM uniq_slugs us
WHERE gc.name_norm = us.name_norm
  AND gc.linkedin_slug IS DISTINCT FROM us.slug;

COMMIT;
