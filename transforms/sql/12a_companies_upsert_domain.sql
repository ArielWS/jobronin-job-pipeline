BEGIN;

WITH src AS (
  SELECT DISTINCT
    s.source, s.source_id, s.source_row_url,
    s.company_name,
    util.company_name_norm(s.company_name) AS name_norm,
    -- raw candidates
    util.org_domain(NULLIF(s.company_domain,'')) AS site_root_raw,
    CASE WHEN util.is_generic_email_domain(s.contact_email_root) THEN NULL ELSE s.contact_email_root END AS email_root_raw,
    NULLIF(s.apply_root,'') AS apply_root_raw,
    -- fillable attrs
    s.company_description_raw, s.company_size_raw, s.company_industry_raw, s.company_logo_url
  FROM silver.unified s
  WHERE s.company_name IS NOT NULL AND btrim(s.company_name) <> ''
    AND util.company_name_norm(s.company_name) IS NOT NULL
    AND NOT util.is_placeholder_company_name(s.company_name)
),
canon AS (
  -- choose best org_root candidate per row (prefer site over email later)
  SELECT
    src.*,
    /* mark which source would be the org root */
    site_root_raw,
    email_root_raw,
    -- compute both options; we'll pick one per name_norm
    site_root_raw  AS org_site,
    email_root_raw AS org_email
  FROM src
),
scored AS (
  -- explode to one row with a chosen org_root: prefer site when available, else email
  SELECT
    c.*,
    CASE WHEN c.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(c.site_root_raw)
           AND NOT util.is_ats_host(c.site_root_raw)
         THEN c.org_site
         WHEN c.email_root_raw IS NOT NULL
         THEN c.org_email
         ELSE NULL
    END AS org_root,
    CASE WHEN c.site_root_raw IS NOT NULL
           AND NOT util.is_aggregator_host(c.site_root_raw)
           AND NOT util.is_ats_host(c.site_root_raw)
         THEN 1 ELSE 0 END AS org_is_site,
    ((c.company_description_raw IS NOT NULL)::int
     + (c.company_size_raw IS NOT NULL)::int
     + (c.company_industry_raw IS NOT NULL)::int
     + (c.company_logo_url IS NOT NULL)::int) AS richness
  FROM canon c
),
brand AS (
  -- infer brand key; coalesce to '' so (domain,brand) uniqueness always works
  SELECT
    s.*,
    COALESCE((
      SELECT r.brand_key
      FROM gold.company_brand_rule r
      WHERE r.active = TRUE
        AND r.domain_root = s.org_root
        AND s.name_norm ~ r.brand_regex
      LIMIT 1
    ), ''::text) AS brand_key_norm
  FROM scored s
),
winner_per_name AS (
  -- *** KEY FIX: keep ONE row per name_norm ***
  SELECT DISTINCT ON (name_norm)
    b.*
  FROM brand b
  ORDER BY
    b.name_norm,
    b.org_is_site DESC,       -- prefer real site over email
    b.richness DESC           -- then pick richer record
),
winner_final AS (
  -- optional extra guard: keep one per (org_root, brand_key) too
  SELECT DISTINCT ON (org_root, brand_key_norm)
    w.*
  FROM winner_per_name w
  ORDER BY
    org_root, brand_key_norm,
    w.richness DESC
)
INSERT INTO gold.company (name, website_domain, brand_key, description, size_raw, industry_raw, logo_url)
SELECT
  w.company_name,
  w.org_root,                 -- may be NULL (then only name will be set)
  COALESCE(w.brand_key_norm, ''),  -- NOT NULL
  w.company_description_raw, w.company_size_raw, w.company_industry_raw, w.company_logo_url
FROM winner_final w
ON CONFLICT ON CONSTRAINT company_name_norm_uniq DO UPDATE
SET name = CASE
             WHEN util.is_placeholder_company_name(gold.company.name) THEN EXCLUDED.name
             ELSE gold.company.name
           END,
    website_domain = COALESCE(gold.company.website_domain, EXCLUDED.website_domain),
    brand_key      = COALESCE(gold.company.brand_key,      EXCLUDED.brand_key),
    description    = COALESCE(gold.company.description,    EXCLUDED.description),
    size_raw       = COALESCE(gold.company.size_raw,       EXCLUDED.size_raw),
    industry_raw   = COALESCE(gold.company.industry_raw,   EXCLUDED.industry_raw),
    logo_url       = COALESCE(gold.company.logo_url,       EXCLUDED.logo_url);

COMMIT;
