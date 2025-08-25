-- transforms/sql/12_gold_company_etl.sql
-- Gold ETL: domain-first → LinkedIn-second → StepStoneID → family-anchor → name+geo → name-last.
-- Plus cohort-best survivorship and safe merges (including slug+placeholder and family collapse).
-- Source: silver.unified_silver

BEGIN;

-- Hygiene: normalize empty brand_key -> NULL
UPDATE gold.company
SET brand_key = NULL
WHERE brand_key = '';

-- ---------------------------------------------------------------------------
-- Source snapshot (+ derived org_root, linkedin_slug, stepstone_id, geo, family)
-- ---------------------------------------------------------------------------
WITH src AS (
  SELECT DISTINCT
    s.source                                            AS source,
    s.source_id::text                                   AS source_id,
    s.source_row_url                                    AS source_row_url,
    s.company_raw                                       AS company_name,
    util.company_name_norm_langless(s.company_raw)      AS name_norm,
    util.company_name_family_key(s.company_raw)         AS family_key,
    lower(util.org_domain(NULLIF(s.company_domain,''))) AS site_root_raw,
    s.company_description_raw                           AS company_description_raw,
    CASE WHEN s.company_size_raw ~ '\d' THEN btrim(s.company_size_raw) END AS company_size_raw,
    s.company_industry_raw                              AS company_industry_raw,
    s.company_logo_url                                  AS company_logo_url,
    CASE
      WHEN s.contact_email_root IS NOT NULL
           AND NOT util.is_generic_email_domain(s.contact_email_root)
      THEN lower(s.contact_email_root)
      ELSE NULL
    END                                                 AS email_root_raw,
    -- LinkedIn slug
    CASE
      WHEN COALESCE(s.company_linkedin_url, s.company_website) ILIKE '%linkedin.com/company/%'
      THEN lower(regexp_replace(
             COALESCE(s.company_linkedin_url, s.company_website),
             E'^https?://[^/]*linkedin\\.com/company/([^/?#]+).*',
             E'\\1'
           ))
      ELSE NULL
    END                                                 AS linkedin_slug,
    -- StepStone org/company id if available
    NULLIF(s.company_stepstone_id::text,'')             AS stepstone_id,
    -- Light geo
    NULLIF(s.city_guess,'')                             AS city_guess,
    NULLIF(s.region_guess,'')                           AS region_guess,
    NULLIF(s.country_guess,'')                          AS country_guess,
    coalesce(NULLIF(s.company_location_raw,''), NULLIF(s.company_address_raw,''), NULLIF(s.location_raw,'')) AS loc_text
  FROM silver.unified_silver s
  WHERE s.company_raw IS NOT NULL
    AND btrim(s.company_raw) <> ''
    AND util.company_name_norm_langless(s.company_raw) IS NOT NULL
    AND util.is_placeholder_company_name(s.company_raw) = FALSE
),
rooted AS (
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
    END AS org_root
  FROM src s
),
rooted_brand AS (
  SELECT
    r.*,
    (
      SELECT r2.brand_key
      FROM gold.company_brand_rule r2
      WHERE r2.active = TRUE
        AND r2.domain_root = r.org_root
        AND r.name_norm ~ r2.brand_regex
      LIMIT 1
    ) AS brand_key_norm
  FROM rooted r
),

-- 1) DOMAIN-FIRST: one winner per (org_root, brand_key)
root_winners AS (
  SELECT *
  FROM (
    SELECT
      rb.*,
      ROW_NUMBER() OVER (
        PARTITION BY rb.org_root, COALESCE(rb.brand_key_norm, '')
        ORDER BY
          (rb.company_description_raw IS NOT NULL
           OR rb.company_size_raw IS NOT NULL
           OR rb.company_industry_raw IS NOT NULL
           OR rb.company_logo_url IS NOT NULL) DESC,
          length(coalesce(rb.company_name,'')) DESC
      ) AS rn
    FROM rooted_brand rb
    WHERE rb.org_root IS NOT NULL
  ) t
  WHERE rn = 1
),
ins_domain AS (
  INSERT INTO gold.company AS gc (name, brand_key, website_domain, description, size_raw, industry_raw, logo_url)
  SELECT
    w.company_name,
    w.brand_key_norm,
    lower(w.org_root),
    w.company_description_raw,
    w.company_size_raw,
    w.company_industry_raw,
    w.company_logo_url
  FROM root_winners w
  ON CONFLICT DO NOTHING
  RETURNING gc.company_id, gc.name_norm, gc.website_domain
),

-- 2) LINKEDIN-SECOND: attach slug to unique domain twin or create slug-anchored row
unique_domain_for_name AS (
  SELECT name_norm, MIN(company_id) AS company_id, COUNT(*) AS n
  FROM gold.company
  WHERE website_domain IS NOT NULL
  GROUP BY name_norm
),
upd_slug_on_unique_domain AS (
  UPDATE gold.company gc
  SET linkedin_slug = s.linkedin_slug
  FROM src s
  JOIN unique_domain_for_name u
    ON u.name_norm = s.name_norm AND u.n = 1
  WHERE gc.company_id = u.company_id
    AND s.linkedin_slug IS NOT NULL
    AND gc.linkedin_slug IS DISTINCT FROM s.linkedin_slug
  RETURNING 1
),
linkedin_candidates AS (
  SELECT DISTINCT s.company_name, s.name_norm, s.linkedin_slug
  FROM src s
  WHERE s.linkedin_slug IS NOT NULL
),
ins_linkedin AS (
  INSERT INTO gold.company AS gc (name, linkedin_slug)
  SELECT lc.company_name, lc.linkedin_slug
  FROM linkedin_candidates lc
  WHERE NOT EXISTS (SELECT 1 FROM gold.company g WHERE lower(g.linkedin_slug) = lc.linkedin_slug)
  ON CONFLICT DO NOTHING
  RETURNING gc.company_id, gc.name_norm
),

-- Current anchors in gold
domain_map AS (
  SELECT gc.website_domain, gc.company_id
  FROM gold.company gc
  WHERE gc.website_domain IS NOT NULL
),
slug_map AS (
  SELECT lower(gc.linkedin_slug) AS slug, gc.company_id
  FROM gold.company gc
  WHERE gc.linkedin_slug IS NOT NULL
),

-- FAMILY ANCHOR: one and only one domain owner per family_key
family_anchor_domain AS (
  SELECT fk.family_key, MIN(gc.company_id) AS company_id
  FROM (
    SELECT DISTINCT util.company_name_family_key(name) AS family_key, company_id
    FROM gold.company
    WHERE website_domain IS NOT NULL
  ) gc
  JOIN LATERAL (SELECT gc.family_key) fk ON TRUE
  WHERE fk.family_key IS NOT NULL
  GROUP BY fk.family_key
  HAVING COUNT(*) = 1
),

-- STEPSTONE COHORT ANCHOR: if any member has an anchor, reuse it
stepstone_anchor AS (
  SELECT
    s.stepstone_id,
    COALESCE(dm.company_id, sm.company_id) AS anchor_company_id
  FROM src s
  LEFT JOIN rooted r ON r.source = s.source AND r.source_id = s.source_id
  LEFT JOIN domain_map dm ON dm.website_domain = r.org_root
  LEFT JOIN slug_map   sm ON sm.slug = r.linkedin_slug
  WHERE s.stepstone_id IS NOT NULL
    AND (dm.company_id IS NOT NULL OR sm.company_id IS NOT NULL)
),

-- Name+geo anchored map (same name_norm AND same geo as an anchored row)
anchored_resolved AS (
  SELECT
    r.name_norm,
    COALESCE(dm.company_id, sm.company_id) AS company_id,
    r.city_guess, r.region_guess, r.country_guess
  FROM rooted r
  LEFT JOIN domain_map dm ON dm.website_domain = r.org_root
  LEFT JOIN slug_map   sm ON sm.slug = r.linkedin_slug
  WHERE dm.company_id IS NOT NULL OR sm.company_id IS NOT NULL
),

-- 3) Final resolution priority per row
resolved AS (
  SELECT
    s.source, s.source_id, s.source_row_url,
    s.company_name, s.name_norm, s.family_key,
    s.email_root_raw,
    s.org_root AS org_root_candidate,
    s.linkedin_slug,
    s.stepstone_id,
    s.loc_text,
    s.city_guess, s.region_guess, s.country_guess,
    COALESCE(
      -- (1) by domain
      (SELECT gc.company_id
         FROM gold.company gc
        WHERE gc.website_domain IS NOT NULL
          AND gc.website_domain = s.org_root
        LIMIT 1),
      -- (2) by linkedin slug
      (SELECT gc.company_id
         FROM gold.company gc
        WHERE s.linkedin_slug IS NOT NULL
          AND lower(gc.linkedin_slug) = s.linkedin_slug
        LIMIT 1),
      -- (3) by StepStone cohort anchor
      (SELECT sa.anchor_company_id
         FROM stepstone_anchor sa
        WHERE sa.stepstone_id = s.stepstone_id
        LIMIT 1),
      -- (4) by unique family-domain anchor
      (SELECT fad.company_id
         FROM family_anchor_domain fad
        WHERE fad.family_key = s.family_key
        LIMIT 1),
      -- (5) by name + geo to an anchored company (unique candidate)
      (SELECT ar.company_id
         FROM anchored_resolved ar
        WHERE ar.name_norm = s.name_norm
          AND (
                (ar.city_guess    IS NOT NULL AND ar.city_guess    = s.city_guess) OR
                (ar.region_guess  IS NOT NULL AND ar.region_guess  = s.region_guess) OR
                (ar.country_guess IS NOT NULL AND ar.country_guess = s.country_guess)
              )
        GROUP BY ar.company_id
        HAVING COUNT(*) >= 1
        LIMIT 1),
      -- (6) by name_norm (prefer those with domain)
      (SELECT gc2.company_id
         FROM gold.company gc2
        WHERE gc2.name_norm = s.name_norm
        ORDER BY (gc2.website_domain IS NOT NULL) DESC, gc2.company_id ASC
        LIMIT 1)
    ) AS company_id
  FROM rooted s
),

-- 4) Insert NAME-ONLY placeholders where needed (ONE per name_norm),
--    and only when NO company row exists yet for that name_norm (prevents slug+placeholder dupes).
ins_placeholders AS (
  INSERT INTO gold.company AS gc (name)
  SELECT DISTINCT ON (r.name_norm) r.company_name
  FROM resolved r
  WHERE r.company_id IS NULL
    AND NOT EXISTS (
      SELECT 1 FROM gold.company g WHERE g.name_norm = r.name_norm
    )
  ORDER BY r.name_norm, length(r.company_name) DESC, r.company_name
  ON CONFLICT DO NOTHING
  RETURNING gc.company_id, gc.name_norm
),

-- Force evaluation of ins_placeholders before evidence write
ins_ref AS (
  SELECT 1 FROM ins_placeholders LIMIT 1
),

-- 5) Evidence write (website/email/stepstone_id/location) — after placeholders exist
add_evidence AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT *
  FROM (
    SELECT DISTINCT
      COALESCE(
        r.company_id,
        (SELECT gc.company_id
           FROM gold.company gc
          WHERE gc.name_norm = r.name_norm
          ORDER BY (gc.website_domain IS NOT NULL) DESC, gc.company_id
          LIMIT 1)
      ) AS company_id,
      kv.kind,
      kv.val,
      r.source,
      r.source_id
    FROM resolved r
    CROSS JOIN ins_ref
    CROSS JOIN LATERAL (
      VALUES
        ('website', lower(r.org_root_candidate)),
        ('email',   CASE WHEN r.email_root_raw IS NOT NULL AND NOT util.is_generic_email_domain(r.email_root_raw)
                         THEN lower(r.email_root_raw) END),
        ('stepstone_id', r.stepstone_id),
        ('location', NULLIF(r.loc_text,''))
    ) AS kv(kind, val)
    WHERE kv.val IS NOT NULL
  ) z
  WHERE z.company_id IS NOT NULL
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
),

-- 6) Cohort-best survivorship (per company_id), overwrite deterministically
profile_candidates AS (
  SELECT
    COALESCE(r.company_id,
             (SELECT gc.company_id FROM gold.company gc WHERE gc.name_norm = r.name_norm ORDER BY (gc.website_domain IS NOT NULL) DESC, gc.company_id LIMIT 1)
    ) AS company_id,
    (r.org_root_candidate IS NOT NULL) AS has_org_root,
    r.source, r.source_id,
    s.company_description_raw,
    s.company_size_raw,
    s.company_industry_raw,
    s.company_logo_url
  FROM resolved r
  JOIN src s
    ON s.name_norm = r.name_norm
   AND s.source = r.source
   AND s.source_id = r.source_id
  WHERE (r.company_id IS NOT NULL OR EXISTS (SELECT 1 FROM ins_placeholders))
),
profile_best AS (
  SELECT DISTINCT
    pc.company_id,

    FIRST_VALUE(pc.company_description_raw) OVER (
      PARTITION BY pc.company_id
      ORDER BY (pc.company_description_raw IS NOT NULL) DESC,
               pc.has_org_root DESC,
               length(coalesce(pc.company_description_raw,'')) DESC,
               coalesce(pc.source,'~') ASC,
               coalesce(pc.source_id,'~') ASC
    ) AS best_description,

    (SELECT val FROM (
       SELECT pc2.company_size_raw AS val, COUNT(*) AS cnt, BOOL_OR(pc2.has_org_root) AS any_root
       FROM profile_candidates pc2
       WHERE pc2.company_id = pc.company_id AND pc2.company_size_raw IS NOT NULL
       GROUP BY pc2.company_size_raw
       ORDER BY cnt DESC, any_root DESC, length(pc2.company_size_raw) DESC, val ASC
       LIMIT 1
     ) x) AS best_size,

    (SELECT val FROM (
       SELECT pc3.company_industry_raw AS val, COUNT(*) AS cnt, BOOL_OR(pc3.has_org_root) AS any_root
       FROM profile_candidates pc3
       WHERE pc3.company_id = pc.company_id AND pc3.company_industry_raw IS NOT NULL
       GROUP BY pc3.company_industry_raw
       ORDER BY cnt DESC, any_root DESC, length(pc3.company_industry_raw) DESC, val ASC
       LIMIT 1
     ) y) AS best_industry,

    FIRST_VALUE(pc.company_logo_url) OVER (
      PARTITION BY pc.company_id
      ORDER BY (pc.company_logo_url IS NOT NULL) DESC,
               pc.has_org_root DESC,
               length(coalesce(pc.company_logo_url,'')) DESC,
               coalesce(pc.source,'~') ASC,
               coalesce(pc.source_id,'~') ASC
    ) AS best_logo

  FROM profile_candidates pc
),
upd_profile AS (
  UPDATE gold.company gc
  SET description  = pb.best_description,
      size_raw     = pb.best_size,
      industry_raw = pb.best_industry,
      logo_url     = pb.best_logo
  FROM profile_best pb
  WHERE gc.company_id = pb.company_id
  RETURNING 1
)
-- close the WITH-chain
SELECT 1;

-- 7) Promote website_domain from WEBSITE evidence (trustworthy > email)
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
        OR EXISTS (SELECT 1 FROM gold.company_evidence_domain e
                   WHERE e.company_id = gc.company_id AND e.kind = 'email' AND e.value = gc.website_domain)
        OR util.is_aggregator_host(gc.website_domain)
        OR util.is_ats_host(gc.website_domain)
        OR util.is_career_host(gc.website_domain)
      )
  AND NOT EXISTS (
        SELECT 1 FROM gold.company c2
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
        SELECT 1 FROM gold.company c2
        WHERE c2.company_id <> gc.company_id
          AND c2.website_domain = lower(w.value)
          AND COALESCE(c2.brand_key,'') = COALESCE(gc.brand_key,'')
      );

-- 8) Aliases (map visible source names to resolved companies)
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

-- 9) Cleanup merges

-- 9a) slug placeholders → unique domain twin (same name_norm)
WITH twins_slug AS (
  SELECT p.company_id AS placeholder_id, d.company_id AS domain_id
  FROM gold.company p
  JOIN gold.company d
    ON d.name_norm = p.name_norm
   AND p.company_id <> d.company_id
  JOIN (
    SELECT name_norm, MIN(company_id) AS domain_id, COUNT(*) AS n
    FROM gold.company
    WHERE website_domain IS NOT NULL
    GROUP BY name_norm
  ) u ON u.name_norm = p.name_norm AND u.n = 1 AND u.domain_id = d.company_id
  WHERE p.website_domain IS NULL
    AND p.linkedin_slug IS NOT NULL
),
set_slug AS (
  UPDATE gold.company d
  SET linkedin_slug = COALESCE(d.linkedin_slug, p.linkedin_slug)
  FROM twins_slug t
  JOIN gold.company p ON p.company_id = t.placeholder_id
  WHERE d.company_id = t.domain_id
    AND p.linkedin_slug IS NOT NULL
  RETURNING 1
),
move_aliases_slug AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT t.domain_id, ca.alias
  FROM twins_slug t
  JOIN gold.company_alias ca ON ca.company_id = t.placeholder_id
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),
move_evidence_slug AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT t.domain_id, ce.kind, ce.value, ce.source, ce.source_id
  FROM twins_slug t
  JOIN gold.company_evidence_domain ce ON ce.company_id = t.placeholder_id
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)
DELETE FROM gold.company c
USING twins_slug t
WHERE c.company_id = t.placeholder_id;

-- 9b) pure placeholders → domain twin (same name_norm)
WITH twins AS (
  SELECT a.company_id AS placeholder_id, b.company_id AS domain_id
  FROM gold.company a
  JOIN gold.company b
    ON a.name_norm = b.name_norm
   AND a.company_id <> b.company_id
  WHERE a.website_domain IS NULL
    AND a.linkedin_slug IS NULL
    AND b.website_domain IS NOT NULL
),
moved_aliases AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT t.domain_id, ca.alias
  FROM twins t
  JOIN gold.company_alias ca ON ca.company_id = t.placeholder_id
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),
moved_evidence AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT t.domain_id, ce.kind, ce.value, ce.source, ce.source_id
  FROM twins t
  JOIN gold.company_evidence_domain ce ON ce.company_id = t.placeholder_id
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)
DELETE FROM gold.company c
USING twins t
WHERE c.company_id = t.placeholder_id;

-- 9c) FAMILY placeholders → unique family domain anchor (handles "adesso SE" vs "adesso.de")
WITH fam_anchor AS (
  SELECT
    util.company_name_family_key(name) AS family_key,
    MIN(company_id) AS domain_id
  FROM gold.company
  WHERE website_domain IS NOT NULL
    AND util.company_name_family_key(name) IS NOT NULL
  GROUP BY util.company_name_family_key(name)
  HAVING COUNT(*) = 1
),
fam_twins AS (
  SELECT p.company_id AS placeholder_id, fa.domain_id
  FROM gold.company p
  JOIN fam_anchor fa
    ON util.company_name_family_key(p.name) = fa.family_key
  WHERE p.website_domain IS NULL
),
fam_aliases AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT ft.domain_id, ca.alias
  FROM fam_twins ft
  JOIN gold.company_alias ca ON ca.company_id = ft.placeholder_id
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),
fam_evidence AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT ft.domain_id, ce.kind, ce.value, ce.source, ce.source_id
  FROM fam_twins ft
  JOIN gold.company_evidence_domain ce ON ce.company_id = ft.placeholder_id
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)
DELETE FROM gold.company c
USING fam_twins ft
WHERE c.company_id = ft.placeholder_id;

-- 10) FINAL SAFETY NET:
-- Collapse any remaining duplicates per name_norm where ALL rows lack a website_domain.
-- Prefer survivor with a LinkedIn slug; otherwise lowest company_id.
WITH grp AS (
  SELECT
    name_norm,
    BOOL_OR(website_domain IS NOT NULL) AS any_domain
  FROM gold.company
  GROUP BY name_norm
  HAVING COUNT(*) > 1
),
targets AS (
  SELECT g.name_norm
  FROM grp g
  WHERE g.any_domain = FALSE  -- only groups with no domains at all
),
ranked AS (
  SELECT
    c.name_norm,
    c.company_id,
    (c.linkedin_slug IS NOT NULL) AS has_slug,
    ROW_NUMBER() OVER (
      PARTITION BY c.name_norm
      ORDER BY (c.linkedin_slug IS NOT NULL) DESC, c.company_id ASC
    ) AS rnk
  FROM gold.company c
  JOIN targets t ON t.name_norm = c.name_norm
),
survivors AS (
  SELECT name_norm, company_id AS survivor_id
  FROM ranked
  WHERE rnk = 1
),
victims AS (
  SELECT r.name_norm, r.company_id AS victim_id, s.survivor_id
  FROM ranked r
  JOIN survivors s USING (name_norm)
  WHERE r.company_id <> s.survivor_id
),
move_alias_final AS (
  INSERT INTO gold.company_alias (company_id, alias)
  SELECT v.survivor_id, ca.alias
  FROM victims v
  JOIN gold.company_alias ca ON ca.company_id = v.victim_id
  ON CONFLICT (company_id, alias_norm) DO NOTHING
  RETURNING 1
),
move_ev_final AS (
  INSERT INTO gold.company_evidence_domain (company_id, kind, value, source, source_id)
  SELECT v.survivor_id, ce.kind, ce.value, ce.source, ce.source_id
  FROM victims v
  JOIN gold.company_evidence_domain ce ON ce.company_id = v.victim_id
  ON CONFLICT (company_id, kind, value) DO NOTHING
  RETURNING 1
)
DELETE FROM gold.company c
USING victims v
WHERE c.company_id = v.victim_id;

COMMIT;

DO $$
BEGIN
  -- Validate FK on gold.contact → gold.company if present and not yet validated
  IF EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold'
      AND rel.relname='contact'
      AND con.conname='fk_contact_company'
      AND con.contype='f'
      AND NOT con.convalidated
  ) THEN
    ALTER TABLE gold.contact VALIDATE CONSTRAINT fk_contact_company;
  END IF;

  -- Validate FK on gold.contact_affiliation → gold.company if present and not yet validated
  IF EXISTS (
    SELECT 1
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = rel.relnamespace
    WHERE n.nspname='gold'
      AND rel.relname='contact_affiliation'
      AND con.conname='fk_contact_affil_company'
      AND con.contype='f'
      AND NOT con.convalidated
  ) THEN
    ALTER TABLE gold.contact_affiliation VALIDATE CONSTRAINT fk_contact_affil_company;
  END IF;
END
$$;
