-- transforms/sql/15_gold_contact_etl.sql
-- Deterministic, idempotent ETL to populate gold.contact* from silver.unified_silver

SET search_path = public;

-- Parameters
-- Active affiliation window (days)
WITH params AS (
  SELECT 270::int AS active_days
),

unified AS (
  SELECT
    us.source,
    us.source_site,
    us.source_id,
    us.source_row_url,
    COALESCE(us.scraped_at, now()) AS scraped_at,
    us.date_posted,

    -- Company hints
    NULLIF(us.company_domain,'')                         AS company_domain,
    NULLIF(us.company_website,'')                        AS company_website,
    NULLIF(us.company_linkedin_url,'')                   AS company_linkedin_url,
    NULLIF(us.company_stepstone_id,'')                   AS company_stepstone_id,

    -- Contact bits
    NULLIF(us.emails_raw,'')                             AS emails_raw,
    us.emails_all                                        AS emails_all,
    us.contacts_raw                                      AS contacts_raw,        -- <== changed: no jsonb_safe()
    NULLIF(us.contact_person_raw,'')                     AS contact_person_raw,
    NULLIF(us.contact_phone_raw,'')                      AS contact_phone_raw,

    -- Location (weak)
    us.city_guess, us.region_guess, us.country_guess
  FROM silver.unified_silver us
),

-- Derive company resolution inputs
company_hint AS (
  SELECT
    u.*,
    util.linkedin_slug(u.company_linkedin_url)                                 AS company_slug,
    util.org_domain(NULLIF(u.company_domain,''))                               AS company_root
  FROM unified u
),

-- Resolve to gold.company (ranked: stepstone -> domain -> same_org -> slug -> email_root)
company_resolved AS (
  SELECT
    h.*,
    COALESCE(
      -- 1) StepStone company id (via company evidence)
      (
        SELECT ced.company_id
        FROM gold.company_evidence_domain ced
        WHERE ced.kind = 'stepstone_id'
          AND ced.value = h.company_stepstone_id
        LIMIT 1
      ),
      -- 2) exact domain
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE h.company_root IS NOT NULL
          AND gc.website_domain = h.company_root
        LIMIT 1
      ),
      -- 3) same org domain
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE h.company_root IS NOT NULL
          AND util.same_org_domain(gc.website_domain, h.company_root)
        LIMIT 1
      ),
      -- 4) linkedin slug
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE h.company_slug IS NOT NULL
          AND gc.linkedin_slug = h.company_slug
        LIMIT 1
      )
      -- 5) email root handled later per-atom if needed
    ) AS company_id_hint
  FROM company_hint h
),

-- A) ATOMS from contacts_raw (arrays of people objects)
json_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    COALESCE(
      NULLIF(trim(both from (c.value->>'personName')),''),
      NULLIF(trim(both from (c.value->>'name')),'')
    ) AS person_name,
    COALESCE(
      NULLIF(lower(c.value->>'emailAddress'),''),
      NULLIF(lower(c.value->>'email'),''),
      NULLIF(lower(c.value->>'mail'),'')
    ) AS email,
    COALESCE(
      NULLIF(c.value->>'personTitle',''),
      NULLIF(c.value->>'title','')
    ) AS title,
    COALESCE(
      NULLIF(c.value->>'phoneNumber',''),
      NULLIF(c.value->>'phone',''),
      NULLIF(c.value->>'tel','')
    ) AS phone,
    'json_contacts'::text AS fact_src
  FROM company_resolved cr
  LEFT JOIN LATERAL jsonb_array_elements(
    CASE
      WHEN cr.contacts_raw IS NOT NULL AND jsonb_typeof(cr.contacts_raw) = 'array' THEN cr.contacts_raw
      ELSE '[]'::jsonb
    END
  ) AS c(value) ON TRUE
),

-- B) ATOMS from emails_all
emails_all_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    NULL::text AS person_name,
    lower(e)   AS email,
    NULL::text AS title,
    NULL::text AS phone,
    'emails_all'::text AS fact_src
  FROM company_resolved cr
  CROSS JOIN LATERAL unnest(COALESCE(cr.emails_all, '{}'::text[])) AS e
  WHERE e ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
),

-- C) ATOMS from emails_raw (split common separators)
emails_raw_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    NULL::text AS person_name,
    lower(trim(both from e)) AS email,
    NULL::text AS title,
    NULL::text AS phone,
    'emails_raw'::text AS fact_src
  FROM company_resolved cr,
       LATERAL (
         SELECT regexp_split_to_table(cr.emails_raw, '\s*[;,]\s*') AS e
       ) s
  WHERE cr.emails_raw IS NOT NULL
    AND s.e ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
),

-- D) ATOMS from raw person/phone
raw_person_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    cr.contact_person_raw AS person_name,
    NULL::text AS email,
    NULL::text AS title,
    cr.contact_phone_raw AS phone,
    'contact_raw'::text AS fact_src
  FROM company_resolved cr
  WHERE cr.contact_person_raw IS NOT NULL OR cr.contact_phone_raw IS NOT NULL
),

-- UNION + normalize
atoms AS (
  SELECT DISTINCT
    a.source, a.source_id, a.source_row_url, a.scraped_at, a.date_posted,
    a.company_id_hint, a.company_root, a.company_slug,
    NULLIF(a.person_name,'') AS person_name,
    NULLIF(a.email,'')       AS email,
    NULLIF(a.title,'')       AS title,
    NULLIF(a.phone,'')       AS phone,
    a.fact_src,

    -- Derived
    util.email_domain(a.email)                              AS email_domain,
    util.org_domain(util.email_domain(a.email))             AS email_root,
    util.is_generic_email_domain(util.email_domain(a.email)) AS is_generic_domain,
    util.is_generic_mailbox(a.email)                        AS is_generic_mailbox,
    util.person_name_norm(a.person_name)                    AS name_norm,
    util.phone_norm(a.phone)                                AS phone_norm
  FROM (
    SELECT * FROM json_atoms
    UNION ALL
    SELECT * FROM emails_all_atoms
    UNION ALL
    SELECT * FROM emails_raw_atoms
    UNION ALL
    SELECT * FROM raw_person_atoms
  ) a
),

-- Re-resolve company from email_root if no hint found yet (and the root is usable)
atoms_with_company AS (
  SELECT
    at.*,
    COALESCE(
      at.company_id_hint,
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE at.email_root IS NOT NULL
          AND NOT util.is_aggregator_host(at.email_root)
          AND NOT util.is_ats_host(at.email_root)
          AND NOT util.is_career_host(at.email_root)
          AND gc.website_domain = at.email_root
        LIMIT 1
      ),
      (
        SELECT gc.company_id
        FROM gold.company gc
        WHERE at.email_root IS NOT NULL
          AND NOT util.is_aggregator_host(at.email_root)
          AND NOT util.is_ats_host(at.email_root)
          AND NOT util.is_career_host(at.email_root)
          AND util.same_org_domain(gc.website_domain, at.email_root)
        LIMIT 1
      )
    ) AS company_id
  FROM atoms at
),

-- Creation policy:
-- Create a contact if:
--  - email present AND NOT (generic domain OR generic mailbox), OR
--  - person_name present, OR
--  - phone present
atoms_eligible AS (
  SELECT *
  FROM atoms_with_company
  WHERE
    (email IS NOT NULL AND NOT (is_generic_domain OR is_generic_mailbox))
    OR person_name IS NOT NULL
    OR phone IS NOT NULL
),

-- Try to find existing contacts
existing_by_email AS (
  SELECT DISTINCT
    a.source, a.source_id, a_
