-- 15_gold_contact_etl.sql
-- Contacts GOLD ETL (idempotent)
-- Deterministic email-first person resolution, evidence capture,
-- affiliation writing, and safe merges.

SET search_path = public, util, gold, silver;

WITH
-- 1) SOURCE NORMALIZATION ------------------------------------------------------
src AS (
  SELECT
      us.source,
      us.source_id,
      us.source_row_url,
      us.scraped_at,
      us.date_posted,

      -- company hints
      NULLIF(us.company_domain,'')                                          AS company_domain_root,
      NULLIF(us.company_linkedin_url,'')                                    AS company_linkedin_url,
      NULLIF(us.company_name_norm,'')                                       AS company_name_norm,

      -- location hints
      NULLIF(us.city_guess,'') AS city_guess,
      NULLIF(us.region_guess,'') AS region_guess,
      NULLIF(us.country_guess,'') AS country_guess,

      -- contacts: emails (prefer emails_all; else split emails_raw + first_email(description_raw))
      CASE
        WHEN us.emails_all IS NOT NULL AND cardinality(us.emails_all) > 0
          THEN us.emails_all
        ELSE
          (
            SELECT ARRAY(
              SELECT DISTINCT lower(trim(e))
              FROM (
                SELECT regexp_split_to_table(coalesce(us.emails_raw,''), '\s*[;,]\s*') AS e
                UNION ALL
                SELECT util.first_email(coalesce(us.description_raw,''))
              ) s
              WHERE e ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
            )
          )
      END                                                                      AS email_array,

      -- contacts: phone + person hints
      NULLIF(us.contact_person_raw,'')                                        AS contact_person_raw,
      NULLIF(us.contact_phone_raw,'')                                         AS contact_phone_raw,

      -- StepStone JSON contacts (already cleaned in silvers) — keep only if array/object
      CASE
        WHEN jsonb_typeof(us.contacts_raw) IN ('array','object') THEN us.contacts_raw
        ELSE NULL::jsonb
      END                                                                      AS contacts_json
  FROM silver.unified_silver us
),

-- 2) COMPANY RESOLUTION --------------------------------------------------------
resolved_company AS (
  WITH base AS (
    SELECT s.*,
           util.org_domain(s.company_domain_root) AS org_root
    FROM src s
  ),
  dom AS (
    SELECT b.source, b.source_id,
           c.company_id,
           1 AS prio
    FROM base b
    JOIN gold.company c
      ON c.website_domain IS NOT NULL
     AND c.website_domain = b.org_root
  ),
  slug AS (
    SELECT b.source, b.source_id,
           c.company_id,
           2 AS prio
    FROM base b
    CROSS JOIN LATERAL (
      SELECT lower(regexp_replace(coalesce(b.company_linkedin_url,''), '^.*linkedin\.com/(company|school)/', '')) AS slug
    ) l
    JOIN gold.company c
      ON c.linkedin_slug IS NOT NULL
     AND c.linkedin_slug = NULLIF(l.slug,'')
  ),
  name_geo AS (
    SELECT b.source, b.source_id,
           c.company_id,
           3 AS prio
    FROM base b
    JOIN gold.company c
      ON c.name_norm = b.company_name_norm
     AND (b.city_guess IS NOT NULL OR b.region_guess IS NOT NULL OR b.country_guess IS NOT NULL)
  ),
  name_only AS (
    SELECT b.source, b.source_id,
           c.company_id,
           4 AS prio
    FROM base b
    JOIN gold.company c
      ON c.name_norm = b.company_name_norm
  ),
  unioned AS (
    SELECT * FROM dom
    UNION ALL SELECT * FROM slug
    UNION ALL SELECT * FROM name_geo
    UNION ALL SELECT * FROM name_only
  )
  SELECT b.source, b.source_id,
         (SELECT company_id FROM unioned u
           WHERE u.source=b.source AND u.source_id=b.source_id
           ORDER BY prio ASC
           LIMIT 1) AS company_id
  FROM src b
),

-- 3) ATOMS ---------------------------------------------------------------------
atoms_pre AS (
  -- from email_array
  SELECT
    s.source, s.source_id, s.source_row_url, s.scraped_at, s.date_posted,
    rc.company_id,
    lower(e)                               AS email,
    NULL::text                              AS person_name,
    NULL::text                              AS person_title,
    NULL::text                              AS phone_raw,
    'email_array'::text                     AS fact_src
  FROM src s
  JOIN resolved_company rc USING (source, source_id)
  LEFT JOIN LATERAL unnest(coalesce(s.email_array, ARRAY[]::text[])) e ON true

  UNION ALL

  -- from contacts_json (array of objects or single object)
  SELECT
    s.source, s.source_id, s.source_row_url, s.scraped_at, s.date_posted,
    rc.company_id,
    lower(NULLIF(coalesce(obj->>'emailAddress', obj->>'email', obj->>'mail', obj->>'contactEmail'),'')) AS email,
    NULLIF(coalesce(obj->>'personName',
                    (obj->>'firstName')||' '||(obj->>'lastName')), '')                                  AS person_name,
    NULLIF(coalesce(obj->>'personTitle',obj->>'title'),'')                                              AS person_title,
    NULLIF(coalesce(obj->>'phoneNumber', obj->>'phone', obj->>'tel'),'')                                AS phone_raw,
    'json_contacts'::text                                                                               AS fact_src
  FROM src s
  JOIN resolved_company rc USING (source, source_id)
  LEFT JOIN LATERAL (
    -- If array: one row per element
    SELECT elem AS obj
    FROM jsonb_array_elements(s.contacts_json) AS elem
    WHERE jsonb_typeof(s.contacts_json) = 'array'
    UNION ALL
    -- If object: a single row
    SELECT s.contacts_json AS obj
    WHERE jsonb_typeof(s.contacts_json) = 'object'
  ) j ON TRUE

  UNION ALL

  -- raw phone + person fallback
  SELECT
    s.source, s.source_id, s.source_row_url, s.scraped_at, s.date_posted,
    rc.company_id,
    NULL::text                      AS email,
    NULLIF(s.contact_person_raw,'') AS person_name,
    NULL::text                      AS person_title,
    NULLIF(s.contact_phone_raw,'')  AS phone_raw,
    'raw_contact_fields'::text      AS fact_src
  FROM src s
  JOIN resolved_company rc USING (source, source_id)
),

atoms AS (
  SELECT
    a.*,
    NULLIF(util.person_name_norm(coalesce(a.person_name,'')),'')  AS name_norm,
    CASE
      WHEN a.phone_raw ~ '\d' THEN regexp_replace(a.phone_raw, '[^0-9+]', '', 'g')
      ELSE NULL
    END AS phone_norm,

    CASE WHEN a.email IS NOT NULL THEN util.email_domain(a.email) END AS email_domain,
    CASE WHEN a.email IS NOT NULL THEN util.org_domain(util.email_domain(a.email)) END AS email_root,

    CASE WHEN a.email IS NULL THEN false
         ELSE util.is_generic_email_domain(util.email_domain(a.email)) END AS is_generic_domain,

    CASE
      WHEN a.email IS NULL THEN false
      ELSE lower(split_part(a.email,'@',1)) ~* '^(info|career|careers|jobs|recruit|recruiting|hr|bewerbung|kontakt|hello|support|admin|office|team|sbv|service|mail|kontakt|karriere)$'
    END AS is_generic_mailbox
  FROM atoms_pre a
),

-- Only accept a name for an email if seen together in JSON
email_name_pairs AS (
  SELECT DISTINCT lower(email) AS email, name_norm
  FROM atoms
  WHERE email IS NOT NULL AND name_norm IS NOT NULL AND fact_src='json_contacts'
),

-- 5) SEEDS ---------------------------------------------------------------------
seeds AS (
  SELECT
    'email'::text AS seed_kind,
    lower(email)  AS seed_key,
    company_id,
    array_agg(row_to_json(atoms.*)) AS atom_rows
  FROM atoms
  WHERE email IS NOT NULL
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    'name_company'::text AS seed_kind,
    name_norm            AS seed_key,
    company_id,
    array_agg(row_to_json(atoms.*)) AS atom_rows
  FROM atoms
  WHERE email IS NULL AND name_norm IS NOT NULL AND company_id IS NOT NULL
  GROUP BY 1,2,3
),

-- 6) BEST CHOICE ---------------------------------------------------------------
best_per_seed AS (
  SELECT
    s.seed_kind,
    s.seed_key,
    s.company_id,

    -- best email (prefer non-generic domain/mailbox, then earliest)
    (
      SELECT a->>'email'
      FROM (
        SELECT (a)::jsonb AS a
        FROM unnest(s.atom_rows) a
        WHERE (a->>'email') IS NOT NULL
        ORDER BY
          ((a->>'is_generic_domain')::boolean) ASC,
          ((a->>'is_generic_mailbox')::boolean) ASC,
          (a->>'scraped_at') ASC
      ) z
      LIMIT 1
    ) AS best_email,

    -- best phone: longest normalized
    (
      SELECT a->>'phone_norm'
      FROM (
        SELECT (a)::jsonb AS a
        FROM unnest(s.atom_rows) a
        WHERE NULLIF(a->>'phone_norm','') IS NOT NULL
        ORDER BY length(a->>'phone_norm') DESC NULLS LAST, (a->>'scraped_at') ASC
      ) z
      LIMIT 1
    ) AS best_phone_norm,

    -- best title: tie to chosen email if possible
    (
      SELECT a->>'person_title'
      FROM (
        SELECT (a)::jsonb AS a
        FROM unnest(s.atom_rows) a
        WHERE NULLIF(a->>'person_title','') IS NOT NULL
        ORDER BY length(a->>'person_title') DESC NULLS LAST, (a->>'scraped_at') ASC
      ) z
      LIMIT 1
    ) AS best_title,

    -- best name: for email seed, only accept a name seen with that email; else seed name
    CASE
      WHEN s.seed_kind = 'email' THEN (
        SELECT enp.name_norm
        FROM email_name_pairs enp
        WHERE enp.email = (
          SELECT lower(a->>'email')
          FROM (
            SELECT (a)::jsonb AS a
            FROM unnest(s.atom_rows) a
            WHERE (a->>'email') IS NOT NULL
            ORDER BY
              ((a->>'is_generic_domain')::boolean) ASC,
              ((a->>'is_generic_mailbox')::boolean) ASC,
              (a->>'scraped_at') ASC
            LIMIT 1
          ) e3
        )
        LIMIT 1
      )
      ELSE s.seed_key
    END AS best_name_norm,

    -- for affiliations
    (
      SELECT (a->>'scraped_at')::timestamptz
      FROM unnest(s.atom_rows) a
      ORDER BY (a->>'scraped_at')::timestamptz ASC
      LIMIT 1
    ) AS first_seen_at,
    (
      SELECT (a->>'scraped_at')::timestamptz
      FROM unnest(s.atom_rows) a
      ORDER BY (a->>'scraped_at')::timestamptz DESC
      LIMIT 1
    ) AS last_seen_at
  FROM seeds s
),

-- 7) CLASSIFY EMAIL ------------------------------------------------------------
candidates AS (
  SELECT
    bps.seed_kind,
    bps.seed_key,
    bps.company_id,
    NULLIF(bps.best_email,'')                          AS email,
    NULLIF(bps.best_phone_norm,'')                     AS phone_norm,
    NULLIF(bps.best_title,'')                          AS title_raw,
    NULLIF(bps.best_name_norm,'')                      AS name_norm,
    bps.first_seen_at,
    bps.last_seen_at,
    CASE WHEN bps.best_email IS NULL THEN NULL
         ELSE util.is_generic_email_domain(util.email_domain(bps.best_email)) END AS is_generic_domain,
    CASE WHEN bps.best_email IS NULL THEN NULL
         ELSE lower(split_part(bps.best_email,'@',1)) ~* '^(info|career|careers|jobs|recruit|recruiting|hr|bewerbung|kontakt|hello|support|admin|office|team|sbv|service|mail|kontakt|karriere)$' END AS is_generic_mailbox
  FROM best_per_seed bps
),

-- 8) FILTER OUT EMPTY SHELLS ---------------------------------------------------
final_candidates AS (
  SELECT *
  FROM candidates
  WHERE (email IS NOT NULL OR name_norm IS NOT NULL OR phone_norm IS NOT NULL)
),

-- 9) UPSERT CONTACTS -----------------------------------------------------------
-- 9a) Email-based upsert (only non-generic domain & mailbox)
ins_email AS (
  INSERT INTO gold.contact (full_name, name_norm, primary_email, primary_phone, title_raw, primary_company_id)
  SELECT
    NULL::text AS full_name,
    fc.name_norm,
    lower(fc.email) AS primary_email,
    fc.phone_norm   AS primary_phone,
    fc.title_raw    AS title_raw,
    fc.company_id   AS primary_company_id
  FROM final_candidates fc
  WHERE fc.email IS NOT NULL
    AND COALESCE(fc.is_generic_domain,false) = false
    AND COALESCE(fc.is_generic_mailbox,false) = false
  -- IMPORTANT: index inference matching ux_contact_primary_email_lower
  ON CONFLICT ((lower(primary_email))) WHERE primary_email IS NOT NULL
  DO UPDATE SET
    name_norm = COALESCE(EXCLUDED.name_norm, gold.contact.name_norm),
    primary_phone = COALESCE(EXCLUDED.primary_phone, gold.contact.primary_phone),
    title_raw = COALESCE(EXCLUDED.title_raw, gold.contact.title_raw),
    primary_company_id = COALESCE(EXCLUDED.primary_company_id, gold.contact.primary_company_id),
    updated_at = now()
  RETURNING contact_id, lower(primary_email) AS primary_email_lower
),

-- 9b) Capture generic mailbox emails as generic_email on a contact
generic_email_targets AS (
  SELECT
    fc.seed_kind, fc.seed_key, fc.company_id,
    lower(fc.email) AS generic_email_lower,
    fc.name_norm,
    fc.phone_norm,
    fc.title_raw
  FROM final_candidates fc
  WHERE fc.email IS NOT NULL
    AND (
      COALESCE(fc.is_generic_domain,false) = true
      OR COALESCE(fc.is_generic_mailbox,false) = true
    )
),

-- 9c) Name+company upsert for rows with NO email or generic-only email
selected_name_company AS (
  SELECT seed_kind, seed_key, company_id, name_norm, phone_norm, title_raw
  FROM final_candidates
  WHERE email IS NULL
  UNION ALL
  SELECT seed_kind, seed_key, company_id, name_norm, phone_norm, title_raw
  FROM generic_email_targets
),
matched AS (
  SELECT s.*,
         c.contact_id AS existing_contact_id
  FROM selected_name_company s
  LEFT JOIN gold.contact c
    ON c.primary_email IS NULL
   AND c.name_norm = s.name_norm
   AND c.primary_company_id = s.company_id
  WHERE s.name_norm IS NOT NULL
),
ins_no_email AS (
  INSERT INTO gold.contact (full_name, name_norm, primary_email, primary_phone, title_raw, primary_company_id, generic_email)
  SELECT
    NULL::text,
    m.name_norm,
    NULL::text,
    m.phone_norm,
    m.title_raw,
    m.company_id,
    CASE
      WHEN m.seed_kind='email' THEN lower(m.seed_key)
      ELSE NULL::text
    END AS generic_email
  FROM matched m
  WHERE m.existing_contact_id IS NULL
  RETURNING contact_id, name_norm, primary_company_id
),
upd_no_email AS (
  UPDATE gold.contact c
  SET
    name_norm = COALESCE(c.name_norm, m.name_norm),
    primary_phone = COALESCE(c.primary_phone, m.phone_norm),
    title_raw = COALESCE(c.title_raw, m.title_raw),
    generic_email = COALESCE(c.generic_email,
                             CASE WHEN m.seed_kind='email' THEN lower(m.seed_key) ELSE NULL END),
    updated_at = now()
  FROM matched m
  WHERE m.existing_contact_id IS NOT NULL
    AND c.contact_id = m.existing_contact_id
  RETURNING c.contact_id
),

-- 10) Attach generic email to email-contacts if same address
attach_generic_to_email_contacts AS (
  UPDATE gold.contact c
  SET generic_email = COALESCE(c.generic_email, g.generic_email_lower),
      updated_at = now()
  FROM generic_email_targets g
  JOIN ins_email ie
    ON ie.primary_email_lower = g.generic_email_lower
  WHERE lower(c.primary_email) = ie.primary_email_lower
  RETURNING c.contact_id
),

-- 11) CONTACT LOOKUP for evidence/affiliations --------------------------------
contact_lookup AS (
  SELECT lower(c.primary_email) AS email_lower, c.contact_id, c.primary_company_id
  FROM gold.contact c
  WHERE c.primary_email IS NOT NULL
  UNION ALL
  SELECT NULL::text AS email_lower, c.contact_id, c.primary_company_id
  FROM gold.contact c
  WHERE c.primary_email IS NULL
),

atoms_for_ev AS (
  SELECT
    a.source, a.source_id, a.source_row_url, a.scraped_at,
    a.company_id, a.email, a.name_norm, a.phone_norm,
    a.person_title, a.fact_src
  FROM atoms a
),

atoms_with_contact AS (
  SELECT
    a.*,
    COALESCE(
      (SELECT cl.contact_id FROM contact_lookup cl WHERE cl.email_lower = lower(a.email) LIMIT 1),
      (SELECT c2.contact_id FROM gold.contact c2
         WHERE c2.primary_email IS NULL
           AND c2.name_norm = a.name_norm
           AND c2.primary_company_id = a.company_id
         ORDER BY c2.created_at DESC
         LIMIT 1)
    ) AS contact_id
  FROM atoms_for_ev a
),

-- 12) EVIDENCE -----------------------------------------------------------------
ev_email AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT
    awc.contact_id, 'email', lower(awc.email), awc.source, awc.source_id,
    jsonb_build_object('from', awc.fact_src)
  FROM atoms_with_contact awc
  WHERE awc.contact_id IS NOT NULL
    AND awc.email IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
),
ev_phone AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT
    awc.contact_id, 'phone', awc.phone_norm, awc.source, awc.source_id,
    jsonb_build_object('from', awc.fact_src)
  FROM atoms_with_contact awc
  WHERE awc.contact_id IS NOT NULL
    AND awc.phone_norm IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
),
ev_title AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT
    awc.contact_id, 'title', awc.person_title, awc.source, awc.source_id,
    jsonb_build_object('from', awc.fact_src,
                       'email', lower(awc.email),
                       'person_name', awc.name_norm)
  FROM atoms_with_contact awc
  WHERE awc.contact_id IS NOT NULL
    AND awc.person_title IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
),

-- 13) AFFILIATIONS -------------------------------------------------------------
aff_base AS (
  SELECT DISTINCT
    awc.contact_id,
    awc.company_id,
    MIN(awc.scraped_at)::timestamptz AS first_seen,
    MAX(awc.scraped_at)::timestamptz AS last_seen,
    awc.source,
    awc.source_id
  FROM atoms_with_contact awc
  WHERE awc.contact_id IS NOT NULL
    AND awc.company_id IS NOT NULL
  GROUP BY 1,2,5,6
),
aff_upsert AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT
    a.contact_id, a.company_id, NULL::text, NULL::text,
    a.first_seen, a.last_seen,
    CASE WHEN a.last_seen >= now() - interval '180 days' THEN true ELSE false END,
    a.source, a.source_id
  FROM aff_base a
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen  = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active     = EXCLUDED.active,
        source     = COALESCE(EXCLUDED.source, gold.contact_affiliation.source),
        source_id  = COALESCE(EXCLUDED.source_id, gold.contact_affiliation.source_id)
  RETURNING contact_id
),

-- 14) PROMOTE TITLES -----------------------------------------------------------
promote_titles AS (
  UPDATE gold.contact c
  SET title_raw = COALESCE(
      -- title tied to primary email
      (
        SELECT ce.value
        FROM gold.contact_evidence ce
        WHERE ce.contact_id=c.contact_id
          AND ce.kind='title'
          AND c.primary_email IS NOT NULL
          AND lower(ce.detail->>'email') = lower(c.primary_email)
        ORDER BY ce.created_at DESC
        LIMIT 1
      ),
      -- title tied to name if no email
      (
        SELECT ce.value
        FROM gold.contact_evidence ce
        WHERE ce.contact_id=c.contact_id
          AND ce.kind='title'
          AND c.primary_email IS NULL
          AND util.person_name_norm(coalesce(ce.detail->>'person_name','')) = c.name_norm
        ORDER BY ce.created_at DESC
        LIMIT 1
      ),
      -- fallback: any title
      (
        SELECT ce.value
        FROM gold.contact_evidence ce
        WHERE ce.contact_id=c.contact_id AND ce.kind='title'
        ORDER BY ce.created_at DESC
        LIMIT 1
      ),
      c.title_raw
    ),
    updated_at = now()
  WHERE TRUE
  RETURNING c.contact_id
)

SELECT 'ok' AS status;

-- =============================================================================
-- SAFE MERGE #1: collapse no-email duplicates into the unique email-keeper
-- =============================================================================
WITH
groups AS (
  SELECT name_norm, primary_company_id,
         MAX(CASE WHEN primary_email IS NOT NULL THEN contact_id END) AS keep_with_email,
         ARRAY_AGG(contact_id ORDER BY created_at ASC)                AS all_ids
  FROM gold.contact
  WHERE name_norm IS NOT NULL AND primary_company_id IS NOT NULL
  GROUP BY 1,2
  HAVING COUNT(*) > 1
     AND MAX(CASE WHEN primary_email IS NOT NULL THEN 1 ELSE 0 END) = 1
),
to_merge AS (
  SELECT unnest(array_remove(all_ids, keep_with_email)) AS dup_id,
         keep_with_email                                 AS keep_id
  FROM groups
),
m_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias)
  SELECT t.keep_id, ca.alias
  FROM to_merge t
  JOIN gold.contact_alias ca ON ca.contact_id=t.dup_id
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING contact_id
),
m_ev AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT t.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail
  FROM to_merge t
  JOIN gold.contact_evidence ce ON ce.contact_id=t.dup_id
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
),
m_aff AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT t.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id
  FROM to_merge t
  JOIN gold.contact_affiliation a ON a.contact_id=t.dup_id
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active = EXCLUDED.active
  RETURNING contact_id
)
DELETE FROM gold.contact c
USING to_merge t
WHERE c.contact_id = t.dup_id
;

-- =============================================================================
-- SAFE MERGE #2: collapse duplicates where NONE has an email
-- Keep the row with a phone first, then with a title, else the oldest.
-- =============================================================================
WITH
groups0 AS (
  SELECT name_norm, primary_company_id,
         ARRAY_AGG(contact_id ORDER BY
           (primary_phone IS NULL) ASC,
           (title_raw IS NULL) ASC,
           created_at ASC
         ) AS ids
  FROM gold.contact
  WHERE name_norm IS NOT NULL
    AND primary_company_id IS NOT NULL
    AND primary_email IS NULL
  GROUP BY 1,2
  HAVING COUNT(*) > 1
),
to_merge0 AS (
  SELECT
    (ids[1]) AS keep_id,
    unnest(ids[2:]) AS dup_id
  FROM groups0
),
m0_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias)
  SELECT t.keep_id, ca.alias
  FROM to_merge0 t
  JOIN gold.contact_alias ca ON ca.contact_id=t.dup_id
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING contact_id
),
m0_ev AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT t.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail
  FROM to_merge0 t
  JOIN gold.contact_evidence ce ON ce.contact_id=t.dup_id
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING contact_id
),
m0_aff AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT t.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id
  FROM to_merge0 t
  JOIN gold.contact_affiliation a ON a.contact_id=t.dup_id
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active = EXCLUDED.active
  RETURNING contact_id
),
m0_generic AS (
  -- if dup has generic_email and keeper lacks, promote it
  UPDATE gold.contact keep
  SET generic_email = COALESCE(keep.generic_email, dup.generic_email),
      updated_at = now()
  FROM to_merge0 t
  JOIN gold.contact dup ON dup.contact_id = t.dup_id
  WHERE keep.contact_id = t.keep_id
    AND dup.generic_email IS NOT NULL
    AND keep.generic_email IS NULL
  RETURNING keep.contact_id
)
DELETE FROM gold.contact c
USING to_merge0 t
WHERE c.contact_id = t.dup_id
;
