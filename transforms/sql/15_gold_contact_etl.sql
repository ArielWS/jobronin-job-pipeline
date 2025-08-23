-- transforms/sql/15_gold_contact_etl.sql
-- Gold Contact ETL (idempotent)
-- - De-dupes email-less inserts (anti-join on existing (name_norm, company))
-- - Maps email-less winners so evidence & affiliations write
-- - Guards low-quality names
-- - Never promotes generic mailbox to primary_email
-- - Final safe-merge collapses duplicate (name_norm, company) no-email rows

SET search_path = public;

WITH
params AS (SELECT 270::int AS active_days),

unified AS (
  SELECT
    us.source,
    us.source_site,
    us.source_id,
    us.source_row_url,
    COALESCE(us.scraped_at, now()) AS scraped_at,
    us.date_posted,
    NULLIF(us.company_domain,'')       AS company_domain,
    NULLIF(us.company_website,'')      AS company_website,
    NULLIF(us.company_linkedin_url,'') AS company_linkedin_url,
    NULLIF(us.company_stepstone_id,'') AS company_stepstone_id,
    NULLIF(us.emails_raw,'')           AS emails_raw,
    us.emails_all                      AS emails_all,
    us.contacts_raw                    AS contacts_raw,
    NULLIF(us.contact_person_raw,'')   AS contact_person_raw,
    NULLIF(us.contact_phone_raw,'')    AS contact_phone_raw,
    us.city_guess, us.region_guess, us.country_guess
  FROM silver.unified_silver us
),

company_hint AS (
  SELECT u.*,
         util.linkedin_slug(u.company_linkedin_url)   AS company_slug,
         util.org_domain(NULLIF(u.company_domain,'')) AS company_root
  FROM unified u
),

company_resolved AS (
  SELECT h.*,
         COALESCE(
           (SELECT ced.company_id FROM gold.company_evidence_domain ced
             WHERE ced.kind='stepstone_id' AND ced.value=h.company_stepstone_id LIMIT 1),
           (SELECT gc.company_id FROM gold.company gc
             WHERE h.company_root IS NOT NULL AND gc.website_domain=h.company_root LIMIT 1),
           (SELECT gc.company_id FROM gold.company gc
             WHERE h.company_root IS NOT NULL AND util.same_org_domain(gc.website_domain,h.company_root) LIMIT 1),
           (SELECT gc.company_id FROM gold.company gc
             WHERE h.company_slug IS NOT NULL AND gc.linkedin_slug=h.company_slug LIMIT 1)
         ) AS company_id_hint
  FROM company_hint h
),

json_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    COALESCE(NULLIF(trim(both from (c.value->>'personName')),''),
             NULLIF(trim(both from (c.value->>'name')),'')) AS person_name_raw,
    COALESCE(NULLIF(lower(c.value->>'emailAddress'),''),
             NULLIF(lower(c.value->>'email'),''),
             NULLIF(lower(c.value->>'mail'),'') ) AS email,
    COALESCE(NULLIF(c.value->>'personTitle',''),
             NULLIF(c.value->>'title','')) AS title,
    COALESCE(NULLIF(c.value->>'phoneNumber',''),
             NULLIF(c.value->>'phone',''),
             NULLIF(c.value->>'tel','')) AS phone,
    'json_contacts'::text AS fact_src
  FROM company_resolved cr
  LEFT JOIN LATERAL jsonb_array_elements(
    CASE WHEN cr.contacts_raw IS NOT NULL AND jsonb_typeof(cr.contacts_raw)='array'
         THEN cr.contacts_raw ELSE '[]'::jsonb END
  ) AS c(value) ON TRUE
),

emails_all_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    NULL::text AS person_name_raw,
    lower(e)   AS email,
    NULL::text AS title,
    NULL::text AS phone,
    'emails_all'::text AS fact_src
  FROM company_resolved cr
  CROSS JOIN LATERAL unnest(COALESCE(cr.emails_all,'{}'::text[])) AS e
  WHERE e ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
),

emails_raw_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    NULL::text AS person_name_raw,
    lower(trim(both from e)) AS email,
    NULL::text AS title,
    NULL::text AS phone,
    'emails_raw'::text AS fact_src
  FROM company_resolved cr,
       LATERAL (SELECT regexp_split_to_table(cr.emails_raw, '\s*[;,]\s*') AS e) s
  WHERE cr.emails_raw IS NOT NULL
    AND s.e ~* '^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$'
),

raw_person_atoms AS (
  SELECT
    cr.source, cr.source_id, cr.source_row_url, cr.scraped_at, cr.date_posted,
    cr.company_id_hint, cr.company_root, cr.company_slug,
    cr.contact_person_raw AS person_name_raw,
    NULL::text AS email,
    NULL::text AS title,
    cr.contact_phone_raw AS phone,
    'contact_raw'::text AS fact_src
  FROM company_resolved cr
  WHERE cr.contact_person_raw IS NOT NULL OR cr.contact_phone_raw IS NOT NULL
),

atoms AS (
  SELECT DISTINCT
    a.source, a.source_id, a.source_row_url, a.scraped_at, a.date_posted,
    a.company_id_hint, a.company_root, a.company_slug,
    CASE
      WHEN length(regexp_replace(coalesce(a.person_name_raw,''), '[^[:alpha:]]', '', 'g')) >= 2 THEN a.person_name_raw
      ELSE NULL
    END AS person_name,
    NULLIF(a.email,'')       AS email,
    NULLIF(a.title,'')       AS title,
    NULLIF(a.phone,'')       AS phone,
    a.fact_src,
    util.email_domain(a.email)                               AS email_domain,
    util.org_domain(util.email_domain(a.email))              AS email_root,
    util.is_generic_email_domain(util.email_domain(a.email)) AS is_generic_domain,
    util.is_generic_mailbox(a.email)                         AS is_generic_mailbox,
    util.person_name_norm(
      CASE
        WHEN length(regexp_replace(coalesce(a.person_name_raw,''), '[^[:alpha:]]', '', 'g')) >= 2 THEN a.person_name_raw
        ELSE NULL
      END
    )                                                         AS name_norm,
    util.phone_norm(a.phone)                                  AS phone_norm
  FROM (
    SELECT * FROM json_atoms
    UNION ALL SELECT * FROM emails_all_atoms
    UNION ALL SELECT * FROM emails_raw_atoms
    UNION ALL SELECT * FROM raw_person_atoms
  ) a
),

atoms_with_company AS (
  SELECT
    at.*,
    COALESCE(
      at.company_id_hint,
      (SELECT gc.company_id FROM gold.company gc
         WHERE at.email_root IS NOT NULL
           AND NOT util.is_aggregator_host(at.email_root)
           AND NOT util.is_ats_host(at.email_root)
           AND NOT util.is_career_host(at.email_root)
           AND gc.website_domain = at.email_root LIMIT 1),
      (SELECT gc.company_id FROM gold.company gc
         WHERE at.email_root IS NOT NULL
           AND NOT util.is_aggregator_host(at.email_root)
           AND NOT util.is_ats_host(at.email_root)
           AND NOT util.is_career_host(at.email_root)
           AND util.same_org_domain(gc.website_domain, at.email_root) LIMIT 1)
    ) AS company_id
  FROM atoms at
),

atoms_eligible AS (
  SELECT *
  FROM atoms_with_company
  WHERE (email IS NOT NULL AND NOT (is_generic_domain OR is_generic_mailbox))
     OR person_name IS NOT NULL
     OR phone IS NOT NULL
),

-- Existing matches
existing_by_email AS (
  SELECT ce.value AS email, ce.contact_id
  FROM gold.contact_evidence ce
  WHERE ce.kind='email'
),

existing_by_name_company AS (
  SELECT DISTINCT a.name_norm, a.company_id, ca.contact_id
  FROM atoms_eligible a
  JOIN gold.contact_alias ca ON ca.alias_norm=a.name_norm
  JOIN gold.contact_affiliation aff ON aff.contact_id=ca.contact_id AND aff.company_id=a.company_id
  WHERE a.name_norm IS NOT NULL AND a.company_id IS NOT NULL
),

existing_by_phone_company AS (
  SELECT DISTINCT a.phone_norm, a.company_id, ce.contact_id
  FROM atoms_eligible a
  JOIN gold.contact_evidence ce ON ce.kind='phone' AND ce.value=a.phone_norm
  JOIN gold.contact_affiliation aff ON aff.contact_id=ce.contact_id AND aff.company_id=a.company_id
  WHERE a.phone_norm IS NOT NULL AND a.company_id IS NOT NULL
),

-- Seeds
seeds AS (
  SELECT a.*,
         CASE
           WHEN a.email IS NOT NULL AND NOT (a.is_generic_domain OR a.is_generic_mailbox)
             THEN 'email:'||a.email
           WHEN a.name_norm IS NOT NULL AND a.company_id IS NOT NULL
             THEN 'nameco:'||a.name_norm||':'||a.company_id::text
           WHEN a.phone_norm IS NOT NULL AND a.company_id IS NOT NULL
             THEN 'phoneco:'||a.phone_norm||':'||a.company_id::text
           ELSE 'row:'||a.source||':'||a.source_id
         END AS seed_key
  FROM atoms_eligible a
),

atoms_mapped AS (
  SELECT s.*,
         COALESCE(ebe.contact_id, enc.contact_id, epc.contact_id) AS contact_id_existing
  FROM seeds s
  LEFT JOIN existing_by_email ebe
    ON ebe.email = s.email
  LEFT JOIN existing_by_name_company enc
    ON enc.name_norm=s.name_norm AND enc.company_id=s.company_id
  LEFT JOIN existing_by_phone_company epc
    ON epc.phone_norm=s.phone_norm AND epc.company_id=s.company_id
),

-- Best choice per seed
seed_best AS (
  SELECT
    seed_key,
    (ARRAY_AGG(person_name ORDER BY person_name IS NULL, length(person_name) DESC))[1] AS best_name,
    (ARRAY_AGG(email ORDER BY (NOT (is_generic_domain OR is_generic_mailbox)) DESC, email IS NULL))[1] AS best_email,
    (ARRAY_AGG(phone ORDER BY phone IS NULL, length(coalesce(phone_norm,'')) DESC))[1] AS best_phone
  FROM seeds
  GROUP BY seed_key
),

cand_seeds AS (
  SELECT am.seed_key
  FROM atoms_mapped am
  WHERE am.contact_id_existing IS NULL
),

seed_meta AS (
  SELECT s.seed_key,
         max(s.scraped_at) AS latest_scraped,
         bool_or(s.person_name IS NOT NULL) AS has_name
  FROM seeds s
  GROUP BY s.seed_key
),

seed_best_aug AS (
  SELECT
    sb.seed_key,
    sb.best_name,
    util.person_name_norm(sb.best_name)                AS best_name_norm,
    sb.best_email,
    util.email_domain(sb.best_email)                   AS best_email_domain,
    util.org_domain(util.email_domain(sb.best_email))  AS best_email_root,
    util.is_generic_email_domain(util.email_domain(sb.best_email)) AS best_email_generic_domain,
    util.is_generic_mailbox(sb.best_email)             AS best_email_generic_mailbox,
    sb.best_phone,
    util.phone_norm(sb.best_phone)                     AS best_phone_norm,
    sm.latest_scraped,
    sm.has_name,
    (
      SELECT s2.company_id
      FROM seeds s2
      WHERE s2.seed_key = sb.seed_key
      ORDER BY (s2.email = sb.best_email) DESC NULLS LAST, s2.scraped_at DESC
      LIMIT 1
    ) AS best_company_id
  FROM seed_best sb
  JOIN cand_seeds cs USING (seed_key)
  LEFT JOIN seed_meta sm ON sm.seed_key = sb.seed_key
),

seed_best_final AS (
  SELECT
    seed_key,
    best_name,
    best_name_norm,
    CASE WHEN best_email IS NOT NULL
           AND NOT (best_email_generic_domain OR best_email_generic_mailbox)
         THEN best_email
         ELSE NULL
    END AS best_email,
    best_phone,
    best_phone_norm,
    best_company_id,
    latest_scraped,
    has_name
  FROM seed_best_aug
),

-- Winners
winners_email AS (
  SELECT DISTINCT ON (lower(best_email))
    seed_key, best_name, best_name_norm, best_email, best_phone, best_phone_norm, best_company_id
  FROM seed_best_final
  WHERE best_email IS NOT NULL
  ORDER BY lower(best_email), has_name DESC, latest_scraped DESC, seed_key
),

no_email_pool AS (
  SELECT * FROM seed_best_final
  WHERE best_email IS NULL AND best_company_id IS NOT NULL
),

winners_phoneco AS (
  SELECT DISTINCT ON (best_phone_norm, best_company_id)
    seed_key, best_name, best_name_norm, NULL::text AS best_email, best_phone, best_phone_norm, best_company_id
  FROM no_email_pool
  WHERE best_phone_norm IS NOT NULL
  ORDER BY best_phone_norm, best_company_id, length(coalesce(best_name,'')) DESC NULLS LAST, latest_scraped DESC, seed_key
),

winners_nameco AS (
  SELECT DISTINCT ON (p.best_name_norm, p.best_company_id)
    p.seed_key, p.best_name, p.best_name_norm, NULL::text AS best_email, p.best_phone, p.best_phone_norm, p.best_company_id
  FROM no_email_pool p
  LEFT JOIN winners_phoneco ph
    ON ph.best_company_id = p.best_company_id
   AND ph.best_phone_norm IS NOT DISTINCT FROM p.best_phone_norm
  WHERE ph.seed_key IS NULL
  ORDER BY p.best_name_norm, p.best_company_id,
           length(coalesce(p.best_phone_norm,'')) DESC NULLS LAST,
           length(coalesce(p.best_name,'')) DESC NULLS LAST,
           p.latest_scraped DESC, p.seed_key
),

final_candidates AS (
  SELECT * FROM winners_email
  UNION ALL
  SELECT * FROM winners_phoneco
  UNION ALL
  SELECT * FROM winners_nameco
),

-- ==========================================
-- PREVENT RE-INSERTION OF NAMECO ON RERUNS
-- ==========================================
final_candidates_to_insert AS (
  SELECT fc.*
  FROM final_candidates fc
  LEFT JOIN gold.contact c
    ON (fc.best_email IS NOT NULL AND c.primary_email_lower = lower(fc.best_email))
    OR (fc.best_email IS NULL
        AND c.name_norm = fc.best_name_norm
        AND c.primary_company_id IS NOT DISTINCT FROM fc.best_company_id)
  WHERE c.contact_id IS NULL
),

-- Insert / upsert
ins_contacts AS (
  INSERT INTO gold.contact (full_name, primary_email, primary_phone, title_raw, primary_company_id)
  SELECT
    fc.best_name,
    fc.best_email,
    fc.best_phone_norm,
    NULL,
    fc.best_company_id
  FROM final_candidates_to_insert fc
  ON CONFLICT (primary_email_lower) DO UPDATE
    SET full_name = COALESCE(EXCLUDED.full_name, gold.contact.full_name),
        primary_phone = COALESCE(gold.contact.primary_phone, EXCLUDED.primary_phone),
        title_raw = COALESCE(gold.contact.title_raw, EXCLUDED.title_raw),
        primary_company_id = COALESCE(gold.contact.primary_company_id, EXCLUDED.primary_company_id),
        updated_at = now()
  RETURNING contact_id, full_name, primary_email, primary_company_id
),

inserted_map AS (
  SELECT primary_email, contact_id FROM ins_contacts WHERE primary_email IS NOT NULL
),

nameco_winners AS (
  SELECT seed_key, best_name_norm, best_company_id
  FROM winners_phoneco
  UNION ALL
  SELECT seed_key, best_name_norm, best_company_id
  FROM winners_nameco
),

nameco_map AS (
  SELECT nw.seed_key, c.contact_id
  FROM nameco_winners nw
  JOIN gold.contact c
    ON c.name_norm = nw.best_name_norm
   AND c.primary_company_id IS NOT DISTINCT FROM nw.best_company_id
),

atom_contact AS (
  SELECT
    am.source, am.source_id, am.seed_key,
    COALESCE(
      am.contact_id_existing,
      (SELECT im.contact_id
         FROM inserted_map im
        WHERE im.primary_email IS NOT DISTINCT FROM
              (SELECT best_email FROM seed_best_final WHERE seed_key = am.seed_key)
        LIMIT 1),
      (SELECT nm.contact_id
         FROM nameco_map nm
        WHERE nm.seed_key = am.seed_key
        LIMIT 1)
    ) AS contact_id
  FROM atoms_mapped am
),

-- Aliases
ins_aliases AS (
  INSERT INTO gold.contact_alias (contact_id, alias, primary_flag)
  SELECT DISTINCT ac.contact_id, a.person_name, false
  FROM atom_contact ac
  JOIN atoms_eligible a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.person_name IS NOT NULL
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING 1
),

primary_alias_choice AS (
  SELECT
    ac.contact_id,
    (ARRAY_AGG(sbf.best_name ORDER BY length(coalesce(sbf.best_name,'')) DESC NULLS LAST))[1] AS primary_alias
  FROM (SELECT DISTINCT contact_id, seed_key FROM atom_contact WHERE contact_id IS NOT NULL) ac
  JOIN seed_best_final sbf ON sbf.seed_key = ac.seed_key
  WHERE sbf.best_name IS NOT NULL
  GROUP BY ac.contact_id
),

ins_primary_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias, primary_flag)
  SELECT pac.contact_id, pac.primary_alias, true
  FROM primary_alias_choice pac
  WHERE pac.primary_alias IS NOT NULL
    AND (SELECT COUNT(*) FROM gold.contact_alias ca WHERE ca.contact_id=pac.contact_id AND ca.primary_flag) = 0
  ON CONFLICT (contact_id, alias_norm) DO UPDATE SET primary_flag = TRUE
  RETURNING 1
),

-- Evidence
ins_ev_email AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT ac.contact_id, 'email', a.email, a.source, a.source_id,
         jsonb_build_object(
           'is_generic_domain', a.is_generic_domain,
           'is_generic_mailbox', a.is_generic_mailbox,
           'email_domain', a.email_domain,
           'email_root', a.email_root,
           'from', a.fact_src
         )
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.email IS NOT NULL
  ON CONFLICT DO NOTHING
  RETURNING 1
),

ins_ev_phone AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'phone', a.phone_norm, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.phone_norm IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_name AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'name', a.name_norm, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.name_norm IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_title AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'title', a.title, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.title IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_row AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT ac.contact_id, 'source_row', a.source_row_url, a.source, a.source_id,
         jsonb_build_object('scraped_at', a.scraped_at, 'date_posted', a.date_posted, 'fact_src', a.fact_src)
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.source_row_url IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_company_hint AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'company_hint', COALESCE(a.company_root, a.email_root), a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND COALESCE(a.company_root, a.email_root) IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

-- Affiliations
aff_src AS (
  SELECT
    ac.contact_id,
    a.company_id,
    max(NULLIF(a.title,'')) FILTER (WHERE a.title IS NOT NULL) AS role,
    NULL::text AS seniority,
    min(a.scraped_at) AS first_seen,
    max(a.scraped_at) AS last_seen
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE ac.contact_id IS NOT NULL AND a.company_id IS NOT NULL
  GROUP BY ac.contact_id, a.company_id
),

aff_upsert AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT
    s.contact_id,
    s.company_id,
    s.role,
    s.seniority,
    s.first_seen,
    s.last_seen,
    (s.last_seen >= (now() - (SELECT make_interval(days := p.active_days) FROM params p))),
    NULL::text AS source,
    NULL::text AS source_id
  FROM aff_src s
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active = EXCLUDED.active
  RETURNING 1
),

-- Promote primaries
promote_primary AS (
  UPDATE gold.contact c
  SET
    primary_email = COALESCE(
      (SELECT ce.value
       FROM gold.contact_evidence ce
       JOIN gold.contact_affiliation ca ON ca.contact_id=c.contact_id AND ca.active=TRUE
       JOIN gold.company gc ON gc.company_id=ca.company_id
       WHERE ce.contact_id=c.contact_id AND ce.kind='email'
         AND NOT coalesce((ce.detail->>'is_generic_domain')::boolean,false)
         AND NOT coalesce((ce.detail->>'is_generic_mailbox')::boolean,false)
         AND util.org_domain(util.email_domain(ce.value))=gc.website_domain
       ORDER BY ce.created_at DESC LIMIT 1),
      (SELECT ce.value
       FROM gold.contact_evidence ce
       WHERE ce.contact_id=c.contact_id AND ce.kind='email'
         AND NOT coalesce((ce.detail->>'is_generic_domain')::boolean,false)
         AND NOT coalesce((ce.detail->>'is_generic_mailbox')::boolean,false)
       ORDER BY ce.created_at DESC LIMIT 1)
    ),
    generic_email = COALESCE(
      CASE WHEN
        (SELECT COUNT(*) FROM gold.contact_evidence ce
          WHERE ce.contact_id=c.contact_id AND ce.kind='email'
            AND NOT coalesce((ce.detail->>'is_generic_domain')::boolean,false)
            AND NOT coalesce((ce.detail->>'is_generic_mailbox')::boolean,false)
        ) = 0
      THEN
        (SELECT ce.value
         FROM gold.contact_evidence ce
         WHERE ce.contact_id=c.contact_id AND ce.kind='email'
         ORDER BY ce.created_at DESC LIMIT 1)
      ELSE c.generic_email END,
      c.generic_email
    ),
    primary_phone = (
      SELECT ce.value
      FROM gold.contact_evidence ce
      WHERE ce.contact_id=c.contact_id AND ce.kind='phone'
      ORDER BY length(coalesce(ce.value,'')) DESC NULLS LAST, ce.created_at DESC
      LIMIT 1
    ),
    title_raw = (
      SELECT ce.value
      FROM gold.contact_evidence ce
      WHERE ce.contact_id=c.contact_id AND ce.kind='title'
      ORDER BY ce.created_at DESC LIMIT 1
    ),
    primary_company_id = COALESCE(
      (SELECT ca.company_id
       FROM gold.contact_affiliation ca
       WHERE ca.contact_id=c.contact_id AND c.primary_email IS NOT NULL
       ORDER BY (util.org_domain(util.email_domain(c.primary_email)) =
                 (SELECT gc.website_domain FROM gold.company gc WHERE gc.company_id=ca.company_id)) DESC,
                ca.last_seen DESC NULLS LAST
       LIMIT 1),
      (SELECT ca2.company_id
       FROM gold.contact_affiliation ca2
       WHERE ca2.contact_id=c.contact_id
       ORDER BY ca2.last_seen DESC NULLS LAST
       LIMIT 1),
      c.primary_company_id
    ),
    updated_at = now()
  WHERE EXISTS (SELECT 1 FROM gold.contact_evidence ce WHERE ce.contact_id=c.contact_id)
  RETURNING 1
),

-- ==========================================
-- SAFE MERGE: collapse duplicate (name_norm, company) where primary_email IS NULL
-- ==========================================
dupe_groups AS (
  SELECT
    name_norm,
    primary_company_id,
    (ARRAY_AGG(contact_id ORDER BY created_at ASC))[1] AS keep_id,
    ARRAY_REMOVE(ARRAY_AGG(contact_id ORDER BY created_at ASC),
                 (ARRAY_AGG(contact_id ORDER BY created_at ASC))[1]) AS dup_ids
  FROM gold.contact
  WHERE primary_email IS NULL
    AND name_norm IS NOT NULL
    AND primary_company_id IS NOT NULL
  GROUP BY 1,2
  HAVING COUNT(*) > 1
),

dupe_contacts AS (
  SELECT unnest(dup_ids) AS dup_id, keep_id
  FROM dupe_groups
),

move_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias, primary_flag)
  SELECT d.keep_id, ca.alias, ca.primary_flag
  FROM dupe_contacts d
  JOIN gold.contact_alias ca ON ca.contact_id=d.dup_id
  ON CONFLICT (contact_id, alias_norm)
  DO UPDATE SET primary_flag = gold.contact_alias.primary_flag OR EXCLUDED.primary_flag
  RETURNING 1
),

move_evidence AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT d.keep_id, ce.kind, ce.value, ce.source, ce.source_id, ce.detail
  FROM dupe_contacts d
  JOIN gold.contact_evidence ce ON ce.contact_id=d.dup_id
  ON CONFLICT DO NOTHING
  RETURNING 1
),

move_aff AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT d.keep_id, a.company_id, a.role, a.seniority, a.first_seen, a.last_seen, a.active, a.source, a.source_id
  FROM dupe_contacts d
  JOIN gold.contact_affiliation a ON a.contact_id=d.dup_id
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active = EXCLUDED.active
  RETURNING 1
),

delete_dupes AS (
  DELETE FROM gold.contact c
  USING dupe_contacts d
  WHERE c.contact_id=d.dup_id
  RETURNING 1
)

SELECT 'ok' AS status;
