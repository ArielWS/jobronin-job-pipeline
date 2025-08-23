-- transforms/sql/15_gold_contact_etl.sql
-- Deterministic, idempotent ETL to populate gold.contact* from silver.unified_silver
-- Uses ON CONFLICT (primary_email_lower) so it works with either a unique index or constraint.

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
    us.contacts_raw                    AS contacts_raw,  -- JSONB already cleaned in silvers
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
             NULLIF(trim(both from (c.value->>'name')),'')) AS person_name,
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
    NULL::text AS person_name,
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
    NULL::text AS person_name,
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
    cr.contact_person_raw AS person_name,
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
    NULLIF(a.person_name,'') AS person_name,
    NULLIF(a.email,'')       AS email,
    NULLIF(a.title,'')       AS title,
    NULLIF(a.phone,'')       AS phone,
    a.fact_src,
    util.email_domain(a.email)                               AS email_domain,
    util.org_domain(util.email_domain(a.email))              AS email_root,
    util.is_generic_email_domain(util.email_domain(a.email)) AS is_generic_domain,
    util.is_generic_mailbox(a.email)                         AS is_generic_mailbox,
    util.person_name_norm(a.person_name)                     AS name_norm,
    util.phone_norm(a.phone)                                 AS phone_norm
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

existing_by_email AS (
  SELECT DISTINCT a.source, a.source_id, a.email, ce.contact_id
  FROM atoms_eligible a
  JOIN gold.contact_evidence ce ON ce.kind='email' AND ce.value=a.email
  WHERE a.email IS NOT NULL
),
existing_by_name_company AS (
  SELECT DISTINCT a.source, a.source_id, a.name_norm, a.company_id, ca.contact_id
  FROM atoms_eligible a
  JOIN gold.contact_alias ca ON ca.alias_norm=a.name_norm
  JOIN gold.contact_affiliation aff ON aff.contact_id=ca.contact_id AND aff.company_id=a.company_id
  WHERE a.name_norm IS NOT NULL AND a.company_id IS NOT NULL
),
existing_by_phone_company AS (
  SELECT DISTINCT a.source, a.source_id, a.phone_norm, a.company_id, ce.contact_id
  FROM atoms_eligible a
  JOIN gold.contact_evidence ce ON ce.kind='phone' AND ce.value=a.phone_norm
  JOIN gold.contact_affiliation aff ON aff.contact_id=ce.contact_id AND aff.company_id=a.company_id
  WHERE a.phone_norm IS NOT NULL AND a.company_id IS NOT NULL
),

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
    ON ebe.source=s.source AND ebe.source_id=s.source_id AND ebe.email=s.email
  LEFT JOIN existing_by_name_company enc
    ON enc.source=s.source AND enc.source_id=s.source_id
   AND enc.name_norm=s.name_norm AND enc.company_id=s.company_id
  LEFT JOIN existing_by_phone_company epc
    ON epc.source=s.source AND epc.source_id=s.source_id
   AND epc.phone_norm=s.phone_norm AND epc.company_id=s.company_id
),

seed_best AS (
  SELECT
    seed_key,
    (ARRAY_AGG(person_name ORDER BY person_name IS NULL, length(person_name) DESC))[1] AS best_name,
    (ARRAY_AGG(email ORDER BY (NOT (is_generic_domain OR is_generic_mailbox)) DESC, email IS NULL))[1] AS best_email,
    (ARRAY_AGG(phone ORDER BY phone IS NULL, length(coalesce(phone_norm,'')) DESC))[1] AS best_phone
  FROM seeds
  GROUP BY seed_key
),

-- Upsert by column (works with a unique index)
ins_contacts AS (
  INSERT INTO gold.contact (full_name, primary_email, primary_phone, title_raw, primary_company_id)
  SELECT
    sb.best_name,
    sb.best_email,
    util.phone_norm(sb.best_phone),
    NULL,
    (
      SELECT s2.company_id
      FROM seeds s2
      WHERE s2.seed_key = sb.seed_key
      ORDER BY (s2.email = sb.best_email) DESC NULLS LAST, s2.scraped_at DESC
      LIMIT 1
    )
  FROM seed_best sb
  WHERE sb.seed_key IN (SELECT seed_key FROM atoms_mapped WHERE contact_id_existing IS NULL)
  ON CONFLICT (primary_email_lower) DO UPDATE
    SET full_name = COALESCE(EXCLUDED.full_name, gold.contact.full_name),
        primary_phone = COALESCE(gold.contact.primary_phone, EXCLUDED.primary_phone),
        title_raw = COALESCE(gold.contact.title_raw, EXCLUDED.title_raw),
        primary_company_id = COALESCE(gold.contact.primary_company_id, EXCLUDED.primary_company_id),
        updated_at = now()
  RETURNING contact_id, full_name, primary_email, primary_phone
),

atom_contact AS (
  SELECT
    am.source, am.source_id, am.seed_key,
    COALESCE(
      am.contact_id_existing,
      (SELECT ic.contact_id
       FROM ins_contacts ic
       WHERE ic.primary_email IS NOT DISTINCT FROM (SELECT best_email FROM seed_best WHERE seed_key=am.seed_key)
         AND ic.full_name     IS NOT DISTINCT FROM (SELECT best_name  FROM seed_best WHERE seed_key=am.seed_key)
       LIMIT 1)
    ) AS contact_id
  FROM atoms_mapped am
),

ins_aliases AS (
  INSERT INTO gold.contact_alias (contact_id, alias, primary_flag)
  SELECT DISTINCT ac.contact_id, a.person_name, false
  FROM atom_contact ac
  JOIN atoms_eligible a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.person_name IS NOT NULL
  ON CONFLICT (contact_id, alias_norm) DO NOTHING
  RETURNING 1
),

ins_primary_alias AS (
  INSERT INTO gold.contact_alias (contact_id, alias, primary_flag)
  SELECT DISTINCT ac.contact_id,
         (SELECT best_name FROM seed_best sb WHERE sb.seed_key=ac.seed_key),
         true
  FROM atom_contact ac
  WHERE (SELECT COUNT(*) FROM gold.contact_alias ca WHERE ca.contact_id=ac.contact_id AND ca.primary_flag)=0
  ON CONFLICT (contact_id, alias_norm) DO UPDATE SET primary_flag=TRUE
  RETURNING 1
),

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
  WHERE a.email IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_phone AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'phone', a.phone_norm, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.phone_norm IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_name AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'name', a.name_norm, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.name_norm IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_title AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'title', a.title, a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.title IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_row AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id, detail)
  SELECT DISTINCT ac.contact_id, 'source_row', a.source_row_url, a.source, a.source_id,
         jsonb_build_object('scraped_at', a.scraped_at, 'date_posted', a.date_posted, 'fact_src', a.fact_src)
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.source_row_url IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

ins_ev_company_hint AS (
  INSERT INTO gold.contact_evidence (contact_id, kind, value, source, source_id)
  SELECT DISTINCT ac.contact_id, 'company_hint', COALESCE(a.company_root, a.email_root), a.source, a.source_id
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE COALESCE(a.company_root, a.email_root) IS NOT NULL
  ON CONFLICT (contact_id, kind, value) DO NOTHING
  RETURNING 1
),

aff_src AS (
  SELECT
    ac.contact_id,
    a.company_id,
    a.source, a.source_id,
    max(NULLIF(a.title,'')) FILTER (WHERE a.title IS NOT NULL) AS role,
    NULL::text AS seniority,
    min(a.scraped_at) AS first_seen,
    max(a.scraped_at) AS last_seen
  FROM atom_contact ac
  JOIN atoms_with_company a ON a.source=ac.source AND a.source_id=ac.source_id
  WHERE a.company_id IS NOT NULL
  GROUP BY ac.contact_id, a.company_id, a.source, a.source_id
),

aff_upsert AS (
  INSERT INTO gold.contact_affiliation (contact_id, company_id, role, seniority, first_seen, last_seen, active, source, source_id)
  SELECT
    s.contact_id, s.company_id, s.role, s.seniority, s.first_seen, s.last_seen,
    (s.last_seen >= (now() - (SELECT make_interval(days := p.active_days) FROM params p))),
    s.source, s.source_id
  FROM aff_src s
  ON CONFLICT (contact_id, company_id) DO UPDATE
    SET role = COALESCE(EXCLUDED.role, gold.contact_affiliation.role),
        seniority = COALESCE(EXCLUDED.seniority, gold.contact_affiliation.seniority),
        first_seen = LEAST(gold.contact_affiliation.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(gold.contact_affiliation.last_seen, EXCLUDED.last_seen),
        active = EXCLUDED.active
  RETURNING 1
),

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
       ORDER BY ce.created_at DESC LIMIT 1),
      (SELECT ce.value
       FROM gold.contact_evidence ce
       WHERE ce.contact_id=c.contact_id AND ce.kind='email'
       ORDER BY ce.created_at DESC LIMIT 1)
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
       LIMIT 1)
    )
  WHERE EXISTS (SELECT 1 FROM gold.contact_evidence ce WHERE ce.contact_id=c.contact_id)
  RETURNING 1
)

SELECT 'ok' AS status;
