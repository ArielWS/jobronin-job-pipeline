-- transforms/sql/04b_util_person_functions.sql
-- Person-centric utility helpers (safe to run multiple times)

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS util;

-- Touch-updated-at trigger helper (shared)
CREATE OR REPLACE FUNCTION util.tg_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- Normalize a human name:
-- - lowercases
-- - strips accents
-- - removes common academic titles / honorifics (CEE + EN/DE)
-- - collapses whitespace
CREATE OR REPLACE FUNCTION util.person_name_norm(p_text text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(
           trim(
             regexp_replace(
               regexp_replace(
                 lower(unaccent(coalesce(p_text,''))),
                 '\b(mgr|mag|ing|ing\.|bc|bc\.|bsc|msc|phd|dr|dr\.|doc\.|prof|prof\.|ing\. arch\.|dipl\-?ing(\.)?)\b',
                 '',
                 'gi'
               ),
               '\s+',
               ' ',
               'g'
             )
           ),
           ''
         )
$$;

-- Extract a LinkedIn slug (company/person). Returns NULL if none.
CREATE OR REPLACE FUNCTION util.linkedin_slug(u text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF((
    WITH x AS (
      SELECT regexp_replace(coalesce(u,''), '\?.*$', '') AS clean
    )
    SELECT CASE
      WHEN x.clean ~* 'linkedin\.com/(company|school)/[^/]+/?$'
        THEN regexp_replace(x.clean, '^.*linkedin\.com/(company|school)/', '')
      WHEN x.clean ~* 'linkedin\.com/in/[^/]+/?$'
        THEN regexp_replace(x.clean, '^.*linkedin\.com/in/', '')
      ELSE NULL
    END
    FROM x
  ), '')
$$;

-- Detects if an email's local-part is a generic/team mailbox (returns TRUE if generic).
-- Examples: jobs@, hr@, recruiting@, careers@, info@, support@, professionals@, bewerbung@ ...
CREATE OR REPLACE FUNCTION util.is_generic_mailbox(email text)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  local_raw text;
  local_norm text;
BEGIN
  IF email IS NULL OR position('@' in email) = 0 THEN
    RETURN TRUE;
  END IF;

  local_raw := lower(split_part(email, '@', 1));
  -- strip separators and collapse
  local_norm := regexp_replace(local_raw, '[-._]+', '', 'g');

  -- exact tokens and common substrings (EN + DACH + CEE recruiting terms)
  IF local_norm IN (
    'info','office','contact','kontakt','hello','hi','support','service','sales','marketing',
    'pr','press','finance','billing','accounting','invoices','legal','privacy','dpo','datenschutz',
    'postmaster','abuse','webmaster','admin','noreply','noreply','donotreply','notifications',
    'job','jobs','recruiting','recruitment','talent','hr','hrteam','careers','career','candidate',
    'candidateexp','professionals','help','helpjoin','join','joinus','team','bewerbung','karriere',
    'ausbildung','werkstudent','praktikum','graduates'
  ) THEN
    RETURN TRUE;
  END IF;

  -- heuristic contains checks (handles things like edt_talent_acquisition)
  IF local_norm LIKE '%talent%' OR
     local_norm LIKE '%career%' OR
     local_norm LIKE '%job%' OR
     local_norm LIKE '%recruit%' OR
     local_norm LIKE '%hr%' OR
     local_norm LIKE '%bewerbung%' OR
     local_norm LIKE '%karriere%' OR
     local_norm LIKE '%candidate%' OR
     local_norm LIKE '%professionals%' OR
     local_norm LIKE '%support%' OR
     local_norm LIKE '%help%' OR
     local_norm LIKE '%team%'
  THEN
    RETURN TRUE;
  END IF;

  RETURN FALSE;
END;
$$;

-- (Optional) very permissive phone normalizer (keeps leading + and digits)
CREATE OR REPLACE FUNCTION util.phone_norm(p text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT NULLIF(
           regexp_replace(
             coalesce(p,''),
             '(?!^\+)\D',  -- replace all non-digits except leading +
             '',
             'g'
           ),
           ''
         )
$$;
