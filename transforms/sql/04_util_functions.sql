-- transforms/sql/04_util_functions.sql
CREATE SCHEMA IF NOT EXISTS util;
CREATE EXTENSION IF NOT EXISTS plpgsql;

-- host from URL (lowercase, strip www)
CREATE OR REPLACE FUNCTION util.url_host(u text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN u IS NULL OR btrim(u) = '' THEN NULL
  ELSE regexp_replace(lower((regexp_match(u, '^(?:[a-z]+://)?([^/?#]+)'))[1]), '^www\.', '')
END
$$;

-- collapse to org root (eTLD+1-ish)
CREATE OR REPLACE FUNCTION util.org_domain(h text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  host text := lower(coalesce(h,''));
  labels text[];
  last2 text; last3 text;
BEGIN
  IF host = '' THEN RETURN NULL; END IF;
  labels := regexp_split_to_array(host, '\.');
  IF array_length(labels,1) IS NULL OR array_length(labels,1) < 2 THEN
    RETURN host;
  END IF;
  last2 := labels[array_length(labels,1)-1] || '.' || labels[array_length(labels,1)];
  last3 := CASE WHEN array_length(labels,1) >= 3
          THEN labels[array_length(labels,1)-2] || '.' || last2 END;
  IF last2 IN ('co.uk','com.au','com.br','com.mx','com.tr','co.jp','com.cn','com.hk')
  THEN RETURN last3; ELSE RETURN last2; END IF;
END
$$;

-- email domain
CREATE OR REPLACE FUNCTION util.email_domain(e text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN e IS NULL OR btrim(e) = '' THEN NULL
  ELSE lower(split_part(e, '@', 2))
END
$$;

-- free-mail providers (don’t use to identify companies)
CREATE OR REPLACE FUNCTION util.is_generic_email_domain(d text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN d IS NULL THEN TRUE
  WHEN d ~ '(gmail\.com|yahoo\.|outlook\.|hotmail\.|icloud\.|proton\.|gmx\.|web\.de|aol\.com)' THEN TRUE
  ELSE FALSE
END
$$;

-- job boards / aggregators
CREATE OR REPLACE FUNCTION util.is_aggregator_host(h text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN h IS NULL THEN FALSE
  WHEN h ~ '(indeed\.)|(glassdoor\.)|(stepstone\.)|(linkedin\.)|(xing\.)|(welcometothejungle\.)|(monster\.)' THEN TRUE
  ELSE FALSE
END
$$;

-- treat parent/child domains as same org
CREATE OR REPLACE FUNCTION util.same_org_domain(d1 text, d2 text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN d1 IS NULL OR d2 IS NULL THEN FALSE
  WHEN lower(d1) = lower(d2) THEN TRUE
  WHEN lower(d1) LIKE '%.' || lower(d2) THEN TRUE
  WHEN lower(d2) LIKE '%.' || lower(d1) THEN TRUE
  ELSE FALSE
END
$$;

-- NEW: normalize company names (drop legal suffixes, punctuation)
CREATE OR REPLACE FUNCTION util.company_name_norm(n text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  WITH raw AS (SELECT lower(coalesce(n,'')) s),
       d1  AS (SELECT regexp_replace(s,'[àáâãäåāæ]','a','g') s FROM raw),
       d2  AS (SELECT regexp_replace(s,'[èéêëē]','e','g') s FROM d1),
       d3  AS (SELECT regexp_replace(s,'[ìíîïī]','i','g') s FROM d2),
       d4  AS (SELECT regexp_replace(s,'[òóôõöō]','o','g') s FROM d3),
       d5  AS (SELECT regexp_replace(s,'[ùúûüū]','u','g') s FROM d4),
       cleaned AS (
         SELECT regexp_replace(regexp_replace(s,'[\u00A0–—\-]+',' ','g'),
                               '[^a-z0-9&.+ ]+', ' ', 'g') s FROM d5
       ),
       strip_legal AS (
         SELECT regexp_replace(
           s,
           '\b(gmbh|ag|se|kgaa|kg|ug|bv|b\.v\.|sarl|sas|ltd|plc|llc|inc|oy|ab|as|s\.r\.o\.|sp\. z o\.o\.)\b',
           ' ','g') s
         FROM cleaned
       ),
       -- keep this minimal: remove generic company “noise”, but NOT languages/countries
       strip_noise AS (
         SELECT regexp_replace(s, '\b(group|holding)\b', ' ', 'g') s
         FROM strip_legal
       ),
       squashed AS (SELECT regexp_replace(s,'\s+',' ','g') s FROM strip_noise)
  SELECT NULLIF(trim(s),'') FROM squashed;
$$;
-- Extract the first valid email address from a noisy string
CREATE OR REPLACE FUNCTION util.first_email(t text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT (regexp_match(coalesce(t,''), '([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})','i'))[1]
$$;

-- Detect common ATS hosts (we'll separate them from real apply domains)
CREATE OR REPLACE FUNCTION util.is_ats_host(h text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN h IS NULL THEN FALSE
  WHEN h ~ '(greenhouse\.io|lever\.co|myworkdayjobs\.com|workday\.com|bamboohr\.com|smartrecruiters\.com|recruitee\.com|ashbyhq\.com|jobs\.personio\.de|personio\.com|icims\.com|teamtailor\.com)'
    THEN TRUE
  ELSE FALSE
END
$$;

-- prevent appending  "Not Found" like names
CREATE OR REPLACE FUNCTION util.is_placeholder_company_name(n text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN n IS NULL THEN TRUE
    WHEN btrim(lower(n)) IN ('not found','unknown','n/a','na','-','n.a.','none') THEN TRUE
    ELSE FALSE
  END
$$;

-- util: career/careers/.jobs hosts should not be canonical identity
CREATE OR REPLACE FUNCTION util.is_career_host(h text)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN h IS NULL OR btrim(h)='' THEN FALSE
  WHEN h ~ '\.jobs$'                         THEN TRUE
  WHEN h ~ '(^|[.-])careers?([.-]|$)'        THEN TRUE
  ELSE FALSE
END
$$;


-- Strip a trailing language marker like " - English", "(DE)", " – Deutsch", etc.
CREATE OR REPLACE FUNCTION util.company_name_strip_lang_suffix(n text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT btrim(
  regexp_replace(
    coalesce(n,''),
    '\s*(?:-|–|—)?\s*(english|deutsch|german|français|francais|español|spanish|italiano|portugu[eê]s|nederlands|polski|русский|рус|\(en\)|\(de\)|\(fr\)|\(es\)|\(it\)|\(pt\)|\(nl\)|\(pl\)|\(ru\))\s*$',
    '',
    'gi'
  )
)
$$;

-- Normalization that first strips language suffixes, then applies your base normalizer
CREATE OR REPLACE FUNCTION util.company_name_norm_langless(n text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
SELECT util.company_name_norm(util.company_name_strip_lang_suffix(n))
$$;
