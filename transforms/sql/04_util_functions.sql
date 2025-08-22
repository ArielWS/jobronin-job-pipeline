-- transforms/sql/04_util_functions.sql
-- Utility helpers for URL handling, company normalization, email/domain parsing,
-- simple location parsing, and JSON cleaning used across Silver/Unified/Gold.

CREATE SCHEMA IF NOT EXISTS util;
CREATE EXTENSION IF NOT EXISTS plpgsql;

--------------------------------------------------------------------------------
-- URL HELPERS
--------------------------------------------------------------------------------

-- Return the host of a URL (lowercased, with leading "www." stripped).
CREATE OR REPLACE FUNCTION util.url_host(u text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN u IS NULL OR btrim(u) = '' THEN NULL
  ELSE regexp_replace(
         lower( (regexp_match(u, '^(?:[a-z]+://)?([^/?#]+)'))[1] ),
         '^www\.',
         ''
       )
END
$$;

-- Canonicalize a URL:
-- - Trim whitespace
-- - Preserve scheme if http/https is present; otherwise default to https
-- - Lowercase scheme and host; strip leading "www."
-- - Remove query string and fragment
-- - Collapse duplicate slashes in the path
-- - Drop trailing slash (except when path is just "/")
CREATE OR REPLACE FUNCTION util.url_canonical(u text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s         text := NULLIF(btrim(u), '');
  scheme    text;
  host      text;
  path_qf   text;  -- path + [ ?query ][ #fragment ]
  path_only text;
BEGIN
  IF s IS NULL THEN
    RETURN NULL;
  END IF;

  -- Extract scheme (http/https) if present; lower it
  scheme := (regexp_match(lower(s), '^(https?)://'))[1];

  -- Extract canonical host (lower + strip www)
  host := util.url_host(s);
  IF host IS NULL OR btrim(host) = '' THEN
    RETURN NULL;
  END IF;

  -- Remove the scheme + authority, keep the remainder (path?query#fragment)
  path_qf := regexp_replace(s, '^(?:[a-z]+://)?[^/?#]+', '');

  -- Strip query and fragment
  path_only := regexp_replace(path_qf, '[?#].*$', '');

  -- Collapse duplicate slashes in the path
  path_only := regexp_replace(path_only, '/{2,}', '/', 'g');

  -- Ensure leading slash if there is any path content
  IF path_only IS NULL OR path_only = '' THEN
    path_only := '';
  ELSIF path_only !~ '^/' THEN
    path_only := '/' || path_only;
  END IF;

  -- Drop trailing slash (but keep root "/")
  IF length(path_only) > 1 THEN
    path_only := regexp_replace(path_only, '/+$', '');
  END IF;

  RETURN COALESCE(scheme, 'https') || '://' || host || path_only;
END
$$;

--------------------------------------------------------------------------------
-- LOCATION PARSING
--------------------------------------------------------------------------------

-- Parse a free-form location string like "City, Region, Country"
CREATE OR REPLACE FUNCTION util.location_parse(loc text)
RETURNS TABLE(city text, region text, country text)
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  parts text[];
BEGIN
  IF loc IS NULL OR btrim(loc) = '' THEN
    RETURN;
  END IF;

  parts := regexp_split_to_array(loc, '\\s*,\\s*');
  city    := NULLIF(parts[1], '');
  region  := NULLIF(parts[2], '');
  country := NULLIF(parts[3], '');

  RETURN;
END
$$;

--------------------------------------------------------------------------------
-- DOMAIN / EMAIL HELPERS
--------------------------------------------------------------------------------

-- Collapse a host to its org root (approx eTLD+1), handling common multi-label TLDs.
CREATE OR REPLACE FUNCTION util.org_domain(h text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  host   text := lower(coalesce(h,''));
  labels text[];
  last2  text;
  last3  text;
BEGIN
  IF host = '' THEN
    RETURN NULL;
  END IF;

  labels := regexp_split_to_array(host, '\.');
  IF array_length(labels,1) IS NULL OR array_length(labels,1) < 2 THEN
    RETURN host;
  END IF;

  last2 := labels[array_length(labels,1)-1] || '.' || labels[array_length(labels,1)];
  last3 := CASE WHEN array_length(labels,1) >= 3
                THEN labels[array_length(labels,1)-2] || '.' || last2
           END;

  IF last2 IN ('co.uk','com.au','com.br','com.mx','com.tr','co.jp','com.cn','com.hk')
  THEN
    RETURN last3;
  ELSE
    RETURN last2;
  END IF;
END
$$;

-- Extract the domain portion of an email address.
CREATE OR REPLACE FUNCTION util.email_domain(e text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN e IS NULL OR btrim(e) = '' THEN NULL
  ELSE lower(split_part(e, '@', 2))
END
$$;

-- Identify generic/free-mail providers that should not be used for company identity.
CREATE OR REPLACE FUNCTION util.is_generic_email_domain(d text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN d IS NULL THEN TRUE
  WHEN d ~ '(gmail\.com|yahoo\.|outlook\.|hotmail\.|icloud\.|proton\.|gmx\.|web\.de|aol\.com)' THEN TRUE
  ELSE FALSE
END
$$;

-- Identify known job boards / aggregators (not company identity).
CREATE OR REPLACE FUNCTION util.is_aggregator_host(h text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN h IS NULL THEN FALSE
  WHEN h ~ '(indeed\.)|(glassdoor\.)|(stepstone\.)|(linkedin\.)|(xing\.)|(welcometothejungle\.)|(monster\.|(profesia\.sk)' THEN TRUE
  ELSE FALSE
END
$$;

-- Treat parent/child domains as same org, e.g., "foo.com" ~ "eu.foo.com".
CREATE OR REPLACE FUNCTION util.same_org_domain(d1 text, d2 text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN d1 IS NULL OR d2 IS NULL THEN FALSE
  WHEN lower(d1) = lower(d2) THEN TRUE
  WHEN lower(d1) LIKE '%.' || lower(d2) THEN TRUE
  WHEN lower(d2) LIKE '%.' || lower(d1) THEN TRUE
  ELSE FALSE
END
$$;

--------------------------------------------------------------------------------
-- COMPANY NAME NORMALIZATION
--------------------------------------------------------------------------------

-- Strip a trailing language marker like " - English", "(DE)", " – Deutsch", etc.
CREATE OR REPLACE FUNCTION util.company_name_strip_lang_suffix(n text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT btrim(
  regexp_replace(
    coalesce(n,''),
    '\s*(?:-|–|—)?\s*(english|deutsch|german|français|francais|español|spanish|italiano|portugu[eê]s|nederlands|polski|русский|рус|\(en\)|\(de\)|\(fr\)|\(es\)|\(it\)|\(pt\)|\(nl\)|\(pl\)|\(ru\))\s*$',
    '',
    'gi'
  )
)
$$;

-- Normalize company names: lowercase, de-accent, strip punctuation & legal suffixes, squash spaces.
CREATE OR REPLACE FUNCTION util.company_name_norm(n text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
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
       strip_noise AS (
         SELECT regexp_replace(s, '\b(group|holding)\b', ' ', 'g') s
         FROM strip_legal
       ),
       squashed AS (SELECT regexp_replace(s,'\s+',' ','g') s FROM strip_noise)
  SELECT NULLIF(trim(s),'') FROM squashed;
$$;

-- Normalization that first strips language suffixes, then applies the base normalizer.
CREATE OR REPLACE FUNCTION util.company_name_norm_langless(n text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT util.company_name_norm(util.company_name_strip_lang_suffix(n))
$$;

-- Detect placeholder company names we don't want to treat as real identities.
CREATE OR REPLACE FUNCTION util.is_placeholder_company_name(n text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN n IS NULL THEN TRUE
  WHEN btrim(lower(n)) IN ('not found','unknown','n/a','na','-','n.a.','none') THEN TRUE
  ELSE FALSE
END
$$;

--------------------------------------------------------------------------------
-- EMAIL EXTRACTION
--------------------------------------------------------------------------------

-- Extract the first valid email address from a noisy string.
CREATE OR REPLACE FUNCTION util.first_email(t text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
SELECT (regexp_match(coalesce(t,''), '([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})','i'))[1]
$$;

--------------------------------------------------------------------------------
-- APPLY HOST CLASSIFICATION
--------------------------------------------------------------------------------

-- Detect common ATS hosts (kept separate from real company websites).
CREATE OR REPLACE FUNCTION util.is_ats_host(h text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN h IS NULL THEN FALSE
  WHEN h ~ '(greenhouse\.io|lever\.co|myworkdayjobs\.com|workday\.com|bamboohr\.com|smartrecruiters\.com|recruitee\.com|ashbyhq\.com|jobs\.personio\.de|personio\.com|icims\.com|teamtailor\.com|taleo\.net|oraclecloud\.com|successfactors\.com|successfactors\.eu|brassring\.com|jobvite\.com|eightfold\.ai|avature.net|grnh.se|recruit\.zoho\.com|snaphunt\.com)'
    THEN TRUE
  ELSE FALSE
END
$$;

-- Career-specific hosts should not be used as the canonical company identity.
CREATE OR REPLACE FUNCTION util.is_career_host(h text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
SELECT CASE
  WHEN h IS NULL OR btrim(h) = '' THEN FALSE
  WHEN h ~ '\.jobs$'                  THEN TRUE
  WHEN h ~ '(^|[.-])careers?([.-]|$)' THEN TRUE
  WHEN h ~ '(^|[.-])jobs([.-]|$)'     THEN TRUE
  ELSE FALSE
END
$$;

--------------------------------------------------------------------------------
-- JSON CLEANING (handy for StepStone and other JSON-heavy feeds)
--------------------------------------------------------------------------------

-- Safe JSONB cast: replace NaN/Infinity/None with null, return NULL on failure
CREATE OR REPLACE FUNCTION util.jsonb_safe(t text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s text;
BEGIN
  IF t IS NULL OR btrim(t) = '' THEN
    RETURN NULL;
  END IF;

  s := regexp_replace(t, '\bNaN\b', 'null', 'gi');
  s := regexp_replace(s, '\bInfinity\b', 'null', 'gi');
  s := regexp_replace(s, '\b-?Infinity\b', 'null', 'gi');
  s := regexp_replace(s, '\bNone\b', 'null', 'gi');

  RETURN s::jsonb;
EXCEPTION WHEN others THEN
  RETURN NULL;
END
$$;

-- Safe JSON cleaner: replace bare NaN/Infinity/None with null, then cast
CREATE OR REPLACE FUNCTION util.json_clean(t text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s text := t;
BEGIN
  IF s IS NULL OR btrim(s) = '' THEN
    RETURN NULL;
  END IF;

  -- Use PG word boundaries: [[:<:]] and [[:>:]]
  s := regexp_replace(s, '[[:<:]]NaN[[:>:]]',      'null', 'g');
  s := regexp_replace(s, '[[:<:]]Infinity[[:>:]]', 'null', 'g');
  s := regexp_replace(s, '[[:<:]]None[[:>:]]',     'null', 'g');

  RETURN s::jsonb;
END
$$;
