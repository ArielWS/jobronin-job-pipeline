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

-- consider parent/child domains same org
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
