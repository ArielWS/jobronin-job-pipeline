CREATE SCHEMA IF NOT EXISTS util;

-- Extract host from URL; lowercased; strips leading www.
CREATE OR REPLACE FUNCTION util.url_host(u text)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN u IS NULL OR btrim(u) = '' THEN NULL
  ELSE regexp_replace(lower((regexp_match(u, '^(?:[a-z]+://)?([^/?#]+)'))[1]), '^www\.', '')
END
$$;

-- Extract domain from email (right of @), lowercased
CREATE OR REPLACE FUNCTION util.email_domain(e text)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN e IS NULL OR btrim(e) = '' THEN NULL
  ELSE lower(split_part(e, '@', 2))
END
$$;

-- Is an email domain generic (free providers)?
CREATE OR REPLACE FUNCTION util.is_generic_email_domain(d text)
RETURNS boolean
LANGUAGE sql IMMUTABLE AS $$
SELECT CASE
  WHEN d IS NULL THEN TRUE
  WHEN d ~ '(gmail\.com|yahoo\.|outlook\.|hotmail\.|icloud\.|proton\.|gmx\.)' THEN TRUE
  ELSE FALSE
END
$$;

-- Clean company name: lower, trim, remove legal suffixes & punctuation
CREATE OR REPLACE FUNCTION util.company_name_norm(n text)
RETURNS text
LANGUAGE sql IMMUTABLE AS $$
SELECT NULLIF(
  regexp_replace(
    regexp_replace(
      lower(coalesce(n,'')),
      '\b(gmbh|ag|se|s\.r\.o\.|sp\. z o\.o\.|llc|inc\.?|ltd\.?|bv|sarl|sas|gmbh & co\. kg|kg|oy|ab)\b', '', 'g'
    ),
    '[^a-z0-9 ]+', '', 'g'
  )::text, ''
)
$$;
