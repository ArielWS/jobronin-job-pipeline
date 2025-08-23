-- transforms/sql/00_extensions.sql
-- Enable required Postgres extensions (idempotent)

-- UUID generation (gen_random_uuid)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Text accent stripping used by util.person_name_norm
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Helpful for fuzzy/QA (safe to keep even if unused today)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Case-insensitive text (optional; harmless if unused)
CREATE EXTENSION IF NOT EXISTS citext;
