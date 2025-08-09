-- transforms/sql/00_extensions.sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE SCHEMA IF NOT EXISTS silver;
-- (optional for later) CREATE SCHEMA IF NOT EXISTS gold;