CREATE TABLE IF NOT EXISTS gold.job_post (
  job_id            BIGSERIAL PRIMARY KEY,
  company_id        BIGINT NOT NULL REFERENCES gold.company(company_id),
  title_norm        TEXT NOT NULL,
  city              TEXT,
  region            TEXT,
  country           TEXT,
  date_posted       DATE,
  job_url_direct    TEXT,
  apply_url_clean   TEXT,
  description       TEXT,
  is_remote         BOOLEAN,
  contract_type_raw TEXT,
  salary_min        NUMERIC,
  salary_max        NUMERIC,
  currency          TEXT,
  created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS job_apply_url_uidx
  ON gold.job_post(apply_url_clean);

CREATE INDEX IF NOT EXISTS job_company_date_idx ON gold.job_post(company_id, date_posted);
CREATE INDEX IF NOT EXISTS job_date_idx ON gold.job_post(date_posted);
CREATE INDEX IF NOT EXISTS job_title_trgm_idx ON gold.job_post USING gin (title_norm gin_trgm_ops);

CREATE TABLE IF NOT EXISTS gold.job_source_link (
  source         TEXT NOT NULL,
  source_id      TEXT NOT NULL,
  job_id         BIGINT NOT NULL REFERENCES gold.job_post(job_id) ON DELETE CASCADE,
  source_row_url TEXT,
  created_at     TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source, source_id)
);
