-- transforms/sql/00_jobspy_raw.sql
-- Declare expected schema for the raw JobSpy scrape table (Bronze).
-- This must stay aligned with the scraper output and with the silver view’s expectations.

CREATE TABLE IF NOT EXISTS public.jobspy_job_scrape (
    job_url                varchar NOT NULL,
    id                     uuid    NOT NULL,
    site                   varchar NULL,
    job_url_direct         varchar NULL,
    search_term            varchar NULL,
    "time_stamp"           timestamptz NULL,
    title                  varchar NULL,
    company                varchar NULL,
    "location"             varchar NULL,
    date_posted            date    NULL,
    job_type               varchar NULL,
    salary_source          varchar NULL,
    "interval"             varchar NULL,
    min_amount             float8  NULL,
    max_amount             float8  NULL,
    currency               varchar NULL,
    is_remote              bool    NULL,
    job_level              varchar NULL,
    job_function           varchar NULL,
    listing_type           varchar NULL,
    emails                 varchar NULL,
    description            varchar NULL,
    company_industry       varchar NULL,
    company_url            varchar NULL,
    company_logo           varchar NULL,
    company_url_direct     varchar NULL,
    company_addresses      varchar NULL,
    company_num_employees  varchar NULL,
    company_revenue        varchar NULL,
    company_description    varchar NULL,
    skills                 varchar NULL,
    experience_range       varchar NULL,
    company_rating         float8  NULL,
    company_reviews_count  int4    NULL,
    vacancy_count          int4    NULL,
    work_from_home_type    varchar NULL,
    CONSTRAINT jobspy_job_scrape_id_key UNIQUE (id),
    CONSTRAINT jobspy_job_scrape_pkey PRIMARY KEY (job_url)
);

-- Verify/alert on schema drift.
DO $$
DECLARE
    expected_columns TEXT[] := ARRAY[
        'job_url','id','site','job_url_direct','search_term','time_stamp','title','company','location',
        'date_posted','job_type','salary_source','interval','min_amount','max_amount','currency','is_remote',
        'job_level','job_function','listing_type','emails','description','company_industry','company_url',
        'company_logo','company_url_direct','company_addresses','company_num_employees','company_revenue',
        'company_description','skills','experience_range','company_rating','company_reviews_count',
        'vacancy_count','work_from_home_type'
    ];
    missing_columns TEXT[];
    extra_columns TEXT[];
BEGIN
    SELECT ARRAY(
        SELECT col FROM unnest(expected_columns) col
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = 'jobspy_job_scrape'
              AND column_name  = col
        )
    ) INTO missing_columns;

    SELECT ARRAY(
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'jobspy_job_scrape'
          AND column_name <> ALL(expected_columns)
        ORDER BY column_name
    ) INTO extra_columns;

    IF missing_columns IS NOT NULL AND array_length(missing_columns, 1) > 0 THEN
        RAISE EXCEPTION 'jobspy_job_scrape missing columns: %', missing_columns;
    END IF;

    IF extra_columns IS NOT NULL AND array_length(extra_columns, 1) > 0 THEN
        RAISE WARNING 'jobspy_job_scrape has unexpected columns: %', extra_columns;
    END IF;
END $$;
