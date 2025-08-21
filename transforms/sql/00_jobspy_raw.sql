-- Declare expected schema for the raw JobSpy scrape table.
-- This helps surface upstream schema changes early in the pipeline.
CREATE TABLE IF NOT EXISTS public.jobspy_job_scrape (
    id BIGINT,
    title TEXT,
    job_type TEXT,
    company TEXT,
    company_industry TEXT,
    company_url TEXT,
    company_url_direct TEXT,
    job_url TEXT,
    job_url_direct TEXT,
    company_logo TEXT,
    company_description TEXT,
    emails TEXT,
    location TEXT,
    company_addresses TEXT,
    job_data JSONB,
    date_posted TIMESTAMPTZ
);

-- Verify table columns: raise on missing, warn on unexpected extras.
DO $$
DECLARE
    expected_columns TEXT[] := ARRAY[
        'id', 'title', 'job_type', 'company', 'company_industry',
        'company_url', 'company_url_direct', 'job_url', 'job_url_direct',
        'company_logo', 'company_description', 'emails', 'location',
        'company_addresses', 'job_data', 'date_posted'
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
