-include .env

install:
	pip install -r requirements.txt

api:
	uvicorn api.main:app --host 0.0.0.0 --port $(APP_PORT)

worker:
	python worker/runner.py

# End-to-end gold pipeline (idempotent, ordered)
PIPELINE_SQL = \
  transforms/sql/00_extensions.sql \
  transforms/sql/00_jobspy_raw.sql \
  transforms/sql/04_util_functions.sql \
  transforms/sql/04b_util_person_functions.sql \
  transforms/sql/01_silver_jobspy.sql \
  transforms/sql/02_silver_profesia_sk.sql \
  transforms/sql/02_silver_stepstone.sql \
  transforms/sql/03_unified_stage.sql \
  transforms/sql/10_gold_company.sql \
  transforms/sql/11_gold_company_brand_rules.sql \
  transforms/sql/12_gold_company_etl.sql \
  transforms/sql/13_gold_company_checks.sql \
  transforms/sql/14_gold_contact_schema.sql \
  transforms/sql/15_gold_contact_etl.sql \
  transforms/sql/16_gold_contact_checks.sql

pipeline:
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	@for f in $(PIPELINE_SQL); do \
	        echo ">> $$f"; \
	        psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f $$f || exit 1; \
	done

run-sql: pipeline

nightly:
	python orchestration/run_nightly.py

# Explicit company SQL sequence (unchanged order)
sql-companies:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/00_extensions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/00_jobspy_raw.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/04_util_functions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/01_silver_jobspy.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/02_silver_profesia_sk.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/02_silver_stepstone.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/03_unified_stage.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/10_gold_company.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/11_gold_company_brand_rules.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12_gold_company_etl.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/13_gold_company_checks.sql

# Explicit contacts SQL sequence
# NOTE: now includes extensions + core utils to guarantee prerequisites
sql-contacts:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/00_extensions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/04_util_functions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/04b_util_person_functions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/14_gold_contact_schema.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/15_gold_contact_etl.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/16_gold_contact_checks.sql

sanity:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/sanity.sql

trace-pipeline:
	python scripts/trace_pipeline.py SOURCE=$(SOURCE) OFFSET=$(OFFSET)
