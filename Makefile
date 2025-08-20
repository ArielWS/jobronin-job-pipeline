-include .env

install:
	pip install -r requirements.txt

api:
	uvicorn api.main:app --host 0.0.0.0 --port $(APP_PORT)

worker:
	python worker/runner.py

# Minimal company pipeline (idempotent)
PIPELINE_SQL = \
  transforms/sql/04_util_functions.sql \
  transforms/sql/01_silver_jobspy.sql \
  transforms/sql/02_silver_profesia_sk.sql \
  transforms/sql/02_silver_stepstone.sql \
  transforms/sql/03_unified_stage.sql \
  transforms/sql/10_gold_company.sql \
  transforms/sql/12c_company_brand_rules.sql \
  transforms/sql/12a_companies_seed_by_email.sql \
  transforms/sql/12a_companies_seed_by_website.sql \
  transforms/sql/12a_companies_upsert.sql \
  transforms/sql/12a_company_evidence.sql

pipeline:
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	@for f in $(PIPELINE_SQL); do \
	        echo ">> $$f"; \
	        psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f $$f || exit 1; \
	done

run-sql: pipeline

nightly:
	python orchestration/run_nightly.py

sql-companies:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/00_extensions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/04_util_functions.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/01_silver_jobspy.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/02_silver_profesia_sk.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/02_silver_stepstone.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/03_unified_stage.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/10_gold_company.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12c_company_brand_rules.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_companies_seed_by_email.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_companies_seed_by_website.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_companies_upsert.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_company_evidence.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12e_company_promote_domain.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12b_company_fill_nulls.sql
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12d_company_monitoring_checks.sql

sanity:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/sanity.sql
