-include .env

.PHONY: install api worker pipeline run-sql nightly sql-silver sql-gold sql-all sanity trace-pipeline sql-contacts

install:
	pip install -r requirements.txt

api:
	uvicorn api.main:app --host 0.0.0.0 --port $(APP_PORT)

worker:
	python worker/runner.py

# ---------------------------
# SQL file groups (ordered)
# ---------------------------

# Shared prerequisites
PRELUDE_SQL = \
  transforms/sql/00_extensions.sql \
  transforms/sql/04_util_functions.sql \
  transforms/sql/04b_util_person_functions.sql

# Silver-only (builds the normalized source layer)
SILVER_SQL = \
  transforms/sql/00_jobspy_raw.sql \
  transforms/sql/01_silver_jobspy.sql \
  transforms/sql/02_silver_profesia_sk.sql \
  transforms/sql/02_silver_stepstone.sql \
  transforms/sql/03_unified_stage.sql

# Gold (company + contacts), requires Silver
GOLD_SQL = \
  transforms/sql/10_gold_company.sql \
  transforms/sql/11_gold_company_brand_rules.sql \
  transforms/sql/12_gold_company_etl.sql \
  transforms/sql/13_gold_company_checks.sql \
  transforms/sql/14_gold_contact_schema.sql \
  transforms/sql/14b_gold_contact_schema_alter.sql \
  transforms/sql/15_gold_contact_etl.sql \
  transforms/sql/16_gold_contact_checks.sql

# Full end-to-end pipeline
PIPELINE_SQL = $(PRELUDE_SQL) $(SILVER_SQL) $(GOLD_SQL)

# ---------------------------
# Top-level pipeline targets
# ---------------------------

pipeline:
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	@for f in $(PIPELINE_SQL); do \
	    echo ">> $$f"; \
	    psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f $$f || exit 1; \
	done

run-sql: pipeline

nightly:
	python orchestration/run_nightly.py

# ---------------------------
# Layered targets
# ---------------------------

# Build Silver layer only (plus prerequisites)
sql-silver:
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	@for f in $(PRELUDE_SQL) $(SILVER_SQL); do \
	    echo ">> $$f"; \
	    psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f $$f || exit 1; \
	done

# Build Gold layer (depends on Silver). Keeps order: company → contacts.
sql-gold: sql-silver
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	@for f in $(GOLD_SQL); do \
	    echo ">> $$f"; \
	    psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f $$f || exit 1; \
	done

# Convenience alias (previous name)
sql-contacts: sql-gold

# Everything split but in two calls (useful for CI steps)
sql-all: sql-silver sql-gold

# ---------------------------
# Misc
# ---------------------------

sanity:
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/sanity.sql

trace-pipeline:
	python scripts/trace_pipeline.py SOURCE=$(SOURCE) OFFSET=$(OFFSET)
