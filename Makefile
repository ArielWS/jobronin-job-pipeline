-include .env

install:
	pip install -r requirements.txt

api:
	uvicorn api.main:app --host 0.0.0.0 --port $(APP_PORT)

worker:
	python worker/runner.py

run-sql:
	@if [ -z "$(DATABASE_URL)" ]; then echo "DATABASE_URL not set"; exit 1; fi
	for f in transforms/sql/*.sql; do \
	  echo ">>> Running $$f"; \
	  psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f"; \
	done

nightly:
	python orchestration/run_nightly.py
