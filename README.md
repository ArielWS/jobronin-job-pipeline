# JobRonin — Jobs Pipeline

This service turns raw job posts from multiple sources into **canonical, query-ready data** and provides a thin API/worker surface the rest of JobRonin can use for matching.

## Scope in the broader project

- **Owned here**
  - SQL pipeline: **Bronze → Silver → Gold** (normalize, dedupe, field-level enrichment).
  - Matching storage: `Agent`, `SearchSession`, `MatchResult` (results cached per session).
  - Minimal API (health now; matching endpoints later).
  - Nightly orchestration (local and GitHub Actions).

- **Not owned here**
  - Scrapers (JobSpy, StepStone, …) — separate repos push into Bronze tables.
  - Contact enrichment + Browser-Use automations — sibling worker/service.
  - Front-ends (Lovable web app, Chrome extension) — consume this repo’s API.

## 📚 Documentation

**Business (high level)**
- 🧭 One-pager: [docs/business/one-pager.md](docs/business/one-pager.md)
- 📋 Business requirements: [docs/business/requirements.md](docs/business/requirements.md)

**Engineering (diagrams)**
- 🧱 System architecture: [docs/engineering/system-architecture.md](docs/engineering/system-architecture.md)
- 🌙 Nightly ETL sequence: [docs/engineering/nightly-etl-sequence.md](docs/engineering/nightly-etl-sequence.md)
- 🤖 Agent search & enrichment sequence: [docs/engineering/agent-search-sequence.md](docs/engineering/agent-search-sequence.md)
- 🗺️ Conceptual ERD: [docs/engineering/conceptual-erd.md](docs/engineering/conceptual-erd.md)

## Repository layout

```text
.
├── api/                      # Thin FastAPI app (health now; search endpoints later)
│   └── main.py
├── worker/                   # Background worker stub (LLM verify, contacts, Browser-Use later)
│   └── runner.py
├── orchestration/            # Local runner for SQL transforms
│   └── run_nightly.py
├── transforms/
│   └── sql/                  # SQL-first pipeline (executed in lexical order)
│       ├── 00_extensions.sql
│       ├── 01_silver_jobspy.sql
│       ├── 02_silver_stepstone.sql
│       ├── 03_unified_stage.sql
│       ├── 10_gold_company.sql
│       ├── 11_gold_job_post.sql
│       ├── 12_upsert_companies.sql
│       ├── 13_upsert_jobs_deterministic.sql
│       ├── 14_upsert_jobs_fuzzy.sql
│       ├── 20_enrich_apply_url.sql
│       ├── 21_enrich_salary.sql
│       ├── 22_enrich_description.sql
│       ├── 23_enrich_emails.sql
│       └── 40_matching.sql
├── infra/                    # Placeholder for IaC/deploy
├── scripts/                  # Helper scripts (optional)
├── .github/
│   └── workflows/
│       └── nightly.yml       # Nightly SQL transforms on main
├── .env                      # Local config (gitignored)
├── .env.sample               # Template for .env
├── requirements.txt          # Minimal runtime deps
├── Makefile                  # install, api, worker, run-sql, nightly
└── docs/                     # Business + engineering docs

```

## Environment & config

Create `.env` from the sample:

DATABASE_URL=postgres://USER:PASS@HOST:5432/DBNAME
APP_PORT=8000

- `DATABASE_URL` is required for DB ops (`make run-sql`, API health, orchestration).
- In CI/GitHub Actions, set `DATABASE_URL` as a **repository secret**.

## Getting started (local)

cp .env.sample .env
pip install -r requirements.txt
make run-sql
make api
# optional
make worker

## Nightly pipeline (CI on main)

The workflow at .github/workflows/nightly.yml runs on **main** at 0 20 * * * (20:00 UTC). To enable:

1. GitHub → Settings → Secrets and variables → Actions → New repository secret
   - Name: DATABASE_URL
   - Value: your production Postgres URL
2. Ensure SQL files are correctly ordered (lexical order = run order).
3. You can manually trigger via Actions → nightly → Run workflow.

## How it fits together

- Scrapers write Bronze raw tables.
- Silver views normalize per source; Gold tables dedupe into canonical Company/JobPost.
- Enrichment fills missing fields per-field (no regress); Features precompute search signals.
- Matching writes session results so UIs can page/click without recompute.
- Decision-makers & contacts happen in a sibling worker/service; API can read those canonical people tables.

See docs/engineering/ for diagrams.

## Troubleshooting

- Mermaid diagrams not rendering on GitHub: each block must start with ```mermaid and end with ```. Keep labels simple.
- make run-sql says “DATABASE_URL not set”: ensure .env exists; or export to shell: export $(cat .env | xargs)
- DB connect errors: verify network/allowlist/SSL (try ?sslmode=require).
- Action fails on psql: confirm DATABASE_URL secret exists on the repo and branch main.

## Next steps

- Fill Silver views for JobSpy & StepStone with the unified column set.
- Implement Gold DDL for company, company_alias, job_post, job_source_link.
- Add deterministic + blocked fuzzy upserts, then field-level enrichment SQL.
- Create matching tables and match_fn(session_id) (Filter + Rank).
- Expose /v1/search and /v1/search/{session_id}/results in api/.
- Stand up the separate decision-maker worker for contact fetch & caching.

Default branch: main
git branch -M main
git push -u origin main
