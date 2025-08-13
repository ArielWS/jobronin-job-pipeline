# JobRonin — Jobs Pipeline

This service turns raw job posts from multiple sources into canonical, query-ready data and exposes a thin API/worker surface the rest of JobRonin uses for matching.

---

## Scope in the broader project

**Owned here**
- SQL pipeline: **Bronze → Silver → Gold** (normalize, dedupe, brand-aware identity, field-level enrichment).
- Matching storage: `Agent`, `SearchSession`, `MatchResult` (results cached per session).
- Minimal API (health now; matching endpoints later).
- Nightly orchestration (local and GitHub Actions).

**Not owned here**
- Scrapers (JobSpy, StepStone, …) — separate repos write **Bronze** raw tables.
- Contact enrichment + Browser-Use automations — sibling worker/service.
- Front-ends (Lovable web app, Chrome extension) — consume this repo’s API.

---

## Repository layout

.
├── api/ # Thin FastAPI app (health now; search endpoints later)
│ └── main.py
├── worker/ # Background worker stub (LLM verify, contacts, Browser-Use later)
│ └── runner.py
├── orchestration/ # Local runner for SQL transforms
│ └── run_nightly.py
├── transforms/
│ └── sql/ # SQL-first pipeline (executed in lexical order unless run manually)
│ ├── 00_extensions.sql
│ ├── 01_silver_jobspy.sql
│ ├── 02_silver_stepstone.sql
│ ├── 03_unified_stage.sql
│ ├── 04_util_functions.sql # utility fns used by Silver/Gold (see below)
│ ├── 10_gold_company.sql # gold.company, gold.company_alias, gold.company_evidence_domain
│ ├── 11_gold_job_post.sql
│ ├── 12a_companies_upsert.sql # deterministic, brand-aware company upsert (no JSON parsing here)
│ ├── 12a_company_evidence.sql # website/email/apply evidence (no JSON parsing here)
│ ├── 12b_company_fill_nulls.sql # optional top-ups (no regress)
│ ├── 12c_company_brand_rules.sql # brand rules table + seed rows (DACH/V4 examples)
│ ├── 12d_company_monitoring_checks.sql # optional quality checks
│ ├── 12e_company_promote_domain.sql # optional post-upsert promote (legacy helper)
│ ├── 13_upsert_jobs_deterministic.sql
│ ├── 14_upsert_jobs_fuzzy.sql
│ ├── 20_enrich_apply_url.sql
│ ├── 21_enrich_salary.sql
│ ├── 22_enrich_description.sql
│ ├── 23_enrich_emails.sql
│ └── 40_matching.sql
├── infra/ # Placeholder for IaC/deploy
├── scripts/ # Helper scripts (optional)
├── .github/
│ └── workflows/
│ └── nightly.yml # Nightly SQL transforms on main
├── .env # Local config (gitignored)
├── .env.sample # Template for .env
├── requirements.txt # Minimal runtime deps
├── Makefile # install, api, worker, run-sql, nightly
└── docs/ # Business + engineering docs

---

## Data flow

### Bronze (raw)
Scraper repos populate raw tables (e.g., `public.jobspy_job_scrape`, `public.stepstone_job_scrape`).

### Silver (source-normalized views)
- `01_silver_jobspy.sql` — parses JobSpy rows to the unified Silver shape.
- `02_silver_stepstone.sql` — parses StepStone rows. **JSON is sanitized once** via `util.json_clean(text) → jsonb` (converts `NaN`/`Infinity`/`None` to `null`). Company name is derived from JSON when present; otherwise conservatively guessed from the title suffix (`" - ACME" | " @ ACME"`) to avoid mislabeling agency posts.
- `03_unified_stage.sql` — unions Silver sources into `silver.unified` with consistent columns:
  - `company_name`, `company_domain`, `contact_email_root`, `apply_root`, `company_description_raw`, `company_size_raw`, `company_industry_raw`, `company_logo_url`, `source`, `source_id`, `source_row_url`, `title_*`, `location_*`, etc.

### Gold (canonical tables)
- `10_gold_company.sql` creates:
  - `gold.company` with:
    - `name` and **generated** `name_norm` = `util.company_name_norm(name)`
    - optional `website_domain`
    - optional `brand_key` (sub-brand discriminator, e.g., SAP vs SAP-SuccessFactors)
    - **uniques**: `(website_domain, brand_key)` and `(name_norm)`
  - `gold.company_alias` `(company_id, alias, alias_norm)`; PK on `(company_id, alias_norm)`.
  - `gold.company_evidence_domain` `(company_id, kind, value)`; PK on all three; kinds: `website`, `email`, `apply`.
- `12c_company_brand_rules.sql` defines `gold.company_brand_rule(domain_root, brand_regex, brand_key, active)` and seeds practical DACH/V4 examples.
- `12a_companies_upsert.sql` performs a **one-pass, collision-free, brand-aware upsert**:
  1. Choose one “best” row per normalized name (prefer real site over email; prefer richer attributes). Insert **name-only** rows with `ON CONFLICT (name_norm)` non-regressive updates.
  2. Choose one **domain winner** per `(org_root, brand_key)` and set `website_domain`/`brand_key` **only** on that row (prevents `ON CONFLICT DO UPDATE … second time` errors).
  3. Resolve all source rows to `company_id`; write `company_alias` and `company_evidence_domain`; top-up attributes **without regress**.
- `12a_company_evidence.sql` writes website/email/apply evidence **only** from `silver.unified` (no JSON parsing here).

*Jobs* tables (`11_gold_job_post.sql`, `13_*`, `14_*`, `20-23_*`, `40_matching.sql`) are present but not the focus of this README refresh.

---

## Utility functions (04\_util\_functions.sql)

Key helpers used across the pipeline:
- `util.url_host(text)` — host from URL (lowercase, strip `www.`)
- `util.org_domain(text)` — collapses host to org root (eTLD+1-ish with common PSL exceptions)
- `util.email_domain(text)` — domain from email
- `util.is_generic_email_domain(text)` — filters freemail providers
- `util.is_aggregator_host(text)` / `util.is_ats_host(text)` / `util.is_career_host(text)`
- `util.same_org_domain(d1,d2)` — tolerant parent/child comparison
- `util.company_name_norm(text)` — lowercases, deburrs, strips legals/noise
- `util.company_name_strip_lang_suffix(text)` / `util.company_name_norm_langless(text)`
- `util.first_email(text)` — extracts first RFC-ish email from text
- **NEW** `util.json_clean(text) → jsonb` — replaces `NaN`/`Infinity`/`None` with `null` before casting
- `util.is_placeholder_company_name(text)` — guards against junk like “Not found”

---

## Environment & config

Create `.env` from the sample:

DATABASE_URL=postgres://USER:PASS@HOST:5432/DBNAME
APP_PORT=8000

`DATABASE_URL` is required for DB ops (Makefile targets, API health, orchestration).
In CI/GitHub Actions, set `DATABASE_URL` as a repository secret.

---

## Getting started (local)

```bash
cp .env.sample .env
pip install -r requirements.txt
make run-sql        # or run the SQL files manually as shown below
make api
# optional
make worker
Manual SQL run (clean rebuild of company dimension)
Tip: some views depend on util functions. If you see “function does not exist”, run 04_util_functions.sql first.
# Utilities first (safe to re-run)
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/04_util_functions.sql

# Silver
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/01_silver_jobspy.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/02_silver_stepstone.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/03_unified_stage.sql

# Gold DDL
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/10_gold_company.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/11_gold_job_post.sql

# Brand rules (table + seed)
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12c_company_brand_rules.sql

# Upsert + evidence
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_companies_upsert.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12a_company_evidence.sql
# optional top-ups and checks
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12b_company_fill_nulls.sql || true
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f transforms/sql/12d_company_monitoring_checks.sql || true
```
If Postgres reports cannot drop columns from view while replacing a Silver view, drop dependents and recreate:
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "DROP VIEW IF EXISTS silver.unified CASCADE;"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "DROP VIEW IF EXISTS silver.stepstone CASCADE;"
# then recreate the views again (02, then 03)
Nightly pipeline (CI on main)
The workflow at .github/workflows/nightly.yml runs on main at 0 20 * * * (20:00 UTC). To enable:
GitHub → Settings → Secrets and variables → Actions → New repository secret
Name: DATABASE_URL — Value: your production Postgres URL
Ensure SQL files are correctly ordered (lexical order = run order) or call them explicitly in run_nightly.py.
You can manually trigger via Actions → nightly → Run workflow.
Sanity checks
# Counts
psql "$DATABASE_URL" -c "SELECT COUNT(*) AS companies FROM gold.company;"
psql "$DATABASE_URL" -c "SELECT COUNT(*) AS with_site FROM gold.company WHERE website_domain IS NOT NULL;"
psql "$DATABASE_URL" -c "SELECT website_domain, COUNT(*) FROM gold.company WHERE website_domain IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1;"

# Spot check a few brands
psql "$DATABASE_URL" -c "SELECT company_id, name, website_domain FROM gold.company WHERE name ILIKE 'zendesk%' OR name ILIKE 'sellerx%' OR name ILIKE 'tourlane%' OR name ILIKE 'amazon%';"

# Evidence distribution
psql "$DATABASE_URL" -c "SELECT kind, COUNT(*) FROM gold.company_evidence_domain GROUP BY 1 ORDER BY 1;"
Troubleshooting
Invalid JSON (NaN/Infinity/None): Silver parsing sanitizes JSON via util.json_clean(text). If you still see JSON cast errors, make sure 02_silver_stepstone.sql is the latest version and 04_util_functions.sql has been applied.
“cannot drop columns from view”: PostgreSQL can’t remove columns with CREATE OR REPLACE VIEW. Drop the view (and its dependents) and recreate (see above).
ON CONFLICT DO UPDATE … second time: Caused by multiple rows targeting the same uniqueness constraint. Our upsert chooses one domain winner per (org_root, brand_key) to avoid this. Ensure you’re on the current 12a_companies_upsert.sql.
Missing company names in StepStone: The StepStone Silver view now extracts from JSON and only falls back to parsing the title suffix when safe. We intentionally avoid filling names from aggregator domains or client_name.
make run-sql says “DATABASE_URL not set”: ensure .env exists; or export $(cat .env | xargs).
DB connect errors: verify network/allowlist/SSL (try ?sslmode=require).
Docs
Business
🧭 docs/business/one-pager.md
📋 docs/business/requirements.md
Engineering (diagrams)
🧱 docs/engineering/system-architecture.md
🌙 docs/engineering/nightly-etl-sequence.md
🤖 docs/engineering/agent-search-sequence.md
🗺️ docs/engineering/conceptual-erd.md
Next steps
Expand brand rules coverage (DACH + V4) in 12c_company_brand_rules.sql.
Finalize matching endpoints in API: /v1/search, /v1/search/{session_id}/results.
Stand up the decision-maker worker for contact fetch & caching (separate service).
CI hardening and infra packaging in infra/.
