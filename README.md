# JobRonin — Jobs Pipeline

Turns raw job posts from multiple sources into canonical, query-ready data with a thin API/worker surface for matching.

## Scope

**Owned here**
- SQL pipeline: **Bronze → Silver → Unified → Gold** (normalize, dedupe, per-field enrichment).
- Canonical storage: `gold.company`, `gold.company_alias`, `gold.job_post`, `gold.job_source_link`, plus evidence tables.
- Minimal API (health now; search endpoints later).
- Nightly orchestration (local + GitHub Actions).

**Not owned here**
- Scrapers (JobSpy, StepStone, …) feed Bronze tables in `public.*`.
- Contact enrichment / Browser automation — sibling worker/service.
- Front-ends consume this repo’s API.

## Repository layout

.
├── api/ # Thin FastAPI app (health now; search later)
├── worker/ # Background worker stub
├── orchestration/
│ └── run_nightly.py
├── transforms/
│ └── sql/
│ ├── 00_extensions.sql
│ ├── 01_silver_jobspy.sql
│ ├── 02_silver_stepstone.sql
│ ├── 03_unified_stage.sql
│ ├── 04_util_functions.sql
│ ├── 10_gold_company.sql
│ ├── 11_gold_job_post.sql
│ ├── 12a_companies_upsert.sql
│ ├── 12a_company_evidence.sql
│ ├── 12b_company_fill_nulls.sql
│ ├── 12c_company_brand_rules.sql
│ ├── 12c_company_domain_from_evidence.sql
│ ├── 12d_company_monitoring_checks.sql
│ ├── 12e_company_promote_domain.sql # ← new domain promotion/upgrade
│ └── 12f_company_claim_domains_reassign.sql (optional)
├── scripts/
│ └── sanity.sql # quick checks (created below)
├── .github/workflows/nightly.yml
├── Makefile
├── .env.sample
└── docs/

## Environment

Create `.env` from sample:

DATABASE_URL=postgres://USER:PASS@HOST:5432/DBNAME
APP_PORT=8000

Export in shell for `make` targets if you don't use `dotenv`:

```bash
export $(cat .env | xargs)
```

Pipeline (run order)
Lexical order = run order. For companies:
00_extensions.sql
04_util_functions.sql
01_silver_jobspy.sql
02_silver_stepstone.sql
03_unified_stage.sql
10_gold_company.sql — DDL (brand_key, constraints, triggers)
12c_company_brand_rules.sql — brand inference rules (DACH + V4 focused)
12a_companies_upsert.sql — one-pass, collision-free upsert (name-first, domain later)
12a_company_evidence.sql — write website / email / apply evidence
12e_company_promote_domain.sql — upgrade website_domain from evidence (idempotent)
12c_company_domain_from_evidence.sql — backfill website_domain from email/apply evidence
12b_company_fill_nulls.sql — non-regressive fill for attrs
12d_company_monitoring_checks.sql— duplicates / coverage checks

Notes:
Silver views sanitize StepStone JSON via util.json_clean(text) to avoid NaN/Infinity/None errors.
util.company_name_norm_langless() strips trailing language markers like " - English" before normalization.

Domain promotion policy (deterministic)
Keep all evidence in gold.company_evidence_domain.
gold.company.website_domain is the best current identity:
Prefer website (non-aggregator / non-ATS / non-career).
If earlier we only had EMAIL, we auto-upgrade to WEBSITE later.
Collision-safe against (website_domain, brand_key).
Idempotent: safe to run nightly.

Make targets
make sql-companies — run the company portion (silver → gold + evidence + promotion + checks)
make sanity — quick counts/joins from scripts/sanity.sql
make trace-pipeline SOURCE=jobspy OFFSET=0 — debug a single job through the pipeline (requires DATABASE_URL)

Getting started (local)
cp .env.sample .env
pip install -r requirements.txt

# Run companies flow
make sql-companies

# Sanity
make sanity

CI
.github/workflows/nightly.yml runs SQL in lexical order. Ensure DATABASE_URL is set as a repo secret.

Troubleshooting
JSON “NaN” error: confirm 02_silver_stepstone.sql uses util.json_clean(text) (it does) and that CI runs 04_util_functions.sql first.
ON CONFLICT affects row twice: you’re inserting multiple candidates for the same (website_domain, brand_key) in the same statement. The upsert splits name inserts and domain winners to avoid this.
Duplicates by legal suffix: use util.company_name_norm_langless(n) and util.company_name_norm(n) (already wired).

Markets
Initial heuristics focus on DACH + V4 (Germany, Austria, Switzerland + Czechia, Slovakia, Poland, Hungary) via rule sets in 12c_company_brand_rules.sql and StepStone field mapping.
