# JobRonin — Platform One-Pager (Business Architecture)

## Vision
Turn sourcing into efficient business development: automatically find newly posted, high-fit jobs, surface the right decision-makers, and deliver outreach-ready targets every day.

## Who it serves
- **Recruiters & solo operators** needing fresh, relevant roles with contacts.
- **Small teams** that want automation without heavy setup or cost.
- **Multiple UIs** (Lovable web app, Chrome extension, future mobile) using one backend.

## Core outcomes
1) **Canonical jobs & companies** — deduped, enriched, and query-ready.
2) **Agent matching** — user “agents” run daily/adhoc searches and return ranked jobs.
3) **Decision-makers** — likely roles per company/job with cached, on-demand contacts.
4) **API-first** — one backend that cleanly supports many front-ends.

## Operating principles
- **Layered data**: Bronze (raw) → Silver (normalized views) → Gold (canonical tables).
- **Cascade matching**: Filter (rules) → Rank (cheap similarity) → *optional* LLM verify on a tiny shortlist.
- **Cost control**: cache summaries/fit judgements/contacts; only pay when needed; per-workspace quotas.
- **Multi-tenant**: workspaces, roles, entitlements enforced at the API.
- **Incremental jobs**: nightly ETL; small, idempotent updates; easy backfills.

## System overview (logical)
Ingestion (per source) → Silver views → Gold (Company, CompanyAlias, JobPost, JobSourceLink) + field-level enrichment → Features (tokens/text index/optional vectors) → Match API (Filter+Rank) → optional LLM verify for top-K → Decision-maker cache & contact enrichment → results to clients.

## Data lifecycle (nightly)
1) **Ingest** new posts (append-only).
2) **Normalize** per source via Silver views.
3) **Deduplicate & merge** into Gold:
   - Deterministic (ATS token or direct apply URL),
   - Heuristic (company + location + date window + title similarity).
4) **Enrich per field** (fill gaps; never regress; keep provenance).
5) **Features**: precompute searchable signals (title tokens/text index; optional embeddings).
6) **Ready by morning** for agents.

## Matching flow (per agent run)
- **Input**: candidate or job brief + filters (regions, size caps, salary, remote, contract).
- **Filter**: contract/work type, salary band overlap, geography, recency, company size.
- **Rank**: title similarity + token overlap + freshness; vectors later if needed.
- **(Optional) Verify**: LLM checks top-K (10–20); results cached.
- **Output**: ranked matches stored per session; reused by all UIs.

## Decision-maker flow (cost-aware)
- **Role prediction**: infer “who to contact” (e.g., Hiring Manager, Head of Function, Talent Lead).
- **Contact enrichment**: check cache (company + role); if miss and within budget, call provider; store canonical people + links; set TTL cache.
- **Daily refresh**: agents surface new jobs and newly discovered contacts.

## Multi-tenant & billing (logical)
- **Workspace + membership** define data boundaries and roles.
- **Entitlements** per plan: agents, daily matches, contact lookups, Browser-Use minutes.
- **Usage metering**: track consumption per workspace/day; block or upsell on limits.
- **Auth**: accept Lovable JWTs/API keys; authorize every request.

## KPIs
- **Freshness**: % sources ingested nightly, time-to-ready.
- **Match quality**: acceptance/engagement rate on top-N.
- **Cost efficiency**: LLM/contact cost per accepted match; cache hit rates.
- **Latency**: P95 under 500ms for non-LLM matches.
- **Growth**: agents created, daily active sessions, retained workspaces.

## Risks & mitigations
- **LLM/contact cost creep** → top-K gating, aggressive caching, quotas.
- **Duplicate jobs** → strengthen deterministic keys; periodic reclustering.
- **Source drift** → isolate parsing in Silver views; add source-specific checks/alerts.
- **Provider limits** → cache + multi-provider fallback later.

## MVP scope
1) Nightly ETL for two sources → Gold (dedupe + per-field enrichment).
2) Match API: Filter+Rank in SQL; store session results; return top matches.
3) Decision-maker cache + one contact provider; fetch on demand.
4) Workspace auth + quotas; minimal usage metrics.
5) Integrate Lovable & Chrome extension against the same API.

## Phase 2 (after traction)
- Add sources (new Silver views only).
- Optional semantic vectors for recall.
- Browser-Use worker for org inference / apply-URL fixes.
- Learned reranking from user feedback.
- Admin analytics & quality tooling.
