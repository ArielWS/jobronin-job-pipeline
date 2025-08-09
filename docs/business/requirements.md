# JobRonin — Business Requirements (Logical)

## 1) Goals & non-goals
**Goals**
- Provide daily, high-quality job matches tailored to each user’s “agent”.
- Maintain a canonical, deduped job/company graph across many sources.
- Surface the most likely decision-makers and fetch contact details on demand with budget controls.
- Expose a stable, UI-agnostic API for Lovable, the Chrome extension, and future clients.

**Non-goals (MVP)**
- Full ATS syncing for candidates or CRM features.
- Complex workflow automation beyond daily agent runs.
- Heavy real-time LLM processing across the full corpus (LLMs only on shortlists).

## 2) Personas & core use cases
- **Recruiter/Operator (primary)**: create an agent profile (role, region, size caps) → receive daily matches + contacts → outreach.
- **Team Lead (secondary)**: manage seats, quotas; view aggregate results; adjust agent settings.
- **Admin (internal)**: monitor pipeline health, costs, data quality.

## 3) User journeys (logical)
**Candidate-to-Jobs**
1) Paste/upload candidate brief.
2) Agent applies filters (contract/remote/location/salary/company size).
3) Receive ranked matches; open a job to see decision-makers; fetch contacts if needed.

**Job-to-Jobs (similar roles)**
1) Paste a target job summary.
2) Agent finds similar jobs using title/tokens (and optionally vectors).
3) Same review and contact flow as above.

**Daily refresh**
- Agents rerun automatically on recent jobs; show only deltas (new matches & new contacts).

## 4) Functional requirements (logical)
**Ingestion & pipeline**
- Append raw posts nightly per source (Bronze).
- Normalize per source via Silver views.
- Merge into Gold with deterministic keys first; fallback to blocked fuzzy.
- Field-level enrichment to fill gaps without regressions; keep provenance.

**Features & searchability**
- Precompute title tokens/text index (and optional embeddings later).
- Indexes to support fast filters on date, company, location, and title similarity.

**Matching**
- Filter by contract/work type, geography, recency, salary overlap, company size caps.
- Rank by title similarity + token overlap + freshness.
- Optional LLM verify/rerank on top-K with caching.

**Decision-makers**
- Role inference rules per function/seniority/size.
- Contact enrichment via provider APIs with cache & TTL; store canonical people + links.
- Budget gates per workspace; respect daily/monthly limits.

**Agents & schedules**
- CRUD agents (filters, schedule, budget).
- Daily or manual runs; persist `SearchSession` and `MatchResult`.
- Show deltas since last run.

**API & front-end integration**
- Versioned REST `/v1`; accepts JWTs (Lovable) or API keys.
- CORS configured for Lovable domains and extension origin.
- Consistent error shapes; idempotency for writes.

**Admin & billing**
- Workspaces, memberships (roles).
- Plans and entitlements; usage metering; webhook to update entitlements.
- Minimal admin metrics: ingest counts, match volumes, LLM/contact spend, cache hit rate.

## 5) Non-functional requirements
- **Performance**: non-LLM match response P95 ≤ 500ms for top-N; ETL completes before morning in target region(s).
- **Scale**: support millions of jobs with indexes and optional monthly partitions.
- **Reliability**: ETL idempotent; retries; alert on failures; API availability targets appropriate for MVP.
- **Security**: no direct DB access from clients; authN/Z on every call; audit key actions.
- **Privacy/GDPR**: PII limited to decision-makers; deletion on request; encrypted at rest.
- **Cost controls**: per-workspace quotas; caches for summaries, fit judgements, and contacts; top-K LLM gate.

## 6) Data retention & TTLs (guidance)
- Jobs: keep indefinitely; archive when stale.
- Match results: 90 days by default (configurable).
- Decision-maker contacts: refresh TTL 30–60 days; honor deletion requests.
- Caches (LLM & contacts): TTL aligned to provider freshness and budget.

## 7) Success metrics
- Match acceptance/engagement rate on top-N.
- % of jobs deduped/enriched; coverage across sources.
- LLM/contact spend per accepted match; cache hit rate.
- Agent retention (agents that keep producing accepted matches week-over-week).

## 8) Milestones & scope (phased)
**M1 — Data foundation**  
- Two sources → Gold with dedupe + enrichment; feature indexes; basic metrics.

**M2 — Matching API**  
- `/v1/search` (Filter+Rank), `MatchResult` persisted; `/v1/jobs`, `/v1/companies`.

**M3 — Decision-makers**  
- Role inference; one contact provider; caching & quotas; `/v1/contacts/for-company`.

**M4 — Agents & schedules**  
- Agent CRUD; scheduled runs; deltas; usage reporting.

**M5 — Enhancements**  
- Optional vectors; Browser-Use worker; smarter reranking; admin quality tools.

## 9) Dependencies & assumptions
- Reliable access to job sources (scrapers or feeds).
- Contact provider with acceptable SLA/cost; ability to cache and store PII per policy.
- Lovable (or other) front-ends can authenticate via JWT and call REST endpoints.

## 10) Open questions
- Which regions and default recency window (30/45/60 days)?
- Preferred contact providers and pricing?
- Initial plan tiers and entitlements?
- Exact geo normalization rules (cities/regions mapping)?
- What “acceptance” signal we’ll treat as ground truth for training rerankers later?
