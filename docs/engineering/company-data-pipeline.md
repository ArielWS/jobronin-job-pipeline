# Company Data Pipeline

This document describes how company-related information flows through the JobRonin data pipeline. It focuses on the layers that transform raw source tables into canonical "gold" records, the brand rule mechanism, alias resolution, and how domain evidence is tracked and promoted.

## Overview

1. **Raw layer** – Source-specific tables populated by scrapers.
2. **Silver layer** – Views that normalize each source into a shared schema and clean company signals.
3. **Unified silver** – Union of all silver views.
4. **Gold layer** – Deduplicated company dimension with alias and evidence tables, enriched using brand rules and domain promotion.

## Raw Tables

Scrapers append rows into source-specific tables in the `public` schema.

| Source       | Raw table                     | Notes |
|--------------|------------------------------|-------|
| JobSpy       | `public.jobspy_job_scrape`   | Text fields for company name, URLs, emails, location, etc. |
| StepStone    | `public.stepstone_job_scrape`| JSON payload in `job_data`; contains company, location, salary and contact objects. |
| Profesia.sk  | `public.profesia_sk_job_scrape` | Similar to StepStone JSON structure for the Slovak portal. |

These tables are append-only and contain source-specific quirks (embedded JSON, inconsistent names, aggregator URLs). They are not queried directly by downstream code.

## Silver Layer

Each source has a corresponding view in the `silver` schema that sanitizes and standardizes its raw rows.

### `silver.jobspy`
* Parses location text into city/region/country using `util.location_parse`.
* Extracts company website and filters out aggregator/ATS hosts.
* Pulls a representative contact email and derive `contact_email_domain` and `contact_email_root`.
* Extracts apply URL host (`apply_domain` and `apply_root`).
* Keeps raw enrichment fields such as industry, logo and description.

### `silver.stepstone`
* Cleans the JSON payload via `util.json_clean` to handle `NaN`/`Infinity`/`None` values.
* Normalizes company name and title from multiple possible JSON keys.
* Flattens arrays of job locations and emails into text fields.
* Captures enrichment fields like size, industry and logo URLs.
* Derives apply and company domains similar to JobSpy.

### `silver.profesia_sk`
* Applies the same normalization approach for the Profesia.sk feed (not shown in detail here).

### `silver.unified`
The unified view unions all per-source silver views into a consistent column set used by downstream gold transformations.

## Gold Layer

### `gold.company`
Primary company dimension table with strong uniqueness guarantees:

* One row per normalized name (`company_name_norm_uniq`).
* One row per `(website_domain, brand_key)` to allow multiple brands on the same root domain.
* Optional fields for LinkedIn slug, size, industry, description and logo.

### `gold.company_alias`
Stores alternate spellings or scraped names linked to a company. The table enforces uniqueness per `(company_id, alias_norm)`.

### `gold.company_evidence_domain`
Records evidence for domains or email roots observed in the sources. Each row captures the kind (`website`, `email`, `ats_handle`), the value, and the originating source row. The primary key is `(company_id, kind, value)` so evidence is retained even after promotion to the main record.

### Brand Rules
`gold.company_brand_rule` defines regex-based splits for organizations that host multiple brands on a single domain (e.g. `amazon.com` → `aws`). Rules are keyed by `domain_root` and `brand_key` and can be toggled via an `active` flag.

### Upsert and Alias/Evidence Population
`12a_companies_upsert.sql` ingests rows from `silver.unified` and performs the heavy lifting:

1. **Source extraction** – collects cleaned company signals per source row.
2. **Best-per-name selection** – chooses one representative row per normalized name, preferring rows with richer metadata and trustworthy domains.
3. **Brand key lookup** – matches `org_root` and name against `gold.company_brand_rule` to assign `brand_key`.
4. **Insert/Update** – inserts new companies by name, fills profile fields, and sets the winning `website_domain` for each `(org_root, brand_key)` pair.
5. **Alias & evidence** – inserts the scraped name into `gold.company_alias` and stores domain/email evidence in `gold.company_evidence_domain`.

Additional helpers:

* `12a_companies_aliases.sql` seeds missing aliases directly from `silver.unified` when a company_id can be resolved by domain or name.
* `12a_company_evidence.sql` writes website/email evidence for all resolvable rows, ensuring evidence accumulation even when the upsert script skips a row.
* `12f_company_linkedin.sql` extracts LinkedIn slugs from any silver view and updates `gold.company.linkedin_slug`.

### Domain Promotion and Backfill
Evidence records are later used to improve the canonical domain:

* `12e_company_promote_domain.sql` upgrades `gold.company.website_domain` to a trustworthy website if evidence shows a better candidate, or fills it when missing. It respects brand boundaries and avoids aggregator/ATS/career hosts.
* `12c_company_domain_from_evidence.sql` backfills `website_domain` from email roots when no website evidence exists.

## Cohort Population Example
For a cohort of newly scraped companies:

1. Scrapers append raw rows into the `public.*` tables.
2. Silver views normalize each row, extracting domains and emails while filtering out generic or aggregator hosts.
3. `silver.unified` aggregates all rows and feeds the upsert process.
4. `12a_companies_upsert.sql` selects the best row per normalized name, applies brand rules, inserts/updates `gold.company`, and records aliases and evidence.
5. `12a_company_evidence.sql` and `12e_company_promote_domain.sql` ensure domain evidence is stored and the canonical `website_domain` is upgraded when better information arrives.
6. Subsequent runs accumulate additional aliases, LinkedIn slugs and evidence, leading to a richer, deduplicated company profile.

This deterministic flow allows nightly jobs to converge on a single accurate record per real-world company while preserving the trail of evidence for auditing and future upgrades.
