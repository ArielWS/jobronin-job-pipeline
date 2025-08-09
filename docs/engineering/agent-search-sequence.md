```markdown
# Agent Search & Enrichment (Sequence)

```mermaid
sequenceDiagram
  autonumber
  participant Client as Client (Lovable/Extension)
  participant API as Public API /v1
  participant DB as Postgres
  participant Q as Worker Queue
  participant W as Workers (LLM/Contacts)
  participant Provider as Contact Provider API

  Client->>API: POST /v1/search {payload, filters}
  API->>DB: Insert SearchSession
  API->>DB: Call match function (Filter + Rank on Gold)
  DB-->>API: Top-N written to MatchResult
  API-->>Client: 200 {session_id, results}

  par Optional verify and contacts
    API->>Q: Enqueue verify/contact for top-K
    Q->>W: Dispatch task
    W->>DB: Cache job/candidate summaries
    W->>DB: Store fit verdicts (candidate, job)
    W->>DB: Role inference and DM cache lookup
    W->>Provider: Fetch contacts if cache miss and budget ok
    Provider-->>W: Contacts
    W->>DB: Upsert DecisionMaker and links; update cache
  end

  Client->>API: GET /v1/search/{session}/results
  API->>DB: Read MatchResult (+ DMs if available)
  DB-->>API: Rows
  API-->>Client: Results (paged) and DM preview
```
