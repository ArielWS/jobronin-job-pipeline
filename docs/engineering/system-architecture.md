# System Architecture (Logical)

```mermaid
flowchart LR
  %% LAYERS
  subgraph Clients
    CE[Chrome Extension]
    LW[Lovable Web]
    Admin[Admin Console]
  end

  subgraph API["API Layer"]
    APIGW[Public API v1]
    Auth[Auth and RBAC]
    Quotas[Usage and Quotas]
    Match[Match Orchestrator]
    DMAPI[Decision Maker API]
  end

  subgraph Workers
    Queue[Worker Queue]
    LLM[LLM Verify]
    Contacts[Contact Enrich]
    Browser[Browser Use]
    Sched[Scheduler]
  end

  subgraph Data
    Bronze[Bronze Raw]
    Silver[Silver Views]
    Gold[Gold Canonical]
    Features[Features]
    Matching[Matching Tables]
    People[Decision Makers]
  end

  subgraph Ingestion
    JS[JobSpy]
    SS[StepStone]
    OS[Other Sources]
  end

  %% CLIENTS -> API
  CE -->|HTTPS requests| APIGW
  LW -->|HTTPS requests| APIGW
  Admin -->|HTTPS requests| APIGW

  %% API INTERNALS
  APIGW --> Auth
  APIGW --> Quotas
  APIGW --> Match
  APIGW --> DMAPI

  %% MATCH PATH (WHY: fast read + write results)
  Match -->|read jobs| Gold
  Match -->|use signals| Features
  Match -->|write results| Matching

  %% DECISION MAKERS (WHY: fetch on demand, cache)
  DMAPI -->|read write| People

  %% BACKGROUND WORK (WHY: keep API fast)
  APIGW -->|enqueue tasks| Queue
  Sched -->|run agents daily| APIGW
  Queue --> LLM
  Queue --> Contacts
  Queue --> Browser
  LLM -->|verify top K| Matching
  Contacts -->|upsert contacts| People
  Browser -->|org hints and fixes| Gold

  %% DATA PIPELINE (WHY: clean, dedupe, enrich)
  JS -->|raw posts nightly| Bronze
  SS -->|raw posts nightly| Bronze
  OS -->|raw posts nightly| Bronze

  Bronze -->|normalize per source| Silver
  Silver -->|dedupe and merge| Gold
  Gold -->|build search signals| Features
