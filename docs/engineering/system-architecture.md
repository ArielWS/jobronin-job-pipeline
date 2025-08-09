# System Architecture (Logical)

```mermaid
flowchart LR
  subgraph Clients
    CE[Chrome Extension]
    LW[Lovable Web App]
    Admin[Internal Admin]
  end

  subgraph API
    AuthRBAC[Auth and RBAC (JWT + API Keys)]
    Match[Match Orchestrator (Filter + Rank SQL)]
    Quotas[Usage and Quotas]
    DMAPI[Decision-Maker Endpoints]
  end

  subgraph Workers
    LLM[LLM Verify or Rerank (top-K only, cached)]
    Contacts[Contact Enrichment (provider APIs, cached)]
    Browser[Browser-Use Automations (optional)]
    Sched[Scheduler (agent runs)]
  end

  subgraph Data
    Bronze[Bronze Raw]
    Silver[Silver Views per source]
    Gold[Gold Canonical: Company, CompanyAlias, JobPost, JobSourceLink]
    Features[Features: text index, tokens, optional vectors]
    Matching[Matching: Agent, SearchSession, MatchResult]
    People[Decision-Makers: DecisionMaker, DMCompanyLink, DMJobLink, DMSourceCache]
  end

  subgraph Ingestion
    JS[JobSpy]
    SS[StepStone]
    Others[Future sources]
  end

  CE -->|HTTPS| API
  LW -->|HTTPS| API
  Admin -->|HTTPS| API

  API --> AuthRBAC
  API --> Quotas
  API --> Match
  API --> DMAPI

  Match --> Gold
  Match --> Features
  API --> Matching

  DMAPI --> People
  API -->|enqueue| Workers
  Workers --> People
  Workers --> Matching
  Workers --> Gold

  JS --> Bronze
  SS --> Bronze
  Others --> Bronze

  Bronze --> Silver --> Gold --> Features

  Sched --> API
