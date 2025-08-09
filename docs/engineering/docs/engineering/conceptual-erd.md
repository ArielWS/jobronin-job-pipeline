```markdown
# Conceptual ERD (Relationships Only)

```mermaid
erDiagram
  COMPANY ||--o{ COMPANY_ALIAS : has
  COMPANY ||--o{ JOB_POST : offers
  JOB_POST ||--o{ JOB_SOURCE_LINK : provenance

  WORKSPACE ||--o{ MEMBERSHIP : includes
  WORKSPACE ||--o{ AGENT : owns
  AGENT ||--o{ SEARCH_SESSION : runs
  SEARCH_SESSION ||--o{ MATCH_RESULT : yields
  JOB_POST ||--o{ MATCH_RESULT : appears_in

  COMPANY ||--o{ DM_COMPANY_LINK : employs
  DECISION_MAKER ||--o{ DM_COMPANY_LINK : linked_to
  JOB_POST ||--o{ DM_JOB_LINK : relates_to
  DECISION_MAKER ||--o{ DM_JOB_LINK : linked_to

  COMPANY ||--o{ DM_SOURCE_CACHE : cached_roles
