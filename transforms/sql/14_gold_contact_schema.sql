-- transforms/sql/14_gold_contact_schema.sql
-- Canonical people model (tables, indexes, triggers) + rollup view

SET search_path = public;

CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS util;

-- ----------------------------
-- gold.contact
-- ----------------------------
CREATE TABLE IF NOT EXISTS gold.contact (
  contact_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  full_name text,
  name_norm text GENERATED ALWAYS AS (util.person_name_norm(full_name)) STORED,

  -- primaries
  primary_email text,
  primary_phone text,
  primary_linkedin_slug text,
  title_raw text,

  -- company linkage
  primary_company_id bigint NULL REFERENCES gold.company(company_id) ON DELETE SET NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Ensure generated helper column exists (for CI-unique upserts)
-- If the table already existed from an older version, this will add it now.
ALTER TABLE gold.contact
  ADD COLUMN IF NOT EXISTS primary_email_lower text GENERATED ALWAYS AS (lower(primary_email)) STORED;

-- Clean up any legacy index from earlier iterations
DROP INDEX IF EXISTS ux_contact_primary_email_lower;
-- Recreate as a UNIQUE CONSTRAINT (works with ON CONFLICT ON CONSTRAINT ...)
ALTER TABLE gold.contact
  DROP CONSTRAINT IF EXISTS ux_contact_primary_email_lower,
  ADD CONSTRAINT ux_contact_primary_email_lower UNIQUE (primary_email_lower);

-- Unique linkedin slug when present
CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_primary_linkedin_slug
  ON gold.contact (primary_linkedin_slug)
  WHERE primary_linkedin_slug IS NOT NULL;

-- Helpful for weak matching / QA
CREATE INDEX IF NOT EXISTS ix_contact_name_norm_company
  ON gold.contact (name_norm, primary_company_id);

CREATE INDEX IF NOT EXISTS ix_contact_primary_company
  ON gold.contact (primary_company_id);

-- Touch updated_at on UPDATE
DROP TRIGGER IF EXISTS trg_contact_touch ON gold.contact;
CREATE TRIGGER trg_contact_touch
BEFORE UPDATE ON gold.contact
FOR EACH ROW EXECUTE FUNCTION util.tg_touch_updated_at();

-- ----------------------------
-- gold.contact_alias
-- ----------------------------
CREATE TABLE IF NOT EXISTS gold.contact_alias (
  contact_id uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  alias text NOT NULL,
  alias_norm text GENERATED ALWAYS AS (util.person_name_norm(alias)) STORED,
  primary_flag boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, alias_norm)
);

-- ----------------------------
-- gold.contact_evidence
-- ----------------------------
CREATE TABLE IF NOT EXISTS gold.contact_evidence (
  contact_id uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('email','phone','name','linkedin','title','source_row','company_hint')),
  value text NOT NULL,
  source text,
  source_id text,
  detail jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, kind, value)
);

-- One person per email / linkedin globally (prevents same anchor on two people)
CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_evidence_email_global
  ON gold.contact_evidence (value)
  WHERE kind = 'email';

CREATE UNIQUE INDEX IF NOT EXISTS ux_contact_evidence_linkedin_global
  ON gold.contact_evidence (value)
  WHERE kind = 'linkedin';

-- ----------------------------
-- gold.contact_affiliation
-- ----------------------------
CREATE TABLE IF NOT EXISTS gold.contact_affiliation (
  contact_id uuid NOT NULL REFERENCES gold.contact(contact_id) ON DELETE CASCADE,
  company_id bigint NOT NULL REFERENCES gold.company(company_id) ON DELETE CASCADE,
  role text,
  seniority text,
  first_seen timestamptz,
  last_seen timestamptz,
  active boolean,
  source text,
  source_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (contact_id, company_id)
);

CREATE INDEX IF NOT EXISTS ix_contact_aff_company ON gold.contact_affiliation (company_id);
CREATE INDEX IF NOT EXISTS ix_contact_aff_active ON gold.contact_affiliation (active);

-- ----------------------------
-- Rollup view: gold.contact_plus
-- ----------------------------
CREATE OR REPLACE VIEW gold.contact_plus AS
WITH emails AS (
  SELECT
    ce.contact_id,
    array_agg(ce.value ORDER BY ce.value) FILTER (
      WHERE ce.kind = 'email'
        AND NOT coalesce((ce.detail->>'is_generic_domain')::boolean, false)
        AND NOT coalesce((ce.detail->>'is_generic_mailbox')::boolean, false)
    ) AS non_generic_emails,
    array_agg(ce.value ORDER BY ce.value) FILTER (
      WHERE ce.kind = 'email'
        AND (
          coalesce((ce.detail->>'is_generic_domain')::boolean, false)
          OR coalesce((ce.detail->>'is_generic_mailbox')::boolean, false)
        )
    ) AS generic_emails
  FROM gold.contact_evidence ce
  GROUP BY ce.contact_id
),
active_aff AS (
  SELECT
    ca.contact_id,
    ca.company_id,
    gc.website_domain,
    ROW_NUMBER() OVER (PARTITION BY ca.contact_id ORDER BY ca.last_seen DESC) AS rn
  FROM gold.contact_affiliation ca
  JOIN gold.company gc ON gc.company_id = ca.company_id
  WHERE ca.active = TRUE
),
generic_best AS (
  SELECT
    ce.contact_id,
    (
      SELECT ce2.value
      FROM gold.contact_evidence ce2
      LEFT JOIN active_aff aa
        ON aa.contact_id = ce2.contact_id
      WHERE ce2.contact_id = ce.contact_id
        AND ce2.kind = 'email'
        AND (
          coalesce((ce2.detail->>'is_generic_domain')::boolean, false)
          OR coalesce((ce2.detail->>'is_generic_mailbox')::boolean, false)
        )
      ORDER BY
        (util.org_domain(util.email_domain(ce2.value)) = aa.website_domain) DESC NULLS LAST,
        (coalesce(ce2.detail->>'from','') = 'json_contacts') DESC,
        ce2.created_at DESC
      LIMIT 1
    ) AS generic_email_best
  FROM gold.contact_evidence ce
  GROUP BY ce.contact_id
),
current_company AS (
  SELECT
    ca.contact_id,
    COALESCE(
      (SELECT company_id FROM active_aff aa WHERE aa.contact_id = ca.contact_id AND aa.rn = 1),
      (SELECT company_id
       FROM gold.contact_affiliation ca2
       WHERE ca2.contact_id = ca.contact_id
       ORDER BY ca2.last_seen DESC NULLS LAST
       LIMIT 1)
    ) AS current_company_id
  FROM gold.contact_affiliation ca
  GROUP BY ca.contact_id
)
SELECT
  c.*,
  e.non_generic_emails,
  e.generic_emails,
  g.generic_email_best,
  cc.current_company_id
FROM gold.contact c
LEFT JOIN emails e ON e.contact_id = c.contact_id
LEFT JOIN generic_best g ON g.contact_id = c.contact_id
LEFT JOIN current_company cc ON cc.contact_id = c.contact_id;
