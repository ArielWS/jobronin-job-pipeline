import os
import psycopg

FILES = [
    "transforms/sql/04_util_functions.sql",
    "transforms/sql/01_silver_jobspy.sql",
    "transforms/sql/02_silver_profesia_sk.sql",
    "transforms/sql/02_silver_stepstone.sql",
    "transforms/sql/03_unified_stage.sql",
    "transforms/sql/10_gold_company.sql",
    "transforms/sql/12c_company_brand_rules.sql",
    "transforms/sql/12a_companies_upsert.sql",
    "transforms/sql/12a_company_evidence.sql",
    "transforms/sql/12e_company_promote_domain.sql",
    "transforms/sql/12b_company_fill_nulls.sql",
    "transforms/sql/12c_company_domain_from_evidence.sql",
]

DB = os.getenv("DATABASE_URL")
if not DB:
    raise SystemExit("DATABASE_URL not set")

with psycopg.connect(DB) as conn:
    with conn.cursor() as cur:
        for f in FILES:
            print(">>> Running", f, flush=True)
            with open(f, "r", encoding="utf-8") as fh:
                cur.execute(fh.read())
    conn.commit()
print("Done.")
