import json
import os
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

PIPELINE_FILES = [
    "transforms/sql/00_extensions.sql",
    "transforms/sql/00_jobspy_raw.sql",
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
    "transforms/sql/12c_company_domain_from_evidence.sql",
    "transforms/sql/12f_company_linkedin.sql",
    "transforms/sql/12d_company_monitoring_checks.sql",
]

RAW_QUERIES = {
    "jobspy": (
        "SELECT id::text AS source_id, * "
        "FROM public.jobspy_job_scrape "
        "ORDER BY id DESC OFFSET %(off)s LIMIT 1"
    ),
    "profesia_sk": "SELECT id, md5(util.json_clean(job_data)->>'job_url') AS source_id, * FROM public.profesiask_job_scrape ORDER BY scraped_at DESC OFFSET %(off)s LIMIT 1",
    "stepstone": "SELECT id::text AS source_id, * FROM public.stepstone_job_scrape ORDER BY timestamp DESC OFFSET %(off)s LIMIT 1",
}

RAW_TABLES = {
    "jobspy": "public.jobspy_job_scrape",
    "profesia_sk": "public.profesiask_job_scrape",
    "stepstone": "public.stepstone_job_scrape",
}


def parse_args() -> dict:
    args = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            args[k.upper()] = v
    return args


def run_fetch(cur: psycopg.Cursor, query: str, params=None):
    cur.execute(query, params or [])
    rows = cur.fetchall()
    return {"query": query, "rows": rows}


def main() -> None:
    args = parse_args()
    source = args.get("SOURCE")
    if source not in RAW_QUERIES:
        print("Invalid or missing SOURCE", file=sys.stderr)
        sys.exit(1)
    offset = int(args.get("OFFSET", 0))

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    log = []
    ctx: dict = {"source": source}

    with psycopg.connect(db_url, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # fetch raw row and source_id
            cur.execute(RAW_QUERIES[source], {"off": offset})
            raw_row = cur.fetchone()
            if not raw_row:
                print("No raw row found", file=sys.stderr)
                sys.exit(1)
            ctx["source_id"] = raw_row["source_id"]
            log.append({"step": "raw", "table": RAW_TABLES[source], "rows": [raw_row]})

            for file in PIPELINE_FILES:
                sql = Path(file).read_text()
                cur.execute(sql)

                # determine follow-up queries
                fetches = []
                if file.endswith(f"silver_{source}.sql"):
                    fetches.append(
                        run_fetch(
                            cur,
                            f"SELECT * FROM silver.{source} WHERE source_id = %s",
                            [ctx["source_id"]],
                        )
                    )
                elif file.endswith("03_unified_stage.sql"):
                    fetches.append(
                        run_fetch(
                            cur,
                            "SELECT * FROM silver.unified WHERE source = %s AND source_id = %s",
                            [ctx["source"], ctx["source_id"]],
                        )
                    )
                elif file.endswith("12a_companies_upsert.sql"):
                    company = run_fetch(
                        cur,
                        "SELECT * FROM gold.company WHERE name = (SELECT company_name FROM silver.unified WHERE source = %s AND source_id = %s)",
                        [ctx["source"], ctx["source_id"]],
                    )
                    fetches.append(company)
                    if company["rows"]:
                        ctx["company_id"] = company["rows"][0]["company_id"]
                        fetches.append(
                            run_fetch(
                                cur,
                                "SELECT * FROM gold.company_alias WHERE company_id = %s",
                                [ctx["company_id"]],
                            )
                        )
                elif file.endswith("12a_company_evidence.sql") and ctx.get("company_id"):
                    fetches.append(
                        run_fetch(
                            cur,
                            "SELECT * FROM gold.company_evidence_domain WHERE company_id = %s",
                            [ctx["company_id"]],
                        )
                    )
                elif (
                    file.endswith("12e_company_promote_domain.sql")
                    or file.endswith("12c_company_domain_from_evidence.sql")
                    or file.endswith("12f_company_linkedin.sql")
                ) and ctx.get("company_id"):
                    fetches.append(
                        run_fetch(
                            cur,
                            "SELECT * FROM gold.company WHERE company_id = %s",
                            [ctx["company_id"]],
                        )
                    )
                elif file.endswith("12d_company_monitoring_checks.sql") and ctx.get("company_id"):
                    fetches.append(
                        run_fetch(
                            cur,
                            "SELECT * FROM gold.company_monitoring_checks WHERE company_id = %s",
                            [ctx["company_id"]],
                        )
                    )

                for f in fetches:
                    log.append({"step": file, "query": f["query"], "rows": f["rows"]})

    print(json.dumps(log, indent=2, default=str))


if __name__ == "__main__":
    main()
