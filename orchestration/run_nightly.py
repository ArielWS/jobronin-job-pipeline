import os, glob
import psycopg

DB = os.getenv("DATABASE_URL")
if not DB:
    raise SystemExit("DATABASE_URL not set")

files = sorted(glob.glob("transforms/sql/*.sql"))
if not files:
    raise SystemExit("No SQL files found in transforms/sql")

with psycopg.connect(DB) as conn:
    with conn.cursor() as cur:
        for f in files:
            print(">>> Running", f, flush=True)
            with open(f, "r", encoding="utf-8") as fh:
                cur.execute(fh.read())
    conn.commit()
print("Done.")
