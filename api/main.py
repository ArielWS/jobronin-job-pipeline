import os
from fastapi import FastAPI, HTTPException
import psycopg

app = FastAPI(title="JobRonin API")
DATABASE_URL = os.getenv("DATABASE_URL")

@app.get("/health")
def health():
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL not set")
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
