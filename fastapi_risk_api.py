"""
FastAPI - Drug Risk Intelligence API
Connects to DRUG_INTEL_DB.ANALYTICS_ANALYTICS schema
"""

import os
from typing import Optional, List
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from snowflake.connector import Error as SnowflakeError
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER", "SANDHIYABK"),
    "password": os.getenv("SNOWFLAKE_PASSWORD", "9jcwpx9kGwfyAC6"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "chizcdk-zm51873"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DRUG_RISK_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "DRUG_INTEL_DB"),
    "schema": "ANALYTICS",
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

app = FastAPI(
    title="Drug Risk Intelligence API",
    description="FAERS Signal Detection API",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RiskSignal(BaseModel):
    drug_name: str
    reaction_term: str
    report_count: int
    prr: float
    ror: float
    signal_strength: str
    is_significant_signal: bool


class SignalSummary(BaseModel):
    total_pairs: int
    high_signals: int
    elevated_signals: int
    total_reports: int
    unique_drugs: int


def get_snowflake_connection():
    try:
        return snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    except SnowflakeError as e:
        raise HTTPException(status_code=503, detail=f"Connection failed: {e}")


@app.get("/")
def read_root():
    return {"service": "Drug Risk API", "version": "1.1.0", "status": "active"}


@app.get("/health")
def health_check():
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")


@app.get("/signals", response_model=List[RiskSignal])
def get_signals(
    drug_name: str = Query(None, description="Filter by drug name"),
    signal_strength: str = Query(None, description="Filter by signal strength (HIGH/ELEVATED/LOW)"),
    min_reports: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get drug risk signals with optional filters."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = "SELECT DRUG_NAME, REACTION_TERM, REPORT_COUNT, PRR, ROR, SIGNAL_STRENGTH, IS_SIGNIFICANT_SIGNAL FROM FCT_RISK_SIGNALS WHERE 1=1"
        params = {}

        if drug_name:
            query += " AND UPPER(DRUG_NAME) LIKE UPPER(%(drug_name)s)"
            params["drug_name"] = f"%{drug_name}%"

        if signal_strength:
            query += " AND UPPER(SIGNAL_STRENGTH) = UPPER(%(signal_strength)s)"
            params["signal_strength"] = signal_strength

        if min_reports > 0:
            query += " AND REPORT_COUNT >= %(min_reports)s"
            params["min_reports"] = min_reports

        query += " ORDER BY PRR DESC LIMIT %(limit)s"
        params["limit"] = limit

        cursor.execute(query, params)
        rows = cursor.fetchall()

        signals = [
            RiskSignal(
                drug_name=row[0],
                reaction_term=row[1],
                report_count=row[2],
                prr=row[3],
                ror=row[4],
                signal_strength=row[5],
                is_significant_signal=bool(row[6]),
            )
            for row in rows
        ]

        return signals

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/summary", response_model=SignalSummary)
def get_signal_summary():
    """Get overall signal summary statistics."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT 
                COUNT(*), 
                SUM(CASE WHEN SIGNAL_STRENGTH='HIGH' THEN 1 ELSE 0 END),
                SUM(CASE WHEN SIGNAL_STRENGTH='ELEVATED' THEN 1 ELSE 0 END),
                SUM(REPORT_COUNT),
                COUNT(DISTINCT DRUG_NAME)
            FROM FCT_RISK_SIGNALS
        """)
        row = cursor.fetchone()

        return SignalSummary(
            total_pairs=row[0] or 0,
            high_signals=row[1] or 0,
            elevated_signals=row[2] or 0,
            total_reports=row[3] or 0,
            unique_drugs=row[4] or 0,
        )

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)