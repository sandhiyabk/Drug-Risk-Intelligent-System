"""
FastAPI Consumption Layer - Drug Risk Intelligence
Connects to Snowflake ANALYTICS schema and provides risk signal endpoints.
Refactored for PEP 8 compliance, error handling, and logging.
"""

import os
import logging
from typing import Optional, List
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from snowflake.connector import Error as SnowflakeError

# Logging Configuration
LOG_DIR = os.path.join(os.getcwd(), "logs")
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "api.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fastapi_risk_api")

# Configuration from environment
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "DRUG_INTEL_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

app = FastAPI(
    title="Drug Risk Intelligence API",
    description="FAERS Drug-Reaction Risk Signal API",
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
    """Model for a single risk signal."""
    drug_name: str
    reaction_term: str
    report_count: int
    ror: Optional[float]
    prr: Optional[float]
    signal_strength: str
    is_significant_signal: bool


class DrugRiskResponse(BaseModel):
    """Model for drug risk search response."""
    drug_name: str
    signal_count: int
    signals: List[RiskSignal]


def get_snowflake_connection():
    """Create Snowflake database connection with error handling."""
    try:
        return snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    except SnowflakeError as e:
        logger.error(f"Snowflake connection error: {e}")
        raise HTTPException(
            status_code=503,
            detail="Database connection failed. Please try again later."
        )


@app.get("/")
def read_root():
    """Root endpoint."""
    return {"service": "Drug Risk Intelligence API", "version": "1.1.0"}


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


@app.get("/check-risk", response_model=DrugRiskResponse)
def check_drug_risk(
    drug_name: str = Query(..., description="Drug name to search for"),
    min_threshold: int = Query(50, description="Minimum report count threshold"),
    min_prr: float = Query(2.0, description="Minimum PRR threshold"),
):
    """
    Get significant risk signals for a specific drug.
    """
    logger.info(f"Checking risk for drug: {drug_name} (min_prr: {min_prr})")
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT 
            drug_name, reaction_term, report_count, ror, prr,
            signal_strength, is_significant_signal
        FROM fct_risk_signals
        WHERE UPPER(drug_name) = UPPER(%(drug_name)s)
            AND is_significant_signal = TRUE
            AND report_count >= %(min_threshold)s
            AND prr >= %(min_prr)s
        ORDER BY prr DESC, report_count DESC
        """
        
        cursor.execute(query, {
            "drug_name": drug_name,
            "min_threshold": min_threshold,
            "min_prr": min_prr,
        })
        
        rows = cursor.fetchall()
        
        if not rows:
            logger.info(f"No significant signals for {drug_name}")
            raise HTTPException(
                status_code=404,
                detail=f"No significant signals found for drug: {drug_name}"
            )
        
        signals = [
            RiskSignal(
                drug_name=row[0],
                reaction_term=row[1],
                report_count=row[2],
                ror=row[3],
                prr=row[4],
                signal_strength=row[5],
                is_significant_signal=bool(row[6]),
            )
            for row in rows
        ]
        
        return DrugRiskResponse(
            drug_name=drug_name,
            signal_count=len(signals),
            signals=signals,
        )
        
    except SnowflakeError as e:
        logger.error(f"Query execution error: {e}")
        raise HTTPException(status_code=500, detail="Database query failed.")
    finally:
        cursor.close()
        conn.close()


@app.get("/drugs/search")
def search_drugs(
    query: str = Query("", description="Partial drug name to search"),
    limit: int = Query(20, ge=1, le=100),
):
    """ Search for drugs in the risk signals table. """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        if query:
            sql = """
            SELECT DISTINCT drug_name FROM fct_risk_signals
            WHERE UPPER(drug_name) LIKE UPPER(%(query)s)
            ORDER BY drug_name LIMIT %(limit)s
            """
            cursor.execute(sql, {"query": f"%{query}%", "limit": limit})
        else:
            sql = "SELECT DISTINCT drug_name FROM fct_risk_signals ORDER BY drug_name LIMIT %(limit)s"
            cursor.execute(sql, {"limit": limit})
        
        drugs = [row[0] for row in cursor.fetchall()]
        return {"drugs": drugs, "count": len(drugs)}
        
    except SnowflakeError as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail="Database search error.")
    finally:
        cursor.close()
        conn.close()


@app.get("/top-reactions")
def get_top_reactions(limit: int = Query(15, ge=1, le=100)):
    """ Get the top N most frequent adverse reactions. """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        sql = """
        SELECT reaction_term, SUM(report_count) AS total_reports
        FROM fct_risk_signals
        GROUP BY reaction_term
        ORDER BY total_reports DESC LIMIT %(limit)s
        """
        cursor.execute(sql, {"limit": limit})
        return [{"reaction": row[0], "total_reports": row[1]} for row in cursor.fetchall()]
        
    except SnowflakeError as e:
        logger.error(f"Top reactions query failed: {e}")
        raise HTTPException(status_code=500, detail="Database query error.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)