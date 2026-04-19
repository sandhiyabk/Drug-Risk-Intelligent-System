"""
FastAPI Consumption Layer - Drug Risk Intelligence
Connects to Snowflake ANALYTICS schema and provides risk signal endpoints
"""

import os
from typing import Optional
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException
import snowflake.connector
from snowflake.connector import pandas
from fastapi.middleware.cors import CORSMiddleware

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "ONCOLOGY_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "GOLD"),
    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
}

app = FastAPI(
    title="Drug Risk Intelligence API",
    description="FAERS Drug-Reaction Risk Signal API",
    version="1.0.0",
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
    ror: Optional[float]
    signal_strength: str
    is_significant_signal: bool


class DrugRiskResponse(BaseModel):
    drug_name: str
    signal_count: int
    signals: list[RiskSignal]


def get_snowflake_connection():
    """Create Snowflake database connection."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


@app.get("/")
def read_root():
    return {"service": "Drug Risk Intelligence API", "version": "1.0.0"}


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {str(e)}")


@app.get("/check-risk", response_model=DrugRiskResponse)
def check_drug_risk(
    drug_name: str = Query(..., description="Drug name to search for"),
    min_threshold: int = Query(50, description="Minimum report count threshold"),
    min_ror: float = Query(2.0, description="Minimum ROR threshold"),
):
    """
    Get significant risk signals for a specific drug.
    
    Returns all reactions where ROR > min_threshold for the given drug.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = """
        SELECT 
            drug_name,
            reaction_term,
            report_count,
            ror,
            signal_strength,
            is_significant_signal
        FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
        WHERE 
            UPPER(drug_name) = UPPER(%(drug_name)s)
            AND is_significant_signal = TRUE
            AND report_count >= %(min_threshold)s
            AND ror >= %(min_ror)s
        ORDER BY ror DESC, report_count DESC
        """
        
        cursor.execute(query, {
            "drug_name": drug_name,
            "min_threshold": min_threshold,
            "min_ror": min_ror,
        })
        
        rows = cursor.fetchall()
        
        if not rows:
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
                signal_strength=row[4],
                is_significant_signal=bool(row[5]),
            )
            for row in rows
        ]
        
        return DrugRiskResponse(
            drug_name=drug_name,
            signal_count=len(signals),
            signals=signals,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@app.get("/drugs/search")
def search_drugs(
    query: str = Query("", description="Partial drug name to search"),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Search for drugs in the risk signals table.
    Useful for autocomplete functionality.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        if query:
            sql = """
            SELECT DISTINCT drug_name
            FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
            WHERE UPPER(drug_name) LIKE UPPER(%(query)s)
            ORDER BY drug_name
            LIMIT %(limit)s
            """
            cursor.execute(sql, {"query": f"%{query}%", "limit": limit})
        else:
            sql = """
            SELECT DISTINCT drug_name
            FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
            ORDER BY drug_name
            LIMIT %(limit)s
            """
            cursor.execute(sql, {"limit": limit})
        
        rows = cursor.fetchall()
        drugs = [row[0] for row in rows]
        
        return {"drugs": drugs, "count": len(drugs)}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@app.get("/top-reactions")
def get_top_reactions(
    limit: int = Query(15, ge=1, le=100),
):
    """
    Get the top N most frequent adverse reactions.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        sql = """
        SELECT 
            reaction_term,
            SUM(report_count) AS total_reports
        FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
        GROUP BY reaction_term
        ORDER BY total_reports DESC
        LIMIT %(limit)s
        """
        cursor.execute(sql, {"limit": limit})
        rows = cursor.fetchall()
        
        return [
            {"reaction": row[0], "total_reports": row[1]}
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)