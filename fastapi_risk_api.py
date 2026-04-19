"""
FastAPI - Oncology Patient Risk Intelligence
Connects to ONCOLOGY_DB.GOLD schema
"""

import os
from typing import Optional, List
from pydantic import BaseModel
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
from snowflake.connector import Error as SnowflakeError

SNOWFLAKE_CONFIG = {
    "user": "SANDHIYABK",
    "password": "k66T4jKv_LQDHXe",
    "account": "rwcfeut-wb78109",
    "warehouse": "COMPUTE_WH",
    "database": "ONCOLOGY_DB",
    "schema": "GOLD",
    "role": "ACCOUNTADMIN",
}

app = FastAPI(
    title="Oncology Risk Intelligence API",
    description="Patient Risk Assessment API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class PatientRisk(BaseModel):
    patient_id: str
    age: Optional[int]
    gender: Optional[str]
    cancer_type: Optional[str]
    risk_level: Optional[str]
    high_risk_flag: int
    pre_score: Optional[int]


class PatientListResponse(BaseModel):
    patients: List[PatientRisk]
    count: int


class RiskSummary(BaseModel):
    total_patients: int
    high_risk_count: int
    avg_age: float
    cancer_types: dict


def get_snowflake_connection():
    try:
        return snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    except SnowflakeError as e:
        raise HTTPException(status_code=503, detail=f"Connection failed: {e}")


@app.get("/")
def read_root():
    return {"service": "Oncology Risk API", "version": "1.0.0"}


@app.get("/health")
def health_check():
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {e}")


@app.get("/patients", response_model=PatientListResponse)
def get_patients(
    risk_level: str = Query(None, description="Filter by risk level (LOW/MEDIUM/HIGH)"),
    cancer_type: str = Query(None, description="Filter by cancer type"),
    min_age: int = Query(None, ge=0, le=120, description="Minimum age"),
    max_age: int = Query(None, ge=0, le=120, description="Maximum age"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get patients with optional filters."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = "SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, RISK_LEVEL, HIGH_RISK_FLAG, PRE_SCORE FROM GOLD_PATIENT_RISK WHERE 1=1"
        params = {}

        if risk_level:
            query += " AND UPPER(RISK_LEVEL) = UPPER(%(risk_level)s)"
            params["risk_level"] = risk_level

        if cancer_type:
            query += " AND UPPER(CANCER_TYPE) = UPPER(%(cancer_type)s)"
            params["cancer_type"] = cancer_type

        if min_age is not None:
            query += " AND AGE >= %(min_age)s"
            params["min_age"] = min_age

        if max_age is not None:
            query += " AND AGE <= %(max_age)s"
            params["max_age"] = max_age

        query += " ORDER BY PRE_SCORE DESC LIMIT %(limit)s"
        params["limit"] = limit

        cursor.execute(query, params)
        rows = cursor.fetchall()

        patients = [
            PatientRisk(
                patient_id=row[0],
                age=row[1],
                gender=row[2],
                cancer_type=row[3],
                risk_level=row[4],
                high_risk_flag=row[5],
                pre_score=row[6],
            )
            for row in rows
        ]

        return PatientListResponse(patients=patients, count=len(patients))

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/patients/{patient_id}", response_model=PatientRisk)
def get_patient(patient_id: str):
    """Get a specific patient by ID."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, RISK_LEVEL, HIGH_RISK_FLAG, PRE_SCORE FROM GOLD_PATIENT_RISK WHERE PATIENT_ID = %(patient_id)s",
            {"patient_id": patient_id},
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")

        return PatientRisk(
            patient_id=row[0],
            age=row[1],
            gender=row[2],
            cancer_type=row[3],
            risk_level=row[4],
            high_risk_flag=row[5],
            pre_score=row[6],
        )

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/summary", response_model=RiskSummary)
def get_risk_summary():
    """Get overall risk summary statistics."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*), SUM(HIGH_RISK_FLAG), AVG(AGE) FROM GOLD_PATIENT_RISK")
        row = cursor.fetchone()

        cursor.execute("SELECT CANCER_TYPE, COUNT(*) FROM GOLD_PATIENT_RISK GROUP BY CANCER_TYPE")
        cancer_counts = {r[0]: r[1] for r in cursor.fetchall() if r[0]}

        return RiskSummary(
            total_patients=row[0],
            high_risk_count=row[1] or 0,
            avg_age=round(row[2] or 0, 1),
            cancer_types=cancer_counts,
        )

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


@app.get("/high-risk")
def get_high_risk_patients(limit: int = Query(50, ge=1, le=500)):
    """Get high risk patients."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, PRE_SCORE, RISK_FACTORS_DERIVED FROM GOLD_PATIENT_RISK WHERE HIGH_RISK_FLAG = 1 ORDER BY PRE_SCORE DESC LIMIT %(limit)s",
            {"limit": limit},
        )
        rows = cursor.fetchall()

        return [
            {
                "patient_id": r[0],
                "age": r[1],
                "gender": r[2],
                "cancer_type": r[3],
                "pre_score": r[4],
                "risk_factors": r[5],
            }
            for r in rows
        ]

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)