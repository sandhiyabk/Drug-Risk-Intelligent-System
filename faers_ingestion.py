"""
FAERS JSON Ingestion: FastAPI + Snowflake Connector
Phase 1: Upload local JSON file to Snowflake Stage and load to Raw Table
"""

import os
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import snowflake.connector
from snowflake.connector import (
    dict_to_connection as dict2conn,
    Error as SnowflakeError,
)
from pydantic import BaseModel
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks

# Configuration from environment
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DRUG_RISK_WH"),
    "database": "DRUG_INTEL_DB",
    "schema": "RAW",
    "role": "DATA_ENG_ROLE",
}

# FAERS JSON file storage directory
FAERS_UPLOAD_DIR = Path(os.getenv("FAERS_UPLOAD_DIR", "./faers_data"))

app = FastAPI(title="Drug Risk Intelligence Pipeline - FAERS Ingestion")


class IngestionResponse(BaseModel):
    file_name: str
    status: str
    file_id: Optional[int] = None
    message: str


def get_snowflake_connection():
    """Create Snowflake connection with error handling."""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Snowflake connection failed: {e}")


def upload_to_stage(conn, file_path: Path, file_name: str) -> bool:
    """
    Upload a local JSON file to Snowflake internal stage.
    Uses PUT command for internal stage upload.
    """
    stage_name = "FAERS_INTERNAL_STAGE"
    table_name = "FAERS_RAW_DATA"

    try:
        cursor = conn.cursor()

        # Step 1: Upload file to internal stage
        put_sql = f"""
        PUT 'file://{file_path.as_posix()}' @%{stage_name}/{file_name}
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE
        """
        cursor.execute(put_sql)
        put_result = cursor.fetchone()
        cursor.close()

        if put_result and put_result[3] == "UPLOADED":
            return True
        return False

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Stage upload failed: {e}")


def load_to_raw_table(
    conn,
    file_name: str,
    source_stage: str = "FAERS_INTERNAL_STAGE",
    table_name: str = "FAERS_RAW_DATA",
) -> int:
    """
    Load JSON data from stage into raw table.
    Returns the file_id assigned by Snowflake.
    """
    cursor = conn.cursor()

    try:
        # Step 2: Copy into raw table using COPY INTO
        copy_sql = f"""
        COPY INTO {table_name} (
            FILE_NAME,
            INGESTION_TIMESTAMP,
            RAW_JSON,
            FILE_STATUS
        )
        FROM (
            SELECT
                '{file_name}' AS FILE_NAME,
                CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP,
                $1 AS RAW_JSON,
                'PROCESSED' AS FILE_STATUS
            FROM @%{source_stage}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = 'FAERS_JSON_FORMAT')
        ON_ERROR = 'SKIP_FILE'
        """
        cursor.execute(copy_sql)
        copy_result = cursor.fetchone()
        cursor.close()

        # Get the file_id from the table
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT FILE_ID FROM {table_name} WHERE FILE_NAME = '{file_name}' ORDER BY FILE_ID DESC LIMIT 1"
        )
        row = cursor.fetchone()
        cursor.close()

        if row:
            return row[0]
        return 0

    except SnowflakeError as e:
        raise HTTPException(status_code=500, detail=f"Table load failed: {e}")


async def process_file(file_path: Path, file_name: str) -> IngestionResponse:
    """
    Process a single FAERS JSON file: upload to stage, then load to table.
    """
    conn = get_snowflake_connection()

    try:
        # Validate JSON file
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)  # Validate JSON is parseable

        # Upload to stage
        upload_success = upload_to_stage(conn, file_path, file_name)
        if not upload_success:
            return IngestionResponse(
                file_name=file_name,
                status="FAILED",
                message="Failed to upload to stage",
            )

        # Load to raw table
        file_id = load_to_raw_table(conn, file_name)

        return IngestionResponse(
            file_name=file_name,
            status="SUCCESS",
            file_id=file_id,
            message="File ingested successfully into FAERS_RAW_DATA",
        )

    finally:
        conn.close()


def cleanup_staged_file(conn, file_name: str, stage_name: str = "FAERS_INTERNAL_STAGE"):
    """Remove file from stage after successful load."""
    cursor = conn.cursor()
    try:
        remove_sql = f"REMOVE @{stage_name}/{file_name}"
        cursor.execute(remove_sql)
    finally:
        cursor.close()


@app.get("/")
def read_root():
    return {"service": "FAERS Ingestion API", "version": "1.0.0"}


@app.post("/ingest", response_model=IngestionResponse)
async def ingest_faers_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """
    Upload and ingest a FAERS JSON file.
    - Validates JSON structure
    - Uploads to Snowflake internal stage
    - Loads into FAERS_RAW_DATA table
    """
    # Validate file extension
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only JSON files are accepted")

    # Create uploads directory if not exists
    FAERS_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Save uploaded file
    file_path = FAERS_UPLOAD_DIR / file.filename

    try:
        content = await file.read()
        # Validate JSON before saving
        json.loads(content)

        # Save to local directory
        with open(file_path, "wb") as f:
            f.write(content)

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save failed: {e}")

    # Process file
    result = await process_file(file_path, file.filename)
    result.message

    # Cleanup local file after processing
    try:
        file_path.unlink()
    except OSError:
        pass  # Best effort cleanup

    return result


@app.post("/ingest/batch", response_model=list[IngestionResponse])
async def ingest_batch(
    background_tasks: BackgroundTasks,
    files: list[UploadFile] = File(...),
):
    """
    Batch ingest multiple FAERS JSON files.
    """
    results = []

    for file in files:
        # Check extension
        if not file.filename.endswith(".json"):
            results.append(
                IngestionResponse(
                    file_name=file.filename,
                    status="FAILED",
                    message="Only JSON files are accepted",
                )
            )
            continue

        # Save file
        file_path = FAERS_UPLOAD_DIR / file.filename

        try:
            content = await file.read()
            json.loads(content)  # Validate

            with open(file_path, "wb") as f:
                f.write(content)

            # Process
            result = await process_file(file_path, file.filename)
            results.append(result)

            # Cleanup
            try:
                file_path.unlink()
            except OSError:
                pass

        except Exception as e:
            results.append(
                IngestionResponse(
                    file_name=file.filename,
                    status="FAILED",
                    message=str(e),
                )
            )

    return results


@app.get("/health")
def health_check():
    """Health check endpoint."""
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "snowflake": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "snowflake": "disconnected", "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)