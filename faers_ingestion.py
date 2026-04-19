"""
FAERS JSON Ingestion: FastAPI + Snowflake Connector
Refactored for PEP 8 compliance, advanced error handling, and structured logging.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import snowflake.connector
from snowflake.connector import Error as SnowflakeError
from pydantic import BaseModel
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks

# Logging Configuration
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ingestion.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("faers_ingestion")

# Configuration from environment
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "DRUG_RISK_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "DRUG_INTEL_DB"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    "role": os.getenv("SNOWFLAKE_ROLE", "DATA_ENG_ROLE"),
}

# FAERS JSON file storage directory
FAERS_UPLOAD_DIR = Path(os.getenv("FAERS_UPLOAD_DIR", "./faers_data"))

app = FastAPI(
    title="Drug Risk Intelligence Pipeline - FAERS Ingestion",
    version="1.1.0"
)


class IngestionResponse(BaseModel):
    """Model for ingestion response."""
    file_name: str
    status: str
    file_id: Optional[int] = None
    message: str


def get_snowflake_connection():
    """Create Snowflake connection with specific error handling."""
    try:
        logger.info("Attempting to connect to Snowflake...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info("Snowflake connection established successfully.")
        return conn
    except SnowflakeError as e:
        logger.error(f"Snowflake connection failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Database connection error: {e}"
        )


def upload_to_stage(conn, file_path: Path, file_name: str) -> bool:
    """
    Upload a local JSON file to Snowflake internal stage.
    """
    stage_name = "FAERS_INTERNAL_STAGE"
    try:
        cursor = conn.cursor()
        logger.info(f"Uploading {file_name} to @%{stage_name}...")

        # Snowflake PUT command
        put_sql = f"PUT 'file://{file_path.as_posix()}' @%{stage_name}/{file_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        put_result = cursor.fetchone()
        cursor.close()

        if put_result and put_result[3] == "UPLOADED":
            logger.info(f"Successfully uploaded {file_name} to stage.")
            return True
        
        logger.warning(f"Upload result for {file_name}: {put_result}")
        return False

    except SnowflakeError as e:
        logger.error(f"Failed to upload {file_name} to stage: {e}")
        raise HTTPException(status_code=500, detail=f"Stage upload failed: {e}")


def load_to_raw_table(
    conn,
    file_name: str,
    source_stage: str = "FAERS_INTERNAL_STAGE",
    table_name: str = "FAERS_RAW_DATA",
) -> int:
    """
    Load JSON data from stage into raw table using COPY INTO.
    """
    try:
        cursor = conn.cursor()
        logger.info(f"Loading {file_name} from stage into {table_name}...")

        copy_sql = f"""
        COPY INTO {table_name} (FILE_NAME, INGESTION_TIMESTAMP, RAW_JSON, FILE_STATUS)
        FROM (
            SELECT '{file_name}', CURRENT_TIMESTAMP(), $1, 'PROCESSED'
            FROM @%{source_stage}/{file_name}
        )
        FILE_FORMAT = (FORMAT_NAME = 'FAERS_JSON_FORMAT')
        ON_ERROR = 'SKIP_FILE'
        """
        cursor.execute(copy_sql)
        cursor.close()

        # Retrieve generated FILE_ID
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT FILE_ID FROM {table_name} WHERE FILE_NAME = %s ORDER BY FILE_ID DESC LIMIT 1",
            (file_name,)
        )
        row = cursor.fetchone()
        cursor.close()

        file_id = row[0] if row else 0
        logger.info(f"Load complete. Assigned File ID: {file_id}")
        return file_id

    except SnowflakeError as e:
        logger.error(f"Failed to load {file_name} into table: {e}")
        raise HTTPException(status_code=500, detail=f"Table load failed: {e}")


async def process_file(file_path: Path, file_name: str) -> IngestionResponse:
    """ Orchestrates the upload and load process for a single file. """
    conn = get_snowflake_connection()
    try:
        # Validate JSON
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)

        if not upload_to_stage(conn, file_path, file_name):
            return IngestionResponse(
                file_name=file_name,
                status="FAILED",
                message="File upload to internal stage failed."
            )

        file_id = load_to_raw_table(conn, file_name)
        return IngestionResponse(
            file_name=file_name,
            status="SUCCESS",
            file_id=file_id,
            message=f"Ingested into Snowflake with ID {file_id}"
        )
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON content in {file_name}: {e}")
        return IngestionResponse(
            file_name=file_name,
            status="FAILED",
            message=f"Invalid JSON structure: {e}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error processing {file_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/")
def read_root():
    """Service metadata endpoint."""
    return {"service": "FAERS Ingestion API", "version": "1.1.0", "status": "active"}


@app.post("/ingest", response_model=IngestionResponse)
async def ingest_faers_file(file: UploadFile = File(...)):
    """ Endpoint to upload and ingest a single FAERS JSON file. """
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are supported.")

    FAERS_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_path = FAERS_UPLOAD_DIR / file.filename

    try:
        content = await file.read()
        json.loads(content)  # Validation

        with open(file_path, "wb") as f:
            f.write(content)
        
        logger.info(f"Received file: {file.filename}")
        result = await process_file(file_path, file.filename)
        return result

    except Exception as e:
        logger.error(f"Ingestion endpoint failed for {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if file_path.exists():
            file_path.unlink()


@app.post("/ingest/batch", response_model=List[IngestionResponse])
async def ingest_batch(files: List[UploadFile] = File(...)):
    """ Batch ingestion endpoint for multiple files. """
    results = []
    for file in files:
        try:
            res = await ingest_faers_file(file)
            results.append(res)
        except HTTPException as e:
            results.append(IngestionResponse(
                file_name=file.filename,
                status="ERROR",
                message=e.detail
            ))
    return results


@app.get("/health")
def health_check():
    """ Detailed health check including Snowflake connectivity. """
    try:
        conn = get_snowflake_connection()
        conn.close()
        return {"status": "healthy", "snowflake": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)