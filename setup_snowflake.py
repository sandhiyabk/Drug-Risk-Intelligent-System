import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def run_setup():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role="ACCOUNTADMIN"
    )
    
    try:
        cursor = conn.cursor()
        
        # 1. Database
        cursor.execute("CREATE DATABASE IF NOT EXISTS DRUG_INTEL_DB")
        cursor.execute("USE DATABASE DRUG_INTEL_DB")
        
        # 2. Warehouse
        cursor.execute("CREATE WAREHOUSE IF NOT EXISTS DRUG_RISK_WH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=300 AUTO_RESUME=TRUE")
        cursor.execute("USE WAREHOUSE DRUG_RISK_WH")
        
        # 3. Schemas
        for schema in ["RAW", "STAGING", "ANALYTICS"]:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"Schema {schema} verified.")
            
        # 4. Table
        cursor.execute("USE SCHEMA RAW")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FAERS_RAW_DATA (
                FILE_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
                FILE_NAME VARCHAR(255) NOT NULL,
                INGESTION_Timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                RAW_JSON VARIANT NOT NULL,
                FILE_STATUS VARCHAR(20) DEFAULT 'PENDING'
            )
        """)
        print("Table FAERS_RAW_DATA verified.")
        
        # 5. File Format
        cursor.execute("CREATE OR REPLACE FILE FORMAT FAERS_JSON_FORMAT TYPE='JSON' STRIP_OUTER_ARRAY=TRUE")
        
        # 6. Stage
        cursor.execute("CREATE OR REPLACE STAGE FAERS_INTERNAL_STAGE FILE_FORMAT=FAERS_JSON_FORMAT")
        
        print("Setup completed successfully.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_setup()
