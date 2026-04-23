import snowflake.connector, os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="DRUG_RISK_WH",
    database="DRUG_INTEL_DB",
    role="ACCOUNTADMIN",
)
cur = conn.cursor()

print("=== SCHEMAS in DRUG_INTEL_DB ===")
cur.execute("SHOW SCHEMAS IN DATABASE DRUG_INTEL_DB")
for r in cur.fetchall():
    print(" ", r[1])

print("\n=== TABLES / VIEWS in all schemas ===")
all_schemas = ["RAW", "STAGING", "ANALYTICS", "ANALYTICS_STAGING", "ANALYTICS_ANALYTICS"]
for schema in all_schemas:
    try:
        cur.execute(f"SHOW OBJECTS IN SCHEMA DRUG_INTEL_DB.{schema}")
        rows = cur.fetchall()
        print(f"--- Schema: {schema} ---")
        for r in rows:
            print(f"  {r[1]}  ({r[3]})")
    except Exception as e:
        print(f"--- Schema: {schema} (Error: {e}) ---")

conn.close()
