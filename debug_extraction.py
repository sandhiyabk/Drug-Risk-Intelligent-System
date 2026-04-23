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

print("--- Testing extraction from RAW.FAERS_RAW_DATA ---")
sql = """
SELECT
    COALESCE(
        RAW_JSON:safetyreport[0].safetyreportid::VARCHAR,
        RAW_JSON:safetyreport[0].id::VARCHAR
    ) AS report_id,
    RAW_JSON:safetyreport[0].receivedate::VARCHAR as received_date_str,
    RAW_JSON:safetyreport[0].patient[0].drug[0].medicinalproduct::VARCHAR as drug,
    RAW_JSON:safetyreport[0].patient[0].reaction[0].reactionmeddrapt::VARCHAR as reaction
FROM RAW.FAERS_RAW_DATA
LIMIT 5
"""
cur.execute(sql)
rows = cur.fetchall()
if not rows:
    print("No rows found in RAW.FAERS_RAW_DATA")
else:
    for row in rows:
        print(f"Report ID: {row[0]}, Date: {row[1]}, Drug: {row[2]}, Reaction: {row[3]}")

conn.close()
