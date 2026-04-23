import snowflake.connector, os, json, random
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def generate_sample_data():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="DRUG_RISK_WH",
        database="DRUG_INTEL_DB",
        role="ACCOUNTADMIN",
    )
    cur = conn.cursor()

    print("Cleaning old mock data...")
    cur.execute("DELETE FROM RAW.FAERS_RAW_DATA WHERE FILE_NAME LIKE 'mock_%'")

    print("Generating more realistic sample data...")
    
    reports = []
    
    def add_report(drug, reaction, count):
        for i in range(count):
            reports.append({
                "id": f"REP-{drug}-{reaction}-{len(reports)}",
                "receivedate": (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y%m%d"),
                "patient": [{
                    "age": [{"value": random.randint(20, 80)}],
                    "sex": random.randint(1, 2),
                    "drug": [{"medicinalproduct": drug}],
                    "reaction": [{"reactionmeddrapt": reaction}]
                }]
            })

    # High signal pair: DrugA + ReactionX (Significant disproportionality)
    add_report("DrugA", "ReactionX", 60)
    add_report("DrugA", "Nausea", 20)
    add_report("DrugB", "ReactionX", 5)
    add_report("DrugB", "Nausea", 100) # DrugB is baseline for Nausea
    
    # Background noise (Many other drugs and reactions)
    other_drugs = ["Aspirin", "Ibuprofen", "Paracetamol", "Stat-10", "Z-Pack"]
    other_reactions = ["Headache", "Dizziness", "Rash", "Fatigue", "Insomnia"]
    
    for _ in range(300):
        add_report(random.choice(other_drugs), random.choice(other_reactions), 1)

    # Insert into Snowflake
    insert_sql = "INSERT INTO RAW.FAERS_RAW_DATA (FILE_NAME, RAW_JSON, FILE_STATUS) SELECT %s, PARSE_JSON(%s), 'PROCESSED'"
    
    for r in reports:
        json_data = json.dumps({"safetyreport": [r]})
        cur.execute(insert_sql, (f"mock_{r['id']}.json", json_data))

    print(f"Inserted {len(reports)} reports successfully.")
    conn.close()

if __name__ == "__main__":
    generate_sample_data()
