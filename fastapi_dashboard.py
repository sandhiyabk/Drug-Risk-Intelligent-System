"""
FastAPI Dashboard - HTML UI for Oncology Patient Risk
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import snowflake.connector
import pandas as pd

app = FastAPI(title="Oncology Risk Dashboard")

SNOWFLAKE_CONFIG = {
    "user": "SANDHIYABK",
    "password": "k66T4jKv_LQDHXe",
    "account": "rwcfeut-wb78109",
    "warehouse": "COMPUTE_WH",
    "database": "ONCOLOGY_DB",
    "schema": "GOLD",
}


def get_data(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description] if cur.description else []
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=columns)


@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        df = get_data("SELECT COUNT(*) as total, SUM(HIGH_RISK_FLAG) as high_risk, AVG(AGE) as avg_age FROM GOLD_PATIENT_RISK")
        total = int(df.iat[0, 0])
        high_risk = int(df.iat[0, 1])
        avg_age = round(float(df.iat[0, 2]), 1)
        
        df_c = get_data("SELECT CANCER_TYPE, COUNT(*) FROM GOLD_PATIENT_RISK GROUP BY CANCER_TYPE ORDER BY 2 DESC")
        df_r = get_data("SELECT RISK_LEVEL, COUNT(*) FROM GOLD_PATIENT_RISK GROUP BY RISK_LEVEL")
        df_p = get_data("SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, RISK_LEVEL, PRE_SCORE FROM GOLD_PATIENT_RISK ORDER BY PRE_SCORE DESC LIMIT 50")
        df_h = get_data("SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, PRE_SCORE, RISK_FACTORS_DERIVED FROM GOLD_PATIENT_RISK WHERE HIGH_RISK_FLAG = 1 ORDER BY PRE_SCORE DESC LIMIT 20")
        
        cancer = "".join([f"<tr><td>{r[0]}</td><td>{int(r[1])}</td></tr>" for r in df_c.values.tolist()])
        risk = "".join([f"<tr><td>{r[0]}</td><td>{int(r[1])}</td></tr>" for r in df_r.values.tolist()])
        
        patients = "".join([f"""<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td><td>{r[4]}</td><td>{r[5]}</td></tr>""" for r in df_p.values.tolist()])
        high = "".join([f"""<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td><td>{r[3]}</td><td>{r[4]}</td><td>{r[5]}</td></tr>""" for r in df_h.values.tolist()])
        
        html = f"""<!DOCTYPE html><html><head><title>Oncology Risk</title>
<style>body{{font-family:Arial;margin:20px;background:#f5f5f5}}h1{{color:#2c3e50}}
.metric{{display:inline-block;background:white;padding:20px;margin:10px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1)}}
.metric h3{{margin:0;color:#7f8c8d}}metric div{{font-size:32px;font-weight:bold;color:#2c3e50}}
table{{border-collapse:collapse;width:100%;background:white;margin:10px 0}}th,td{{border:1px solid #ddd;padding:12px;text-align:left}}
th{{background:#3498db;color:white}}.section{{background:white;padding:20px;margin:10px 0;border-radius:8px}}</style>
</head><body><h1>🧬 Oncology Patient Risk Dashboard</h1>
<div class="metric"><h3>Total Patients</h3><div>{total}</div></div>
<div class="metric"><h3>High Risk</h3><div style="color:#e74c3c">{high_risk}</div></div>
<div class="metric"><h3>Average Age</h3><div>{avg_age}</div></div>
<div class="section"><h2>Cancer Type Distribution</h2><table><tr><th>Cancer Type</th><th>Count</th></tr>{cancer}</table></div>
<div class="section"><h2>Risk Level Distribution</h2><table><tr><th>Risk Level</th><th>Count</th></tr>{risk}</table></div>
<div class="section"><h2>Patient Records (Top 50)</h2><table><tr><th>Patient ID</th><th>Age</th><th>Gender</th><th>Cancer Type</th><th>Risk Level</th><th>Score</th></tr>{patients}</table></div>
<div class="section"><h2>High Risk Patients (Top 20)</h2><table><tr><th>Patient ID</th><th>Age</th><th>Gender</th><th>Cancer Type</th><th>Score</th><th>Risk Factors</th></tr>{high}</table></div>
</body></html>"""
        return html
    except Exception as e:
        import traceback
        return f"<html><body><h1>Error: {e}</h1><pre>{traceback.format_exc()}</pre></body></html>"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)