"""
FastAPI Dashboard - Drug Risk Intelligence
HTML-based dashboard - no Streamlit
"""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import snowflake.connector
import pandas as pd

app = FastAPI(title="Drug Risk Dashboard")

SNOWFLAKE_CONFIG = {
    "user": "SANDHIYABK",
    "password": "9jcwpx9kGwfyAC6",
    "account": "chizcdk-zm51873",
    "warehouse": "COMPUTE_WH",
    "database": "DRUG_INTEL_DB",
    "schema": "ANALYTICS",
}


def get_data(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols), cols


@app.get("/", response_class=HTMLResponse)
def dashboard(risk: str = "All", min_rep: int = 0):
    try:
        # Get drugs with filter
        df, _ = get_data(f"SELECT * FROM FCT_DRUG_SUMMARY WHERE TOTAL_REPORTS >= {min_rep}")
        
        if risk != "All":
            df = df[df['RISK_LEVEL'] == risk]
        
        total_drugs = len(df)
        total_reports = int(df['TOTAL_REPORTS'].sum()) if not df.empty else 0
        avg_age = round(df['AVG_AGE'].mean(), 1) if not df.empty else 0
        risk_count = len(df[df['RISK_LEVEL'] == risk]) if risk != "All" else total_drugs
        
        # Get signals
        df_signals, _ = get_data("SELECT * FROM FCT_DRUG_REACTIONS ORDER BY REPORT_COUNT DESC LIMIT 30")
        
        risk_colors = {"HIGH": "#FF4444", "MEDIUM": "#FFAA44", "LOW": "#44AA44"}
        
        # Build table rows
        rows_html = ""
        for _, r in df.iterrows():
            color = risk_colors.get(r['RISK_LEVEL'], "#888")
            rows_html += f"""<tr style="background:{color}20">
                <td>{r['DRUG_NAME']}</td>
                <td>{int(r['TOTAL_REPORTS'])}</td>
                <td>{round(r['AVG_AGE'],1)}</td>
                <td style="font-weight:bold;color:{color}">{r['RISK_LEVEL']}</td>
            </tr>"""
        
        # Build select options
        select_opts = f"""<option {'selected' if risk=='All' else ''}>All</option>
            <option {'selected' if risk=='HIGH' else ''}>HIGH</option>
            <option {'selected' if risk=='MEDIUM' else ''}>MEDIUM</option>
            <option {'selected' if risk=='LOW' else ''}>LOW</option>"""
        
        return f"""<!DOCTYPE html>
<html>
<head>
    <title>Drug Risk Dashboard</title>
    <style>
        body{{font-family:Arial;margin:20px;background:#f5f5f5}}
        h1{{color:#2c3e50}}
        .metric{{display:inline-block;background:white;padding:20px;margin:10px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);min-width:120px}}
        .metric h3{{margin:0;color:#7f8c8d;font-size:14px}}
        .metric div{{font-size:28px;font-weight:bold}}
        .filter{{background:white;padding:15px;margin:10px 0;border-radius:8px}}
        select,input{{padding:8px;font-size:14px;margin:5px}}
        button{{padding:8px 20px;background:#3498db;color:white;border:none;border-radius:4px;cursor:pointer}}
        button:hover{{background:#2980b9}}
        table{{border-collapse:collapse;width:100%;background:white;margin:10px 0}}
        th,td{{border:1px solid #ddd;padding:12px;text-align:left}}
        th{{background:#3498db;color:white}}
        tr:nth-child(even){{background:#f9f9f9}}
        .chart{{background:white;padding:20px;margin:10px 0;border-radius:8px}}
    </style>
</head>
<body>
    <h1>💊 Drug Risk Intelligence Dashboard</h1>
    
    <form class="filter">
        <label>Risk Level: 
            <select name="risk">
                {select_opts}
            </select>
        </label>
        <label>Min Reports: 
            <input type="number" name="min_rep" value="{min_rep}" min="0" max="100">
        </label>
        <button type="submit">Apply</button>
    </form>
    
    <div class="metric">
        <h3>Total Drugs</h3><div>{total_drugs}</div>
    </div>
    <div class="metric">
        <h3>Total Reports</h3><div>{total_reports}</div>
    </div>
    <div class="metric">
        <h3>Avg Age</h3><div>{avg_age}</div>
    </div>
    <div class="metric">
        <h3>{risk} Risk</h3><div>{risk_count}</div>
    </div>
    
    <div class="chart">
        <h2>Drug Risk Summary</h2>
        <table>
            <tr><th>Drug Name</th><th>Reports</th><th>Avg Age</th><th>Risk Level</th></tr>
            {rows_html}
        </table>
    </div>
    
    <div class="chart">
        <h2>Top Drug-Reaction Signals</h2>
        <table>
            <tr><th>Drug</th><th>Reaction</th><th>Reports</th></tr>
            {''.join(f"<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td></tr>" for r in df_signals.head(15).values)}
        </table>
    </div>
</body>
</html>"""
    except Exception as e:
        return f"<h1>Error: {e}</h1>"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)