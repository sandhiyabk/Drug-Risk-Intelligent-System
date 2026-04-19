"""
Streamlit Dashboard - Oncology Patient Risk
Simplified version without charts
"""

import streamlit as st
import snowflake.connector
import pandas as pd

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
    df = pd.read_sql(query, conn)
    conn.close()
    return df


st.set_page_config(page_title="Oncology Risk", page_icon="🧬", layout="wide")

st.title("🧬 Oncology Patient Risk Dashboard")

with st.sidebar:
    st.caption("Data Source: ONCOLOGY_DB.GOLD")

try:
    df_summary = get_data("""
        SELECT 
            COUNT(*) as total_patients,
            SUM(HIGH_RISK_FLAG) as high_risk_count,
            AVG(AGE) as avg_age
        FROM GOLD_PATIENT_RISK
    """)
    
    st.metric("Total Patients", int(df_summary.iloc[0]['total_patients']))
    st.metric("High Risk", int(df_summary.iloc[0]['high_risk_count']))
    st.metric("Avg Age", round(float(df_summary.iloc[0]['avg_age']), 1))
    
    st.divider()
    st.subheader("Cancer Type Distribution")
    df_cancer = get_data("""
        SELECT CANCER_TYPE, COUNT(*) as cnt 
        FROM GOLD_PATIENT_RISK 
        GROUP BY CANCER_TYPE 
        ORDER BY cnt DESC
    """)
    
    if not df_cancer.empty:
        for _, row in df_cancer.iterrows():
            st.write(f"{row['CANCER_TYPE']}: {int(row['cnt'])}")
    
    st.divider()
    st.subheader("Risk Level Distribution")
    df_risk = get_data("""
        SELECT RISK_LEVEL, COUNT(*) as cnt 
        FROM GOLD_PATIENT_RISK 
        GROUP BY RISK_LEVEL
    """)
    
    if not df_risk.empty:
        for _, row in df_risk.iterrows():
            st.write(f"{row['RISK_LEVEL']}: {int(row['cnt'])}")
    
    st.divider()
    st.subheader("Patient Records")
    df_patients = get_data("""
        SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, RISK_LEVEL, PRE_SCORE
        FROM GOLD_PATIENT_RISK 
        ORDER BY PRE_SCORE DESC 
        LIMIT 50
    """)
    st.dataframe(df_patients, use_container_width=True)
    
    st.divider()
    st.subheader("High Risk Patients")
    df_high = get_data("""
        SELECT PATIENT_ID, AGE, GENDER, CANCER_TYPE, PRE_SCORE, RISK_FACTORS_DERIVED
        FROM GOLD_PATIENT_RISK 
        WHERE HIGH_RISK_FLAG = 1
        ORDER BY PRE_SCORE DESC 
        LIMIT 20
    """)
    st.dataframe(df_high, use_container_width=True)

except Exception as e:
    st.error(f"Failed: {e}")