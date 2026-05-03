"""
Streamlit Dashboard - Drug Risk Intelligence System (DRIS)
FAERS Drug-Reaction Risk Signals
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import altair as alt

SNOWFLAKE_CONFIG = {
    "user": "SANDHIYABK",
    "password": "9jcwpx9kGwfyAC6",
    "account": "chizcdk-zm51873",
    "warehouse": "COMPUTE_WH",
    "database": "DRUG_INTEL_DB",
    "schema": "ANALYTICS",
}


@st.cache_data(ttl=60)
def run_query(query):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
    finally:
        conn.close()
    return df


st.set_page_config(page_title="Drug Risk Intelligence", page_icon="💊", layout="wide")

st.title("💊 Drug Risk Intelligence Dashboard")
st.caption("FAERS Adverse Event Signal Detection")

with st.sidebar:
    st.header("Filters")
    
    # Risk Level Filter
    risk_level = st.selectbox(
        "Risk Level",
        ["All", "HIGH", "MEDIUM", "LOW"],
        help="Filter by risk level"
    )
    
    # Min reports
    min_reports = st.slider("Minimum Reports", 0, 100, 0)
    
    st.divider()
    st.caption("Source: DRUG_INTEL_DB.ANALYTICS")

try:
    # Get data with filters
    query = f"SELECT * FROM FCT_DRUG_SUMMARY WHERE TOTAL_REPORTS >= {min_reports}"
    df_drugs = run_query(query)
    
    # Apply risk level filter
    if risk_level != "All":
        df_drugs = df_drugs[df_drugs['RISK_LEVEL'] == risk_level]
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Drugs", len(df_drugs))
    col2.metric("Total Reports", int(df_drugs['TOTAL_REPORTS'].sum()))
    col3.metric("Avg Age", round(float(df_drugs['AVG_AGE'].mean()), 1))
    col4.metric(f"{risk_level} Risk", len(df_drugs[df_drugs['RISK_LEVEL'] == risk_level]) if risk_level != "All" else len(df_drugs))
    
    st.divider()
    
    # Color mapping for risk levels
    risk_colors = {"HIGH": "#FF4444", "MEDIUM": "#FFAA44", "LOW": "#44AA44"}
    
    # Drug chart
    st.subheader("Top Drugs by Report Count")
    if not df_drugs.empty:
        chart = (
            alt.Chart(df_drugs.head(10))
            .mark_bar()
            .encode(
                x=alt.X("TOTAL_REPORTS", title="Number of Reports"),
                y=alt.Y("DRUG_NAME", sort="-x", title=None),
                color=alt.Color("RISK_LEVEL", type="nominal", scale=alt.Scale(domain=["HIGH", "MEDIUM", "LOW"], range=["#FF4444", "#FFAA44", "#44AA44"])),
                tooltip=["DRUG_NAME", "TOTAL_REPORTS", "AVG_AGE", "RISK_LEVEL"],
            )
            .properties(height=300)
        )
        st.altair_chart(chart, use_container_width=True)
    
    st.divider()
    
    # Drug-Reaction signals
    st.subheader("Drug-Reaction Signals")
    df_signals = run_query("SELECT * FROM FCT_DRUG_REACTIONS ORDER BY REPORT_COUNT DESC LIMIT 50")
    
    if not df_signals.empty:
        # Filter by selected risk level drugs
        if risk_level != "All":
            high_drugs = df_drugs[df_drugs['RISK_LEVEL'] == risk_level]['DRUG_NAME'].tolist()
            df_signals = df_signals[df_signals['DRUG_NAME'].isin(high_drugs)]
        
        if not df_signals.empty:
            chart2 = (
                alt.Chart(df_signals.head(15))
                .mark_bar(color="#44AAFF")
                .encode(
                    x=alt.X("REPORT_COUNT", title="Reports"),
                    y=alt.Y("DRUG_NAME", sort="-x", title=None),
                    tooltip=["DRUG_NAME", "REACTION_TERM", "REPORT_COUNT"],
                )
                .properties(height=300, title="Drug-Reaction Pairs")
            )
            st.altair_chart(chart2, use_container_width=True)
    
    st.divider()
    
    # Data table with risk level coloring
    st.subheader("Drug Summary Table")
    
    # Apply filter to display
    if risk_level != "All":
        display_df = df_drugs[df_drugs['RISK_LEVEL'] == risk_level]
    else:
        display_df = df_drugs
    
    st.dataframe(
        display_df.style.format({
            'TOTAL_REPORTS': '{:,.0f}',
            'AVG_AGE': '{:.1f}'
        }),
        use_container_width=True
    )

except Exception as e:
    st.error(f"Error: {e}")