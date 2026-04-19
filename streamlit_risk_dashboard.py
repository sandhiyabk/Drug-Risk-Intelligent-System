"""
Streamlit Dashboard - Drug Risk Intelligence
Displays top 15 adverse reactions and drug search/profile
Uses snowflake.connector.pandas for efficient DataFrame loading
"""

import os
import sys
import streamlit as st
import snowflake.connector
from snowflake.connector import pandas as pd
import altair as alt
import pandas as pd

SNOWFLAKE_CONFIG = {
    "user": "SANDHIYABK",
    "password": "k66T4jKv_LQDHXe",
    "account": "rwcfeut-wb78109",
    "warehouse": "COMPUTE_WH",
    "database": "ONCOLOGY_DB",
    "schema": "GOLD",
    "role": "ACCOUNTADMIN",
}


@st.cache_data(ttl=3600)
def get_connection():
    """Create cached Snowflake connection."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


@st.cache_data(ttl=3600)
def fetch_top_reactions(limit: int = 15):
    """Fetch top N most frequent adverse reactions."""
    conn = get_connection()
    query = """
    SELECT 
        reaction_term,
        SUM(report_count) AS total_reports
    FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
    GROUP BY reaction_term
    ORDER BY total_reports DESC
    LIMIT %s
    """
    df = pd.read_sql(query, conn, params=(limit,))
    conn.close()
    return df


@st.cache_data(ttl=3600)
def fetch_drug_list():
    """Fetch list of all unique drugs."""
    conn = get_connection()
    query = """
    SELECT DISTINCT drug_name
    FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
    ORDER BY drug_name
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df["drug_name"].tolist()


@st.cache_data(ttl=3600)
def fetch_drug_risk_profile(drug_name: str, min_count: int = 50, min_ror: float = 1.0):
    """Fetch risk profile for a specific drug."""
    conn = get_connection()
    query = """
    SELECT 
        drug_name,
        reaction_term,
        report_count,
        ror,
        signal_strength,
        is_significant_signal
    FROM DRUG_INTEL_DB.ANALYTICS.fct_risk_signals
    WHERE 
        UPPER(drug_name) = UPPER(%s)
        AND report_count >= %s
        AND ror >= %s
    ORDER BY ror DESC, report_count DESC
    """
    df = pd.read_sql(query, conn, params=(drug_name, min_count, min_ror))
    conn.close()
    return df


def main():
    st.set_page_config(
        page_title="Drug Risk Intelligence Dashboard",
        page_icon="💊",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    
    st.title("💊 Drug Risk Intelligence Dashboard")
    st.markdown("### FAERS Adverse Event Signal Detection")
    
    with st.sidebar:
        st.header("🔍 Drug Search")
        
        drug_list = fetch_drug_list()
        selected_drug = st.selectbox(
            "Select a drug:",
            [""] + drug_list,
            index=0,
            help="Choose a drug to view its risk profile",
        )
        
        st.divider()
        
        st.subheader("⚙️ Filters")
        min_threshold = st.slider(
            "Minimum Report Count",
            min_value=10,
            max_value=500,
            value=50,
            step=10,
            help="Filter signals by minimum report count",
        )
        
        min_ror = st.slider(
            "Minimum ROR",
            min_value=1.0,
            max_value=10.0,
            value=1.0,
            step=0.1,
            help="Filter signals by minimum Reporting Odds Ratio",
        )
        
        st.divider()
        st.caption(
            "Data Source: FAERS Drug-Reaction Risk Signals\n\n"
            "Significance: ROR > 2.0 AND report_count >= 50"
        )
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📊 Top 15 Most Frequent Adverse Reactions")
        
        try:
            top_reactions = fetch_top_reactions(15)
            
            if top_reactions.empty:
                st.warning("No reaction data available.")
            else:
                chart = (
                    alt.Chart(top_reactions)
                    .mark_bar(color="#FF6B6B")
                    .encode(
                        x=alt.X("total_reports", title="Total Reports"),
                        y=alt.Y(
                            "reaction_term",
                            sort="-x",
                            title="Adverse Reaction",
                        ),
                        tooltip=[
                            "reaction_term",
                            "total_reports",
                        ],
                    )
                    .properties(height=400)
                    .configure_axis(labelFontSize=11, titleFontSize=12)
                )
                st.altair_chart(chart, use_container_width=True)
                
        except Exception as e:
            st.error(f"Failed to load reaction data: {e}")
    
    with col2:
        st.subheader("📈 Statistics")
        
        try:
            total_signals = fetch_top_reactions(1)
            st.metric(
                "Tracked Drug-Reaction Pairs",
                f"{len(fetch_drug_list())} drugs",
            )
        except:
            st.metric("Tracked Drug-Reaction Pairs", "N/A")
    
    st.divider()
    
    if selected_drug:
        st.subheader(f"⚠️ Risk Profile: {selected_drug}")
        
        try:
            df = fetch_drug_risk_profile(
                selected_drug,
                min_threshold,
                min_ror,
            )
            
            if df.empty:
                st.warning(
                    f"No significant signals found for {selected_drug} "
                    f"with current thresholds."
                )
            else:
                sig_count = df["is_significant_signal"].sum()
                
                col_m1, col_m2, col_m3 = st.columns(3)
                with col_m1:
                    st.metric("Total Signals", len(df))
                with col_m2:
                    st.metric("Significant", sig_count)
                with col_m3:
                    if sig_count > 0:
                        max_ror = df[df["is_significant_signal"]]["ror"].max()
                        st.metric("Max ROR", f"{max_ror:.2f}")
                
                st.dataframe(
                    df.style.format(
                        {
                            "report_count": "{:,.0f}",
                            "ror": "{:.2f}",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
                
                if sig_count > 0:
                    sig_df = df[df["is_significant_signal"]].head(10)
                    
                    chart = (
                        alt.Chart(sig_df)
                        .mark_bar()
                        .encode(
                            x=alt.X("ror", title="Reporting Odds Ratio (ROR)"),
                            y=alt.Y(
                                "reaction_term",
                                sort="-x",
                                title="Adverse Reaction",
                            ),
                            color=alt.Color(
                                "signal_strength",
                                scale=alt.Scale(
                                    domain=["HIGH", "ELEVATED"],
                                    range=["#FF4444", "#FFAA44"],
                                ),
                            ),
                            tooltip=[
                                "reaction_term",
                                "report_count",
                                "ror",
                                "signal_strength",
                            ],
                        )
                        .properties(height=300)
                    )
                    st.altair_chart(chart, use_container_width=True)
        
        except Exception as e:
            st.error(f"Failed to load drug profile: {e}")
    
    else:
        st.info(
            "👈 Select a drug from the sidebar to view its risk profile. "
            "Use the filters to adjust thresholds."
        )


if __name__ == "__main__":
    main()