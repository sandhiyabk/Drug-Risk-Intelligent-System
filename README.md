# Drug Risk Intelligent System (DRIS) 💊🔍

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.0+-orange.svg)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Cloud-blue)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)](https://streamlit.io/)

An end-to-end intelligent system designed to monitor and analyze drug safety signals using FDA FAERS data, Snowflake, dbt, and AI-driven insights.

## 🌟 Overview

The **Drug Risk Intelligent System (DRIS)** is a state-of-the-art pharmaceutical safety surveillance platform. It automates the ingestion of FDA Adverse Event Reporting System (FAERS) data, performs sophisticated signal detection (PRR/ROR) via dbt, and provides a real-time risk dashboard for clinical safety teams.

## 🚀 Key Features

- **Automated Data Ingestion**: Robust Python-based pipeline for fetching and staging FDA FAERS JSON data.
- **Modern Data Stack**: Built on **Snowflake** for high-performance analytics and **dbt** for modular SQL transformations.
- **Risk Signal Analytics**: Calculates disproportionality metrics (Proportional Reporting Ratio) to identify potential safety signals.
- **Intelligent API**: FastAPI backend providing high-risk drug-reaction pair insights.
- **Premium Dashboard**: Interactive Streamlit interface with advanced visualizations (Risk Matrix, Signal Heatmaps).

## 🛠️ Tech Stack

- **Cloud Data Warehouse**: Snowflake
- **Data Transformation**: dbt Core
- **Data Ingestion**: Python (Pandas, Requests)
- **Backend API**: FastAPI / Uvicorn
- **Frontend**: Streamlit
- **AI/ML Logic**: Statistical Signal Detection (Disproportionality Analysis)

## 📁 Project Structure

```text
├── dbt_project/               # dbt models for signal detection
│   ├── models/
│   │   ├── staging/           # Raw data cleaning
│   │   ├── intermediate/      # Relationship mapping
│   │   └── analytics/         # Signal calculation (fct_risk_signals)
├── faers_ingestion.py         # Python script for FDA data fetch
├── fastapi_risk_api.py        # Backend API for risk data access
├── streamlit_risk_dashboard.py # Interactive safety dashboard
├── phase1_snowflake_setup.sql # Snowflake infrastructure setup
└── profiles.yml               # dbt connection configuration
```

## ⚙️ Setup Instructions

1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/sandhiyabk/Drug-Risk-Intelligent-System.git
    cd Drug-Risk-Intelligent-System
    ```

2.  **Environment Variables**:
    Configure the following environment variables for Snowflake connectivity:
    - `SNOWFLAKE_ACCOUNT`
    - `SNOWFLAKE_USER`
    - `SNOWFLAKE_PASSWORD`

3.  **Database Setup**:
    Execute the SQL scripts in `phase1_snowflake_setup.sql` within your Snowflake environment.

4.  **Run Ingestion**:
    ```bash
    python faers_ingestion.py
    ```

5.  **Transform Data (dbt)**:
    ```bash
    cd dbt_project
    dbt build
    ```

6.  **Launch Dashboard**:
    ```bash
    streamlit run streamlit_risk_dashboard.py
    ```

## 📊 Analytics Methodology

DRIS utilizes **Disproportionality Analysis** to identify signals. The primary metric used is the **Proportional Reporting Ratio (PRR)**:
- **PRR > 2**: Indicates a potential safety signal.
- **Chi-Square > 4**: Confirms statistical significance.

---
*Developed with focus on Clinical Safety Excellence.*
