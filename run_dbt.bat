@echo off
set SNOWFLAKE_USER=SANDHIYABK
set SNOWFLAKE_PASSWORD=k66T4jKv_LQDHXe
set SNOWFLAKE_ACCOUNT=rwcfeut-wb78109
set SNOWFLAKE_WAREHOUSE=DRUG_RISK_WH
set SNOWFLAKE_DATABASE=DRUG_INTEL_DB
set SNOWFLAKE_SCHEMA=RAW
set SNOWFLAKE_ROLE=ACCOUNTADMIN

cd dbt_project
C:\Users\sandhiya\AppData\Local\Programs\Python\Python313\Scripts\dbt.exe deps --profiles-dir ..
C:\Users\sandhiya\AppData\Local\Programs\Python\Python313\Scripts\dbt.exe build --profiles-dir ..
