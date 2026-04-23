{{
    config(
        materialized='incremental',
        unique_key='report_id',
        increment_by='src_ld_ts',
        cluster_by=['received_date'],
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

WITH source AS (
    SELECT
        COALESCE(
            RAW_JSON:safetyreport[0].safetyreportid::VARCHAR,
            RAW_JSON:safetyreport[0].id::VARCHAR
        ) AS report_id,
        COALESCE(
            TRY_TO_DATE(RAW_JSON:safetyreport[0].receivedate::VARCHAR, 'YYYYMMDD'),
            TRY_TO_DATE(RAW_JSON:safetyreport[0].receivedate::VARCHAR, 'YYYY-MM-DD')
        ) AS received_date,
        TRY_TO_DATE(RAW_JSON:safetyreport[0].receivedate.receptiondate::VARCHAR, 'YYYYMMDD') AS reception_date,
        RAW_JSON:safetyreport[0].safetyreportversion::NUMBER(3, 0) AS safety_report_version,
        TRY_TO_DATE(RAW_JSON:safetyreport[0].transmissiondate::VARCHAR, 'YYYYMMDD') AS transmission_date,
        RAW_JSON:safetyreport[0].patient[0].age[0].value::NUMBER(5, 2) AS patient_age,
        RAW_JSON:safetyreport[0].patient[0].age[0].agedays::NUMBER(6, 0) AS patient_age_days,
        RAW_JSON:safetyreport[0].patient[0].age[0].ageyears::NUMBER(4, 0) AS patient_age_years,
        RAW_JSON:safetyreport[0].patient[0].sex::NUMBER(1, 0) AS patient_sex,
        RAW_JSON:safetyreport[0].patient[0].onsetdate[0]::DATE AS reaction_onset_date,
        RAW_JSON:safetyreport[0].patient[0].weight[0].weight::NUMBER(6, 2) AS patient_weight_kg,
        RAW_JSON:safetyreport[0].patient[0].height[0].height::NUMBER(5, 1) AS patient_height_cm,
        RAW_JSON:safetyreport[0].primarysource[0].reportercountry::VARCHAR AS source_country,
        INGESTION_TIMESTAMP AS src_ld_ts,
        INGESTION_TIMESTAMP::DATE AS src_ld_date,
        FILE_NAME AS source_file
    FROM {{ source('raw', 'FAERS_RAW_DATA') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY report_id
            ORDER BY src_ld_ts DESC
        ) AS _dup_rank
    FROM source
)

SELECT
    report_id,
    received_date,
    reception_date,
    safety_report_version,
    transmission_date,
    patient_age,
    patient_age_days,
    patient_age_years,
    patient_sex,
    reaction_onset_date,
    patient_weight_kg,
    patient_height_cm,
    source_country,
    src_ld_ts,
    src_ld_date,
    source_file
FROM deduplicated
WHERE _dup_rank = 1
  AND report_id IS NOT NULL

{% if is_incremental() %}
  AND src_ld_ts > (SELECT COALESCE(MAX(src_ld_ts), '1900-01-01') FROM {{ this }})
{% endif %}