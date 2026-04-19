{{
    config(
        materialized='incremental',
        unique_key='report_id',
        increment_by='src_ld_ts',
        cluster_by=['received_date'],
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

-- stg_faers_reports: Staging model extracting top-level fields from FAERS_RAW_DATA
-- Uses Snowflake colon notation for VARIANT access
-- Implements incremental logic based on src_ld_ts to reduce warehouse costs

WITH source AS (
    SELECT
        -- Extract unique report identifier from the FAERS JSON structure
        -- FAERS uses 'safetyreport' as top-level, then various ID fields
        -- Mapping: safetyreport.safetyreportid or safetyreport.id depending on FAERS version
        TRY_CAST(
            COALESCE(
                RAW_JSON:safetyreport[0].safetyreportid,
                RAW_JSON:safetyreport[0].id
            ) AS VARCHAR
        ) AS report_id,

        -- Extract received date from primary source
        TRY_CAST(
            RAW_JSON:safetyreport[0].receivedate AS DATE
        ) AS received_date,

        -- Extract reception date for additional context
        TRY_CAST(
            RAW_JSON:safetyreport[0].receivedate.receptiondate AS DATE
        ) AS reception_date,

        -- Extract primary safety report version
        TRY_CAST(
            RAW_JSON:safetyreport[0].safetyreportversion AS NUMBER(3, 0)
        ) AS safety_report_version,

        -- Extract transmission date
        TRY_CAST(
            RAW_JSON:safetyreport[0].transmissiondate AS DATE
        ) AS transmission_date,

        -- Patient Information Section
        -- Extract patient age - FAERS uses patient.age[0].{value,agedays,ageyears}
        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].age[0].value AS NUMBER(5, 2)
        ) AS patient_age,

        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].age[0].agedays AS NUMBER(6, 0)
        ) AS patient_age_days,

        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].age[0].ageyears AS NUMBER(4, 0)
        ) AS patient_age_years,

        -- Extract patient sex: 0=Male, 1=Female, 2=Unknown
        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].sex AS NUMBER(1, 0)
        ) AS patient_sex,

        -- Extractonset date - when adverse reaction started
        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].onsetdate[0] AS DATE
        ) AS reaction_onset_date,

        -- Extract weight in kg
        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].weight[0].weight AS NUMBER(6, 2)
        ) AS patient_weight_kg,

        -- Extract height in cm
        TRY_CAST(
            RAW_JSON:safetyreport[0].patient[0].height[0].height AS NUMBER(5, 1)
        ) AS patient_height_cm,

        -- Source Information (required for deduplication)
        RAW_JSON:safetyreport[0].primarysource[0].reportercountry AS source_country,

        -- Load metadata
        INGESTION_TIMESTAMP AS src_ld_ts,
        INGESTION_TIMESTAMP::DATE AS src_ld_date,
        FILE_NAME AS source_file

    FROM {{ source('raw', 'FAERS_RAW_DATA') }}
),

deduplicated AS (
    -- Deduplicate based on report_id, keeping the most recent load
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
        source_file,

        ROW_NUMBER() OVER (
            PARTITION BY report_id
            ORDER BY src_ld_ts DESC
        ) AS _dup_rank

    FROM source
    WHERE report_id IS NOT NULL
),

final AS (
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

{% if is_incremental() %}
-- Incremental logic: only process records with src_ld_ts greater than max in target
WHERE src_ld_ts > (
    SELECT COALESCE(MAX(src_ld_ts), '1900-01-01')
    FROM {{ this }}
)
{% endif %}