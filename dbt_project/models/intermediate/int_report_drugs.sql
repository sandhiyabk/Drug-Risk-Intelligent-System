{{
    config(
        materialized='incremental',
        unique_key='report_drug_id',
        cluster_by=['report_id', 'drug_sequence'],
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

-- int_report_drugs: Intermediate model exploding patient.drug JSON array
-- Uses Snowflake LATERAL FLATTEN to create one row per drug while preserving report_id
-- Each drug in a FAERS report is stored as a separate row for analytics

WITH staging_data AS (
    SELECT
        report_id,
        received_date,
        patient_sex,
        patient_age,
        src_ld_ts,
        src_ld_date

    FROM {{ ref('stg_faers_reports') }}
),

drugs_flat AS (
    -- Re-read raw data to access the nested drug array via LATERAL FLATTEN
    -- This creates one row for each drug in each report
    SELECT
        s.report_id,
        s.received_date,
        s.patient_sex,
        s.patient_age,
        s.src_ld_ts,
        s.src_ld_date,

        -- Drug elements from patient[0].drug array
        f.VALUE:drgname::VARCHAR AS drug_name,
        f.VALUE:drgcode[0].value::VARCHAR AS drug_code,
        f.VALUE:drgcode[0].codingSystem::VARCHAR AS drug_coding_system,
        f.VALUE:activesubstancename::VARCHAR AS active_substance_name,
        f.VALUE:medicinalproductname::VARCHAR AS medicinal_product_name,

        -- Drug characterization: 1=Suspect, 2=Concomitant, 3=Interaction
        TRY_CAST(f.VALUE:drugcharacterization::NUMBER(1, 0)) AS drug_characterization,

        -- Drug dosage information
        f.VALUE:drugdosageform::VARCHAR AS drug_dosage_form,
        f.VALUE:drugdosage::VARCHAR AS drug_dosage_text,
        f.VALUE:drugadministrationroute[0].routecode::VARCHAR AS route_code,
        f.VALUE:drugauthorizationnumber::VARCHAR AS authorization_number,

        -- Indication (what the drug was prescribed for)
        f.VALUE:drugindication[0].value::VARCHAR AS drug_indication,
        f.VALUE:drugindication[0].codingSystem::VARCHAR AS indication_coding_system,

        -- Cumulative dose information
        f.VALUE:drugcumulativedose[0].value::NUMBER(15, 2) AS cumulative_dose_value,
        f.VALUE:drugcumulativedose[0].doseunit::VARCHAR AS cumulative_dose_unit,
        f.VALUE:timeintervalunitofmeasure::VARCHAR AS dose_time_unit,

        f.INDEX AS drug_sequence,
        f.COUNT AS total_drugs_in_report,

        -- Create composite key for unique identification
        s.report_id || '-' || f.INDEX::VARCHAR AS report_drug_id

    FROM {{ source('raw', 'FAERS_RAW_DATA') }} r
    INNER JOIN staging_data s
        ON TRY_CAST(
            COALESCE(
                r.RAW_JSON:safetyreport[0].safetyreportid,
                r.RAW_JSON:safetyreport[0].id
            ) AS VARCHAR
        ) = s.report_id

    CROSS JOIN LATERAL FLATTEN(
        INPUT => r.RAW_JSON:safetyreport[0].patient[0].drug,
        OUTER => TRUE
    ) f
),

final AS (
    SELECT
        report_drug_id,
        report_id,
        received_date,
        patient_sex,
        patient_age,
        drug_name,
        drug_code,
        drug_coding_system,
        active_substance_name,
        medicinal_product_name,
        drug_characterization,
        drug_dosage_form,
        drug_dosage_text,
        route_code,
        authorization_number,
        drug_indication,
        indication_coding_system,
        cumulative_dose_value,
        cumulative_dose_unit,
        dose_time_unit,
        drug_sequence,
        total_drugs_in_report,
        src_ld_ts,
        src_ld_date

    FROM drugs_flat
    WHERE drug_name IS NOT NULL
        OR active_substance_name IS NOT NULL
)

SELECT
    report_drug_id,
    report_id,
    received_date,
    patient_sex,
    patient_age,
    drug_name,
    drug_code,
    drug_coding_system,
    active_substance_name,
    medicinal_product_name,
    drug_characterization,
    drug_dosage_form,
    drug_dosage_text,
    route_code,
    authorization_number,
    drug_indication,
    indication_coding_system,
    cumulative_dose_value,
    cumulative_dose_unit,
    dose_time_unit,
    drug_sequence,
    total_drugs_in_report,
    src_ld_ts,
    src_ld_date

{% if is_incremental() %}
-- Incremental logic: only process new/updated reports
WHERE src_ld_ts > (
    SELECT COALESCE(MAX(src_ld_ts), '1900-01-01')
    FROM {{ this }}
)
{% endif %}