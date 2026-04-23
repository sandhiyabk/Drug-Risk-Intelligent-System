{{
    config(
        materialized='incremental',
        unique_key='report_drug_id',
        increment_by='src_ld_ts',
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

WITH source AS (
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
    SELECT
        s.report_id,
        s.received_date,
        s.patient_sex,
        s.patient_age,
        s.src_ld_ts,
        s.src_ld_date,
        COALESCE(
            f.VALUE:drgname::VARCHAR,
            f.VALUE:medicinalproduct::VARCHAR,
            f.VALUE:medicinalproductname::VARCHAR
        ) AS drug_name,
        f.VALUE:drgcode[0].value::VARCHAR AS drug_code,
        f.VALUE:drgcode[0].codingSystem::VARCHAR AS drug_coding_system,
        f.VALUE:activesubstancename::VARCHAR AS active_substance_name,
        f.VALUE:medicinalproduct::VARCHAR AS medicinal_product_name,
        f.VALUE:drugcharacterization::NUMBER(1, 0) AS drug_characterization,
        f.VALUE:drugdosageform::VARCHAR AS drug_dosage_form,
        f.VALUE:drugdosage::VARCHAR AS drug_dosage_text,
        f.VALUE:drugadministrationroute[0].routecode::VARCHAR AS route_code,
        f.VALUE:drugauthorizationnumber::VARCHAR AS authorization_number,
        f.VALUE:drugindication[0].value::VARCHAR AS drug_indication,
        f.VALUE:drugindication[0].codingSystem::VARCHAR AS indication_coding_system,
        f.VALUE:drugcumulativedose[0].value::NUMBER(15, 2) AS cumulative_dose_value,
        f.VALUE:drugcumulativedose[0].doseunit::VARCHAR AS cumulative_dose_unit,
        f.VALUE:timeintervalunitofmeasure::VARCHAR AS dose_time_unit,
        f.INDEX AS drug_sequence,
        s.report_id || '-' || f.INDEX::VARCHAR AS report_drug_id
    FROM {{ source('raw', 'FAERS_RAW_DATA') }} r
    INNER JOIN source s
        ON COALESCE(
            r.RAW_JSON:safetyreport[0].safetyreportid::VARCHAR,
            r.RAW_JSON:safetyreport[0].id::VARCHAR
        ) = s.report_id
    CROSS JOIN LATERAL FLATTEN(
        INPUT => r.RAW_JSON:safetyreport[0].patient[0].drug,
        OUTER => TRUE
    ) f
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
    src_ld_ts,
    src_ld_date
FROM drugs_flat
WHERE (drug_name IS NOT NULL OR active_substance_name IS NOT NULL)
{% if is_incremental() %}
  AND src_ld_ts > (SELECT COALESCE(MAX(src_ld_ts), '1900-01-01') FROM {{ this }})
{% endif %}