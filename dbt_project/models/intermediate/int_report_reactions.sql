{{
    config(
        materialized='incremental',
        unique_key='report_reaction_id',
        cluster_by=['report_id', 'reaction_sequence'],
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

-- int_report_reactions: Explodes patient.reaction array
-- One row per adverse reaction per FAERS report

WITH source AS (
    SELECT
        report_id,
        received_date,
        src_ld_ts,
        src_ld_date

    FROM {{ ref('stg_faers_reports') }}
    {% if is_incremental() %}
    WHERE src_ld_ts > (
        SELECT COALESCE(MAX(src_ld_ts), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
),

reactions_raw AS (
    SELECT
        s.report_id,
        s.received_date,
        s.src_ld_ts,
        s.src_ld_date,

        f.VALUE:reactionmeddraversionpt::VARCHAR AS reaction_meddra_version,
        f.VALUE:primarysourcereaction::VARCHAR AS reaction_primary_source,
        f.VALUE:reactionmeddrapt::VARCHAR AS reaction_term,
        f.VALUE:reactionmeddrallt::VARCHAR AS reaction_term_allt,
        f.VALUE:reactionoutcome[0].codingSystem::VARCHAR AS outcome_coding_system,
        TRY_CAST(f.VALUE:reactionoutcome[0].value AS NUMBER(2, 0)) AS outcome_code,

        f.INDEX AS reaction_sequence,

        s.report_id || '-react-' || f.INDEX::VARCHAR AS report_reaction_id

    FROM {{ source('raw', 'FAERS_RAW_DATA') }} r
    INNER JOIN source s
        ON TRY_CAST(
            COALESCE(
                r.RAW_JSON:safetyreport[0].safetyreportid,
                r.RAW_JSON:safetyreport[0].id
            ) AS VARCHAR
        ) = s.report_id

    CROSS JOIN LATERAL FLATTEN(
        INPUT => r.RAW_JSON:safetyreport[0].patient[0].reaction,
        OUTER => TRUE
    ) f
)

SELECT
    report_reaction_id,
    report_id,
    received_date,
    reaction_meddra_version,
    reaction_primary_source,
    reaction_term,
    reaction_term_allt,
    outcome_coding_system,
    outcome_code,
    reaction_sequence,
    src_ld_ts,
    src_ld_date

FROM reactions_raw
WHERE reaction_term IS NOT NULL