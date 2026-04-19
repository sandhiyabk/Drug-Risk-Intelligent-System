{{
    config(
        materialized='incremental',
        unique_key='drug_reaction_pair_id',
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

-- int_drug_reaction_pairs: Cross Join every drug to every reaction within each report_id
-- Creates the matrix for signal detection

WITH drugs AS (
    SELECT
        report_id,
        drug_name,
        drug_sequence,
        drug_characterization,
        report_drug_id,
        src_ld_ts

    FROM {{ ref('int_report_drugs') }}
    {% if is_incremental() %}
    WHERE src_ld_ts > (
        SELECT COALESCE(MAX(src_ld_ts), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
),

reactions AS (
    SELECT
        report_id,
        reaction_term,
        reaction_sequence,
        report_reaction_id,
        src_ld_ts

    FROM {{ ref('int_report_reactions') }}
    {% if is_incremental() %}
    WHERE src_ld_ts > (
        SELECT COALESCE(MAX(src_ld_ts), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
),

cross_joined AS (
    SELECT
        d.report_id,
        d.drug_name,
        d.drug_characterization,
        d.drug_sequence,
        d.report_drug_id,
        r.reaction_term,
        r.reaction_sequence,
        r.report_reaction_id,

        d.report_id || '-' || d.report_drug_id || '-' || r.report_reaction_id AS drug_reaction_pair_id,

        LEAST(d.src_ld_ts, r.src_ld_ts) AS src_ld_ts

    FROM drugs d
    INNER JOIN reactions r
        ON d.report_id = r.report_id
)

SELECT
    drug_reaction_pair_id,
    report_id,
    drug_name,
    drug_characterization,
    drug_sequence,
    report_drug_id,
    reaction_term,
    reaction_sequence,
    report_reaction_id,
    src_ld_ts

FROM cross_joined