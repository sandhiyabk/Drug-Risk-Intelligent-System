{{
    config(
        materialized='table',
        alias='fct_risk_signals',
        schema='ANALYTICS',
        snowflake_warehouse=var('snowflake_warehouse', 'DRUG_RISK_WH')
    )
}}

-- fct_risk_signals: Calculate Reporting Odds Ratio (ROR) for Drug-Reaction pairs
-- Materialized as table for high-speed dashboarding
-- Signal filtering: min 50 reports AND ROR > 2.0

WITH drug_reaction_pairs AS (
    SELECT
        drug_name,
        reaction_term,
        src_ld_ts

    FROM {{ ref('int_drug_reaction_pairs') }}
),

total_reports AS (
    SELECT
        COUNT(DISTINCT report_id) AS total_report_count

    FROM {{ ref('stg_faers_reports') }}
),

drug_counts AS (
    SELECT
        drug_name,
        COUNT(DISTINCT report_id) AS reports_with_drug

    FROM drug_reaction_pairs
    GROUP BY drug_name
),

reaction_counts AS (
    SELECT
        reaction_term,
        COUNT(DISTINCT report_id) AS reports_with_reaction

    FROM drug_reaction_pairs
    GROUP BY reaction_term
),

pair_counts AS (
    SELECT
        drug_name,
        reaction_term,
        COUNT(DISTINCT report_id) AS A_count
            -- A: Reports with Drug AND Reaction
    FROM drug_reaction_pairs
    GROUP BY drug_name, reaction_term
),

A AS (SELECT drug_name, reaction_term, A_count FROM pair_counts),

B AS (
    SELECT
        p.drug_name,
        p.reaction_term,
        (dc.reports_with_drug - COALESCE(p.A_count, 0)) AS B_count
            -- B: Reports with Drug but NOT Reaction
    FROM pair_counts p
    INNER JOIN drug_counts dc ON p.drug_name = dc.drug_name
),

C AS (
    SELECT
        p.drug_name,
        p.reaction_term,
        (rc.reports_with_reaction - COALESCE(p.A_count, 0)) AS C_count
            -- C: Reports with Reaction but NOT Drug
    FROM pair_counts p
    INNER JOIN reaction_counts rc ON p.reaction_term = rc.reaction_term
),

D AS (
    SELECT
        p.drug_name,
        p.reaction_term,
        (tr.total_report_count - dc.reports_with_drug - rc.reports_with_reaction + COALESCE(p.A_count, 0)) AS D_count
            -- D: Reports with neither Drug nor Reaction
    FROM pair_counts p
    CROSS JOIN total_reports tr
    INNER JOIN drug_counts dc ON p.drug_name = dc.drug_name
    INNER JOIN reaction_counts rc ON p.reaction_term = rc.reaction_term
),

ror_calc AS (
    SELECT
        a.drug_name,
        a.reaction_term,
        a.A_count,
        b.B_count,
        c.C_count,
        d.D_count,
        
        -- ROR = (A/B) / (C/D) = (A*D) / (B*C)
        -- Handle nulls and divide by zero
        CASE
            WHEN b.B_count > 0 AND c.C_count > 0 AND d.D_count > 0 
            THEN (a.A_count * d.D_count) * 1.0 / (b.B_count * c.C_count)
            WHEN b.B_count = 0 AND a.A_count > 0 AND c.C_count > 0 
            THEN NULL  -- Cannot calculate (infinite)
            WHEN c.C_count = 0 AND a.A_count > 0 AND b.B_count > 0 
            THEN NULL  -- Cannot calculate
            ELSE NULL
        END AS ror_value,

        -- Simplified ROR when denominator > 0
        CASE
            WHEN b.B_count > 0 AND c.C_count > 0
            THEN (a.A_count * 1.0 / b.B_count) / (c.C_count * 1.0 / d.D_count)
            ELSE NULL
        END AS ror_simple

    FROM A a
    INNER JOIN B b ON a.drug_name = b.drug_name AND a.reaction_term = b.reaction_term
    INNER JOIN C c ON a.drug_name = c.drug_name AND a.reaction_term = c.reaction_term
    INNER JOIN D d ON a.drug_name = d.drug_name AND a.reaction_term = d.reaction_term
),

signals AS (
    SELECT
        ror.drug_name,
        ror.reaction_term,
        ror.A_count AS report_count,
        ror.B_count,
        ror.C_count,
        ror.D_count,
        ror.ror_value AS ror,
        ror.ror_simple AS ror_simple,

        CASE
            WHEN ror.ror_value > 2.0 THEN 'HIGH'
            WHEN ror.ror_value > 1.5 THEN 'ELEVATED'
            WHEN ror.ror_value > 1.0 THEN 'ELEVATED'
            ELSE 'BASELINE'
        END AS signal_strength,

        CASE
            WHEN ror.ror_value > 2.0 
                 AND ror.A_count >= 50
            THEN TRUE
            ELSE FALSE
        END AS is_significant_signal,

        CURRENT_TIMESTAMP() AS signalGenerated_at

    FROM ror_calc ror
    WHERE ror.A_count >= 50
)

SELECT
    drug_name,
    reaction_term,
    report_count,
    B_count,
    C_count,
    D_count,
    ror,
    ror_simple,
    signal_strength,
    is_significant_signal,
    signalGenerated_at

FROM signals
ORDER BY is_significant_signal DESC, ror DESC, report_count DESC