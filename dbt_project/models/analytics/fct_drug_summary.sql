{{
    config(
        materialized='table',
        alias='fct_drug_summary',
        schema='ANALYTICS'
    )
}}

WITH drug_stats AS (
    SELECT
        drug_name,
        COUNT(DISTINCT report_id) AS total_reports,
        AVG(patient_age) AS avg_age
    FROM {{ ref('int_report_drugs') }}
    GROUP BY 1
)

SELECT
    drug_name,
    total_reports,
    COALESCE(avg_age, 0) AS avg_age,
    CASE
        WHEN total_reports >= 50 THEN 'HIGH'
        WHEN total_reports >= 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM drug_stats
ORDER BY total_reports DESC
