{{
    config(
        materialized='table',
        alias='fct_drug_reactions',
        schema='ANALYTICS'
    )
}}

SELECT
    drug_name,
    reaction_term,
    COUNT(DISTINCT report_id) AS report_count
FROM {{ ref('int_drug_reaction_pairs') }}
GROUP BY 1, 2
ORDER BY report_count DESC
