{{ config(materialized='table') }}

WITH region1_analyses AS (
    SELECT
        analysis_id,
        dp.patient_id,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region1_raw_analyses') }} AS a
    INNER JOIN {{ ref('convergence_patients') }} AS dp
        ON a.patient_id = dp.patient_id
),

region2_analyses AS (
    SELECT
        analysis_id,
        dp.patient_id,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region2_raw_analyses') }} AS a
    INNER JOIN {{ ref('convergence_patients') }} AS dp
        ON a.patient_id = dp.patient_id
)

SELECT *
FROM region1_analyses
UNION ALL
SELECT *
FROM region2_analyses
