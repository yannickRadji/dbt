{{ config(materialized='table') }}

WITH region1_analyses AS (
    SELECT
        analysis_id,
        dp.patient_id,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region1_raw_analyses') }} a
    JOIN {{ ref('convergence_patients') }} dp
        ON a.patient_id = dp.patient_id
),
region2_analyses AS (
    SELECT
        analysis_id + 1000000 AS analysis_id,
        dp.patient_id,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region2_raw_analyses') }} a
    JOIN {{ ref('convergence_patients') }} dp
        ON a.patient_id = dp.patient_id
)
SELECT *
FROM region1_analyses
UNION ALL
SELECT *
FROM region2_analyses
