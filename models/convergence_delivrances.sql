{{ config(materialized='table') }}

WITH region1_deliverances AS (
    SELECT
        delivrance_id,
        dp.patient_id,
        delivrance_date,
        blood_type,
        volume_ml
    FROM {{ ref('region1_raw_delivrances') }} AS d
    INNER JOIN {{ ref('convergence_patients') }} AS dp
        ON d.patient_id = dp.patient_id
),

region2_deliverances AS (
    SELECT
        dp.patient_id,
        delivrance_date,
        blood_type,
        volume_ml,
        delivrance_id + 1000000 AS delivrance_id
    FROM {{ ref('region2_raw_delivrances') }} AS d
    INNER JOIN {{ ref('convergence_patients') }} AS dp
        ON d.patient_id = dp.patient_id
)

SELECT *
FROM region1_deliverances
UNION ALL
SELECT *
FROM region2_deliverances
