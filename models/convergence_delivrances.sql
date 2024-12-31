WITH region1 AS (
    SELECT * FROM {{ ref('region1_raw_delivrances') }}
),
region2 AS (
    SELECT * FROM {{ ref('region2_raw_delivrances') }}
),
combined AS (
    SELECT * FROM region1
    UNION ALL
    SELECT * FROM region2
)
SELECT
    delivrance_id,
    patient_id,
    delivrance_date,
    blood_type,
    volume_ml
FROM combined