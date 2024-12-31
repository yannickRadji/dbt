WITH region1 AS (
    SELECT * FROM {{ ref('region1_raw_analyses') }}
),
region2 AS (
    SELECT * FROM {{ ref('region2_raw_analyses') }}
),
combined AS (
    SELECT * FROM region1
    UNION ALL
    SELECT * FROM region2
)
SELECT
    analysis_id,
    patient_id,
    analysis_date,
    blood_group,
    status
FROM combined