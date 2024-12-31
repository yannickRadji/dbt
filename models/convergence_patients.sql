WITH region1 AS (
    SELECT * FROM {{ ref('region1_raw_patients') }}
),
region2 AS (
    SELECT * FROM {{ ref('region2_raw_patients') }}
),
combined AS (
    SELECT 
        COALESCE(r1.patient_id, r2.patient_id) AS patient_id,
        COALESCE(r1.first_name, r2.first_name) AS first_name,
        COALESCE(r1.last_name, r2.last_name) AS last_name,
        COALESCE(r1.birth_date, r2.birth_date) AS birth_date,
        CASE 
            WHEN r1.patient_id IS NOT NULL AND r2.patient_id IS NOT NULL THEN 'Both Regions'
            WHEN r1.patient_id IS NOT NULL THEN 'Region1'
            ELSE 'Region2'
        END AS region,
        COALESCE(r1.start_date, r2.start_date) AS start_date
    FROM region1 r1
    FULL OUTER JOIN region2 r2 ON r1.patient_id = r2.patient_id
)
SELECT
    patient_id,
    first_name,
    last_name,
    birth_date,
    region,
    start_date
FROM combined