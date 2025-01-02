SELECT
    patient_id + 1000000 AS patient_id,
    first_name,
    last_name,
    birth_date,
    region,
    start_date
FROM {{ source('region2_raw', 'patients') }}
