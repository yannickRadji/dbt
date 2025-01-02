SELECT
    patient_id,
    first_name,
    last_name,
    birth_date,
    region,
    start_date
FROM {{ source('region2_raw', 'patients') }}
