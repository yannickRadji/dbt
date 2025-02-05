with source as (
    SELECT * FROM {{ source('region2_raw', 'patients') }}
)

SELECT
    patient_id + 1000000 AS patient_id,
    first_name,
    last_name,
    birth_date,
    region,
    start_date
FROM source
