with source as (
    SELECT * FROM {{ source('region1_raw', 'patients') }}
)


SELECT
    patient_id,
    first_name,
    last_name,
    birth_date,
    region,
    start_date
FROM source
