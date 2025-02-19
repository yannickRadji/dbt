with source as (
    SELECT * FROM {{ source('region1_raw', 'patients') }}
)


SELECT
    patient_id,
    first_name,
    last_name,
    birth_date,
    start_date,
    'region 1' as _source,
    CURRENT_TIMESTAMP as _created_date
FROM source
