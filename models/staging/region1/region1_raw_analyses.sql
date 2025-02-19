with source as (
    SELECT * FROM {{ source('region1_raw', 'analyses') }}
)

SELECT
    analysis_id,
    patient_id,
    analysis_date,
    blood_group,
    status,
    'region 1' as _source,
    CURRENT_TIMESTAMP as _created_date
FROM source
