with source as (
    SELECT * FROM {{ source('region1_raw', 'analyses') }}
)

SELECT
    analysis_id,
    patient_id,
    analysis_date,
    blood_group,
    status
FROM source
