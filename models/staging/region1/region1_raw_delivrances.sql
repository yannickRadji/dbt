with source as (
    SELECT * FROM {{ source('region1_raw', 'delivrances') }}
)

SELECT
    delivrance_id,
    patient_id,
    delivrance_date,
    blood_type,
    volume_ml
FROM source
