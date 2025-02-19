with delivrances as (
    SELECT * FROM {{ source('region2_raw', 'delivrances') }}
),
mapping_blood_types as (
    SELECT * FROM {{ source('national_convergence', 'mapping_blood_types') }}
)


SELECT
    delivrance_id + 1000000 AS delivrance_id,
    patient_id + 1000000 AS patient_id,
    delivrance_date,
    mapping_blood_types.standardized_value AS blood_type,
    -- Convert volume from liters to milliliters
    CAST(volume_l * 1000 AS INT) AS volume_ml,
    'region 2' as _source,
    CURRENT_TIMESTAMP as _created_date
FROM delivrances
LEFT JOIN mapping_blood_types
    ON delivrances.blood_type = mapping_blood_types.source_value
