SELECT
    delivrance_id + 1000000 AS delivrance_id,
    patient_id + 1000000 AS patient_id,
    delivrance_date,
    mbt.standardized_value AS blood_type,
    -- Convert volume from liters to milliliters
    CAST(volume_l * 1000 AS INT) AS volume_ml
FROM {{ source('region2_raw', 'delivrances') }} as d
LEFT JOIN {{ source('national_convergence', 'mapping_blood_types') }} mbt
    ON d.blood_type = mbt.source_value
