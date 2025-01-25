SELECT
    analysis_id + 1000000 AS analysis_id,
    patient_id + 1000000 AS patient_id,
    analysis_date,
    status,
    mbt.standardized_value AS blood_group
FROM {{ source('region2_raw', 'analyses') }} as a
LEFT JOIN {{ source('national_convergence', 'mapping_blood_types') }} mbt
    ON a.blood_group = mbt.source_value
