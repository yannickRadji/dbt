with analyses as (
    SELECT * FROM {{ source('region2_raw', 'analyses') }}
),
mapping_blood_types as (
    SELECT * FROM {{ source('national_convergence', 'mapping_blood_types') }}
)

SELECT
    analysis_id + 1000000 AS analysis_id,
    patient_id + 1000000 AS patient_id,
    analysis_date,
    status,
    mapping_blood_types.standardized_value AS blood_group
FROM analyses
LEFT JOIN mapping_blood_types
    ON analyses.blood_group = mapping_blood_types.source_value
