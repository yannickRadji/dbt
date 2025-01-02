SELECT
    analysis_id,
    patient_id,
    analysis_date,
    status,
    CASE
        WHEN blood_group = 'Oplus' THEN 'O+'
        WHEN blood_group = 'Aplus' THEN 'A+'
        WHEN blood_group = 'Bplus' THEN 'B+'
        WHEN blood_group = 'ABplus' THEN 'AB+'
        WHEN blood_group = 'Ominus' THEN 'O-'
        WHEN blood_group = 'Aminus' THEN 'A-'
        WHEN blood_group = 'Bminus' THEN 'B-'
        WHEN blood_group = 'ABminus' THEN 'AB-'
        ELSE blood_group
    END AS blood_group
FROM {{ source('region2_raw', 'analyses') }}
