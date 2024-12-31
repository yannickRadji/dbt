SELECT
    analysis_id,
    patient_id,
    analysis_date,
    CASE 
        WHEN blood_group = 'Oplus' THEN 'O+'
        WHEN blood_group = 'Aminus' THEN 'A-'
        ELSE blood_group
    END AS blood_group,
    status
FROM {{ source('region2_raw', 'analyses') }}