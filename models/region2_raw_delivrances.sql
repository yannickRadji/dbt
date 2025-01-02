SELECT
    delivrance_id + 1000000 AS delivrance_id,
    patient_id + 1000000 AS patient_id,
    delivrance_date,
    CASE
        WHEN blood_type = 'Oplus' THEN 'O+'
        WHEN blood_type = 'Aplus' THEN 'A+'
        WHEN blood_type = 'Bplus' THEN 'B+'
        WHEN blood_type = 'ABplus' THEN 'AB+'
        WHEN blood_type = 'Ominus' THEN 'O-'
        WHEN blood_type = 'Aminus' THEN 'A-'
        WHEN blood_type = 'Bminus' THEN 'B-'
        WHEN blood_type = 'ABminus' THEN 'AB-'
        ELSE blood_type
    END AS blood_type,
    -- Convert volume from liters to milliliters
    CAST(volume_l * 1000 AS INT) AS volume_ml
FROM {{ source('region2_raw', 'delivrances') }}
