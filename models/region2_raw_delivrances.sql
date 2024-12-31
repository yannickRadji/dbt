SELECT
    delivrance_id,
    patient_id,
    delivrance_date,
    blood_type,
    (volume_l * 1000) AS volume_ml -- Convert volume from liters to milliliters
FROM {{ source('region2_raw', 'delivrances') }}