WITH deduplicated_patients AS (
    SELECT DISTINCT
        COALESCE(r1.patient_id, r2.patient_id) AS patient_id,
        COALESCE(r1.first_name, r2.first_name) AS first_name,
        COALESCE(r1.last_name, r2.last_name) AS last_name,
        COALESCE(r1.birth_date, r2.birth_date) AS birth_date
    FROM region1_raw.patients r1
    FULL OUTER JOIN region2_raw.patients r2
        ON r1.first_name = r2.first_name
        AND r1.last_name = r2.last_name
        AND r1.birth_date = r2.birth_date
),
region1_deliverances AS (
    SELECT
        delivrance_id AS delivrance_id,
        patient_id,
        delivrance_date,
        blood_type,
        volume_ml
    FROM region1_raw.deliverances
),
region2_deliverances AS (
    SELECT
        delivrance_id + 1000000 AS delivrance_id,
        patient_id,
        delivrance_date,
        blood_type,
        volume_ml
    FROM region2_raw.deliverances
)
SELECT *
FROM region1_deliverances
UNION ALL
SELECT *
FROM region2_deliverances;
