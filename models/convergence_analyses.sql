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
region1_analyses AS (
    SELECT
        analysis_id AS analysis_id, 
        patient_id,
        analysis_date,
        blood_group,
        status
    FROM region1_raw.analyses
),
region2_analyses AS (
    SELECT
        analysis_id + 1000000 AS analysis_id, -- Ensure the offset is large enough to avoid overlap with existing IDs in Region without changing type
        patient_id,
        analysis_date,
        blood_group,
        status
    FROM region2_raw.analyses
)
SELECT *
FROM region1_analyses
UNION ALL
SELECT *
FROM region2_analyses
