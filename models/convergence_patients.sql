WITH combined_patients AS (
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM region1_raw.patients
    UNION ALL
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM region2_raw.patients
),
deduplicated_patients AS (
    SELECT DISTINCT ON (first_name, last_name, birth_date)
        patient_id, -- Keep original patient_id
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM combined_patients
    ORDER BY first_name, last_name, birth_date, start_date ASC -- Oldest start date first
)
SELECT *
FROM deduplicated_patients;
