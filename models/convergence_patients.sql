{{ config(materialized='table') }}

WITH combined_patients AS (
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM {{ ref('region1_raw_patients') }}
    UNION ALL
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM {{ ref('region2_raw_patients') }}
),

deduplicated_patients AS (
    SELECT DISTINCT ON (first_name, last_name, birth_date)
        patient_id,
        first_name,
        last_name,
        birth_date,
        region,
        start_date
    FROM combined_patients
    ORDER BY first_name ASC, last_name ASC, birth_date ASC, start_date ASC -- keep Oldest start date first, first_name, last_name, birth_date to align with the DISTINCT ON clause
)
--Sarah & Lucas sont dans les 2 régions et doivent apparaitre qu'une fois
SELECT *
FROM deduplicated_patients
