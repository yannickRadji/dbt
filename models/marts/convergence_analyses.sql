with analysis_region1 as (
    SELECT * FROM {{ ref('region1_raw_analyses') }}
    {% if is_incremental() %}
    WHERE analysis_date > (SELECT MAX(analysis_date) FROM {{ this }})
    {% endif %}
),
analysis_region2 as (
    SELECT * FROM {{ ref('region2_raw_analyses') }}
    {% if is_incremental() %}
    WHERE analysis_date > (SELECT MAX(analysis_date) FROM {{ this }})
    {% endif %}
),
patients_region1 as (
    SELECT * FROM {{ ref('region1_raw_patients') }}
),
patients_region2 as (
    SELECT * FROM {{ ref('region2_raw_patients') }}
),
convergence_patients as (
    SELECT * FROM {{ ref('convergence_patients') }}
),

region1_analyses AS (
    SELECT
        analysis_region1.analysis_id,
        analysis_region1.patient_id,
        patients_region1.first_name,
        patients_region1.last_name,
        patients_region1.birth_date,
        analysis_region1.analysis_date,
        analysis_region1.blood_group,
        analysis_region1.status
    FROM analysis_region1
    INNER JOIN patients_region1
        ON analysis_region1.patient_id = patients_region1.patient_id
),

region2_analyses AS (
    SELECT
        analysis_region2.analysis_id,
        patients_region2.patient_id,
        patients_region2.first_name,
        patients_region2.last_name,
        patients_region2.birth_date,
        analysis_region2.analysis_date,
        analysis_region2.blood_group,
        analysis_region2.status
    FROM analysis_region2
    INNER JOIN patients_region2
        ON analysis_region2.patient_id = patients_region2.patient_id
),

unioned_analyses AS (
    SELECT *
    FROM region1_analyses
    UNION ALL
    SELECT *
    FROM region2_analyses
)

-- En plus d'assembler les resultats des regions on doit mettre seulement l'ID patient qu'on a retenue pour les personnes double région
SELECT
    unioned_analyses.analysis_id,
    convergence_patients.patient_id,
    unioned_analyses.analysis_date,
    unioned_analyses.blood_group,
    unioned_analyses.status
FROM unioned_analyses
INNER JOIN convergence_patients
    ON unioned_analyses.first_name = convergence_patients.first_name
    AND unioned_analyses.last_name = convergence_patients.last_name
    AND unioned_analyses.birth_date = convergence_patients.birth_date