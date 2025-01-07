{{ config(materialized='table') }}

WITH region1_analyses AS (
    SELECT
        analysis_id,
        a.patient_id,
        p.first_name,
        p.last_name,
        p.birth_date,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region1_raw_analyses') }} AS a
    INNER JOIN {{ ref('region1_raw_patients') }} AS p
        ON a.patient_id = p.patient_id
),

region2_analyses AS (
    SELECT
        analysis_id,
        a.patient_id,
        p.first_name,
        p.last_name,
        p.birth_date,
        analysis_date,
        blood_group,
        status
    FROM {{ ref('region2_raw_analyses') }} AS a
    INNER JOIN {{ ref('region2_raw_patients') }} AS p
        ON a.patient_id = p.patient_id
),

unioned_analyses AS (SELECT *
FROM region1_analyses
UNION ALL
SELECT *
FROM region2_analyses)
-- En plus d'assembler les resultats des regions on doit mettre seulement l'ID patient qu'on a retenue pour les personnes double région
SELECT
    analysis_id,
    c.patient_id,
    analysis_date,
    blood_group,
    status
FROM unioned_analyses AS u
INNER JOIN {{ ref("convergence_patients")}} AS c
    ON u.first_name = c.first_name AND u.last_name = c.last_name AND u.birth_date = c.birth_date