{{
    config(
        materialized='incremental',
        unique_key='patient_id',
        on_schema_change='append_new_columns',
        incremental_strategy = 'merge',
        merge_exclude_columns = ['_created_date']
    )
}}

with patients_region1 as (
    SELECT * FROM {{ ref('region1_raw_patients') }}
    {% if is_incremental() %}
        WHERE (first_name, last_name, birth_date) NOT IN (
        SELECT first_name, last_name, birth_date FROM {{ this }})
    {% endif %}
),
patients_region2 as (
    SELECT * FROM {{ ref('region2_raw_patients') }}
    {% if is_incremental() %}
        WHERE (first_name, last_name, birth_date) NOT IN (
        SELECT first_name, last_name, birth_date FROM {{ this }})
    {% endif %}
),
-- incremental sur l'ID uniquement pas possible car sinon il ne voit pas les changes sur les attributs (sauf id & region qui ne peuvent pas changer) (même si je dodge le problème des id similaires inter région grace au staging)
-- au final moins intéressant en terme de perf que sans mais en vrai l'idéal est de le faire à partir d'un stream plutot que sur un feature se basant sur une query sinon il faut le faire sur la start date mais être sur qu'il n'y a pas des reprises/update des données dans le passé
combined_patients AS (
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        start_date,
        _source,
        _created_date
    FROM patients_region1
    UNION ALL
    SELECT
        patient_id,
        first_name,
        last_name,
        birth_date,
        start_date,
        _source,
        _created_date
    FROM patients_region2
),

deduplicated_patients AS (
    SELECT * FROM (
        SELECT
            patient_id,
            first_name,
            last_name,
            birth_date,
            start_date,
            _source,
            _created_date,
            ROW_NUMBER() OVER (PARTITION BY first_name, last_name, birth_date ORDER BY start_date ASC) AS rn
        FROM combined_patients
    )
    WHERE rn = 1
)
--Sarah & Lucas sont dans les 2 régions et doivent apparaitre qu'une fois
SELECT
    patient_id,
    first_name,
    last_name,
    birth_date,
    start_date,
    _source,
    _created_date
FROM deduplicated_patients
