{{
    config(
        materialized='incremental',
        unique_key='delivrance_id',
        on_schema_change='append_new_columns',
        incremental_strategy = 'merge',
        merge_exclude_columns = ['_created_date']
    )
}}

with delivrances_region1 as (
    SELECT * FROM {{ ref('region1_raw_delivrances') }}
    {% if is_incremental() %}
        WHERE (patient_id, blood_type, delivrance_date, volume_ml) NOT IN (
        SELECT patient_id, blood_type, delivrance_date, volume_ml FROM {{ this }})
    {% endif %}
),
delivrances_region2 as (
    SELECT * FROM {{ ref('region2_raw_delivrances') }}
    {% if is_incremental() %}
        WHERE (patient_id, blood_type, delivrance_date, volume_ml) NOT IN (
        SELECT patient_id, blood_type, delivrance_date, volume_ml FROM {{ this }})
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

region1_deliverances AS (
    SELECT
        delivrances_region1.delivrance_id,
        delivrances_region1.patient_id,
        patients_region1.first_name,
        patients_region1.last_name,
        patients_region1.birth_date,
        delivrances_region1.delivrance_date,
        delivrances_region1.blood_type,
        delivrances_region1.volume_ml,
        delivrances_region1._source,
        delivrances_region1._created_date
    FROM delivrances_region1
    INNER JOIN patients_region1
        ON delivrances_region1.patient_id = patients_region1.patient_id
),

region2_deliverances AS (
    SELECT
        delivrances_region2.delivrance_id,
        delivrances_region2.patient_id,
        patients_region2.first_name,
        patients_region2.last_name,
        patients_region2.birth_date,
        delivrances_region2.delivrance_date,
        delivrances_region2.blood_type,
        delivrances_region2.volume_ml,
        delivrances_region2._source,
        delivrances_region2._created_date
    FROM delivrances_region2
    INNER JOIN patients_region2
        ON delivrances_region2.patient_id = patients_region2.patient_id
),

unioned_delivrances AS (
    SELECT *
    FROM region1_deliverances
    UNION ALL
    SELECT *
    FROM region2_deliverances
)

SELECT
    unioned_delivrances.delivrance_id,
    convergence_patients.patient_id,
    unioned_delivrances.delivrance_date,
    unioned_delivrances.blood_type,
    unioned_delivrances.volume_ml,
    unioned_delivrances._source,
    unioned_delivrances._created_date
FROM unioned_delivrances
INNER JOIN convergence_patients
    ON unioned_delivrances.first_name = convergence_patients.first_name
    AND unioned_delivrances.last_name = convergence_patients.last_name
    AND unioned_delivrances.birth_date = convergence_patients.birth_date