WITH region1_deliverances AS (
    SELECT
        delivrance_id,
        d.patient_id,
        dp.first_name,
        dp.last_name,
        dp.birth_date,
        delivrance_date,
        blood_type,
        volume_ml
    FROM {{ ref('region1_raw_delivrances') }} AS d
    INNER JOIN {{ ref('region1_raw_patients') }} AS dp
        ON d.patient_id = dp.patient_id
),

region2_deliverances AS (
    SELECT
        delivrance_id,
        d.patient_id,
        dp.first_name,
        dp.last_name,
        dp.birth_date,
        delivrance_date,
        blood_type,
        volume_ml
    FROM {{ ref('region2_raw_delivrances') }} AS d
    INNER JOIN {{ ref('region2_raw_patients') }} AS dp
        ON d.patient_id = dp.patient_id
),

unioned_delivrances AS (SELECT *
FROM region1_deliverances
UNION ALL
SELECT *
FROM region2_deliverances)

SELECT
    delivrance_id,
    c.patient_id,
    delivrance_date,
    blood_type,
    volume_ml
FROM unioned_delivrances AS u
INNER JOIN {{ ref("convergence_patients")}} AS c
    ON u.first_name = c.first_name AND u.last_name = c.last_name AND u.birth_date = c.birth_date