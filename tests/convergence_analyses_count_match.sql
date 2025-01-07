WITH region1 AS (
    SELECT COUNT(*) AS count_region1
    FROM {{ ref('region1_raw_analyses') }}
),
region2 AS (
    SELECT COUNT(*) AS count_region2
    FROM {{ ref('region2_raw_analyses') }}
),
convergence AS (
    SELECT COUNT(*) AS count_convergence
    FROM {{ ref('convergence_analyses') }}
)
SELECT
    (r1.count_region1 + r2.count_region2) - c.count_convergence AS mismatch_count
FROM region1 r1, region2 r2, convergence c
WHERE (r1.count_region1 + r2.count_region2) != c.count_convergence
