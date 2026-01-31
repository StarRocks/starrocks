-- name: test_variant_comparable
SELECT
    CASE WHEN CAST(1 AS VARIANT) = CAST(1 AS VARIANT) THEN 'PASS' ELSE 'FAIL' END AS v_eq,
    CASE WHEN CAST(1 AS VARIANT) != CAST(2 AS VARIANT) THEN 'PASS' ELSE 'FAIL' END AS v_ne,
    CASE WHEN CAST(NULL AS VARIANT) IS NULL THEN 'PASS' ELSE 'FAIL' END AS v_is_null,
    CASE WHEN CAST(NULL AS VARIANT) IS NOT NULL THEN 'FAIL' ELSE 'PASS' END AS v_is_not_null,
    CASE WHEN CAST(NULL AS VARIANT) <=> CAST(NULL AS VARIANT) THEN 'PASS' ELSE 'FAIL' END AS v_null_safe_eq;
-- result:
PASS	PASS	PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST('a' AS VARIANT) AS VARCHAR) < CAST(CAST('b' AS VARIANT) AS VARCHAR)
        THEN 'PASS' ELSE 'FAIL' END AS v_cast_lt;
-- result:
PASS
-- !result
SELECT CAST(CAST('b' AS VARIANT) AS VARCHAR) AS v ORDER BY v;
-- result:
b
-- !result
SELECT CAST(1 AS VARIANT) < CAST(2 AS VARIANT);
-- result:
1
-- !result
SELECT CAST(1 AS VARIANT) BETWEEN CAST(0 AS VARIANT) AND CAST(2 AS VARIANT);
-- result:
1
-- !result
SELECT CAST(1 AS VARIANT) AS v ORDER BY v;
-- result:
E: (1064, 'Getting analyzing error. Detail message: Type (nested) percentile/hll/bitmap/json/struct/map not support order-by.')
-- !result