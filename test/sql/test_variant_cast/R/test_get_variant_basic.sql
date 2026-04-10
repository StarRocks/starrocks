-- name: test_get_variant_basic
SELECT
    CASE WHEN get_variant_bool(CAST(true AS VARIANT), '$') = true THEN 'PASS' ELSE 'FAIL' END AS v_fn_bool,
    CASE WHEN get_variant_int(CAST(CAST(123 AS BIGINT) AS VARIANT), '$') = 123 THEN 'PASS' ELSE 'FAIL' END AS v_fn_int,
    CASE WHEN ABS(get_variant_double(CAST(3.14 AS VARIANT), '$') - 3.14) < 0.0001
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_double,
    CASE WHEN get_variant_string(CAST('hello' AS VARIANT), '$') = 'hello' THEN 'PASS' ELSE 'FAIL' END AS v_fn_str,
    CASE WHEN get_variant_date(CAST(CAST('2024-01-02' AS DATE) AS VARIANT), '$') = CAST('2024-01-02' AS DATE)
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_date,
    CASE WHEN get_variant_datetime(CAST(CAST('2024-01-02 03:04:05' AS DATETIME) AS VARIANT), '$') =
        CAST('2024-01-02 03:04:05' AS DATETIME) THEN 'PASS' ELSE 'FAIL' END AS v_fn_dt,
    CASE WHEN get_variant_time(CAST(CAST('12:34:56' AS TIME) AS VARIANT), '$') = CAST('12:34:56' AS TIME)
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_time;
-- result:
PASS	PASS	PASS	PASS	PASS	PASS	PASS
-- !result