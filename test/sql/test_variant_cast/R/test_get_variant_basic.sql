-- name: test_get_variant_basic
drop database if exists test_get_variant_basic;
-- result:
-- !result
CREATE DATABASE test_get_variant_basic;
-- result:
-- !result
USE test_get_variant_basic;
-- result:
-- !result
SELECT
    CASE WHEN get_variant_bool(CAST(true AS VARIANT), '$') = true THEN 'PASS' ELSE 'FAIL' END AS v_fn_bool,
    CASE WHEN get_variant_int(CAST(CAST(123 AS BIGINT) AS VARIANT), '$') = 123 THEN 'PASS' ELSE 'FAIL' END AS v_fn_int,
    CASE WHEN ABS(get_variant_double(CAST(3.14 AS VARIANT), '$') - 3.14) < 0.0001
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_double,
    CASE WHEN get_variant_string(CAST('hello' AS VARIANT), '$') = 'hello' THEN 'PASS' ELSE 'FAIL' END AS v_fn_str,
    CASE WHEN get_variant_date(CAST('2024-01-02' AS VARIANT), '$') = CAST('2024-01-02' AS DATE)
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_date,
    CASE WHEN get_variant_datetime(CAST('2024-01-02 03:04:05' AS VARIANT), '$') =
        CAST('2024-01-02 03:04:05' AS DATETIME) THEN 'PASS' ELSE 'FAIL' END AS v_fn_dt,
    CASE WHEN get_variant_time(CAST('12:34:56' AS VARIANT), '$') = CAST('12:34:56' AS TIME)
        THEN 'PASS' ELSE 'FAIL' END AS v_fn_time;
-- result:
PASS	PASS	PASS	PASS	PASS	PASS	PASS
-- !result
