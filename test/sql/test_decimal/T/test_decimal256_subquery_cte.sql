-- name: test_decimal256_subquery_cte
DROP DATABASE IF EXISTS test_decimal256_subquery;
CREATE DATABASE test_decimal256_subquery;
USE test_decimal256_subquery;

-- Create main test table
CREATE TABLE decimal_main_test (
    id INT,
    category VARCHAR(10),
    d50_15 DECIMAL(50,15),
    d76_20 DECIMAL(76,20)
) PROPERTIES("replication_num"="1");

-- Create secondary test table for joins
CREATE TABLE decimal_secondary_test (
    id INT,
    ref_category VARCHAR(10),
    threshold_d50 DECIMAL(50,15),
    threshold_d76 DECIMAL(76,20)
) PROPERTIES("replication_num"="1");

-- Insert test data into main table - using decimal256 values beyond decimal128 range
INSERT INTO decimal_main_test VALUES
(1, 'A', 12345678901234567890123456789012345.123456789012345, 12345678901234567890123456789012345678901234567890123456.12345678901234567890),
(2, 'A', 98765432109876543210987654321098765.456789012345678, 98765432109876543210987654321098765432109876543210987654.45678901234567890123),
(3, 'B', 55555555555555555555555555555555555.789012345678901, 55555555555555555555555555555555555555555555555555555555.78901234567890123456),
(4, 'B', 77777777777777777777777777777777777.012345678901234, 77777777777777777777777777777777777777777777777777777777.01234567890123456789),
(5, 'C', 44444444444444444444444444444444444.567890123456789, 44444444444444444444444444444444444444444444444444444444.56789012345678901234),
(6, 'C', 88888888888888888888888888888888888.901234567890123, 88888888888888888888888888888888888888888888888888888888.90123456789012345678),
(7, 'A', 11111111111111111111111111111111111.234567890123456, 11111111111111111111111111111111111111111111111111111111.23456789012345678901),
(8, 'B', 22222222222222222222222222222222222.678901234567890, 22222222222222222222222222222222222222222222222222222222.67890123456789012345),
(9, 'C', 99999999999999999999999999999999999.345678901234567, 99999999999999999999999999999999999999999999999999999999.34567890123456789012),
(10, 'A', 66666666666666666666666666666666666.111111111111111, 66666666666666666666666666666666666666666666666666666666.11111111111111111111);

-- Insert test data into secondary table - using large threshold values
INSERT INTO decimal_secondary_test VALUES
(1, 'A', 50000000000000000000000000000000000.000000000000000, 50000000000000000000000000000000000000000000000000000000.00000000000000000000),
(2, 'B', 60000000000000000000000000000000000.000000000000000, 60000000000000000000000000000000000000000000000000000000.00000000000000000000),
(3, 'C', 70000000000000000000000000000000000.000000000000000, 70000000000000000000000000000000000000000000000000000000.00000000000000000000);

-- Test 1: Scalar subquery in SELECT
SELECT
    'Test1_SCALAR_SUBQUERY_SELECT' as test_name,
    id,
    category,
    d50_15,
    (SELECT AVG(d50_15) FROM decimal_main_test) as overall_avg,
    d50_15 - (SELECT AVG(d50_15) FROM decimal_main_test) as diff_from_avg
FROM decimal_main_test
ORDER BY id;

-- Test 2: Scalar subquery in WHERE
SELECT
    'Test2_SCALAR_SUBQUERY_WHERE' as test_name,
    id,
    category,
    d50_15,
    d76_20
FROM decimal_main_test
WHERE d50_15 > (SELECT AVG(d50_15) FROM decimal_main_test)
ORDER BY d50_15;

-- Test 3: EXISTS subquery
SELECT
    'Test3_EXISTS_SUBQUERY' as test_name,
    mt.id,
    mt.category,
    mt.d50_15
FROM decimal_main_test mt
WHERE EXISTS (
    SELECT 1 FROM decimal_secondary_test st
    WHERE st.ref_category = mt.category
    AND mt.d50_15 > st.threshold_d50
)
ORDER BY mt.id;

-- Test 4: IN subquery with decimal values
SELECT
    'Test4_IN_SUBQUERY' as test_name,
    id,
    category,
    d76_20
FROM decimal_main_test
WHERE d76_20 IN (
    SELECT d76_20 FROM decimal_main_test
    WHERE category = 'A'
    AND d76_20 > 100.00000000000000000000
)
ORDER BY d76_20;


-- Test 5: Basic CTE
WITH decimal_stats AS (
    SELECT
        category,
        AVG(d50_15) as avg_d50,
        AVG(d76_20) as avg_d76,
        COUNT(*) as cnt
    FROM decimal_main_test
    GROUP BY category
)
SELECT
    'Test6_BASIC_CTE' as test_name,
    ds.category,
    ds.avg_d50,
    ds.avg_d76,
    ds.cnt,
    mt.d50_15,
    mt.d50_15 - ds.avg_d50 as diff_from_cat_avg
FROM decimal_stats ds
JOIN decimal_main_test mt ON ds.category = mt.category
ORDER BY ds.category, mt.d50_15;


-- Test 6: Multiple CTEs
WITH category_stats AS (
    SELECT
        category,
        SUM(d50_15) as total_d50,
        MAX(d76_20) as max_d76
    FROM decimal_main_test
    GROUP BY category
),
threshold_data AS (
    SELECT
        ref_category,
        AVG(threshold_d50) as avg_threshold
    FROM decimal_secondary_test
    GROUP BY ref_category
)
SELECT
    'Test8_MULTIPLE_CTE' as test_name,
    cs.category,
    cs.total_d50,
    cs.max_d76,
    td.avg_threshold,
    CASE
        WHEN cs.total_d50 > td.avg_threshold * 3 THEN 'HIGH'
        WHEN cs.total_d50 > td.avg_threshold THEN 'MEDIUM'
        ELSE 'LOW'
    END as performance_level
FROM category_stats cs
LEFT JOIN threshold_data td ON cs.category = td.ref_category
ORDER BY cs.category;
