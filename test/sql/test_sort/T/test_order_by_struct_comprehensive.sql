-- name: test_order_by_struct_comprehensive
DROP DATABASE IF EXISTS test_order_by_struct_comprehensive;
CREATE DATABASE test_order_by_struct_comprehensive;
USE test_order_by_struct_comprehensive;

-- ==========================================
-- Test 1: Basic Lexicographical Ordering
-- ==========================================
CREATE TABLE t1_basic (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t1_basic VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(1, 1));

-- Ascending order
SELECT s FROM t1_basic ORDER BY s ASC;

-- Descending order
SELECT s FROM t1_basic ORDER BY s DESC;

-- Verify lexicographical order (when first field is same, compare second field)
SELECT s FROM t1_basic WHERE id IN (1, 2, 5) ORDER BY s;

-- ==========================================
-- Test 2: NULL Value Handling
-- ==========================================
SELECT '=== Test 2: NULL Value Handling ===' AS test_name;

CREATE TABLE t2_nulls (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t2_nulls VALUES
(1, row(1, 1)),
(2, row(1, NULL)),
(3, row(NULL, 1)),
(4, row(NULL, NULL)),
(5, NULL);

-- NULLS FIRST (default)
SELECT s FROM t2_nulls ORDER BY s ASC NULLS FIRST;

-- NULLS LAST
SELECT s FROM t2_nulls ORDER BY s ASC NULLS LAST;

-- Verify ordering of internal NULL fields
SELECT s FROM t2_nulls WHERE id BETWEEN 1 AND 4 ORDER BY s;

-- ==========================================
-- Test 3: Different Data Types
-- ==========================================
SELECT '=== Test 3: Different Data Types ===' AS test_name;

-- 3.1 String and Integer
CREATE TABLE t3_mixed_types (
    id INT,
    s STRUCT<name STRING, age INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t3_mixed_types VALUES
(1, row('Alice', 30)),
(2, row('Alice', 25)),
(3, row('Bob', 30)),
(4, row('Bob', 25)),
(5, row('Alice', 30));

SELECT s FROM t3_mixed_types ORDER BY s;

-- 3.2 Float numbers
CREATE TABLE t3_float (
    id INT,
    s STRUCT<x DOUBLE, y DOUBLE>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t3_float VALUES
(1, row(1.1, 2.2)),
(2, row(1.1, 2.1)),
(3, row(1.2, 2.2)),
(4, row(1.0, 2.0));

SELECT s FROM t3_float ORDER BY s;

-- 3.3 Date types
CREATE TABLE t3_date (
    id INT,
    s STRUCT<event_date DATE, event_time DATETIME>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t3_date VALUES
(1, row('2024-01-01', '2024-01-01 10:00:00')),
(2, row('2024-01-01', '2024-01-01 09:00:00')),
(3, row('2024-01-02', '2024-01-02 10:00:00')),
(4, row('2024-01-01', '2024-01-01 10:00:00'));

SELECT s FROM t3_date ORDER BY s;

-- ==========================================
-- Test 4: Nested STRUCT
-- ==========================================
SELECT '=== Test 4: Nested STRUCT ===' AS test_name;

CREATE TABLE t4_nested (
    id INT,
    s STRUCT<outer1 INT, inner1 STRUCT<a INT, b INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t4_nested VALUES
(1, row(1, row(1, 1))),
(2, row(1, row(1, 2))),
(3, row(1, row(2, 1))),
(4, row(2, row(1, 1)));

SELECT s FROM t4_nested ORDER BY s;

-- Verify lexicographical order of nested structures
SELECT s FROM t4_nested WHERE id <= 3 ORDER BY s;

-- ==========================================
-- Test 5: Mixed Column Ordering (STRUCT with other columns)
-- ==========================================
SELECT '=== Test 5: Mixed Column Ordering ===' AS test_name;

CREATE TABLE t5_mixed_columns (
    id INT,
    s STRUCT<a INT, b INT>,
    score INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t5_mixed_columns VALUES
(1, row(1, 1), 100),
(2, row(1, 2), 90),
(3, row(2, 1), 100),
(4, row(1, 1), 95);

-- Sort by STRUCT first, then by score
SELECT s, score FROM t5_mixed_columns ORDER BY s, score;

-- Sort by score first, then by STRUCT
SELECT s, score FROM t5_mixed_columns ORDER BY score, s;

-- ==========================================
-- Test 6: Empty and Single-field STRUCT
-- ==========================================
SELECT '=== Test 6: Empty and Single-field STRUCT ===' AS test_name;

-- Single-field STRUCT
CREATE TABLE t6_single_field (
    id INT,
    s STRUCT<value INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t6_single_field VALUES
(1, row(3)),
(2, row(1)),
(3, row(2)),
(4, row(1));

SELECT s FROM t6_single_field ORDER BY s;

-- ==========================================
-- Test 7: STRUCT with Many Fields
-- ==========================================
SELECT '=== Test 7: STRUCT with Many Fields ===' AS test_name;

CREATE TABLE t7_many_fields (
    id INT,
    s STRUCT<f1 INT, f2 INT, f3 INT, f4 INT, f5 INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t7_many_fields VALUES
(1, row(1, 1, 1, 1, 1)),
(2, row(1, 1, 1, 1, 2)),
(3, row(1, 1, 1, 2, 1)),
(4, row(1, 1, 2, 1, 1)),
(5, row(1, 2, 1, 1, 1)),
(6, row(2, 1, 1, 1, 1));

SELECT s FROM t7_many_fields ORDER BY s;

-- ==========================================
-- Test 8: Sort Stability with Same Values
-- ==========================================
SELECT '=== Test 8: Sort Stability with Same Values ===' AS test_name;

CREATE TABLE t8_stability (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t8_stability VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 1)),
(4, row(2, 2)),
(5, row(2, 2));

-- Multiple sorts should yield consistent results
SELECT id, s FROM t8_stability ORDER BY s, id;
SELECT id, s FROM t8_stability ORDER BY s, id;

-- ==========================================
-- Test 9: LIMIT and OFFSET
-- ==========================================
SELECT '=== Test 9: LIMIT and OFFSET ===' AS test_name;

CREATE TABLE t9_limit (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t9_limit VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1)),
(6, row(3, 2));

-- TOP-N optimization
SELECT s FROM t9_limit ORDER BY s LIMIT 3;

-- OFFSET
SELECT s FROM t9_limit ORDER BY s LIMIT 3 OFFSET 2;

-- ==========================================
-- Test 10: Ordering After Aggregation
-- ==========================================
SELECT '=== Test 10: Ordering After Aggregation ===' AS test_name;

CREATE TABLE t10_aggregate (
    id INT,
    s STRUCT<a INT, b INT>,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t10_aggregate VALUES
(1, row(1, 1), 10),
(2, row(1, 1), 20),
(3, row(1, 2), 30),
(4, row(2, 1), 40);

-- Ordering after GROUP BY
SELECT s, SUM(value) AS total
FROM t10_aggregate
GROUP BY s
ORDER BY s;

-- Ordering after HAVING
SELECT s, SUM(value) AS total
FROM t10_aggregate
GROUP BY s
HAVING SUM(value) > 15
ORDER BY s;

-- ==========================================
-- Test 11: Ordering After JOIN
-- ==========================================
SELECT '=== Test 11: Ordering After JOIN ===' AS test_name;

CREATE TABLE t11_left (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

CREATE TABLE t11_right (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t11_left VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1));

INSERT INTO t11_right VALUES
(1, row(1, 1)),
(2, row(1, 3)),
(3, row(2, 1));

-- Ordering after JOIN
SELECT l.s, r.s
FROM t11_left l
JOIN t11_right r ON l.id = r.id
ORDER BY l.s, r.s;

-- ==========================================
-- Test 12: Ordering After UNION
-- ==========================================
SELECT '=== Test 12: Ordering After UNION ===' AS test_name;

CREATE TABLE t12_union1 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

CREATE TABLE t12_union2 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t12_union1 VALUES
(1, row(1, 1)),
(2, row(2, 1));

INSERT INTO t12_union2 VALUES
(3, row(1, 2)),
(4, row(2, 2));

-- Ordering after UNION ALL
SELECT s FROM t12_union1
UNION ALL
SELECT s FROM t12_union2
ORDER BY s;

-- Ordering after UNION (with deduplication)
SELECT s FROM t12_union1
UNION
SELECT s FROM t12_union2
ORDER BY s;

-- ==========================================
-- Test 13: Ordering in Subqueries
-- ==========================================
SELECT '=== Test 13: Ordering in Subqueries ===' AS test_name;

CREATE TABLE t13_subquery (
    id INT,
    s STRUCT<a INT, b INT>,
    category STRING
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t13_subquery VALUES
(1, row(1, 1), 'A'),
(2, row(1, 2), 'A'),
(3, row(2, 1), 'B'),
(4, row(2, 2), 'B');

-- Ordering in subquery then filtering
SELECT * FROM (
    SELECT s, category FROM t13_subquery ORDER BY s
) sub
WHERE category = 'A';

-- Window function
SELECT 
    s,
    ROW_NUMBER() OVER (ORDER BY s) AS rn
FROM t13_subquery;

-- ==========================================
-- Test 14: Ordering in CTEs
-- ==========================================
SELECT '=== Test 14: Ordering in CTEs ===' AS test_name;

CREATE TABLE t14_cte (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t14_cte VALUES
(1, row(2, 1)),
(2, row(1, 2)),
(3, row(1, 1)),
(4, row(2, 2));

-- CTE with ordering
WITH sorted_data AS (
    SELECT s FROM t14_cte ORDER BY s
)
SELECT * FROM sorted_data;

-- Multiple CTEs
WITH 
    cte1 AS (SELECT s FROM t14_cte WHERE id <= 2),
    cte2 AS (SELECT s FROM t14_cte WHERE id > 2)
SELECT * FROM cte1
UNION ALL
SELECT * FROM cte2
ORDER BY s;

-- ==========================================
-- Test 15: Extreme Values
-- ==========================================
SELECT '=== Test 15: Extreme Values ===' AS test_name;

CREATE TABLE t15_extreme (
    id INT,
    s STRUCT<a BIGINT, b BIGINT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t15_extreme VALUES
(1, row(9223372036854775807, 1)),  -- BIGINT MAX
(2, row(-9223372036854775808, 1)), -- BIGINT MIN
(3, row(0, 0)),
(4, row(1, -9223372036854775808));

SELECT s FROM t15_extreme ORDER BY s;

-- ==========================================
-- Test 16: String Ordering (Case Sensitivity)
-- ==========================================
SELECT '=== Test 16: String Ordering (Case Sensitivity) ===' AS test_name;

CREATE TABLE t16_string (
    id INT,
    s STRUCT<str1 STRING, str2 STRING>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t16_string VALUES
(1, row('abc', 'xyz')),
(2, row('ABC', 'xyz')),
(3, row('abc', 'XYZ')),
(4, row('Abc', 'xyz'));

-- Case-sensitive ordering
SELECT s FROM t16_string ORDER BY s;

-- ==========================================
-- Test 17: DISTINCT with ORDER BY
-- ==========================================
SELECT '=== Test 17: DISTINCT with ORDER BY ===' AS test_name;

CREATE TABLE t17_distinct (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t17_distinct VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 2)),
(4, row(1, 2)),
(5, row(2, 1));

-- Ordering after DISTINCT
SELECT DISTINCT s FROM t17_distinct ORDER BY s;

-- ==========================================
-- Test 18: Multiple STRUCT Column Ordering
-- ==========================================
SELECT '=== Test 18: Multiple STRUCT Column Ordering ===' AS test_name;

CREATE TABLE t18_multi_struct (
    id INT,
    s1 STRUCT<a INT, b INT>,
    s2 STRUCT<x INT, y INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t18_multi_struct VALUES
(1, row(1, 1), row(1, 1)),
(2, row(1, 1), row(1, 2)),
(3, row(1, 2), row(1, 1)),
(4, row(1, 1), row(2, 1));

-- Order by s1 first, then by s2
SELECT s1, s2 FROM t18_multi_struct ORDER BY s1, s2;

-- Order by s2 first, then by s1
SELECT s1, s2 FROM t18_multi_struct ORDER BY s2, s1;

-- ==========================================
-- Test 19: Performance Test (Large Dataset)
-- ==========================================
SELECT '=== Test 19: Performance Test ===' AS test_name;

CREATE TABLE t19_performance (
    id INT,
    s STRUCT<a INT, b INT, c INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- Insert 10000 rows
INSERT INTO t19_performance
SELECT 
    number,
    row(
        number % 100,
        number % 50,
        number % 25
    )
FROM TABLE(generate_series(1, 10000));

-- Test sorting performance
SELECT s FROM t19_performance ORDER BY s LIMIT 100;

-- ==========================================
-- Test 20: STRUCT Comparison in WHERE Clause
-- ==========================================
SELECT '=== Test 20: STRUCT Comparison in WHERE Clause ===' AS test_name;

CREATE TABLE t20_where (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");

INSERT INTO t20_where VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1));

-- STRUCT comparison in WHERE clause
SELECT s FROM t20_where 
WHERE s > row(1, 1) 
ORDER BY s;

-- Equality comparison in WHERE clause
SELECT s FROM t20_where 
WHERE s = row(2, 1)
ORDER BY s;

-- Range query in WHERE clause
SELECT s FROM t20_where 
WHERE s BETWEEN row(1, 2) AND row(2, 2)
ORDER BY s;