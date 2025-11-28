-- name: test_order_by_struct_comprehensive
DROP DATABASE IF EXISTS test_order_by_struct_comprehensive;
-- result:
-- !result
CREATE DATABASE test_order_by_struct_comprehensive;
-- result:
-- !result
USE test_order_by_struct_comprehensive;
-- result:
-- !result
CREATE TABLE t1_basic (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t1_basic VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(1, 1));
-- result:
-- !result
SELECT s FROM t1_basic ORDER BY s ASC;
-- result:
{"a":1,"b":1}
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
{"a":2,"b":2}
-- !result
SELECT s FROM t1_basic ORDER BY s DESC;
-- result:
{"a":2,"b":2}
{"a":2,"b":1}
{"a":1,"b":2}
{"a":1,"b":1}
{"a":1,"b":1}
-- !result
SELECT s FROM t1_basic WHERE id IN (1, 2, 5) ORDER BY s;
-- result:
{"a":1,"b":1}
{"a":1,"b":1}
{"a":1,"b":2}
-- !result
SELECT '=== Test 2: NULL Value Handling ===' AS test_name;
-- result:
=== Test 2: NULL Value Handling ===
-- !result
CREATE TABLE t2_nulls (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t2_nulls VALUES
(1, row(1, 1)),
(2, row(1, NULL)),
(3, row(NULL, 1)),
(4, row(NULL, NULL)),
(5, NULL);
-- result:
-- !result
SELECT s FROM t2_nulls ORDER BY s ASC NULLS FIRST;
-- result:
None
{"a":null,"b":null}
{"a":null,"b":1}
{"a":1,"b":null}
{"a":1,"b":1}
-- !result
SELECT s FROM t2_nulls ORDER BY s ASC NULLS LAST;
-- result:
{"a":1,"b":1}
{"a":1,"b":null}
{"a":null,"b":1}
{"a":null,"b":null}
None
-- !result
SELECT s FROM t2_nulls WHERE id BETWEEN 1 AND 4 ORDER BY s;
-- result:
{"a":null,"b":null}
{"a":null,"b":1}
{"a":1,"b":null}
{"a":1,"b":1}
-- !result
SELECT '=== Test 3: Different Data Types ===' AS test_name;
-- result:
=== Test 3: Different Data Types ===
-- !result
CREATE TABLE t3_mixed_types (
    id INT,
    s STRUCT<name STRING, age INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t3_mixed_types VALUES
(1, row('Alice', 30)),
(2, row('Alice', 25)),
(3, row('Bob', 30)),
(4, row('Bob', 25)),
(5, row('Alice', 30));
-- result:
-- !result
SELECT s FROM t3_mixed_types ORDER BY s;
-- result:
{"name":"Alice","age":25}
{"name":"Alice","age":30}
{"name":"Alice","age":30}
{"name":"Bob","age":25}
{"name":"Bob","age":30}
-- !result
CREATE TABLE t3_float (
    id INT,
    s STRUCT<x DOUBLE, y DOUBLE>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t3_float VALUES
(1, row(1.1, 2.2)),
(2, row(1.1, 2.1)),
(3, row(1.2, 2.2)),
(4, row(1.0, 2.0));
-- result:
-- !result
SELECT s FROM t3_float ORDER BY s;
-- result:
{"x":1,"y":2}
{"x":1.1,"y":2.1}
{"x":1.1,"y":2.2}
{"x":1.2,"y":2.2}
-- !result
CREATE TABLE t3_date (
    id INT,
    s STRUCT<event_date DATE, event_time DATETIME>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t3_date VALUES
(1, row('2024-01-01', '2024-01-01 10:00:00')),
(2, row('2024-01-01', '2024-01-01 09:00:00')),
(3, row('2024-01-02', '2024-01-02 10:00:00')),
(4, row('2024-01-01', '2024-01-01 10:00:00'));
-- result:
-- !result
SELECT s FROM t3_date ORDER BY s;
-- result:
{"event_date":"2024-01-01","event_time":"2024-01-01 09:00:00"}
{"event_date":"2024-01-01","event_time":"2024-01-01 10:00:00"}
{"event_date":"2024-01-01","event_time":"2024-01-01 10:00:00"}
{"event_date":"2024-01-02","event_time":"2024-01-02 10:00:00"}
-- !result
SELECT '=== Test 4: Nested STRUCT ===' AS test_name;
-- result:
=== Test 4: Nested STRUCT ===
-- !result
CREATE TABLE t4_nested (
    id INT,
    s STRUCT<outer1 INT, inner1 STRUCT<a INT, b INT>>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t4_nested VALUES
(1, row(1, row(1, 1))),
(2, row(1, row(1, 2))),
(3, row(1, row(2, 1))),
(4, row(2, row(1, 1)));
-- result:
-- !result
SELECT s FROM t4_nested ORDER BY s;
-- result:
{"outer1":1,"inner1":{"a":1,"b":1}}
{"outer1":1,"inner1":{"a":1,"b":2}}
{"outer1":1,"inner1":{"a":2,"b":1}}
{"outer1":2,"inner1":{"a":1,"b":1}}
-- !result
SELECT s FROM t4_nested WHERE id <= 3 ORDER BY s;
-- result:
{"outer1":1,"inner1":{"a":1,"b":1}}
{"outer1":1,"inner1":{"a":1,"b":2}}
{"outer1":1,"inner1":{"a":2,"b":1}}
-- !result
SELECT '=== Test 5: Mixed Column Ordering ===' AS test_name;
-- result:
=== Test 5: Mixed Column Ordering ===
-- !result
CREATE TABLE t5_mixed_columns (
    id INT,
    s STRUCT<a INT, b INT>,
    score INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t5_mixed_columns VALUES
(1, row(1, 1), 100),
(2, row(1, 2), 90),
(3, row(2, 1), 100),
(4, row(1, 1), 95);
-- result:
-- !result
SELECT s, score FROM t5_mixed_columns ORDER BY s, score;
-- result:
{"a":1,"b":1}	95
{"a":1,"b":1}	100
{"a":1,"b":2}	90
{"a":2,"b":1}	100
-- !result
SELECT s, score FROM t5_mixed_columns ORDER BY score, s;
-- result:
{"a":1,"b":2}	90
{"a":1,"b":1}	95
{"a":1,"b":1}	100
{"a":2,"b":1}	100
-- !result
SELECT '=== Test 6: Empty and Single-field STRUCT ===' AS test_name;
-- result:
=== Test 6: Empty and Single-field STRUCT ===
-- !result
CREATE TABLE t6_single_field (
    id INT,
    s STRUCT<value INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t6_single_field VALUES
(1, row(3)),
(2, row(1)),
(3, row(2)),
(4, row(1));
-- result:
-- !result
SELECT s FROM t6_single_field ORDER BY s;
-- result:
{"value":1}
{"value":1}
{"value":2}
{"value":3}
-- !result
SELECT '=== Test 7: STRUCT with Many Fields ===' AS test_name;
-- result:
=== Test 7: STRUCT with Many Fields ===
-- !result
CREATE TABLE t7_many_fields (
    id INT,
    s STRUCT<f1 INT, f2 INT, f3 INT, f4 INT, f5 INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t7_many_fields VALUES
(1, row(1, 1, 1, 1, 1)),
(2, row(1, 1, 1, 1, 2)),
(3, row(1, 1, 1, 2, 1)),
(4, row(1, 1, 2, 1, 1)),
(5, row(1, 2, 1, 1, 1)),
(6, row(2, 1, 1, 1, 1));
-- result:
-- !result
SELECT s FROM t7_many_fields ORDER BY s;
-- result:
{"f1":1,"f2":1,"f3":1,"f4":1,"f5":1}
{"f1":1,"f2":1,"f3":1,"f4":1,"f5":2}
{"f1":1,"f2":1,"f3":1,"f4":2,"f5":1}
{"f1":1,"f2":1,"f3":2,"f4":1,"f5":1}
{"f1":1,"f2":2,"f3":1,"f4":1,"f5":1}
{"f1":2,"f2":1,"f3":1,"f4":1,"f5":1}
-- !result
SELECT '=== Test 8: Sort Stability with Same Values ===' AS test_name;
-- result:
=== Test 8: Sort Stability with Same Values ===
-- !result
CREATE TABLE t8_stability (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t8_stability VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 1)),
(4, row(2, 2)),
(5, row(2, 2));
-- result:
-- !result
SELECT id, s FROM t8_stability ORDER BY s, id;
-- result:
1	{"a":1,"b":1}
2	{"a":1,"b":1}
3	{"a":1,"b":1}
4	{"a":2,"b":2}
5	{"a":2,"b":2}
-- !result
SELECT id, s FROM t8_stability ORDER BY s, id;
-- result:
1	{"a":1,"b":1}
2	{"a":1,"b":1}
3	{"a":1,"b":1}
4	{"a":2,"b":2}
5	{"a":2,"b":2}
-- !result
SELECT '=== Test 9: LIMIT and OFFSET ===' AS test_name;
-- result:
=== Test 9: LIMIT and OFFSET ===
-- !result
CREATE TABLE t9_limit (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t9_limit VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1)),
(6, row(3, 2));
-- result:
-- !result
SELECT s FROM t9_limit ORDER BY s LIMIT 3;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
-- !result
SELECT s FROM t9_limit ORDER BY s LIMIT 3 OFFSET 2;
-- result:
{"a":2,"b":1}
{"a":2,"b":2}
{"a":3,"b":1}
-- !result
SELECT '=== Test 10: Ordering After Aggregation ===' AS test_name;
-- result:
=== Test 10: Ordering After Aggregation ===
-- !result
CREATE TABLE t10_aggregate (
    id INT,
    s STRUCT<a INT, b INT>,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t10_aggregate VALUES
(1, row(1, 1), 10),
(2, row(1, 1), 20),
(3, row(1, 2), 30),
(4, row(2, 1), 40);
-- result:
-- !result
SELECT s, SUM(value) AS total
FROM t10_aggregate
GROUP BY s
ORDER BY s;
-- result:
{"a":1,"b":1}	30
{"a":1,"b":2}	30
{"a":2,"b":1}	40
-- !result
SELECT s, SUM(value) AS total
FROM t10_aggregate
GROUP BY s
HAVING SUM(value) > 15
ORDER BY s;
-- result:
{"a":1,"b":1}	30
{"a":1,"b":2}	30
{"a":2,"b":1}	40
-- !result
SELECT '=== Test 11: Ordering After JOIN ===' AS test_name;
-- result:
=== Test 11: Ordering After JOIN ===
-- !result
CREATE TABLE t11_left (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE t11_right (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t11_left VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1));
-- result:
-- !result
INSERT INTO t11_right VALUES
(1, row(1, 1)),
(2, row(1, 3)),
(3, row(2, 1));
-- result:
-- !result
SELECT l.s, r.s
FROM t11_left l
JOIN t11_right r ON l.id = r.id
ORDER BY l.s, r.s;
-- result:
{"a":1,"b":1}	{"a":1,"b":1}
{"a":1,"b":2}	{"a":1,"b":3}
{"a":2,"b":1}	{"a":2,"b":1}
-- !result
SELECT '=== Test 12: Ordering After UNION ===' AS test_name;
-- result:
=== Test 12: Ordering After UNION ===
-- !result
CREATE TABLE t12_union1 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE t12_union2 (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t12_union1 VALUES
(1, row(1, 1)),
(2, row(2, 1));
-- result:
-- !result
INSERT INTO t12_union2 VALUES
(3, row(1, 2)),
(4, row(2, 2));
-- result:
-- !result
SELECT s FROM t12_union1
UNION ALL
SELECT s FROM t12_union2
ORDER BY s;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
{"a":2,"b":2}
-- !result
SELECT s FROM t12_union1
UNION
SELECT s FROM t12_union2
ORDER BY s;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
{"a":2,"b":2}
-- !result
SELECT '=== Test 13: Ordering in Subqueries ===' AS test_name;
-- result:
=== Test 13: Ordering in Subqueries ===
-- !result
CREATE TABLE t13_subquery (
    id INT,
    s STRUCT<a INT, b INT>,
    category STRING
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t13_subquery VALUES
(1, row(1, 1), 'A'),
(2, row(1, 2), 'A'),
(3, row(2, 1), 'B'),
(4, row(2, 2), 'B');
-- result:
-- !result
SELECT * FROM (
    SELECT s, category FROM t13_subquery ORDER BY s
) sub
WHERE category = 'A';
-- result:
{"a":1,"b":1}	A
{"a":1,"b":2}	A
-- !result
SELECT 
    s,
    ROW_NUMBER() OVER (ORDER BY s) AS rn
FROM t13_subquery;
-- result:
{"a":1,"b":1}	1
{"a":1,"b":2}	2
{"a":2,"b":1}	3
{"a":2,"b":2}	4
-- !result
SELECT '=== Test 14: Ordering in CTEs ===' AS test_name;
-- result:
=== Test 14: Ordering in CTEs ===
-- !result
CREATE TABLE t14_cte (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t14_cte VALUES
(1, row(2, 1)),
(2, row(1, 2)),
(3, row(1, 1)),
(4, row(2, 2));
-- result:
-- !result
WITH sorted_data AS (
    SELECT s FROM t14_cte ORDER BY s
)
SELECT * FROM sorted_data;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
{"a":2,"b":2}
-- !result
WITH 
    cte1 AS (SELECT s FROM t14_cte WHERE id <= 2),
    cte2 AS (SELECT s FROM t14_cte WHERE id > 2)
SELECT * FROM cte1
UNION ALL
SELECT * FROM cte2
ORDER BY s;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
{"a":2,"b":2}
-- !result
SELECT '=== Test 15: Extreme Values ===' AS test_name;
-- result:
=== Test 15: Extreme Values ===
-- !result
CREATE TABLE t15_extreme (
    id INT,
    s STRUCT<a BIGINT, b BIGINT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t15_extreme VALUES
(1, row(9223372036854775807, 1)),  -- BIGINT MAX
(2, row(-9223372036854775808, 1)), -- BIGINT MIN
(3, row(0, 0)),
(4, row(1, -9223372036854775808));
-- result:
-- !result
SELECT s FROM t15_extreme ORDER BY s;
-- result:
{"a":-9223372036854775808,"b":1}
{"a":0,"b":0}
{"a":1,"b":-9223372036854775808}
{"a":9223372036854775807,"b":1}
-- !result
SELECT '=== Test 16: String Ordering (Case Sensitivity) ===' AS test_name;
-- result:
=== Test 16: String Ordering (Case Sensitivity) ===
-- !result
CREATE TABLE t16_string (
    id INT,
    s STRUCT<str1 STRING, str2 STRING>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t16_string VALUES
(1, row('abc', 'xyz')),
(2, row('ABC', 'xyz')),
(3, row('abc', 'XYZ')),
(4, row('Abc', 'xyz'));
-- result:
-- !result
SELECT s FROM t16_string ORDER BY s;
-- result:
{"str1":"ABC","str2":"xyz"}
{"str1":"Abc","str2":"xyz"}
{"str1":"abc","str2":"XYZ"}
{"str1":"abc","str2":"xyz"}
-- !result
SELECT '=== Test 17: DISTINCT with ORDER BY ===' AS test_name;
-- result:
=== Test 17: DISTINCT with ORDER BY ===
-- !result
CREATE TABLE t17_distinct (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t17_distinct VALUES
(1, row(1, 1)),
(2, row(1, 1)),
(3, row(1, 2)),
(4, row(1, 2)),
(5, row(2, 1));
-- result:
-- !result
SELECT DISTINCT s FROM t17_distinct ORDER BY s;
-- result:
{"a":1,"b":1}
{"a":1,"b":2}
{"a":2,"b":1}
-- !result
SELECT '=== Test 18: Multiple STRUCT Column Ordering ===' AS test_name;
-- result:
=== Test 18: Multiple STRUCT Column Ordering ===
-- !result
CREATE TABLE t18_multi_struct (
    id INT,
    s1 STRUCT<a INT, b INT>,
    s2 STRUCT<x INT, y INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t18_multi_struct VALUES
(1, row(1, 1), row(1, 1)),
(2, row(1, 1), row(1, 2)),
(3, row(1, 2), row(1, 1)),
(4, row(1, 1), row(2, 1));
-- result:
-- !result
SELECT s1, s2 FROM t18_multi_struct ORDER BY s1, s2;
-- result:
{"a":1,"b":1}	{"x":1,"y":1}
{"a":1,"b":1}	{"x":1,"y":2}
{"a":1,"b":1}	{"x":2,"y":1}
{"a":1,"b":2}	{"x":1,"y":1}
-- !result
SELECT s1, s2 FROM t18_multi_struct ORDER BY s2, s1;
-- result:
{"a":1,"b":1}	{"x":1,"y":1}
{"a":1,"b":2}	{"x":1,"y":1}
{"a":1,"b":1}	{"x":1,"y":2}
{"a":1,"b":1}	{"x":2,"y":1}
-- !result
SELECT '=== Test 19: Performance Test ===' AS test_name;
-- result:
=== Test 19: Performance Test ===
-- !result
CREATE TABLE t19_performance (
    id INT,
    s STRUCT<a INT, b INT, c INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t19_performance
SELECT 
    number,
    row(
        number % 100,
        number % 50,
        number % 25
    )
FROM TABLE(generate_series(1, 10000));
-- result:
E: (1064, "Getting analyzing error. Detail message: Column 'number' cannot be resolved.")
-- !result
SELECT s FROM t19_performance ORDER BY s LIMIT 100;
-- result:
-- !result
SELECT '=== Test 20: STRUCT Comparison in WHERE Clause ===' AS test_name;
-- result:
=== Test 20: STRUCT Comparison in WHERE Clause ===
-- !result
CREATE TABLE t20_where (
    id INT,
    s STRUCT<a INT, b INT>
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t20_where VALUES
(1, row(1, 1)),
(2, row(1, 2)),
(3, row(2, 1)),
(4, row(2, 2)),
(5, row(3, 1));
-- result:
-- !result
SELECT s FROM t20_where 
WHERE s > row(1, 1) 
ORDER BY s;
-- result:
E: (1064, 'Getting analyzing error from line 2, column 6 to line 2, column 18. Detail message: Column type struct<a int(11), b int(11)> does not support binary predicate operation with type struct<col1 tinyint(4), col2 tinyint(4)>.')
-- !result
SELECT s FROM t20_where 
WHERE s = row(2, 1)
ORDER BY s;
-- result:
{"a":2,"b":1}
-- !result
SELECT s FROM t20_where 
WHERE s BETWEEN row(1, 2) AND row(2, 2)
ORDER BY s;
-- result:
E: (1064, "Getting analyzing error from line 2, column 8 to line 2, column 38. Detail message: HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate.")
-- !result