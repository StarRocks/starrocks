-- name: test_full_outer_join_with_using
DROP DATABASE IF EXISTS test_full_outer_join_with_using;
CREATE DATABASE test_full_outer_join_using;
USE test_full_outer_join_using;

-- Create simple test tables with NULL values in USING columns
CREATE TABLE t1 (
    k1 INT,
    k2 INT,
    v1 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE t2 (
    k1 INT,
    k2 INT,
    v2 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE t3 (
    k1 INT,
    k2 INT,
    v3 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE t4 (
    k1 INT,
    k2 INT,
    v4 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- Insert test data with NULLs in USING columns
INSERT INTO t1 VALUES
    (1, 10, 'a1'),
    (2, 20, 'a2'),
    (3, NULL, 'a3'),
    (NULL, 40, 'a4'),
    (NULL, NULL, 'a5');

INSERT INTO t2 VALUES
    (1, 10, 'b1'),
    (4, 40, 'b2'),
    (5, NULL, 'b3'),
    (NULL, 60, 'b4'),
    (NULL, NULL, 'b5');

INSERT INTO t3 VALUES
    (1, 10, 'c1'),
    (2, 20, 'c2'),
    (6, 60, 'c3'),
    (NULL, 70, 'c4'),
    (NULL, NULL, 'c5');

INSERT INTO t4 VALUES
    (1, 10, 'd1'),
    (7, 70, 'd2'),
    (8, NULL, 'd3'),
    (NULL, 80, 'd4'),
    (NULL, NULL, 'd5');

-- ===============================================
-- Test 1: Basic FULL OUTER JOIN with single USING column
-- ===============================================
SELECT k1, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1) 
ORDER BY k1, v1, v2;

-- ===============================================
-- Test 2: FULL OUTER JOIN with multiple USING columns
-- ===============================================
SELECT k1, k2, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
ORDER BY k1, k2, v1, v2;

-- ===============================================
-- Test 3: FULL OUTER JOIN with SELECT *
-- ===============================================
SELECT * 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
ORDER BY k1, k2, v1, v2;

-- ===============================================
-- Test 4: FULL OUTER JOIN with WHERE on USING column
-- ===============================================
SELECT k1, k2, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
WHERE k1 IS NOT NULL
ORDER BY k1, k2;

-- ===============================================
-- Test 5: FULL OUTER JOIN with GROUP BY on USING column
-- ===============================================
SELECT k1, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM t1 FULL OUTER JOIN t2 USING(k1) 
GROUP BY k1 
ORDER BY k1;

-- ===============================================
-- Test 6: FULL OUTER JOIN with HAVING on USING column
-- ===============================================
SELECT k1, COUNT(*) as cnt
FROM t1 FULL OUTER JOIN t2 USING(k1) 
GROUP BY k1 
HAVING k1 > 1 OR k1 IS NULL
ORDER BY k1;

-- ===============================================
-- Test 7: Three-table FULL OUTER JOIN (all FULL OUTER)
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
         FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 8: Three-table with LEFT then FULL OUTER
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 LEFT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 9: Three-table with FULL OUTER then LEFT
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        LEFT JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 10: Three-table with INNER then FULL OUTER
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 11: Four-table FULL OUTER JOIN (all FULL OUTER)
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 12: Four-table mixed: LEFT, FULL OUTER, FULL OUTER
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 13: Four-table mixed: FULL OUTER, INNER, FULL OUTER
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        INNER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 14: Four-table mixed: FULL OUTER, FULL OUTER, RIGHT
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        RIGHT JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 15: Four-table all different: LEFT, RIGHT, FULL OUTER
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1, k2)
        RIGHT JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 16: FULL OUTER JOIN with aggregation on USING columns
-- ===============================================
SELECT k1, k2, SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) as t1_count,
       SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) as t2_count
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
GROUP BY k1, k2
ORDER BY k1, k2;

-- ===============================================
-- Test 17: Four-table with complex WHERE clause
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 = 1 AND k2 = 10) OR (v1 IS NULL AND v2 IS NULL)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 18: Four-table with GROUP BY and HAVING
-- ===============================================
SELECT k1, 
       COUNT(*) as total_rows,
       COUNT(v1) as t1_rows,
       COUNT(v2) as t2_rows,
       COUNT(v3) as t3_rows,
       COUNT(v4) as t4_rows
FROM t1 FULL OUTER JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
GROUP BY k1
HAVING COUNT(*) > 1
ORDER BY k1;

-- ===============================================
-- Test 19: Four-table with ORDER BY on USING and non-USING columns
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1 DESC, k2 ASC, v1, v2;

-- ===============================================
-- Test 20: FULL OUTER JOIN with DISTINCT on USING column
-- ===============================================
SELECT DISTINCT k1, k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 21: Four-table with subquery in WHERE
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t1 WHERE k1 IS NOT NULL)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 22: FULL OUTER JOIN with CASE expression on USING column
-- ===============================================
SELECT k1, k2,
       CASE WHEN k1 IS NULL THEN 'NULL_K1'
            WHEN k1 < 3 THEN 'SMALL'
            ELSE 'LARGE' END as k1_category,
       v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 23: Four-table with multiple aggregations
-- ===============================================
SELECT k1, k2,
       COUNT(*) as cnt,
       COUNT(DISTINCT v1) as dist_v1,
       COUNT(DISTINCT v2) as dist_v2,
       MAX(v3) as max_v3,
       MIN(v4) as min_v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
GROUP BY k1, k2
ORDER BY k1, k2;

-- ===============================================
-- Test 24: FULL OUTER JOIN with LIMIT
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2
LIMIT 10;

-- ===============================================
-- Test 25: Four-table with COALESCE on non-USING columns
-- ===============================================
SELECT k1, k2,
       COALESCE(v1, v2, v3, v4) as value
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2;


-- ===============================================
-- Test 26: Complex four-table with nested conditions
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 > 0 OR k1 IS NULL) AND (k2 < 50 OR k2 IS NULL)
ORDER BY k1, k2;

-- ===============================================
-- Test 27: Four-table with GROUP BY ROLLUP
-- ===============================================
SELECT k1, k2, COUNT(*) as cnt
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
GROUP BY ROLLUP(k1, k2)
ORDER BY k1, k2;


-- ===============================================
-- Test 29: Four-table FULL OUTER with single USING column
-- ===============================================
SELECT k1, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
ORDER BY k1, v1, v2, v3, v4;

-- ===============================================
-- Test 30: Complex aggregation with HAVING on aggregated values
-- ===============================================
SELECT k1, 
       COUNT(*) as total,
       SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END) as non_null_values
FROM t1 FULL OUTER JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
GROUP BY k1
HAVING non_null_values >= 2
ORDER BY k1;

-- ===============================================
-- Test 31: INNER, FULL OUTER, LEFT, RIGHT mixed pattern
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        LEFT JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 32: RIGHT, FULL OUTER, INNER, FULL OUTER pattern
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 RIGHT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- ===============================================
-- Test 33: FULL OUTER with subquery on left side
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM (SELECT k1, k2, v1 FROM t1 WHERE k1 IS NOT NULL) as sub1
     FULL OUTER JOIN t2 USING(k1, k2)
     FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 34: FULL OUTER with subquery on right side
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN (SELECT k1, k2, v3 FROM t3 WHERE k2 > 0) as sub3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- ===============================================
-- Test 35: FULL OUTER with CTE
-- ===============================================
WITH cte1 AS (
    SELECT k1, k2, v1 FROM t1 WHERE k1 <= 3 OR k1 IS NULL
),
cte2 AS (
    SELECT k1, k2, v2 FROM t2 WHERE k1 >= 3 OR k1 IS NULL
)
SELECT k1, k2, v1, v2
FROM cte1 FULL OUTER JOIN cte2 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 36: Complex CTE with multiple FULL OUTER JOINs
-- ===============================================
WITH base_join AS (
    SELECT k1, k2, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
)
SELECT k1, k2, v1, v2, v3
FROM base_join FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 37: Nested FULL OUTER JOINs with aggregation
-- ===============================================
SELECT k1, 
       MAX(v1) as max_v1, 
       MIN(v2) as min_v2, 
       AVG(CAST(k2 AS DOUBLE)) as avg_k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1
HAVING MAX(v1) IS NOT NULL OR MIN(v2) IS NOT NULL
ORDER BY k1;

-- ===============================================
-- Test 38: FULL OUTER with UNION
-- ===============================================
SELECT k1, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1)
WHERE k1 = 1
UNION ALL
SELECT k1, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1)
WHERE k1 = 2
ORDER BY k1, v1, v2;

-- ===============================================
-- Test 39: Five-table complex JOIN pattern (mixed types)
-- ===============================================
SELECT k1, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        RIGHT JOIN t4 USING(k1)
        FULL OUTER JOIN (SELECT k1, 'extra' as v_extra FROM t1 WHERE k1 < 5) sub USING(k1)
ORDER BY k1;

-- ===============================================
-- Test 40: FULL OUTER with window functions
-- ===============================================
SELECT k1, k2, v1, v2,
       ROW_NUMBER() OVER (PARTITION BY k1 ORDER BY k2) as rn,
       COUNT(*) OVER (PARTITION BY k1) as cnt_per_k1
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 42: Complex WHERE with multiple conditions on USING columns
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 BETWEEN 1 AND 5) OR 
      (k1 IS NULL AND k2 IS NOT NULL) OR
      (k2 > 50 AND k1 IS NOT NULL)
ORDER BY k1, k2;

-- ===============================================
-- Test 43: FULL OUTER with EXISTS subquery
-- ===============================================
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE EXISTS (
    SELECT 1 FROM t3 WHERE t3.k1 = k1 AND t3.k2 = k2
)
ORDER BY k1, k2;

-- ===============================================
-- Test 44: FULL OUTER with NOT EXISTS subquery
-- ===============================================
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE NOT EXISTS (
    SELECT 1 FROM t3 WHERE t3.k1 = k1
)
ORDER BY k1, k2;

-- ===============================================
-- Test 45: FULL OUTER with IN subquery on USING column
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
WHERE k1 IN (1, 2, 3) OR k1 IS NULL
ORDER BY k1, k2;

-- ===============================================
-- Test 46: Complex aggregation with multiple GROUP BY columns
-- ===============================================
SELECT k1, k2,
       COUNT(DISTINCT CASE WHEN v1 IS NOT NULL THEN v1 END) as dist_v1,
       COUNT(DISTINCT CASE WHEN v2 IS NOT NULL THEN v2 END) as dist_v2,
       CONCAT_WS(',', v1, v2, v3) as combined_values
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1, k2
HAVING COUNT(*) > 1
ORDER BY k1, k2;

-- ===============================================
-- Test 47: FULL OUTER with CASE in SELECT
-- ===============================================
SELECT k1, k2,
       CASE 
         WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 'BOTH'
         WHEN v1 IS NOT NULL THEN 'T1_ONLY'
         WHEN v2 IS NOT NULL THEN 'T2_ONLY'
         ELSE 'NEITHER'
       END as match_status,
       COALESCE(v1, v2) as value
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 48: Four-table with nested subquery in WHERE
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        INNER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE k1 IN (
    SELECT k1 
    FROM t1 FULL OUTER JOIN t2 USING(k1) 
    WHERE k1 IS NOT NULL 
    GROUP BY k1 
    HAVING COUNT(*) > 0
)
ORDER BY k1, k2;

-- ===============================================
-- Test 49: FULL OUTER with LEFT SEMI JOIN
-- ===============================================
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t3 WHERE k1 IS NOT NULL)
ORDER BY k1, k2;

-- ===============================================
-- Test 50: Mixed pattern: INNER, LEFT, FULL, RIGHT, FULL
-- ===============================================
SELECT k1, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1)
        LEFT JOIN (SELECT k1, v3 FROM t3 WHERE k1 < 10) sub3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
ORDER BY k1;

-- ===============================================
-- Test 51: FULL OUTER with derived table
-- ===============================================
SELECT k1, k2, sum_v1, v2
FROM (
    SELECT k1, k2, SUM(CAST(SUBSTRING(v1, 2) AS INT)) as sum_v1
    FROM t1
    GROUP BY k1, k2
) derived
FULL OUTER JOIN t2 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 52: FULL OUTER JOIN with join condition in subquery
-- ===============================================
SELECT outer_k1, COUNT(*) as cnt
FROM (
    SELECT k1 as outer_k1, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(k1)
    WHERE k1 IS NOT NULL
) sub
GROUP BY outer_k1
ORDER BY outer_k1;

-- ===============================================
-- Test 53: FULL OUTER with LATERAL view (if supported)
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
WHERE k1 IS NOT NULL OR k2 IS NOT NULL
ORDER BY k1, k2
LIMIT 20;

-- ===============================================
-- Test 54: Complex pattern with aggregation in subquery
-- ===============================================
SELECT k1, max_v1, v2, v3
FROM (
    SELECT k1, MAX(v1) as max_v1
    FROM t1
    GROUP BY k1
) agg_t1
FULL OUTER JOIN t2 USING(k1)
FULL OUTER JOIN t3 USING(k1)
ORDER BY k1;

-- ===============================================
-- Test 55: Four-table with GROUP BY CUBE
-- ===============================================
SELECT k1, k2, COUNT(*) as cnt, COUNT(v1) as cnt_v1
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 56: FULL OUTER with multiple aggregations and filtering
-- ===============================================
SELECT k1,
       COUNT(*) as total,
       COUNT(DISTINCT v1) as distinct_v1,
       COUNT(DISTINCT v2) as distinct_v2,
       GROUP_CONCAT(DISTINCT v3 ORDER BY v3 SEPARATOR ',') as v3_list
FROM t1 FULL OUTER JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
GROUP BY k1
HAVING total >= 2
ORDER BY k1;

-- ===============================================
-- Test 57: Complex nested JOIN pattern
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM (
    SELECT k1, k2, v1, v2
    FROM t1 LEFT JOIN t2 USING(k1, k2)
) left_result
FULL OUTER JOIN (
    SELECT k1, k2, v3, v4
    FROM t3 RIGHT JOIN t4 USING(k1, k2)
) right_result USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 58: FULL OUTER with arithmetic on USING columns
-- ===============================================
SELECT k1, k2, 
       k1 + k2 as sum_k,
       k1 * k2 as product_k,
       v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IS NOT NULL AND k2 IS NOT NULL
ORDER BY k1, k2;

-- ===============================================
-- Test 60: FULL OUTER with string functions on USING column
-- ===============================================
SELECT k1, k2,
       CONCAT('K1:', CAST(k1 AS STRING)) as k1_str,
       LENGTH(v1) as len_v1,
       UPPER(v2) as v2_upper
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IS NOT NULL
ORDER BY k1, k2;

-- ===============================================
-- Test 61: Four-table zigzag pattern (INNER-FULL-INNER-FULL)
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
ORDER BY k1, k2;

-- ===============================================
-- Test 62: FULL OUTER with ORDER BY expression
-- ===============================================
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY COALESCE(k1, 999), COALESCE(k2, 999), v1;

-- ===============================================
-- Test 63: FULL OUTER with HAVING on USING column expressions
-- ===============================================
SELECT k1, COUNT(*) as cnt, AVG(k2) as avg_k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1
HAVING k1 > 0 AND AVG(k2) > 10
ORDER BY k1;

-- ===============================================
-- Test 64: Complex five-table pattern
-- ===============================================
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        LEFT JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
        RIGHT JOIN (SELECT k1, k2, v1 as v1_dup FROM t1) dup USING(k1, k2)
ORDER BY k1, k2
LIMIT 15;

-- ===============================================
-- Test 66: FULL OUTER with NULL-safe equal simulation
-- ===============================================
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE (k1 = 1 OR k1 IS NULL) AND (k2 = 10 OR k2 IS NULL)
ORDER BY k1, k2;


-- ===============================================
-- Test 68: FULL OUTER with complex CASE aggregation
-- ===============================================
SELECT k1,
       SUM(CASE 
           WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 2
           WHEN v1 IS NOT NULL OR v2 IS NOT NULL THEN 1
           ELSE 0
       END) as match_score,
       COUNT(*) as total
FROM t1 FULL OUTER JOIN t2 USING(k1)
GROUP BY k1
ORDER BY match_score DESC, k1;

-- ===============================================
-- Test 69: Four-table with DISTINCT and complex expressions
-- ===============================================
SELECT DISTINCT 
       COALESCE(k1, -1) as k1_coalesced,
       COALESCE(k2, -1) as k2_coalesced,
       CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END as tables_present
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1_coalesced, k2_coalesced;

-- ===============================================
-- Test 70: FULL OUTER followed by LEFT SEMI JOIN
-- ===============================================
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t3 WHERE v3 IS NOT NULL)
   OR k1 IS NULL
ORDER BY k1, k2;

-- ===============================================
-- Tests with VALUES: Basic scenarios
-- ===============================================

-- Test 71: Simple INNER JOIN USING with VALUES
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);

-- Test 72: Multiple USING keys with VALUES
SELECT * FROM
    (VALUES (1, 'a', 2)) AS t(k1, v1, k2) JOIN
    (VALUES (1, 'b', 2)) AS u(k1, v2, k2) USING (k1, k2);

-- Test 73: LEFT OUTER JOIN with VALUES
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) LEFT JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 74: RIGHT OUTER JOIN with VALUES
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) RIGHT JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 75: FULL OUTER JOIN with VALUES - basic
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 76: FULL OUTER JOIN with VALUES - multiple rows
SELECT * FROM
    (VALUES (0, 1, 2), (3, 6, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, z1, x2, z2;

-- Test 77: FULL OUTER JOIN with VALUES - with NULLs
SELECT y, x1, x2 FROM
    (VALUES (0, 1, 2), (3, 6, 5), (3, NULL, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, x2;

-- Test 78: Type coercion - TINYINT and INT
SELECT * FROM
    (VALUES (CAST(1 AS TINYINT), 'a')) AS t(k, v1) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);

-- Test 79: Type coercion - SMALLINT and BIGINT
SELECT * FROM
    (VALUES (CAST(1 AS SMALLINT), 'a')) AS t(k, v1) JOIN
    (VALUES (CAST(1 AS BIGINT), 'b')) AS u(k, v2) USING (k);

-- Test 80: Alternate column orders with VALUES
SELECT * FROM
    (VALUES ('a', 1)) AS t(v1, k) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);

-- ===============================================
-- Tests mixing VALUES with table relations
-- ===============================================

-- Test 82: VALUES LEFT JOIN with table
SELECT * FROM
    (VALUES (1, 'inline')) AS inline_t(k1, val) LEFT JOIN t1 USING(k1)
ORDER BY k1, val;

-- Test 83: Table FULL OUTER JOIN with VALUES
SELECT * FROM
    t1 FULL OUTER JOIN
    (VALUES (10, 20, 'inline')) AS inline_t(k1, k2, val) USING(k1, k2)
ORDER BY k1, k2;

-- Test 84: Complex chain: table -> VALUES -> table
SELECT k1, k2, v1, val, v2 FROM
    t1 FULL OUTER JOIN
    (VALUES (1, 10, 'inline1'), (2, 20, 'inline2')) AS inline_t(k1, k2, val) USING(k1, k2)
    LEFT JOIN t2 USING(k1, k2)
ORDER BY k1, k2;

-- Test 85: Multiple VALUES relations
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (3, 'd')) AS t3_val(k, v3) USING (k)
ORDER BY k;

-- Test 86: VALUES with aggregation
SELECT k, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (3, 'y')) AS u(k, v2) USING (k)
GROUP BY k
ORDER BY k;

-- Test 87: VALUES in subquery with USING
SELECT k, v1, v2 FROM (
    SELECT * FROM
        (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
        (VALUES (1, 'b'), (2, 'c')) AS u(k, v2) USING (k)
) sub
ORDER BY k;

-- Test 88: VALUES with WHERE clause on USING column
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y'), (4, 'z')) AS u(k, v2) USING (k)
WHERE k >= 2
ORDER BY k;

-- Test 89: VALUES with expressions on USING columns
SELECT k, k * 10 as k_scaled, CONCAT('K', CAST(k AS STRING)) as k_str, v1, v2
FROM
    (VALUES (1, 'a'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y')) AS u(k, v2) USING (k)
WHERE k IS NOT NULL
ORDER BY k;

-- Test 90: VALUES with CASE expressions
SELECT k,
       CASE WHEN k < 2 THEN 'SMALL' ELSE 'LARGE' END as size_cat,
       COALESCE(v1, 'NULL_V1') as v1_safe,
       COALESCE(v2, 'NULL_V2') as v2_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 91: Large VALUES relation FULL OUTER JOIN
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (3, 'x'), (4, 'y'), (5, 'z'), (6, 'w'), (7, 'v')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 92: VALUES with NULL in USING column
SELECT * FROM
    (VALUES (1, 'a'), (NULL, 'b'), (2, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (NULL, 'y'), (3, 'z')) AS u(k, v2) USING (k)
ORDER BY k, v1, v2;

-- Test 93: Three-way JOIN with VALUES
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (1, 'b'), (2, 'bb')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (2, 'cc'), (3, 'ccc')) AS t3_val(k, v3) USING (k)
ORDER BY k;

-- Test 94: VALUES with table, mixed types
SELECT k1, v1, val, v2 FROM
    t1 FULL OUTER JOIN
    (VALUES (CAST(1 AS TINYINT), 100), (CAST(5 AS TINYINT), 500)) AS inline_t(k1, val) USING(k1)
    LEFT JOIN t2 USING(k1)
ORDER BY k1;

-- Test 95: Complex expression in WHERE with VALUES
SELECT * FROM
    (VALUES (1, 10), (2, 20), (3, 30)) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 100), (3, 200), (4, 300)) AS u(k, v2) USING (k)
WHERE (k IS NULL) OR (k BETWEEN 2 AND 3 AND (v1 > 15 OR v2 > 150))
ORDER BY k;

-- Test 97: VALUES with ORDER BY on USING column expressions  
SELECT k, v1, v2,
       k + 100 as k_offset,
       CASE WHEN k IS NULL THEN -1 ELSE k END as k_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd'), (4, 'e')) AS u(k, v2) USING (k)
ORDER BY COALESCE(k, 999), v1, v2;

-- Test 98: VALUES with DISTINCT on result
SELECT DISTINCT k
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (1, 'xx'), (2, 'y')) AS u(k, v2) USING (k)
ORDER BY k;

-- Test 99: VALUES with string USING column
SELECT * FROM
    (VALUES ('apple', 1), ('banana', 2)) AS t(name, v1) FULL OUTER JOIN
    (VALUES ('banana', 10), ('cherry', 20)) AS u(name, v2) USING (name)
ORDER BY name;

-- Test 100: Four-table chain with VALUES mixed in
SELECT k1, v1, val1, v2, val2 FROM
    t1 FULL OUTER JOIN
    (VALUES (1, 'V1'), (2, 'V2')) AS v1_t(k1, val1) USING(k1) LEFT JOIN
    t2 USING(k1) FULL OUTER JOIN
    (VALUES (1, 'V3'), (5, 'V5')) AS v2_t(k1, val2) USING(k1)
ORDER BY k1
LIMIT 20;

-- Cleanup
DROP DATABASE test_full_outer_join_using;



