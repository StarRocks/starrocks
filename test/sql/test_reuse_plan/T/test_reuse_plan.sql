-- name: test_reuse_plan
DROP DATABASE IF EXISTS test_reuse_plan;
CREATE DATABASE test_reuse_plan;
USE test_reuse_plan;

-- base tables
CREATE TABLE t0 (k1 INT, k2 INT, k3 INT, v1 INT, v2 INT) 
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES("replication_num"="1");

CREATE TABLE t1 (k1 INT, k2 INT, v INT) 
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES("replication_num"="1");

CREATE TABLE dim (d INT, tag VARCHAR(10)) 
DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 3 PROPERTIES("replication_num"="1");

CREATE TABLE dim_gap (d INT, tag VARCHAR(10))
DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 3 PROPERTIES("replication_num"="1");

INSERT INTO t0 VALUES
  (1, 10, 100, 1, 10),
  (2, 10, 100, 2, NULL),
  (3, 20, 200, 3, 30),
  (4, 20, 200, NULL, 40),
  (5, 30, 300, 5, 50),
  (6, 30, 300, 6, NULL);

INSERT INTO t1 VALUES
  (1, 100, 10),
  (2, 100, 20),
  (3, 200, 30),
  (4, 200, NULL),
  (5, 300, 50);

INSERT INTO dim VALUES
  (100, 'A'),
  (200, 'B'),
  (300, 'C');

INSERT INTO dim_gap VALUES
  (100, 'A'),
  (200, 'B');

-- ========================================
-- Pattern 1: Simple SPJG - Scan + Project + Agg
-- ========================================

-- 1.1 COUNT(*) with different scan filters (should use count_if + row_count_if gate)
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 1.1b COUNT(*) with one branch matching NO rows (must NOT produce 0-count groups)
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 1.2 SUM with different scan filters (should use sum_if + any_value_if gate)
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 1.2b SUM with one branch matching NO rows (empty result for that branch)
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 1.3 Multiple aggregations (COUNT + SUM + AVG)
SELECT k2, COUNT(*), SUM(v1), AVG(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), SUM(v1), AVG(v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 1.4 Same filter (should fuse without *_if)
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 5 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 5 GROUP BY k2
ORDER BY 1;

-- 1.5 Global aggregation without GROUP BY (no gate, must return both rows)
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 < 3
UNION ALL
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 >= 4
ORDER BY 1;

-- 1.5b Global agg with one branch matching NO rows (must return 0, NULL for that branch)
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 < 3
UNION ALL
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 > 100
ORDER BY 1;

-- ========================================
-- Pattern 2: SPJG with INNER JOIN
-- ========================================

-- 2.1 INNER JOIN + AGG with different filters
SELECT t0.k2, SUM(t0.v1) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 100 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, SUM(t0.v1) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 200 GROUP BY t0.k2
ORDER BY 1;

-- 2.1b INNER JOIN with one branch matching NO rows after join
SELECT t0.k2, COUNT(*) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 100 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, COUNT(*) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 999 GROUP BY t0.k2
ORDER BY 1;

-- 2.2 INNER JOIN with multiple tables
SELECT t0.k2, COUNT(*), SUM(t1.v)
FROM t0 
INNER JOIN t1 ON t0.k1 = t1.k1
INNER JOIN dim ON t0.k3 = dim.d
WHERE t0.k2 = 10 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, COUNT(*), SUM(t1.v)
FROM t0 
INNER JOIN t1 ON t0.k1 = t1.k1
INNER JOIN dim ON t0.k3 = dim.d
WHERE t0.k2 = 20 GROUP BY t0.k2
ORDER BY 1;

-- ========================================
-- Pattern 3: HAVING clause handling
-- ========================================

-- 3.1 Same HAVING (preserved in fused agg)
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 0
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 0
ORDER BY 1;

-- 3.2 Different HAVING (merged with OR in fused agg, then filtered at consumer)
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 1
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 5
ORDER BY 1;

-- ========================================
-- Pattern 4: Multi-branch UNION (3+ branches)
-- ========================================

-- 4.1 Three branches with different filters
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;

-- 4.1b Three branches, one matching NO rows (empty branch should not pollute result)
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 5: Different aggregation functions
-- ========================================

-- 5.1 MIN/MAX
SELECT k2, MIN(v1), MAX(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 5.2 COUNT(col) - nullable column
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 5.2b COUNT(col) with one branch matching NO rows (empty result)
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 5.3 AVG
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 6: Multiple GROUP BY keys
-- ========================================

-- 6.1 Two group keys
SELECT k2, k3, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2, k3
UNION ALL
SELECT k2, k3, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2, k3
ORDER BY 1, 2;

-- ========================================
-- Pattern 7: Complex filter predicates
-- ========================================

-- 7.1 OR condition in filter
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 OR k1 = 2 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 OR k1 = 6 GROUP BY k2
ORDER BY 1;

-- 7.2 IN predicate
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (1, 2) GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (5, 6) GROUP BY k2
ORDER BY 1;

-- 7.3 BETWEEN
SELECT k2, COUNT(*) FROM t0 WHERE k1 BETWEEN 1 AND 2 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 BETWEEN 5 AND 6 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 8: UNION vs UNION ALL
-- ========================================

-- 8.1 UNION (with deduplication)
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION
SELECT k2, COUNT(*) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 9: Set operations (EXCEPT, INTERSECT)
-- ========================================

-- 9.1 EXCEPT
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
EXCEPT
SELECT k2, SUM(v1) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;

-- 9.2 INTERSECT
SELECT k2 FROM t0 WHERE k1 < 3 GROUP BY k2
INTERSECT
SELECT k2 FROM t0 WHERE k1 >= 3 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 10: Aggregation with expression
-- ========================================

-- 10.1 Expression in aggregation
SELECT k2, SUM(v1 + v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 11: Subquery with outer filter
-- ========================================

-- 11.1 Filter on aggregation result
SELECT * FROM (
  SELECT k2, SUM(v1) as s FROM t0 WHERE k1 < 3 GROUP BY k2
  UNION ALL
  SELECT k2, SUM(v1) as s FROM t0 WHERE k1 >= 4 GROUP BY k2
) t WHERE s > 2 ORDER BY 1;

-- ========================================
-- Pattern 12: COUNT(1) vs COUNT(*)
-- ========================================

-- 12.1 COUNT(1)
SELECT k2, COUNT(1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 12.1b COUNT(1) with one branch matching NO rows
SELECT k2, COUNT(1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- ========================================
-- Pattern 13: Critical - Empty result branches (regression test)
-- ========================================

-- 13.1 SUM with non-overlapping groups (no shared k2 values)
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (1, 2) GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (5, 6) GROUP BY k2
ORDER BY 1;

-- 13.2 AVG with one branch completely empty
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 13.3 MIN/MAX with empty branch
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;

-- 13.4 COUNT(col) with empty branch (nullable column)
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 13.5 Mixed aggregations with empty branch (SUM + AVG + MIN + MAX)
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;

-- 13.6 SUM with HAVING and empty branch
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 0
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2 HAVING SUM(v1) > 0
ORDER BY 1;

-- 13.7 AVG with HAVING and empty branch
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING AVG(v1) > 1
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 > 100 GROUP BY k2 HAVING AVG(v1) > 1
ORDER BY 1;

-- 13.8 MAX with JOIN and empty branch
SELECT t0.k2, MAX(t0.v1), MAX(t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k2 = 10 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, MAX(t0.v1), MAX(t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k2 = 999 GROUP BY t0.k2
ORDER BY 1;

-- 13.9 All branches empty with SUM
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 0 GROUP BY k2
ORDER BY 1;

-- 13.10 COUNT(*) vs COUNT(col) with empty branch (different semantics)
SELECT k2, COUNT(*), COUNT(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), COUNT(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
ORDER BY 1;

-- 13.11 Expression in aggregation with empty branch
SELECT k2, SUM(v1 + v2), AVG(v1 * 2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2), AVG(v1 * 2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 13.12 Global aggregation with empty branch (SUM + AVG + MIN + MAX)
SELECT SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 < 3
UNION ALL
SELECT SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 > 100
ORDER BY 1;

-- ========================================
-- Pattern 14: Complex aggregations (expressions, multi-column, nested)
-- ========================================

-- 14.1 Aggregation with complex expressions
SELECT k2, SUM(v1 * 2 + v2), AVG(CASE WHEN v1 > 0 THEN v1 ELSE 0 END) 
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 * 2 + v2), AVG(CASE WHEN v1 > 0 THEN v1 ELSE 0 END) 
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.2 Multiple aggregations on same column with different expressions
SELECT k2, SUM(v1), SUM(v1 * 2), AVG(v1), MAX(v1), MIN(v1)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), SUM(v1 * 2), AVG(v1), MAX(v1), MIN(v1)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.3 Aggregation with COALESCE
SELECT k2, SUM(COALESCE(v2, 0)), AVG(COALESCE(v1, 1))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(COALESCE(v2, 0)), AVG(COALESCE(v1, 1))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.4 Aggregation with IF expression
SELECT k2, SUM(IF(v1 > 2, v1, 0)), COUNT(IF(v2 IS NOT NULL, 1, NULL))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(IF(v1 > 2, v1, 0)), COUNT(IF(v2 IS NOT NULL, 1, NULL))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.5 Multiple COUNT variants
SELECT k2, COUNT(*), COUNT(v1), COUNT(v2), COUNT(DISTINCT v1)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), COUNT(v1), COUNT(v2), COUNT(DISTINCT v1)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.6 Aggregation with arithmetic between columns
SELECT k2, SUM(v1 + v2), SUM(v1 - v2), SUM(v1 * v2), AVG(v1 + v2)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2), SUM(v1 - v2), SUM(v1 * v2), AVG(v1 + v2)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.7 Complex CASE WHEN in aggregation
SELECT k2, 
  SUM(CASE WHEN v1 > 3 THEN v1 * 2 WHEN v1 > 1 THEN v1 ELSE 0 END),
  AVG(CASE WHEN v2 IS NULL THEN 0 ELSE v2 END)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, 
  SUM(CASE WHEN v1 > 3 THEN v1 * 2 WHEN v1 > 1 THEN v1 ELSE 0 END),
  AVG(CASE WHEN v2 IS NULL THEN 0 ELSE v2 END)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.8 Aggregation with JOIN and expressions
SELECT t0.k2, 
  SUM(t0.v1 + t1.v), 
  AVG(t0.v1 * t1.v),
  MAX(t0.v2 + t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k3 = 100 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, 
  SUM(t0.v1 + t1.v), 
  AVG(t0.v1 * t1.v),
  MAX(t0.v2 + t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k3 = 200 GROUP BY t0.k2
ORDER BY 1;

-- 14.9 Multiple aggregations with mixed NULL handling
SELECT k2,
  COUNT(*),
  COUNT(v2),
  SUM(v1),
  SUM(v2),
  AVG(v1),
  AVG(v2),
  MIN(COALESCE(v2, 0)),
  MAX(COALESCE(v2, 0))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2,
  COUNT(*),
  COUNT(v2),
  SUM(v1),
  SUM(v2),
  AVG(v1),
  AVG(v2),
  MIN(COALESCE(v2, 0)),
  MAX(COALESCE(v2, 0))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.10 Aggregation with string concatenation (if supported) and expressions
SELECT k2,
  SUM(v1),
  COUNT(*),
  SUM(v1 + k3),
  AVG(v1 + k3)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2,
  SUM(v1),
  COUNT(*),
  SUM(v1 + k3),
  AVG(v1 + k3)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.11 Complex expression in GROUP BY with multiple aggregations
SELECT k2 + k3 as kg, 
  SUM(v1), 
  AVG(v2), 
  COUNT(*),
  MIN(v1 + v2),
  MAX(v1 * 2)
FROM t0 WHERE k1 < 3 GROUP BY k2 + k3
UNION ALL
SELECT k2 + k3 as kg, 
  SUM(v1), 
  AVG(v2), 
  COUNT(*),
  MIN(v1 + v2),
  MAX(v1 * 2)
FROM t0 WHERE k1 >= 4 GROUP BY k2 + k3
ORDER BY 1;

-- 14.12 Nested expressions with multiple aggregations
SELECT k2,
  SUM(IF(v1 > 0, v1 * 2, v1)),
  AVG(COALESCE(v2, v1)),
  COUNT(CASE WHEN v2 IS NOT NULL THEN 1 END),
  MAX(IF(v1 > v2, v1, v2))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2,
  SUM(IF(v1 > 0, v1 * 2, v1)),
  AVG(COALESCE(v2, v1)),
  COUNT(CASE WHEN v2 IS NOT NULL THEN 1 END),
  MAX(IF(v1 > v2, v1, v2))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;

-- 14.13 Empty branch with complex aggregations
SELECT k2,
  SUM(v1 * 2 + COALESCE(v2, 0)),
  AVG(CASE WHEN v1 > 2 THEN v1 ELSE v2 END),
  COUNT(DISTINCT v1)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2,
  SUM(v1 * 2 + COALESCE(v2, 0)),
  AVG(CASE WHEN v1 > 2 THEN v1 ELSE v2 END),
  COUNT(DISTINCT v1)
FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;

-- 14.14 Multiple aggregations with HAVING on complex expression
SELECT k2,
  SUM(v1),
  AVG(v2),
  COUNT(*),
  MAX(v1 + v2)
FROM t0 WHERE k1 < 3 GROUP BY k2
HAVING SUM(v1) + AVG(COALESCE(v2, 0)) > 1
UNION ALL
SELECT k2,
  SUM(v1),
  AVG(v2),
  COUNT(*),
  MAX(v1 + v2)
FROM t0 WHERE k1 >= 4 GROUP BY k2
HAVING SUM(v1) + AVG(COALESCE(v2, 0)) > 1
ORDER BY 1;

-- 14.15 Global aggregation with complex expressions
SELECT 
  SUM(v1 * 2),
  AVG(COALESCE(v2, 0)),
  COUNT(CASE WHEN v1 > 2 THEN 1 END),
  MIN(v1 + v2),
  MAX(v1 * v2)
FROM t0 WHERE k1 < 3
UNION ALL
SELECT 
  SUM(v1 * 2),
  AVG(COALESCE(v2, 0)),
  COUNT(CASE WHEN v1 > 2 THEN 1 END),
  MIN(v1 + v2),
  MAX(v1 * v2)
FROM t0 WHERE k1 >= 4
ORDER BY 1;

-- ========================================
-- Pattern 15: Self-join with different filters (no UNION needed)
-- ========================================

-- 15.2 Self-join with aggregation in subquery
SELECT a.k2, a.cnt, b.cnt
FROM (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;

-- 15.3 Multiple references to same aggregation pattern
SELECT 
  (SELECT SUM(v1) FROM t0 WHERE k1 < 3 AND k2 = 10) as sum1,
  (SELECT SUM(v1) FROM t0 WHERE k1 >= 4 AND k2 = 10) as sum2,
  (SELECT SUM(v1) FROM t0 WHERE k1 = 5 AND k2 = 10) as sum3;

-- 15.4 Self-join with complex aggregation
SELECT a.k2, a.total, b.total
FROM (SELECT k2, SUM(v1) + AVG(v2) as total FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, SUM(v1) + AVG(v2) as total FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;

-- 15.5 Three-way self-join with different filters
SELECT a.k2, a.cnt + b.cnt + c.cnt as total_cnt
FROM (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 1 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 3 GROUP BY k2) b ON a.k2 = b.k2
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 5 GROUP BY k2) c ON a.k2 = c.k2
ORDER BY 1;

-- 15.6 Self-join with JOIN and aggregation
SELECT a.k2, a.total, b.total
FROM (
  SELECT t0.k2, SUM(t0.v1) as total 
  FROM t0 JOIN t1 ON t0.k1 = t1.k1 
  WHERE t0.k3 = 100 GROUP BY t0.k2
) a
JOIN (
  SELECT t0.k2, SUM(t0.v1) as total 
  FROM t0 JOIN t1 ON t0.k1 = t1.k1 
  WHERE t0.k3 = 200 GROUP BY t0.k2
) b ON a.k2 = b.k2
ORDER BY 1;

-- 15.7 Self-join with empty branch
SELECT a.k2, a.sum_v, COALESCE(b.sum_v, 0) as b_sum
FROM (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2) a
LEFT JOIN (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 > 100 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;

-- 15.8 Nested subqueries with same pattern
SELECT k2, 
  (SELECT COUNT(*) FROM t0 t1 WHERE t1.k2 = t0.k2 AND t1.k1 < 3) as cnt1,
  (SELECT COUNT(*) FROM t0 t1 WHERE t1.k2 = t0.k2 AND t1.k1 >= 4) as cnt2
FROM t0
WHERE k2 IN (10, 20, 30)
GROUP BY k2
ORDER BY 1;

-- 15.9 Self-join with HAVING
SELECT a.k2, a.sum_v, b.sum_v
FROM (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 1) a
JOIN (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 5) b ON a.k2 = b.k2
ORDER BY 1;

-- 15.10 Self-join with multiple aggregations
SELECT a.k2, a.cnt, a.sum_v, b.cnt, b.sum_v
FROM (SELECT k2, COUNT(*) as cnt, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;

-- ========================================
-- Pattern 16: CTE with multiple references and different filters
-- ========================================

-- 16.2 CTE with aggregation, referenced in self-join
WITH agg_data AS (
  SELECT k2, SUM(v1) as sum_v, COUNT(*) as cnt FROM t0 GROUP BY k2
)
SELECT a.k2, a.sum_v + b.sum_v as total_sum
FROM agg_data a
JOIN agg_data b ON a.k2 = b.k2
WHERE a.cnt > 0 AND b.cnt > 0
ORDER BY 1;

-- 16.3 Multiple CTEs with same structure
WITH 
  agg1 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 < 3 GROUP BY k2),
  agg2 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 >= 4 GROUP BY k2),
  agg3 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 = 5 GROUP BY k2)
SELECT a1.k2, a1.s, a2.s, a3.s
FROM agg1 a1
LEFT JOIN agg2 a2 ON a1.k2 = a2.k2
LEFT JOIN agg3 a3 ON a1.k2 = a3.k2
ORDER BY 1;

-- 16.5 Nested CTE with self-join
WITH base AS (
  SELECT k2, k3, v1 FROM t0 WHERE k1 < 10
),
agg_base AS (
  SELECT k2, SUM(v1) as sum_v FROM base GROUP BY k2
)
SELECT a.k2, a.sum_v, b.sum_v
FROM agg_base a
JOIN agg_base b ON a.k2 = b.k2
WHERE a.sum_v > 0
ORDER BY 1;

-- ========================================
-- Pattern 18: Mixed patterns (UNION + self-join)
-- ========================================

-- 18.1 UNION result joined with itself
WITH union_result AS (
  SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
  UNION ALL
  SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2
)
SELECT a.k2, a.sum_v, b.sum_v
FROM union_result a
JOIN union_result b ON a.k2 = b.k2
WHERE a.sum_v > 0
ORDER BY 1;

-- 18.2 Self-join on UNION branches
SELECT a.k2, a.cnt + b.cnt as total
FROM (
  SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
  UNION ALL
  SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 >= 4 GROUP BY k2
) a
JOIN (
  SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 5 GROUP BY k2
  UNION ALL
  SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 6 GROUP BY k2
) b ON a.k2 = b.k2
ORDER BY 1;

-- ========================================
-- Pattern 19: Identical filters
-- Test for row_hit column reuse when multiple pieces have same filter
-- ========================================

-- 19.1 Two branches with identical filter
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;

-- 19.2 Three branches - first and third have identical filter
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;

-- 19.3 Multiple branches with same filter (stress test)
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 >= 4 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 5 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;

-- 19.4 Identical filter with multiple aggregations
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 >= 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;

-- 19.5 Identical complex filter with compound predicates
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 AND k2 = 10 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 40 AND k2 = 20 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 AND k2 = 10 GROUP BY k2
ORDER BY 1, 2;

-- 19.6 Identical filter with JOIN (SPJG pattern)
SELECT t0.k2, SUM(t0.v1) as sum_v 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k1 < 3 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, SUM(t0.v1) as sum_v 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k1 >= 4 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, SUM(t0.v1) as sum_v 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k1 < 3 GROUP BY t0.k2
ORDER BY 1, 2;

-- 19.7 All branches have identical filter
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;

-- 19.8 Identical filter with NULL handling
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 < 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 >= 5 GROUP BY k2
UNION ALL
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 < 4 GROUP BY k2
ORDER BY 1, 2;

-- ========================================
-- Pattern 20: LEFT JOIN residual predicate
-- Test column-ref rewrite for join predicates left above an outer join
-- ========================================

-- 20.1 right-side IS NULL predicate must be rewritten when identical SPJG plans are fused
SELECT t0.k2, COUNT(*) as cnt
FROM t0 LEFT JOIN dim_gap ON t0.k3 = dim_gap.d
WHERE dim_gap.d IS NULL
GROUP BY t0.k2
UNION ALL
SELECT t0.k2, COUNT(*) as cnt
FROM t0 LEFT JOIN dim_gap ON t0.k3 = dim_gap.d
WHERE dim_gap.d IS NULL
GROUP BY t0.k2
ORDER BY 1, 2;

-- ========================================
-- Cleanup
-- ========================================
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS dim_gap;
