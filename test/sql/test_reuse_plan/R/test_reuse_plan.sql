-- name: test_reuse_plan
DROP DATABASE IF EXISTS test_reuse_plan;
-- result:
-- !result
CREATE DATABASE test_reuse_plan;
-- result:
-- !result
USE test_reuse_plan;
-- result:
-- !result
CREATE TABLE t0 (k1 INT, k2 INT, k3 INT, v1 INT, v2 INT) 
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES("replication_num"="1");
-- result:
-- !result
CREATE TABLE t1 (k1 INT, k2 INT, v INT) 
DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES("replication_num"="1");
-- result:
-- !result
CREATE TABLE dim (d INT, tag VARCHAR(10)) 
DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 3 PROPERTIES("replication_num"="1");
-- result:
-- !result
CREATE TABLE dim_gap (d INT, tag VARCHAR(10))
DUPLICATE KEY(d) DISTRIBUTED BY HASH(d) BUCKETS 3 PROPERTIES("replication_num"="1");
-- result:
-- !result
INSERT INTO t0 VALUES
  (1, 10, 100, 1, 10),
  (2, 10, 100, 2, NULL),
  (3, 20, 200, 3, 30),
  (4, 20, 200, NULL, 40),
  (5, 30, 300, 5, 50),
  (6, 30, 300, 6, NULL);
-- result:
-- !result
INSERT INTO t1 VALUES
  (1, 100, 10),
  (2, 100, 20),
  (3, 200, 30),
  (4, 200, NULL),
  (5, 300, 50);
-- result:
-- !result
INSERT INTO dim VALUES
  (100, 'A'),
  (200, 'B'),
  (300, 'C');
-- result:
-- !result
INSERT INTO dim_gap VALUES
  (100, 'A'),
  (200, 'B');
-- result:
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	2
20	1
30	2
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	2
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	3
20	None
30	11
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	3
-- !result
SELECT k2, COUNT(*), SUM(v1), AVG(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), SUM(v1), AVG(v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	2	3	10.0
20	1	None	40.0
30	2	11	50.0
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 5 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 5 GROUP BY k2
ORDER BY 1;
-- result:
10	3
10	3
20	3
20	3
-- !result
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 < 3
UNION ALL
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 >= 4
ORDER BY 1;
-- result:
2	3
3	11
-- !result
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 < 3
UNION ALL
SELECT COUNT(*), SUM(v1) FROM t0 WHERE k1 > 100
ORDER BY 1;
-- result:
0	None
2	3
-- !result
SELECT t0.k2, SUM(t0.v1) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 100 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, SUM(t0.v1) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 200 GROUP BY t0.k2
ORDER BY 1;
-- result:
10	3
20	3
-- !result
SELECT t0.k2, COUNT(*) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 100 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, COUNT(*) 
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1 
WHERE t0.k3 = 999 GROUP BY t0.k2
ORDER BY 1;
-- result:
10	2
-- !result
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
-- result:
10	2	30
20	2	30
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 0
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 0
ORDER BY 1;
-- result:
10	3
30	11
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 1
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 5
ORDER BY 1;
-- result:
10	3
30	11
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;
-- result:
10	1
20	1
30	1
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;
-- result:
10	1
30	1
-- !result
SELECT k2, MIN(v1), MAX(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	1	2
20	None	None
30	5	6
-- !result
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	1
20	1
30	1
-- !result
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	1
-- !result
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	1.5
20	None
30	5.5
-- !result
SELECT k2, k3, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2, k3
UNION ALL
SELECT k2, k3, SUM(v1) FROM t0 WHERE k1 >= 4 GROUP BY k2, k3
ORDER BY 1, 2;
-- result:
10	100	3
20	200	None
30	300	11
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 1 OR k1 = 2 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 = 5 OR k1 = 6 GROUP BY k2
ORDER BY 1;
-- result:
10	2
30	2
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (1, 2) GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (5, 6) GROUP BY k2
ORDER BY 1;
-- result:
10	3
30	11
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 BETWEEN 1 AND 2 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) FROM t0 WHERE k1 BETWEEN 5 AND 6 GROUP BY k2
ORDER BY 1;
-- result:
10	2
30	2
-- !result
SELECT k2, COUNT(*) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION
SELECT k2, COUNT(*) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	2
20	1
30	2
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
EXCEPT
SELECT k2, SUM(v1) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;
-- result:
10	3
-- !result
SELECT k2 FROM t0 WHERE k1 < 3 GROUP BY k2
INTERSECT
SELECT k2 FROM t0 WHERE k1 >= 3 GROUP BY k2
ORDER BY 1;
-- result:
-- !result
SELECT k2, SUM(v1 + v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	11
20	None
30	55
-- !result
SELECT * FROM (
  SELECT k2, SUM(v1) as s FROM t0 WHERE k1 < 3 GROUP BY k2
  UNION ALL
  SELECT k2, SUM(v1) as s FROM t0 WHERE k1 >= 4 GROUP BY k2
) t WHERE s > 2 ORDER BY 1;
-- result:
10	3
30	11
-- !result
SELECT k2, COUNT(1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(1) FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	2
20	1
30	2
-- !result
SELECT k2, COUNT(1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	2
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (1, 2) GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 IN (5, 6) GROUP BY k2
ORDER BY 1;
-- result:
10	3
30	11
-- !result
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	1.5
-- !result
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, MIN(v1), MAX(v2) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;
-- result:
10	1	10
30	5	50
-- !result
SELECT k2, COUNT(v2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(v2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	1
-- !result
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 = 5 GROUP BY k2
ORDER BY 1;
-- result:
10	1	1.0	10	10
30	5	5.0	50	50
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 0
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2 HAVING SUM(v1) > 0
ORDER BY 1;
-- result:
10	3
-- !result
SELECT k2, AVG(v1) FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING AVG(v1) > 1
UNION ALL
SELECT k2, AVG(v1) FROM t0 WHERE k1 > 100 GROUP BY k2 HAVING AVG(v1) > 1
ORDER BY 1;
-- result:
10	1.5
-- !result
SELECT t0.k2, MAX(t0.v1), MAX(t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k2 = 10 GROUP BY t0.k2
UNION ALL
SELECT t0.k2, MAX(t0.v1), MAX(t1.v)
FROM t0 INNER JOIN t1 ON t0.k1 = t1.k1
WHERE t0.k2 = 999 GROUP BY t0.k2
ORDER BY 1;
-- result:
10	2	20
-- !result
SELECT k2, SUM(v1) FROM t0 WHERE k1 > 100 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) FROM t0 WHERE k1 < 0 GROUP BY k2
ORDER BY 1;
-- result:
-- !result
SELECT k2, COUNT(*), COUNT(v2) FROM t0 WHERE k1 = 1 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), COUNT(v2) FROM t0 WHERE k1 = 999 GROUP BY k2
ORDER BY 1;
-- result:
10	1	1
-- !result
SELECT k2, SUM(v1 + v2), AVG(v1 * 2) FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2), AVG(v1 * 2) FROM t0 WHERE k1 > 100 GROUP BY k2
ORDER BY 1;
-- result:
10	11	3.0
-- !result
SELECT SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 < 3
UNION ALL
SELECT SUM(v1), AVG(v1), MIN(v2), MAX(v2) FROM t0 WHERE k1 > 100
ORDER BY 1;
-- result:
None	None	None	None
3	1.5	10	10
-- !result
SELECT k2, SUM(v1 * 2 + v2), AVG(CASE WHEN v1 > 0 THEN v1 ELSE 0 END) 
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 * 2 + v2), AVG(CASE WHEN v1 > 0 THEN v1 ELSE 0 END) 
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	12	1.5
20	None	0.0
30	60	5.5
-- !result
SELECT k2, SUM(v1), SUM(v1 * 2), AVG(v1), MAX(v1), MIN(v1)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1), SUM(v1 * 2), AVG(v1), MAX(v1), MIN(v1)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	3	6	1.5	2	1
20	None	None	None	None	None
30	11	22	5.5	6	5
-- !result
SELECT k2, SUM(COALESCE(v2, 0)), AVG(COALESCE(v1, 1))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(COALESCE(v2, 0)), AVG(COALESCE(v1, 1))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	10	1.5
20	40	1.0
30	50	5.5
-- !result
SELECT k2, SUM(IF(v1 > 2, v1, 0)), COUNT(IF(v2 IS NOT NULL, 1, NULL))
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(IF(v1 > 2, v1, 0)), COUNT(IF(v2 IS NOT NULL, 1, NULL))
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	0	1
20	0	1
30	11	1
-- !result
SELECT k2, COUNT(*), COUNT(v1), COUNT(v2), COUNT(DISTINCT v1)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*), COUNT(v1), COUNT(v2), COUNT(DISTINCT v1)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	2	2	1	2
20	1	0	1	0
30	2	2	1	2
-- !result
SELECT k2, SUM(v1 + v2), SUM(v1 - v2), SUM(v1 * v2), AVG(v1 + v2)
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1 + v2), SUM(v1 - v2), SUM(v1 * v2), AVG(v1 + v2)
FROM t0 WHERE k1 >= 4 GROUP BY k2
ORDER BY 1;
-- result:
10	11	-9	10	11.0
20	None	None	None	None
30	55	-45	250	55.0
-- !result
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
-- result:
10	2	5.0
20	0	40.0
30	22	25.0
-- !result
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
-- result:
10	33	25.0	20
20	33	90.0	60
-- !result
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
-- result:
10	2	1	3	10	1.5	10.0	0	10
20	1	1	None	40	None	40.0	40	40
30	2	1	11	50	5.5	50.0	0	50
-- !result
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
-- result:
10	3	2	203	101.5
20	None	1	None	None
30	11	2	611	305.5
-- !result
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
-- result:
110	3	10.0	2	11	4
220	None	40.0	1	None	None
330	11	50.0	2	55	12
-- !result
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
-- result:
10	6	6.0	1	10
20	None	40.0	1	40
30	22	28.0	1	50
-- !result
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
-- result:
10	16	10.0	2
-- !result
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
-- result:
10	3	10.0	2	11
30	11	50.0	2	55
-- !result
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
-- result:
6	5.0	0	11	10
22	30.0	2	55	250
-- !result
SELECT a.k2, a.cnt, b.cnt
FROM (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;
-- result:
-- !result
SELECT 
  (SELECT SUM(v1) FROM t0 WHERE k1 < 3 AND k2 = 10) as sum1,
  (SELECT SUM(v1) FROM t0 WHERE k1 >= 4 AND k2 = 10) as sum2,
  (SELECT SUM(v1) FROM t0 WHERE k1 = 5 AND k2 = 10) as sum3;
-- result:
3	None	None
-- !result
SELECT a.k2, a.total, b.total
FROM (SELECT k2, SUM(v1) + AVG(v2) as total FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, SUM(v1) + AVG(v2) as total FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;
-- result:
-- !result
SELECT a.k2, a.cnt + b.cnt + c.cnt as total_cnt
FROM (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 1 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 3 GROUP BY k2) b ON a.k2 = b.k2
JOIN (SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 = 5 GROUP BY k2) c ON a.k2 = c.k2
ORDER BY 1;
-- result:
-- !result
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
-- result:
-- !result
SELECT a.k2, a.sum_v, COALESCE(b.sum_v, 0) as b_sum
FROM (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2) a
LEFT JOIN (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 > 100 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;
-- result:
10	3	0
-- !result
SELECT k2, 
  (SELECT COUNT(*) FROM t0 t1 WHERE t1.k2 = t0.k2 AND t1.k1 < 3) as cnt1,
  (SELECT COUNT(*) FROM t0 t1 WHERE t1.k2 = t0.k2 AND t1.k1 >= 4) as cnt2
FROM t0
WHERE k2 IN (10, 20, 30)
GROUP BY k2
ORDER BY 1;
-- result:
10	2	0
20	0	1
30	0	2
-- !result
SELECT a.k2, a.sum_v, b.sum_v
FROM (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2 HAVING SUM(v1) > 1) a
JOIN (SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2 HAVING SUM(v1) > 5) b ON a.k2 = b.k2
ORDER BY 1;
-- result:
-- !result
SELECT a.k2, a.cnt, a.sum_v, b.cnt, b.sum_v
FROM (SELECT k2, COUNT(*) as cnt, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2) a
JOIN (SELECT k2, COUNT(*) as cnt, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2) b ON a.k2 = b.k2
ORDER BY 1;
-- result:
-- !result
WITH agg_data AS (
  SELECT k2, SUM(v1) as sum_v, COUNT(*) as cnt FROM t0 GROUP BY k2
)
SELECT a.k2, a.sum_v + b.sum_v as total_sum
FROM agg_data a
JOIN agg_data b ON a.k2 = b.k2
WHERE a.cnt > 0 AND b.cnt > 0
ORDER BY 1;
-- result:
10	6
20	6
30	22
-- !result
WITH 
  agg1 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 < 3 GROUP BY k2),
  agg2 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 >= 4 GROUP BY k2),
  agg3 AS (SELECT k2, SUM(v1) as s FROM t0 WHERE k1 = 5 GROUP BY k2)
SELECT a1.k2, a1.s, a2.s, a3.s
FROM agg1 a1
LEFT JOIN agg2 a2 ON a1.k2 = a2.k2
LEFT JOIN agg3 a3 ON a1.k2 = a3.k2
ORDER BY 1;
-- result:
10	3	None	None
-- !result
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
-- result:
10	3	3
20	3	3
30	11	11
-- !result
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
-- result:
10	3	3
30	11	11
-- !result
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
-- result:
30	3
30	3
-- !result
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	3
10	3
-- !result
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	3
10	3
20	None
30	11
-- !result
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
-- result:
10	2
10	2
10	2
20	1
30	1
30	2
-- !result
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 >= 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v1, AVG(v2) as avg_v2, COUNT(*) as cnt 
FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	3	10.0	2
10	3	10.0	2
20	None	40.0	1
30	11	50.0	2
-- !result
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 AND k2 = 10 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 >= 40 AND k2 = 20 GROUP BY k2
UNION ALL
SELECT k2, SUM(v1) as sum_v FROM t0 WHERE k1 < 3 AND k2 = 10 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	3
10	3
-- !result
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
-- result:
10	3
10	3
20	None
30	5
-- !result
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
UNION ALL
SELECT k2, COUNT(*) as cnt FROM t0 WHERE k1 < 3 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	2
10	2
10	2
-- !result
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 < 4 GROUP BY k2
UNION ALL
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 >= 5 GROUP BY k2
UNION ALL
SELECT k2, SUM(v2) as sum_v FROM t0 WHERE k1 < 4 GROUP BY k2
ORDER BY 1, 2;
-- result:
10	10
10	10
20	30
20	30
30	50
-- !result
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
-- result:
30	2
30	2
-- !result
DROP TABLE IF EXISTS t0;
-- result:
-- !result
DROP TABLE IF EXISTS t1;
-- result:
-- !result
DROP TABLE IF EXISTS dim;
-- result:
-- !result
DROP TABLE IF EXISTS dim_gap;
-- result:
-- !result
