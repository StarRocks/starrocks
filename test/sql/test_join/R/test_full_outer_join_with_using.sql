-- name: test_full_outer_join_with_using
DROP DATABASE IF EXISTS test_full_outer_join_using;
-- result:
-- !result
CREATE DATABASE test_full_outer_join_using;
-- result:
-- !result
USE test_full_outer_join_using;
-- result:
-- !result
CREATE TABLE t1 (
    k1 INT,
    k2 INT,
    v1 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE t2 (
    k1 INT,
    k2 INT,
    v2 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE t3 (
    k1 INT,
    k2 INT,
    v3 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE t4 (
    k1 INT,
    k2 INT,
    v4 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t1 VALUES
    (1, 10, 'a1'),
    (2, 20, 'a2'),
    (3, NULL, 'a3'),
    (NULL, 40, 'a4'),
    (NULL, NULL, 'a5');
-- result:
-- !result
INSERT INTO t2 VALUES
    (1, 10, 'b1'),
    (4, 40, 'b2'),
    (5, NULL, 'b3'),
    (NULL, 60, 'b4'),
    (NULL, NULL, 'b5');
-- result:
-- !result
INSERT INTO t3 VALUES
    (1, 10, 'c1'),
    (2, 20, 'c2'),
    (6, 60, 'c3'),
    (NULL, 70, 'c4'),
    (NULL, NULL, 'c5');
-- result:
-- !result
INSERT INTO t4 VALUES
    (1, 10, 'd1'),
    (7, 70, 'd2'),
    (8, NULL, 'd3'),
    (NULL, 80, 'd4'),
    (NULL, NULL, 'd5');
-- result:
-- !result
SELECT k1, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1) 
ORDER BY k1, v1, v2;
-- result:
None	None	b4
None	None	b5
None	a4	None
None	a5	None
1	a1	b1
2	a2	None
3	a3	None
4	None	b2
5	None	b3
-- !result
SELECT k1, k2, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
ORDER BY k1, k2, v1, v2;
-- result:
None	None	None	b5
None	None	a5	None
None	40	a4	None
None	60	None	b4
1	10	a1	b1
2	20	a2	None
3	None	a3	None
4	40	None	b2
5	None	None	b3
-- !result
SELECT * 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
ORDER BY k1, k2, v1, v2;
-- result:
None	None	None	b5
None	None	a5	None
None	40	a4	None
None	60	None	b4
1	10	a1	b1
2	20	a2	None
3	None	a3	None
4	40	None	b2
5	None	None	b3
-- !result
SELECT k1, k2, v1, v2 
FROM t1 FULL OUTER JOIN t2 USING(k1, k2) 
WHERE k1 IS NOT NULL
ORDER BY k1, k2;
-- result:
1	10	a1	b1
2	20	a2	None
3	None	a3	None
4	40	None	b2
5	None	None	b3
-- !result
SELECT k1, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM t1 FULL OUTER JOIN t2 USING(k1) 
GROUP BY k1 
ORDER BY k1;
-- result:
None	4	2	2
1	1	1	1
2	1	1	0
3	1	1	0
4	1	0	1
5	1	0	1
-- !result
SELECT k1, COUNT(*) as cnt
FROM t1 FULL OUTER JOIN t2 USING(k1) 
GROUP BY k1 
HAVING k1 > 1 OR k1 IS NULL
ORDER BY k1;
-- result:
None	4
2	1
3	1
4	1
5	1
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
         FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	None	c5
None	None	None	b5	None
None	None	a5	None	None
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 LEFT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	None	c5
None	None	a5	None	None
None	40	a4	None	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
6	60	None	None	c3
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        LEFT JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	b5	None
None	None	a5	None	None
None	40	a4	None	None
None	60	None	b4	None
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
1	10	a1	b1	c1
2	20	a2	None	c2
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	None	c5
None	70	None	None	c4
1	10	a1	b1	c1
2	20	None	None	c2
6	60	None	None	c3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	None	None	None	c5	None
None	None	None	b5	None	None
None	None	a5	None	None	None
None	40	a4	None	None	None
None	60	None	b4	None	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	a3	None	None	None
4	40	None	b2	None	None
5	None	None	b3	None	None
6	60	None	None	c3	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	None	None	None	c5	None
None	None	a5	None	None	None
None	40	a4	None	None	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	a3	None	None	None
6	60	None	None	c3	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        INNER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        RIGHT JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1, k2)
        RIGHT JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	None	None	None	c5	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
6	60	None	None	c3	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) as t1_count,
       SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) as t2_count
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
GROUP BY k1, k2
ORDER BY k1, k2;
-- result:
None	None	1	1
None	40	1	0
None	60	0	1
1	10	1	1
2	20	1	0
3	None	1	0
4	40	0	1
5	None	0	1
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 = 1 AND k2 = 10) OR (v1 IS NULL AND v2 IS NULL)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	None	d5
None	None	None	None	c5	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
6	60	None	None	c3	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
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
-- result:
None	8	2	2	2	2
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1 DESC, k2 ASC, v1, v2;
-- result:
8	None	None	None	None	d3
7	70	None	None	None	d2
6	60	None	None	c3	None
5	None	None	b3	None	None
4	40	None	b2	None	None
3	None	a3	None	None	None
2	20	a2	None	c2	None
1	10	a1	b1	c1	d1
None	None	None	None	None	d5
None	None	None	None	c5	None
None	None	None	b5	None	None
None	None	a5	None	None	None
None	40	a4	None	None	None
None	60	None	b4	None	None
None	70	None	None	c4	None
None	80	None	None	None	d4
-- !result
SELECT DISTINCT k1, k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None
None	40
None	60
None	70
1	10
2	20
3	None
4	40
5	None
6	60
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t1 WHERE k1 IS NOT NULL)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	a3	None	None	None
-- !result
SELECT k1, k2,
       CASE WHEN k1 IS NULL THEN 'NULL_K1'
            WHEN k1 < 3 THEN 'SMALL'
            ELSE 'LARGE' END as k1_category,
       v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	NULL_K1	a5	None	None
None	None	NULL_K1	None	b5	None
None	None	NULL_K1	None	None	c5
None	40	NULL_K1	a4	None	None
None	60	NULL_K1	None	b4	None
None	70	NULL_K1	None	None	c4
1	10	SMALL	a1	b1	c1
2	20	SMALL	a2	None	c2
3	None	LARGE	a3	None	None
4	40	LARGE	None	b2	None
5	None	LARGE	None	b3	None
6	60	LARGE	None	None	c3
-- !result
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
-- result:
None	None	4	1	1	c5	d5
None	40	1	1	0	None	None
None	60	1	0	1	None	None
None	70	1	0	0	c4	None
None	80	1	0	0	None	d4
1	10	1	1	1	c1	d1
2	20	1	1	0	c2	None
3	None	1	1	0	None	None
4	40	1	0	1	None	None
5	None	1	0	1	None	None
6	60	1	0	0	c3	None
7	70	1	0	0	None	d2
8	None	1	0	0	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2
LIMIT 10;
-- result:
None	None	None	None	None	d5
None	None	None	b5	None	None
None	None	None	None	c5	None
None	None	a5	None	None	None
None	40	a4	None	None	None
None	60	None	b4	None	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
-- !result
SELECT k1, k2,
       COALESCE(v1, v2, v3, v4) as value
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	a5
None	None	b5
None	None	c5
None	None	d5
None	40	a4
None	60	b4
None	70	c4
None	80	d4
1	10	a1
2	20	a2
3	None	a3
4	40	b2
5	None	b3
6	60	c3
7	70	d2
8	None	d3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 > 0 OR k1 IS NULL) AND (k2 < 50 OR k2 IS NULL)
ORDER BY k1, k2;
-- result:
None	None	a5	None	None	None
None	None	None	b5	None	None
None	None	None	None	c5	None
None	None	None	None	None	d5
None	40	a4	None	None	None
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	a3	None	None	None
4	40	None	b2	None	None
5	None	None	b3	None	None
8	None	None	None	None	d3
-- !result
SELECT k1, k2, COUNT(*) as cnt
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
GROUP BY ROLLUP(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	8
None	None	4
None	None	16
None	40	1
None	60	1
None	70	1
None	80	1
1	None	1
1	10	1
2	None	1
2	20	1
3	None	1
3	None	1
4	None	1
4	40	1
5	None	1
5	None	1
6	None	1
6	60	1
7	None	1
7	70	1
8	None	1
8	None	1
-- !result
SELECT k1, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
ORDER BY k1, v1, v2, v3, v4;
-- result:
None	None	None	None	d4
None	None	None	None	d5
None	None	None	c4	None
None	None	None	c5	None
None	None	b4	None	None
None	None	b5	None	None
None	a4	None	None	None
None	a5	None	None	None
1	a1	b1	c1	d1
2	a2	None	c2	None
3	a3	None	None	None
4	None	b2	None	None
5	None	b3	None	None
6	None	None	c3	None
7	None	None	None	d2
8	None	None	None	d3
-- !result
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
-- result:
None	8	8
1	1	4
2	1	2
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        LEFT JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
None	None	None	None	c5	None
None	70	None	None	c4	None
1	10	a1	b1	c1	d1
2	20	None	None	c2	None
6	60	None	None	c3	None
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 RIGHT JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;
-- result:
1	10	a1	b1	c1	d1
-- !result
SELECT k1, k2, v1, v2, v3
FROM (SELECT k1, k2, v1 FROM t1 WHERE k1 IS NOT NULL) as sub1
     FULL OUTER JOIN t2 USING(k1, k2)
     FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	None	c5
None	None	None	b5	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN (SELECT k1, k2, v3 FROM t3 WHERE k2 > 0) as sub3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;
-- result:
None	None	None	b5	None
None	None	a5	None	None
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
-- !result
WITH cte1 AS (
    SELECT k1, k2, v1 FROM t1 WHERE k1 <= 3 OR k1 IS NULL
),
cte2 AS (
    SELECT k1, k2, v2 FROM t2 WHERE k1 >= 3 OR k1 IS NULL
)
SELECT k1, k2, v1, v2
FROM cte1 FULL OUTER JOIN cte2 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	None	b5
None	None	a5	None
None	40	a4	None
None	60	None	b4
1	10	a1	None
2	20	a2	None
3	None	a3	None
4	40	None	b2
5	None	None	b3
-- !result
WITH base_join AS (
    SELECT k1, k2, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
)
SELECT k1, k2, v1, v2, v3
FROM base_join FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	a5	None	None
None	None	None	b5	None
None	None	None	None	c5
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
-- !result
SELECT k1, 
       MAX(v1) as max_v1, 
       MIN(v2) as min_v2, 
       AVG(CAST(k2 AS DOUBLE)) as avg_k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1
HAVING MAX(v1) IS NOT NULL OR MIN(v2) IS NOT NULL
ORDER BY k1;
-- result:
None	a5	b4	56.666666666666664
1	a1	b1	10.0
2	a2	None	20.0
3	a3	None	None
4	None	b2	40.0
5	None	b3	None
-- !result
SELECT k1, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1)
WHERE k1 = 1
UNION ALL
SELECT k1, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1)
WHERE k1 = 2
ORDER BY k1, v1, v2;
-- result:
1	a1	b1
2	a2	None
-- !result
SELECT k1, v1, v2, v3, v4
FROM t1 LEFT JOIN t2 USING(k1)
        FULL OUTER JOIN t3 USING(k1)
        RIGHT JOIN t4 USING(k1)
        FULL OUTER JOIN (SELECT k1, 'extra' as v_extra FROM t1 WHERE k1 < 5) sub USING(k1)
ORDER BY k1;
-- result:
None	None	None	None	d4
None	None	None	None	d5
1	a1	b1	c1	d1
2	None	None	None	None
3	None	None	None	None
7	None	None	None	d2
8	None	None	None	d3
-- !result
SELECT k1, k2, v1, v2,
       ROW_NUMBER() OVER (PARTITION BY k1 ORDER BY k2) as rn,
       COUNT(*) OVER (PARTITION BY k1) as cnt_per_k1
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	None	b5	1	4
None	None	a5	None	2	4
None	40	a4	None	3	4
None	60	None	b4	4	4
1	10	a1	b1	1	1
2	20	a2	None	1	1
3	None	a3	None	1	1
4	40	None	b2	1	1
5	None	None	b3	1	1
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
WHERE (k1 BETWEEN 1 AND 5) OR 
      (k1 IS NULL AND k2 IS NOT NULL) OR
      (k2 > 50 AND k1 IS NOT NULL)
ORDER BY k1, k2;
-- result:
None	40	a4	None	None	None
None	60	None	b4	None	None
None	70	None	None	c4	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	a3	None	None	None
4	40	None	b2	None	None
5	None	None	b3	None	None
6	60	None	None	c3	None
7	70	None	None	None	d2
-- !result
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE EXISTS (
    SELECT 1 FROM t3 WHERE t3.k1 = k1 AND t3.k2 = k2
)
ORDER BY k1, k2;
-- result:
None	None	None	b5
None	None	a5	None
None	40	a4	None
None	60	None	b4
1	10	a1	b1
2	20	a2	None
3	None	a3	None
4	40	None	b2
5	None	None	b3
-- !result
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE NOT EXISTS (
    SELECT 1 FROM t3 WHERE t3.k1 = k1
)
ORDER BY k1, k2;
-- result:
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
WHERE k1 IN (1, 2, 3) OR k1 IS NULL
ORDER BY k1, k2;
-- result:
None	None	a5	None	None
None	None	None	b5	None
None	None	None	None	c5
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
-- !result
SELECT k1, k2,
       COUNT(DISTINCT CASE WHEN v1 IS NOT NULL THEN v1 END) as dist_v1,
       COUNT(DISTINCT CASE WHEN v2 IS NOT NULL THEN v2 END) as dist_v2,
       CONCAT_WS(',', v1, v2, v3) as combined_values
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1, k2
HAVING COUNT(*) > 1
ORDER BY k1, k2;
-- result:
E: (1064, "Getting analyzing error from line 4, column 7 to line 4, column 32. Detail message: 'CONCAT_WS(',', `test_full_outer_join_using`.`t1`.`v1`, `test_full_outer_join_using`.`t2`.`v2`, `test_full_outer_join_using`.`t3`.`v3`)' must be an aggregate expression or appear in GROUP BY clause.")
-- !result
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
-- result:
None	None	T2_ONLY	b5
None	None	T1_ONLY	a5
None	40	T1_ONLY	a4
None	60	T2_ONLY	b4
1	10	BOTH	a1
2	20	T1_ONLY	a2
3	None	T1_ONLY	a3
4	40	T2_ONLY	b2
5	None	T2_ONLY	b3
-- !result
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
-- result:
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
-- !result
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t3 WHERE k1 IS NOT NULL)
ORDER BY k1, k2;
-- result:
1	10	a1	b1
2	20	a2	None
-- !result
SELECT k1, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1)
        LEFT JOIN (SELECT k1, v3 FROM t3 WHERE k1 < 10) sub3 USING(k1)
        FULL OUTER JOIN t4 USING(k1)
ORDER BY k1;
-- result:
None	None	None	None	d4
None	None	None	None	d5
1	a1	b1	c1	d1
7	None	None	None	d2
8	None	None	None	d3
-- !result
SELECT k1, k2, sum_v1, v2
FROM (
    SELECT k1, k2, SUM(CAST(SUBSTRING(v1, 2) AS INT)) as sum_v1
    FROM t1
    GROUP BY k1, k2
) derived
FULL OUTER JOIN t2 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	5	None
None	None	None	b5
None	40	4	None
None	60	None	b4
1	10	1	b1
2	20	2	None
3	None	3	None
4	40	None	b2
5	None	None	b3
-- !result
SELECT outer_k1, COUNT(*) as cnt
FROM (
    SELECT k1 as outer_k1, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(k1)
    WHERE k1 IS NOT NULL
) sub
GROUP BY outer_k1
ORDER BY outer_k1;
-- result:
1	1
2	1
3	1
4	1
5	1
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
WHERE k1 IS NOT NULL OR k2 IS NOT NULL
ORDER BY k1, k2
LIMIT 20;
-- result:
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
-- !result
SELECT k1, max_v1, v2, v3
FROM (
    SELECT k1, MAX(v1) as max_v1
    FROM t1
    GROUP BY k1
) agg_t1
FULL OUTER JOIN t2 USING(k1)
FULL OUTER JOIN t3 USING(k1)
ORDER BY k1;
-- result:
None	None	b4	None
None	None	b5	None
None	a5	None	None
None	None	None	c4
None	None	None	c5
1	a1	b1	c1
2	a2	None	c2
3	a3	None	None
4	None	b2	None
5	None	b3	None
6	None	None	c3
-- !result
SELECT k1, k2, COUNT(*) as cnt, COUNT(v1) as cnt_v1
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
GROUP BY CUBE(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	1	1
None	10	1	1
1	None	1	1
1	10	1	1
-- !result
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
-- result:
None	6	2	2	c4,c5
-- !result
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
-- result:
None	None	None	None	None	d5
None	None	a5	None	None	None
None	40	a4	None	None	None
None	80	None	None	None	d4
1	10	a1	b1	c1	d1
2	20	a2	None	None	None
3	None	a3	None	None	None
7	70	None	None	None	d2
8	None	None	None	None	d3
-- !result
SELECT k1, k2, 
       k1 + k2 as sum_k,
       k1 * k2 as product_k,
       v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IS NOT NULL AND k2 IS NOT NULL
ORDER BY k1, k2;
-- result:
1	10	11	10	a1	b1
2	20	22	40	a2	None
4	40	44	160	None	b2
-- !result
SELECT k1, k2,
       CONCAT('K1:', CAST(k1 AS STRING)) as k1_str,
       LENGTH(v1) as len_v1,
       UPPER(v2) as v2_upper
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IS NOT NULL
ORDER BY k1, k2;
-- result:
1	10	K1:1	2	B1
2	20	K1:2	2	None
3	None	K1:3	2	None
4	40	K1:4	None	B2
5	None	K1:5	None	B3
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 INNER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
        INNER JOIN t4 USING(k1, k2)
ORDER BY k1, k2;
-- result:
1	10	a1	b1	c1	d1
-- !result
SELECT k1, k2, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
ORDER BY COALESCE(k1, 999), COALESCE(k2, 999), v1;
-- result:
1	10	a1	b1	c1
2	20	a2	None	c2
3	None	a3	None	None
4	40	None	b2	None
5	None	None	b3	None
6	60	None	None	c3
None	40	a4	None	None
None	60	None	b4	None
None	70	None	None	c4
None	None	None	None	c5
None	None	None	b5	None
None	None	a5	None	None
-- !result
SELECT k1, COUNT(*) as cnt, AVG(k2) as avg_k2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        FULL OUTER JOIN t3 USING(k1, k2)
GROUP BY k1
HAVING k1 > 0 AND AVG(k2) > 10
ORDER BY k1;
-- result:
2	1	20.0
4	1	40.0
6	1	60.0
-- !result
SELECT k1, k2, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
        LEFT JOIN t3 USING(k1, k2)
        FULL OUTER JOIN t4 USING(k1, k2)
        RIGHT JOIN (SELECT k1, k2, v1 as v1_dup FROM t1) dup USING(k1, k2)
ORDER BY k1, k2
LIMIT 15;
-- result:
None	None	None	None	None	None
None	40	None	None	None	None
1	10	a1	b1	c1	d1
2	20	a2	None	c2	None
3	None	None	None	None	None
-- !result
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE (k1 = 1 OR k1 IS NULL) AND (k2 = 10 OR k2 IS NULL)
ORDER BY k1, k2;
-- result:
None	None	a5	None
None	None	None	b5
1	10	a1	b1
-- !result
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
-- result:
None	4	4
1	2	1
2	1	1
3	1	1
4	1	1
5	1	1
-- !result
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
-- result:
-1	-1	1
-1	40	1
-1	60	1
-1	70	1
-1	80	1
1	10	4
2	20	2
3	-1	1
4	40	1
5	-1	1
6	60	1
7	70	1
8	-1	1
-- !result
SELECT k1, k2, v1, v2
FROM t1 FULL OUTER JOIN t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM t3 WHERE v3 IS NOT NULL)
   OR k1 IS NULL
ORDER BY k1, k2;
-- result:
None	None	None	b5
None	None	a5	None
None	40	a4	None
None	60	None	b4
1	10	a1	b1
2	20	a2	None
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);
-- result:
1	a	b
-- !result
SELECT * FROM
    (VALUES (1, 'a', 2)) AS t(k1, v1, k2) JOIN
    (VALUES (1, 'b', 2)) AS u(k1, v2, k2) USING (k1, k2);
-- result:
1	2	a	b
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) LEFT JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
1	a	None
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) RIGHT JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
2	None	b
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
1	a	None
2	None	b
-- !result
SELECT * FROM
    (VALUES (0, 1, 2), (3, 6, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, z1, x2, z2;
-- result:
1	0	2	3	5
4	None	None	0	2
6	3	5	None	None
-- !result
SELECT y, x1, x2 FROM
    (VALUES (0, 1, 2), (3, 6, 5), (3, NULL, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, x2;
-- result:
None	3	None
1	0	3
4	None	0
6	3	None
-- !result
SELECT * FROM
    (VALUES (CAST(1 AS TINYINT), 'a')) AS t(k, v1) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);
-- result:
1	a	b
-- !result
SELECT * FROM
    (VALUES (CAST(1 AS SMALLINT), 'a')) AS t(k, v1) JOIN
    (VALUES (CAST(1 AS BIGINT), 'b')) AS u(k, v2) USING (k);
-- result:
1	a	b
-- !result
SELECT * FROM
    (VALUES ('a', 1)) AS t(v1, k) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);
-- result:
1	a	b
-- !result
SELECT * FROM
    (VALUES (1, 'inline')) AS inline_t(k1, val) LEFT JOIN t1 USING(k1)
ORDER BY k1, val;
-- result:
1	inline	10	a1
-- !result
SELECT * FROM
    t1 FULL OUTER JOIN
    (VALUES (10, 20, 'inline')) AS inline_t(k1, k2, val) USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	a5	None
None	40	a4	None
1	10	a1	None
2	20	a2	None
3	None	a3	None
10	20	None	inline
-- !result
SELECT k1, k2, v1, val, v2 FROM
    t1 FULL OUTER JOIN
    (VALUES (1, 10, 'inline1'), (2, 20, 'inline2')) AS inline_t(k1, k2, val) USING(k1, k2)
    LEFT JOIN t2 USING(k1, k2)
ORDER BY k1, k2;
-- result:
None	None	a5	None	None
None	40	a4	None	None
1	10	a1	inline1	b1
2	20	a2	inline2	None
3	None	a3	None	None
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (3, 'd')) AS t3_val(k, v3) USING (k)
ORDER BY k;
-- result:
1	a	None	c
2	None	b	None
3	None	None	d
-- !result
SELECT k, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (3, 'y')) AS u(k, v2) USING (k)
GROUP BY k
ORDER BY k;
-- result:
1	2	2	2
2	1	1	0
3	1	0	1
-- !result
SELECT k, v1, v2 FROM (
    SELECT * FROM
        (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
        (VALUES (1, 'b'), (2, 'c')) AS u(k, v2) USING (k)
) sub
ORDER BY k;
-- result:
1	a	b
2	None	c
-- !result
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y'), (4, 'z')) AS u(k, v2) USING (k)
WHERE k >= 2
ORDER BY k;
-- result:
2	b	x
3	c	y
4	None	z
-- !result
SELECT k, k * 10 as k_scaled, CONCAT('K', CAST(k AS STRING)) as k_str, v1, v2
FROM
    (VALUES (1, 'a'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y')) AS u(k, v2) USING (k)
WHERE k IS NOT NULL
ORDER BY k;
-- result:
1	10	K1	a	None
2	20	K2	b	x
3	30	K3	None	y
-- !result
SELECT k,
       CASE WHEN k < 2 THEN 'SMALL' ELSE 'LARGE' END as size_cat,
       COALESCE(v1, 'NULL_V1') as v1_safe,
       COALESCE(v2, 'NULL_V2') as v2_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
1	SMALL	a	NULL_V2
2	LARGE	NULL_V1	b
3	LARGE	c	d
-- !result
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (3, 'x'), (4, 'y'), (5, 'z'), (6, 'w'), (7, 'v')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
1	a	None
2	b	None
3	c	x
4	d	y
5	e	z
6	None	w
7	None	v
-- !result
SELECT * FROM
    (VALUES (1, 'a'), (NULL, 'b'), (2, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (NULL, 'y'), (3, 'z')) AS u(k, v2) USING (k)
ORDER BY k, v1, v2;
-- result:
None	None	y
None	b	None
1	a	x
2	c	None
3	None	z
-- !result
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (1, 'b'), (2, 'bb')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (2, 'cc'), (3, 'ccc')) AS t3_val(k, v3) USING (k)
ORDER BY k;
-- result:
1	a	b	c
2	None	bb	cc
3	None	None	ccc
-- !result
SELECT k1, v1, val, v2 FROM
    t1 FULL OUTER JOIN
    (VALUES (CAST(1 AS TINYINT), 100), (CAST(5 AS TINYINT), 500)) AS inline_t(k1, val) USING(k1)
    LEFT JOIN t2 USING(k1)
ORDER BY k1;
-- result:
None	a4	None	None
None	a5	None	None
1	a1	100	b1
2	a2	None	None
3	a3	None	None
5	None	500	b3
-- !result
SELECT * FROM
    (VALUES (1, 10), (2, 20), (3, 30)) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 100), (3, 200), (4, 300)) AS u(k, v2) USING (k)
WHERE (k IS NULL) OR (k BETWEEN 2 AND 3 AND (v1 > 15 OR v2 > 150))
ORDER BY k;
-- result:
2	20	100
3	30	200
-- !result
SELECT k, v1, v2,
       k + 100 as k_offset,
       CASE WHEN k IS NULL THEN -1 ELSE k END as k_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd'), (4, 'e')) AS u(k, v2) USING (k)
ORDER BY COALESCE(k, 999), v1, v2;
-- result:
1	a	None	101	1
2	None	b	102	2
3	c	d	103	3
4	None	e	104	4
-- !result
SELECT DISTINCT k
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (1, 'xx'), (2, 'y')) AS u(k, v2) USING (k)
ORDER BY k;
-- result:
1
2
-- !result
SELECT * FROM
    (VALUES ('apple', 1), ('banana', 2)) AS t(name, v1) FULL OUTER JOIN
    (VALUES ('banana', 10), ('cherry', 20)) AS u(name, v2) USING (name)
ORDER BY name;
-- result:
apple	1	None
banana	2	10
cherry	None	20
-- !result
SELECT k1, v1, val1, v2, val2 FROM
    t1 FULL OUTER JOIN
    (VALUES (1, 'V1'), (2, 'V2')) AS v1_t(k1, val1) USING(k1) LEFT JOIN
    t2 USING(k1) FULL OUTER JOIN
    (VALUES (1, 'V3'), (5, 'V5')) AS v2_t(k1, val2) USING(k1)
ORDER BY k1
LIMIT 20;
-- result:
None	a5	None	None	None
None	a4	None	None	None
1	a1	V1	b1	V3
2	a2	V2	None	None
3	a3	None	None	None
5	None	None	None	V5
-- !result
DROP DATABASE test_full_outer_join_using;
-- result:
-- !result