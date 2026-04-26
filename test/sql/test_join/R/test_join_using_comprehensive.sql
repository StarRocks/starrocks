-- name: test_join_using_comprehensive
CREATE DATABASE IF NOT EXISTS test_join_using_comprehensive;
-- result:
-- !result
USE test_join_using_comprehensive;
-- result:
-- !result
DROP TABLE IF EXISTS t1;
-- result:
-- !result
CREATE TABLE t1 (
    id TINYINT,
    v1 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t2;
-- result:
-- !result
CREATE TABLE t2 (
    id SMALLINT,
    v2 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t3;
-- result:
-- !result
CREATE TABLE t3 (
    id INT,
    v3 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t4;
-- result:
-- !result
CREATE TABLE t4 (
    id BIGINT,
    v4 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t5;
-- result:
-- !result
CREATE TABLE t5 (
    id TINYINT,
    v5 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t6;
-- result:
-- !result
CREATE TABLE t6 (
    id INT,
    v6 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t7;
-- result:
-- !result
CREATE TABLE t7 (
    id BIGINT,
    v7 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
DROP TABLE IF EXISTS t8;
-- result:
-- !result
CREATE TABLE t8 (
    id SMALLINT,
    v8 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO t1 VALUES
(1, 't1_1'),
(2, 't1_2'),
(3, 't1_3'),
(NULL, 't1_null'),
(10, 't1_10');
-- result:
-- !result
INSERT INTO t2 VALUES
(1, 't2_1'),
(2, 't2_2'),
(4, 't2_4'),
(NULL, 't2_null'),
(20, 't2_20');
-- result:
-- !result
INSERT INTO t3 VALUES
(1, 't3_1'),
(3, 't3_3'),
(5, 't3_5'),
(NULL, 't3_null'),
(30, 't3_30');
-- result:
-- !result
INSERT INTO t4 VALUES
(2, 't4_2'),
(4, 't4_4'),
(6, 't4_6'),
(NULL, 't4_null'),
(40, 't4_40');
-- result:
-- !result
INSERT INTO t5 VALUES
(1, 't5_1'),
(3, 't5_3'),
(7, 't5_7'),
(NULL, 't5_null'),
(50, 't5_50');
-- result:
-- !result
INSERT INTO t6 VALUES
(2, 't6_2'),
(5, 't6_5'),
(8, 't6_8'),
(NULL, 't6_null'),
(60, 't6_60');
-- result:
-- !result
INSERT INTO t7 VALUES
(1, 't7_1'),
(4, 't7_4'),
(9, 't7_9'),
(NULL, 't7_null'),
(70, 't7_70');
-- result:
-- !result
INSERT INTO t8 VALUES
(2, 't8_2'),
(6, 't8_6'),
(3, 't8_3'),
(NULL, 't8_null'),
(80, 't8_80');
-- result:
-- !result
SELECT id, v1
FROM t1 FULL OUTER JOIN (SELECT 1 AS id) s1 USING(id)
ORDER BY id, v1;
-- result:
None	t1_null
1	t1_1
2	t1_2
3	t1_3
10	t1_10
-- !result
SELECT id, v1, v2
FROM t1 FULL OUTER JOIN (SELECT 1 AS id) s1 USING(id)
        FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN (SELECT 9 AS id) s2 USING(id)
ORDER BY id, v1, v2;
-- result:
None	None	t2_null
None	t1_null	None
1	t1_1	t2_1
2	t1_2	t2_2
3	t1_3	None
4	None	t2_4
9	None	None
10	t1_10	None
20	None	t2_20
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 100;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None
2	t1_2	t2_2	None	t4_2	None	t6_2
3	t1_3	None	t3_3	None	t5_3	None
4	None	t2_4	None	t4_4	None	None
5	None	None	t3_5	None	None	t6_5
6	None	None	None	t4_6	None	None
7	None	None	None	None	t5_7	None
8	None	None	None	None	None	t6_8
10	t1_10	None	None	None	None	None
20	None	t2_20	None	None	None	None
30	None	None	t3_30	None	None	None
40	None	None	None	t4_40	None	None
50	None	None	None	None	t5_50	None
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None
2	None	None	None	None	None	t6_2
3	t1_3	None	t3_3	None	t5_3	None
5	None	None	None	None	None	t6_5
8	None	None	None	None	None	t6_8
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        RIGHT JOIN t6 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
2	t1_2	t2_2	None	t4_2	None	t6_2
5	None	None	t3_5	None	None	t6_5
8	None	None	None	None	None	t6_8
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 LEFT JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id;
-- result:
1	None	None	None	None	t5_1	None
2	t1_2	t2_2	None	t4_2	None	t6_2
3	None	None	None	None	t5_3	None
7	None	None	None	None	t5_7	None
50	None	None	None	None	t5_50	None
-- !result
SELECT id, v2, v3, v4, v5, v6, v7
FROM t2 RIGHT JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1	None	None	None	t5_1	None	t7_1
3	None	None	None	t5_3	None	None
4	None	None	None	None	None	t7_4
7	None	None	None	t5_7	None	None
9	None	None	None	None	None	t7_9
50	None	None	None	t5_50	None	None
70	None	None	None	None	None	t7_70
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 FULL OUTER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None
2	None	None	None	None	None	t6_2
3	t1_3	None	t3_3	None	t5_3	None
5	None	None	None	None	None	t6_5
8	None	None	None	None	None	t6_8
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 INNER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        RIGHT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None
2	None	None	None	None	None	t6_2
3	None	None	None	None	t5_3	None
5	None	None	None	None	None	t6_5
7	None	None	None	None	t5_7	None
8	None	None	None	None	None	t6_8
50	None	None	None	None	t5_50	None
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
-- !result
SELECT id, v1, v2, v3, v4, v5, v6
FROM t1 LEFT JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        RIGHT JOIN t6 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
2	None	None	None	None	None	t6_2
5	None	None	None	None	None	t6_5
8	None	None	None	None	None	t6_8
60	None	None	None	None	None	t6_60
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 100;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1
2	t1_2	t2_2	None	t4_2	None	t6_2	None
3	t1_3	None	t3_3	None	t5_3	None	None
4	None	t2_4	None	t4_4	None	None	t7_4
5	None	None	t3_5	None	None	t6_5	None
6	None	None	None	t4_6	None	None	None
7	None	None	None	None	t5_7	None	None
8	None	None	None	None	None	t6_8	None
9	None	None	None	None	None	None	t7_9
10	t1_10	None	None	None	None	None	None
20	None	t2_20	None	None	None	None	None
30	None	None	t3_30	None	None	None	None
40	None	None	None	t4_40	None	None	None
50	None	None	None	None	t5_50	None	None
60	None	None	None	None	None	t6_60	None
70	None	None	None	None	None	None	t7_70
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 INNER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 LEFT JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	None	None	None	None	t5_1	None	t7_1
2	t1_2	t2_2	None	t4_2	None	t6_2	None
3	None	None	None	None	t5_3	None	None
4	None	None	None	None	None	None	t7_4
7	None	None	None	None	t5_7	None	None
9	None	None	None	None	None	None	t7_9
50	None	None	None	None	t5_50	None	None
70	None	None	None	None	None	None	t7_70
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        RIGHT JOIN t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	None	None	None	None	None	None	t7_1
4	None	None	None	None	None	None	t7_4
9	None	None	None	None	None	None	t7_9
70	None	None	None	None	None	None	t7_70
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 FULL OUTER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        LEFT JOIN t7 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1
2	None	None	None	None	None	t6_2	None
3	t1_3	None	t3_3	None	t5_3	None	None
5	None	None	None	None	None	t6_5	None
8	None	None	None	None	None	t6_8	None
60	None	None	None	None	None	t6_60	None
-- !result
SELECT id, v2, v3, v4, v5, v6, v7, v8
FROM t2 RIGHT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        LEFT JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t2_1	t3_1	None	t5_1	None	t7_1	None
2	None	None	None	None	t6_2	None	t8_2
3	None	t3_3	None	t5_3	None	None	t8_3
5	None	None	None	None	t6_5	None	None
6	None	None	None	None	None	None	t8_6
8	None	None	None	None	t6_8	None	None
60	None	None	None	None	t6_60	None	None
80	None	None	None	None	None	None	t8_80
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 INNER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        LEFT JOIN t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1
2	None	None	None	None	None	t6_2	None
5	None	None	None	None	None	t6_5	None
8	None	None	None	None	None	t6_8	None
60	None	None	None	None	None	t6_60	None
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        RIGHT JOIN t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1
4	None	None	None	None	None	None	t7_4
9	None	None	None	None	None	None	t7_9
70	None	None	None	None	None	None	t7_70
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM t1 LEFT JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8,
       CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v5 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v6 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v7 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v8 IS NOT NULL THEN 1 ELSE 0 END as table_count
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id > 0 AND id <= 10
ORDER BY id
LIMIT 100;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None	5
2	t1_2	t2_2	None	t4_2	None	t6_2	None	t8_2	5
3	t1_3	None	t3_3	None	t5_3	None	None	t8_3	4
4	None	t2_4	None	t4_4	None	None	t7_4	None	3
5	None	None	t3_5	None	None	t6_5	None	None	2
6	None	None	None	t4_6	None	None	None	t8_6	2
7	None	None	None	None	t5_7	None	None	None	1
8	None	None	None	None	None	t6_8	None	None	1
9	None	None	None	None	None	None	t7_9	None	1
10	t1_10	None	None	None	None	None	None	None	1
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 INNER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
        INNER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None
2	None	None	None	None	None	None	None	t8_2
3	None	None	None	None	None	None	None	t8_3
6	None	None	None	None	None	None	None	t8_6
80	None	None	None	None	None	None	None	t8_80
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        RIGHT JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 100;
-- result:
2	t1_2	t2_2	None	t4_2	None	t6_2	None	t8_2
3	None	None	None	None	None	None	None	t8_3
6	None	None	None	None	None	None	None	t8_6
80	None	None	None	None	None	None	None	t8_80
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 FULL OUTER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        INNER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 100;
-- result:
2	t1_2	t2_2	None	t4_2	None	t6_2	None	t8_2
3	None	None	None	None	t5_3	None	None	t8_3
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 LEFT JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	None	None	None	None	t5_1	None	t7_1	None
2	None	None	None	None	None	None	None	t8_2
3	None	None	None	None	None	None	None	t8_3
6	None	None	None	None	None	None	None	t8_6
80	None	None	None	None	None	None	None	t8_80
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
        LEFT JOIN t8 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None
4	None	None	None	t4_4	None	None	t7_4	None
-- !result
SELECT id, v2, v3, v4, v5, v6, v7, v8
FROM t2 RIGHT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        INNER JOIN t8 USING(id)
        FULL OUTER JOIN t1 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	None	None	None	None	None	None	None
2	None	None	None	None	None	None	None
3	None	t3_3	None	t5_3	None	None	t8_3
10	None	None	None	None	None	None	None
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        LEFT JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None
2	None	None	None	None	None	t6_2	None	t8_2
3	None	None	t3_3	None	t5_3	None	None	t8_3
5	None	None	None	None	None	t6_5	None	None
6	None	None	None	None	None	None	None	t8_6
8	None	None	None	None	None	t6_8	None	None
60	None	None	None	None	None	t6_60	None	None
80	None	None	None	None	None	None	None	t8_80
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 FULL OUTER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        INNER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 100;
-- result:
3	t1_3	None	t3_3	None	t5_3	None	None	t8_3
-- !result
SELECT id, 
       COUNT(*) as row_count,
       COUNT(v1) as t1_count,
       COUNT(v2) as t2_count,
       COUNT(v3) as t3_count,
       COUNT(v4) as t4_count,
       COUNT(v5) as t5_count,
       COUNT(v6) as t6_count
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id > 0
GROUP BY id
ORDER BY id;
-- result:
1	1	1	1	1	0	1	0
2	1	0	0	0	1	0	1
3	1	1	0	1	0	1	0
4	1	0	0	0	1	0	0
5	1	0	0	0	0	0	1
6	1	0	0	0	1	0	0
8	1	0	0	0	0	0	1
40	1	0	0	0	1	0	0
60	1	0	0	0	0	0	1
-- !result
SELECT id,
       CASE WHEN v1 IS NOT NULL THEN 'HAS_V1' ELSE 'NO_V1' END as v1_status,
       CASE WHEN v2 IS NOT NULL THEN 'HAS_V2' ELSE 'NO_V2' END as v2_status,
       CASE WHEN v3 IS NOT NULL THEN 'HAS_V3' ELSE 'NO_V3' END as v3_status,
       COALESCE(v1, v2, v3, v4, v5, 'NONE') as first_valuea
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1	NO_V1	NO_V2	NO_V3	NONE
2	HAS_V1	HAS_V2	NO_V3	t1_2
4	NO_V1	NO_V2	NO_V3	NONE
5	NO_V1	NO_V2	HAS_V3	t3_5
9	NO_V1	NO_V2	NO_V3	NONE
70	NO_V1	NO_V2	NO_V3	NONE
-- !result
SELECT id, 
       id * 2 as id_double,
       id + 100 as id_offset,
       CONCAT('ID_', CAST(id AS STRING)) as id_str,
       v1, v2, v3, v4, v5
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id BETWEEN 1 AND 10
ORDER BY id;
-- result:
1	2	101	ID_1	t1_1	t2_1	t3_1	None	t5_1
2	4	102	ID_2	None	None	None	None	None
3	6	103	ID_3	None	None	None	None	None
4	8	104	ID_4	None	None	None	t4_4	None
6	12	106	ID_6	None	None	None	None	None
-- !result
SELECT id,
       COUNT(*) as cnt,
       MAX(v1) as max_v1,
       MIN(v2) as min_v2
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id > 0
GROUP BY id
HAVING COUNT(*) > 0
ORDER BY id;
-- result:
1	1	t1_1	t2_1
2	1	None	None
3	1	t1_3	None
4	1	None	None
5	1	None	None
6	1	None	None
8	1	None	None
40	1	None	None
60	1	None	None
-- !result
SELECT id,
       COALESCE(v1, v2, v3, 'DEFAULT') as first_val,
       CONCAT(COALESCE(v1, ''), '-', COALESCE(v2, ''), '-', COALESCE(v3, '')) as combined
FROM t1 LEFT JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        INNER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1	DEFAULT	--
2	t1_2	t1_2-t2_2-
3	DEFAULT	--
4	DEFAULT	--
7	DEFAULT	--
9	DEFAULT	--
50	DEFAULT	--
70	DEFAULT	--
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM t1 FULL OUTER JOIN t2 USING(id)
        LEFT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        INNER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        LEFT JOIN t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE (id > 0 OR id IS NULL)
  AND (v1 IS NOT NULL OR v2 IS NOT NULL OR v3 IS NOT NULL)
ORDER BY id;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None
3	t1_3	None	t3_3	None	t5_3	None	None	t8_3
-- !result
SELECT id, v1, v2, v3
FROM (
    SELECT id, v1, v2, v3, v4, v5, v6
    FROM t1 INNER JOIN t2 USING(id)
            FULL OUTER JOIN t3 USING(id)
            LEFT JOIN t4 USING(id)
            FULL OUTER JOIN t5 USING(id)
            INNER JOIN t6 USING(id)
    WHERE id IS NOT NULL
) sub
WHERE id <= 5
ORDER BY id;
-- result:
2	t1_2	t2_2	None
5	None	None	t3_5
-- !result
SELECT DISTINCT id
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN t7 USING(id)
WHERE id > 0
ORDER BY id;
-- result:
1
4
-- !result
SELECT id, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id IN (1, 2, 3, 4, 5)
ORDER BY id;
-- result:
1	t1_1	t2_1	t3_1	None
2	None	None	None	t4_2
3	t1_3	None	t3_3	None
4	None	None	None	t4_4
5	None	None	None	None
-- !result
SELECT id, v1, v2, v3, v4, v5
FROM t1 INNER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        LEFT JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        INNER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
        LEFT JOIN t8 USING(id)
WHERE id IS NOT NULL
ORDER BY COALESCE(id, 999), v1
LIMIT 20;
-- result:
1	None	None	None	None	None
2	t1_2	t2_2	None	t4_2	None
4	None	None	None	None	None
5	None	None	t3_5	None	None
9	None	None	None	None	None
70	None	None	None	None	None
-- !result
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM test_join_using_comprehensive.t1 
        FULL OUTER JOIN t2 USING(id)
        INNER JOIN test_join_using_comprehensive.t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN test_join_using_comprehensive.t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        INNER JOIN test_join_using_comprehensive.t7 USING(id)
        FULL OUTER JOIN t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	t7_1	None
2	None	None	None	None	None	None	None	t8_2
3	None	None	None	None	None	None	None	t8_3
4	None	None	None	t4_4	None	None	t7_4	None
6	None	None	None	None	None	None	None	t8_6
80	None	None	None	None	None	None	None	t8_80
-- !result
WITH base_join AS (
    SELECT id AS join_key,
           v1 AS val_from_t1,
           v2 AS val_from_t2,
           v3 AS val_from_t3,
           v4 AS val_from_t4,
           v5 AS val_from_t5,
           v6 AS val_from_t6,
           v7 AS val_from_t7,
           v8 AS val_from_t8
    FROM t1 FULL OUTER JOIN t2 USING(id)
            INNER JOIN t3 USING(id)
            FULL OUTER JOIN t4 USING(id)
            LEFT JOIN t5 USING(id)
            FULL OUTER JOIN t6 USING(id)
            INNER JOIN t7 USING(id)
            FULL OUTER JOIN t8 USING(id)
    WHERE id > 0
),
aggregated AS (
    SELECT join_key,
           CONCAT(COALESCE(val_from_t1, ''), '-', COALESCE(val_from_t2, '')) AS combined_val,
           CASE WHEN val_from_t3 IS NOT NULL THEN 'HAS_T3' ELSE 'NO_T3' END AS t3_status,
           CASE WHEN val_from_t5 IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN val_from_t6 IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN val_from_t7 IS NOT NULL THEN 1 ELSE 0 END AS table_count
    FROM base_join
    WHERE join_key BETWEEN 1 AND 10
)
SELECT join_key AS final_id,
       combined_val AS final_combined,
       t3_status AS final_status,
       table_count AS final_count
FROM aggregated
WHERE table_count > 0
ORDER BY final_id, final_combined
LIMIT 50;
-- result:
1	t1_1-t2_1	HAS_T3	2
4	-	NO_T3	1
-- !result
SELECT id AS key_id,
       CONCAT('T1:', COALESCE(v1, 'NULL')) AS t1_value,
       CONCAT('T2:', COALESCE(v2, 'NULL')) AS t2_value,
       CONCAT('T3:', COALESCE(v3, 'NULL')) AS t3_value,
       CASE 
           WHEN v4 IS NOT NULL AND v5 IS NOT NULL THEN 'BOTH'
           WHEN v4 IS NOT NULL THEN 'T4_ONLY'
           WHEN v5 IS NOT NULL THEN 'T5_ONLY'
           ELSE 'NEITHER'
       END AS join_result,
       COALESCE(v1, v2, v3, v4, v5, v6, 'NONE') AS first_non_null
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id IS NOT NULL
ORDER BY key_id, join_result
LIMIT 50;
-- result:
1	T1:t1_1	T2:t2_1	T3:t3_1	T5_ONLY	t1_1
2	T1:NULL	T2:NULL	T3:NULL	T4_ONLY	t4_2
3	T1:t1_3	T2:NULL	T3:t3_3	T5_ONLY	t1_3
4	T1:NULL	T2:NULL	T3:NULL	T4_ONLY	t4_4
5	T1:NULL	T2:NULL	T3:NULL	NEITHER	t6_5
6	T1:NULL	T2:NULL	T3:NULL	T4_ONLY	t4_6
8	T1:NULL	T2:NULL	T3:NULL	NEITHER	t6_8
40	T1:NULL	T2:NULL	T3:NULL	T4_ONLY	t4_40
60	T1:NULL	T2:NULL	T3:NULL	NEITHER	t6_60
-- !result
SELECT id, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
WHERE id IN (
    SELECT id 
    FROM t5 
    WHERE id IS NOT NULL
    GROUP BY id
    HAVING COUNT(*) > 0
)
  AND (v1 IS NOT NULL OR v2 IS NOT NULL OR v3 IS NOT NULL)
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1
3	t1_3	None	t3_3
-- !result
SELECT main.id,
       main.total_vals,
       main.avg_id,
       sub.max_id
FROM (
    SELECT id,
           COUNT(*) AS total_vals,
           AVG(id) AS avg_id,
           MAX(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) AS has_v1
    FROM t1 FULL OUTER JOIN t2 USING(id)
            FULL OUTER JOIN t3 USING(id)
    WHERE id IS NOT NULL
    GROUP BY id
    HAVING COUNT(*) > 0
) main
INNER JOIN (
    SELECT id,
           MAX(id) AS max_id,
           SUM(CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END) AS t4_count
    FROM t4 FULL OUTER JOIN t5 USING(id)
            LEFT JOIN t6 USING(id)
    WHERE id > 0
    GROUP BY id
    HAVING SUM(CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END) > 0
) sub USING(id)
WHERE main.has_v1 > 0
ORDER BY main.id
LIMIT 50;
-- result:
2	1	2.0	2
-- !result
WITH combined_data AS (
    SELECT id, v1 AS value, 'T1' AS source
    FROM t1
    WHERE id > 0 AND v1 IS NOT NULL
    UNION ALL
    SELECT id, v2 AS value, 'T2' AS source
    FROM t2
    WHERE id > 0 AND v2 IS NOT NULL
    UNION ALL
    SELECT id, v3 AS value, 'T3' AS source
    FROM t3
    WHERE id > 0 AND v3 IS NOT NULL
)
SELECT id,
       COUNT(*) AS record_count,
       COUNT(DISTINCT source) AS source_count,
       MAX(value) AS max_value,
       MIN(value) AS min_value
FROM combined_data
GROUP BY id
HAVING COUNT(*) >= 1
ORDER BY id
LIMIT 50;
-- result:
1	3	3	t3_1	t1_1
2	2	2	t2_2	t1_2
3	2	2	t3_3	t1_3
4	1	1	t2_4	t2_4
5	1	1	t3_5	t3_5
10	1	1	t1_10	t1_10
20	1	1	t2_20	t2_20
30	1	1	t3_30	t3_30
-- !result
SELECT id, v1, v2, v3
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
WHERE id > 0
  AND id NOT IN (
      SELECT id 
      FROM t6 
      WHERE v6 IS NULL AND id IS NOT NULL
  )
  AND id IN (
      SELECT id FROM t7 WHERE v7 LIKE 't7%'
  )
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1
4	None	t2_4	None
-- !result
WITH base_data AS (
    SELECT id,
           v1, v2, v3, v4, v5, v6,
           CASE WHEN id <= 3 THEN 'LOW' 
                WHEN id <= 6 THEN 'MID' 
                ELSE 'HIGH' END AS id_range
    FROM t1 FULL OUTER JOIN t2 USING(id)
            FULL OUTER JOIN t3 USING(id)
            LEFT JOIN t4 USING(id)
            FULL OUTER JOIN t5 USING(id)
            INNER JOIN t6 USING(id)
    WHERE id > 0
),
aggregated AS (
    SELECT id_range,
           COUNT(*) AS total_count,
           COUNT(DISTINCT id) AS distinct_ids,
           SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) AS v1_count,
           SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) AS v2_count,
           MAX(v3) AS max_v3,
           MIN(v4) AS min_v4,
           AVG(CASE WHEN v5 IS NOT NULL THEN LENGTH(v5) ELSE 0 END) AS avg_v5_len
    FROM base_data
    GROUP BY id_range
    HAVING COUNT(*) > 0
       AND COUNT(DISTINCT id) >= 1
),
with_totals AS (
    SELECT id_range,
           total_count,
           distinct_ids,
           v1_count,
           v2_count,
           SUM(total_count) OVER () AS grand_total,
           ROUND(100.0 * total_count / SUM(total_count) OVER (), 2) AS percentage
    FROM aggregated
)
SELECT id_range,
       total_count,
       distinct_ids,
       v1_count,
       v2_count,
       grand_total,
       percentage
FROM with_totals
WHERE percentage >= 10.0
ORDER BY 
    CASE id_range 
        WHEN 'LOW' THEN 1 
        WHEN 'MID' THEN 2 
        WHEN 'HIGH' THEN 3 
        ELSE 4 
    END;
-- result:
LOW	1	1	1	1	2	50.00
MID	1	1	0	0	2	50.00
-- !result
SELECT main.id,
       main.v1,
       main.v2,
       stats.total_matches,
       stats.avg_len
FROM (
    SELECT id, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(id)
    WHERE id > 0
) main
CROSS JOIN (
    SELECT COUNT(*) AS total_matches,
           AVG(LENGTH(COALESCE(v3, v4, ''))) AS avg_len
    FROM t3 FULL OUTER JOIN t4 USING(id)
    WHERE id > 0
) stats
WHERE main.id IN (
    SELECT id 
    FROM t5 FULL OUTER JOIN t6 USING(id)
    WHERE id IS NOT NULL
    GROUP BY id
    HAVING COUNT(*) > 0
)
ORDER BY main.id
LIMIT 50;
-- result:
1	t1_1	t2_1	8	4.25
2	t1_2	t2_2	8	4.25
3	t1_3	None	8	4.25
-- !result
SELECT id, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
WHERE (id > 0 AND id < 10)
   OR (v1 IS NOT NULL AND v2 IS NOT NULL)
   OR (v3 LIKE 't3%' AND v4 LIKE 't4%')
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None
2	t1_2	t2_2	None	t4_2
3	t1_3	None	t3_3	None
4	None	t2_4	None	t4_4
5	None	None	t3_5	None
6	None	None	None	t4_6
-- !result
WITH join_result AS (
    SELECT id, v1, v2, v3, v4, v5, v6
    FROM t1 FULL OUTER JOIN t2 USING(id)
            FULL OUTER JOIN t3 USING(id)
            FULL OUTER JOIN t4 USING(id)
            FULL OUTER JOIN t5 USING(id)
            FULL OUTER JOIN t6 USING(id)
    WHERE id IS NOT NULL
)
SELECT id,
       COUNT(*) AS row_count,
       MAX(CONCAT(COALESCE(v1, ''), COALESCE(v2, ''))) AS concat_result,
       SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) AS v3_non_null
FROM join_result
GROUP BY id
HAVING COUNT(*) > 0
ORDER BY id
LIMIT 50;
-- result:
1	1	t1_1t2_1	1
2	1	t1_2t2_2	0
3	1	t1_3	1
4	1	t2_4	0
5	1		1
6	1		0
7	1		0
8	1		0
10	1	t1_10	0
20	1	t2_20	0
30	1		1
40	1		0
50	1		0
60	1		0
-- !result
SELECT DISTINCT id, v1, v3, v5
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
WHERE id BETWEEN 1 AND 20
  AND (v1 IS NOT NULL OR v3 IS NOT NULL OR v5 IS NOT NULL)
ORDER BY id, v1, v3
LIMIT 50;
-- result:
1	t1_1	t3_1	t5_1
2	t1_2	None	None
3	t1_3	t3_3	t5_3
5	None	t3_5	None
7	None	None	t5_7
10	t1_10	None	None
-- !result
SELECT id,
       CASE 
           WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 'BOTH'
           WHEN v1 IS NOT NULL THEN 'ONLY_T1'
           WHEN v2 IS NOT NULL THEN 'ONLY_T2'
           ELSE 'NEITHER'
       END AS presence_status,
       COALESCE(v1, 'DEFAULT_1') AS v1_with_default,
       COALESCE(v2, 'DEFAULT_2') AS v2_with_default
FROM t1 FULL OUTER JOIN t2 USING(id)
WHERE id > 0
ORDER BY id, presence_status
LIMIT 50;
-- result:
1	BOTH	t1_1	t2_1
2	BOTH	t1_2	t2_2
3	ONLY_T1	t1_3	DEFAULT_2
4	ONLY_T2	DEFAULT_1	t2_4
10	ONLY_T1	t1_10	DEFAULT_2
20	ONLY_T2	DEFAULT_1	t2_20
-- !result
SELECT id, v1, v2, v3, v4
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
WHERE id IN (SELECT id FROM t5 WHERE id > 0)
  AND id NOT IN (SELECT id FROM t6 WHERE id > 10 AND id IS NOT NULL)
  AND (v1 IS NOT NULL OR v2 IS NOT NULL)
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None
3	t1_3	None	t3_3	None
-- !result
SELECT id,
       CONCAT(COALESCE(v1, 'NULL'), '-', COALESCE(v2, 'NULL')) AS combined_1_2,
       CONCAT(COALESCE(v3, 'NULL'), '-', COALESCE(v4, 'NULL')) AS combined_3_4,
       COALESCE(v5, v6, v7, 'NO_VALUE') AS first_available
FROM t1 FULL OUTER JOIN t2 USING(id)
        FULL OUTER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
        FULL OUTER JOIN t7 USING(id)
WHERE id > 0
  AND (v1 IS NOT NULL OR v2 IS NOT NULL OR v3 IS NOT NULL)
ORDER BY id
LIMIT 50;
-- result:
1	t1_1-t2_1	t3_1-NULL	t5_1
2	t1_2-t2_2	NULL-t4_2	t6_2
3	t1_3-NULL	t3_3-NULL	t5_3
4	NULL-t2_4	NULL-t4_4	t7_4
5	NULL-NULL	t3_5-NULL	t6_5
10	t1_10-NULL	NULL-NULL	NO_VALUE
20	NULL-t2_20	NULL-NULL	NO_VALUE
30	NULL-NULL	t3_30-NULL	NO_VALUE
-- !result
SELECT id, val, v1, v2
FROM (VALUES (1, 'val1'), (2, 'val2'), (3, 'val3'), (NULL, 'valN')) AS vals(id, val)
     FULL OUTER JOIN t1 USING(id)
     FULL OUTER JOIN t2 USING(id)
WHERE id IS NOT NULL OR val IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
None	valN	None	None
1	val1	t1_1	t2_1
2	val2	t1_2	t2_2
3	val3	t1_3	None
4	None	None	t2_4
10	None	t1_10	None
20	None	None	t2_20
-- !result
SELECT id, val1, val2, v1, v2, v3
FROM (VALUES (1, 'A'), (2, 'B'), (5, 'E'), (NULL, 'N1')) AS v1(id, val1)
     FULL OUTER JOIN (VALUES (1, 'X'), (3, 'Y'), (5, 'Z'), (NULL, 'N2')) AS v2(id, val2) USING(id)
     FULL OUTER JOIN t1 USING(id)
     FULL OUTER JOIN t2 USING(id)
     FULL OUTER JOIN t3 USING(id)
WHERE id > 0 OR val1 IS NOT NULL OR val2 IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
None	None	N2	None	None	None
None	N1	None	None	None	None
1	A	X	t1_1	t2_1	t3_1
2	B	None	t1_2	t2_2	None
3	None	Y	t1_3	None	t3_3
4	None	None	None	t2_4	None
5	E	Z	None	None	t3_5
10	None	None	t1_10	None	None
20	None	None	None	t2_20	None
30	None	None	None	None	t3_30
-- !result
WITH all_sources AS (
    SELECT id, 'TABLE_T1' AS source, v1 AS value
    FROM t1
    WHERE v1 IS NOT NULL
    UNION ALL
    SELECT id, 'TABLE_T2' AS source, v2 AS value
    FROM t2
    WHERE v2 IS NOT NULL
    UNION ALL
    SELECT id, 'CONSTANT' AS source, val AS value
    FROM (VALUES (1, 'const1'), (2, 'const2'), (7, 'const7')) AS constants(id, val)
)
SELECT id,
       source,
       value,
       COUNT(*) OVER (PARTITION BY id) AS id_occurrences,
       MAX(value) OVER (PARTITION BY id) AS max_value_per_id
FROM all_sources
WHERE id > 0
ORDER BY id, source
LIMIT 50;
-- result:
1	CONSTANT	const1	3	t2_1
1	TABLE_T1	t1_1	3	t2_1
1	TABLE_T2	t2_1	3	t2_1
2	CONSTANT	const2	3	t2_2
2	TABLE_T1	t1_2	3	t2_2
2	TABLE_T2	t2_2	3	t2_2
3	TABLE_T1	t1_3	1	t1_3
4	TABLE_T2	t2_4	1	t2_4
7	CONSTANT	const7	1	const7
10	TABLE_T1	t1_10	1	t1_10
20	TABLE_T2	t2_20	1	t2_20
-- !result
SELECT id,
       int_val,
       str_val,
       t1.v1,
       t2.v2,
       t3.v3,
       CONCAT(COALESCE(str_val, 'NULL'), '-', COALESCE(t1.v1, 'NULL')) AS combined
FROM (VALUES 
    (1, 100, 'str1'), (2, 200, 'str2'), (3, 300, 'str3'),
    (NULL, 400, 'strN'), (5, NULL, 'str5'), (6, 600, NULL)
) AS vals(id, int_val, str_val)
FULL OUTER JOIN t1 USING(id)
FULL OUTER JOIN t2 USING(id)
FULL OUTER JOIN t3 USING(id)
WHERE (id > 0 AND int_val > 150)
   OR (str_val LIKE 'str%' AND t1.v1 IS NOT NULL)
   OR (id IS NULL AND int_val IS NOT NULL)
ORDER BY id, int_val
LIMIT 50;
-- result:
None	400	strN	None	None	None	strN-NULL
1	100	str1	t1_1	t2_1	t3_1	str1-t1_1
2	200	str2	t1_2	t2_2	None	str2-t1_2
3	300	str3	t1_3	None	t3_3	str3-t1_3
6	600	None	None	None	None	NULL-NULL
-- !result
WITH t1_early AS (
    SELECT id, v1 AS early_value
    FROM t1
    WHERE id <= 5
),
t1_late AS (
    SELECT id, v1 AS late_value
    FROM t1
    WHERE id >= 3
)
SELECT id,
       early_value,
       late_value,
       t2.v2,
       t3.v3,
       CASE 
           WHEN early_value IS NOT NULL AND late_value IS NOT NULL THEN 'OVERLAP'
           WHEN early_value IS NOT NULL THEN 'EARLY_ONLY'
           WHEN late_value IS NOT NULL THEN 'LATE_ONLY'
           ELSE 'NEITHER'
       END AS period_status
FROM t1_early
     FULL OUTER JOIN t1_late USING(id)      -- Self-join: FULL OUTER
     INNER JOIN t2 USING(id)                -- Then INNER
     LEFT JOIN t3 USING(id)                 -- Then LEFT
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	None	t2_1	t3_1	EARLY_ONLY
2	t1_2	None	t2_2	None	EARLY_ONLY
-- !result
WITH base_left AS (
    SELECT id, v1, v2
    FROM t1 INNER JOIN t2 USING(id)                 -- Left branch: INNER
    WHERE id > 0
),
base_right AS (
    SELECT id, v3, v4
    FROM t3 LEFT JOIN t4 USING(id)                  -- Right branch: LEFT
    WHERE id > 0
),
combined_branches AS (
    SELECT id, bl.v1, bl.v2, br.v3, br.v4
    FROM base_left bl
         FULL OUTER JOIN base_right br USING(id)   -- Combine left and right
),
with_center AS (
    SELECT id, cb.v1, cb.v2, cb.v3, cb.v4, t5.v5, t6.v6
    FROM combined_branches cb
         FULL OUTER JOIN t5 USING(id)              -- Add center data
         LEFT JOIN t6 USING(id)
)
SELECT id,
       v1, v2, v3, v4, v5, v6,
       CASE 
           WHEN v1 IS NOT NULL AND v3 IS NOT NULL THEN 'BOTH_SIDES'
           WHEN v1 IS NOT NULL THEN 'LEFT_ONLY'
           WHEN v3 IS NOT NULL THEN 'RIGHT_ONLY'
           ELSE 'CENTER_ONLY'
       END AS data_source
FROM with_center
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	None	BOTH_SIDES
2	t1_2	t2_2	None	None	None	t6_2	LEFT_ONLY
3	None	None	t3_3	None	t5_3	None	RIGHT_ONLY
5	None	None	t3_5	None	None	t6_5	RIGHT_ONLY
7	None	None	None	None	t5_7	None	CENTER_ONLY
30	None	None	t3_30	None	None	None	RIGHT_ONLY
50	None	None	None	None	t5_50	None	CENTER_ONLY
-- !result
WITH level1 AS (
    SELECT id, v1, v2
    FROM t1 LEFT JOIN t2 USING(id)
    WHERE id > 0
),
level2 AS (
    SELECT id, l1.v1, l1.v2, t3.v3
    FROM level1 l1
         FULL OUTER JOIN t3 USING(id)
    WHERE id IS NOT NULL OR id IS NOT NULL
),
level3 AS (
    SELECT id, l2.v1, l2.v2, l2.v3, t4.v4
    FROM level2 l2
         LEFT JOIN t4 USING(id)
    WHERE id > 0 AND (l2.v1 IS NOT NULL OR l2.v2 IS NOT NULL OR l2.v3 IS NOT NULL OR t4.v4 IS NOT NULL)
)
SELECT id,
       v1, v2, v3, v4,
       CONCAT(
           COALESCE(v1, 'N'),
           COALESCE(v2, 'N'),
           COALESCE(v3, 'N'),
           COALESCE(v4, 'N')
       ) AS value_signature,
       LENGTH(CONCAT(COALESCE(v1, ''), COALESCE(v2, ''), COALESCE(v3, ''))) AS combined_length
FROM level3
WHERE id IS NOT NULL
ORDER BY combined_length DESC, id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t1_1t2_1t3_1N	12
2	t1_2	t2_2	None	t4_2	t1_2t2_2Nt4_2	8
3	t1_3	None	t3_3	None	t1_3Nt3_3N	8
10	t1_10	None	None	None	t1_10NNN	5
30	None	None	t3_30	None	NNt3_30N	5
5	None	None	t3_5	None	NNt3_5N	4
-- !result
WITH base_data AS (
    SELECT id, v1, v2, v3
    FROM t1 FULL OUTER JOIN t2 USING(id)
            INNER JOIN t3 USING(id)
    WHERE id > 0
),
extended_data AS (
    SELECT id, bd.v1, bd.v2, bd.v3, t4.v4, t5.v5
    FROM base_data bd
         LEFT JOIN t4 USING(id)
         LEFT JOIN t5 USING(id)
)
SELECT id,
       v1, v2, v3, v4, v5,
       COUNT(*) OVER (PARTITION BY 
           CASE WHEN id <= 3 THEN 'LOW' WHEN id <= 6 THEN 'MID' ELSE 'HIGH' END
       ) AS range_count,
       ROW_NUMBER() OVER (
           PARTITION BY 
               CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END
           ORDER BY id
       ) AS rn_by_v1_presence
FROM extended_data
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
1	t1_1	t2_1	t3_1	None	t5_1	2	1
3	t1_3	None	t3_3	None	t5_3	2	2
-- !result
WITH raw_data AS (
    SELECT id, v1, v2, v3, v4
    FROM t1 INNER JOIN t2 USING(id)
            FULL OUTER JOIN t3 USING(id)
            LEFT JOIN t4 USING(id)
    WHERE id > 0
),
agg_level1 AS (
    SELECT id,
           COUNT(*) AS l1_count,
           MAX(v1) AS l1_max_v1,
           MIN(v2) AS l1_min_v2,
           SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) AS l1_v3_count
    FROM raw_data
    GROUP BY id
)
SELECT agg.id,
       agg.l1_count,
       agg.l1_max_v1,
       agg.l1_min_v2,
       agg.l1_v3_count,
       t5.v5,
       t6.v6,
       CASE 
           WHEN agg.l1_v3_count > 2 THEN 'HIGH'
           WHEN agg.l1_v3_count > 0 THEN 'MEDIUM'
           ELSE 'LOW'
       END AS v3_presence_level
FROM agg_level1 agg
     LEFT JOIN t5 USING(id)
     FULL OUTER JOIN t6 USING(id)
WHERE agg.id IS NOT NULL
ORDER BY agg.l1_count DESC, agg.id
LIMIT 50;
-- result:
E: (1064, "Getting analyzing error. Detail message: Column '`agg`.`id`' cannot be resolved.")
-- !result
WITH branch1 AS (
    SELECT id, v1 AS value, 'BRANCH1' AS source
    FROM t1 LEFT JOIN t2 USING(id)
    WHERE v1 IS NOT NULL
),
branch2 AS (
    SELECT id, v3 AS value, 'BRANCH2' AS source
    FROM t3 FULL OUTER JOIN t4 USING(id)
    WHERE v3 IS NOT NULL
),
branch3 AS (
    SELECT id, v5 AS value, 'BRANCH3' AS source
    FROM t5 INNER JOIN t6 USING(id)
    WHERE v5 IS NOT NULL
),
all_branches AS (
    SELECT * FROM branch1
    UNION ALL
    SELECT * FROM branch2
    UNION ALL
    SELECT * FROM branch3
)
SELECT id,
       COUNT(DISTINCT source) AS source_count,
       COUNT(*) AS total_records,
       MAX(value) AS max_value,
       MIN(value) AS min_value,
       CASE 
           WHEN COUNT(DISTINCT source) = 3 THEN 'ALL_BRANCHES'
           WHEN COUNT(DISTINCT source) = 2 THEN 'TWO_BRANCHES'
           WHEN COUNT(DISTINCT source) = 1 THEN 'ONE_BRANCH'
           ELSE 'NONE'
       END AS branch_coverage
FROM all_branches
WHERE id > 0
GROUP BY id
ORDER BY source_count DESC, id
LIMIT 50;
-- result:
1	2	2	t3_1	t1_1	TWO_BRANCHES
3	2	2	t3_3	t1_3	TWO_BRANCHES
2	1	1	t1_2	t1_2	ONE_BRANCH
5	1	1	t3_5	t3_5	ONE_BRANCH
10	1	1	t1_10	t1_10	ONE_BRANCH
30	1	1	t3_30	t3_30	ONE_BRANCH
-- !result
WITH group_a AS (
    SELECT id, v1, v2
    FROM t1 FULL OUTER JOIN t2 USING(id)
    WHERE id BETWEEN 1 AND 5
),
group_b AS (
    SELECT id, v3, v4
    FROM t3 INNER JOIN t4 USING(id)
    WHERE id BETWEEN 3 AND 7
)
SELECT ga.id AS id_a,
       gb.id AS id_b,
       ga.v1,
       ga.v2,
       gb.v3,
       gb.v4,
       CASE 
           WHEN ga.id = gb.id THEN 'MATCH'
           WHEN ga.id < gb.id THEN 'A_LESS'
           ELSE 'B_LESS'
       END AS relationship
FROM group_a ga
     INNER JOIN group_b gb ON ga.id = gb.id
WHERE ga.id IS NOT NULL AND gb.id IS NOT NULL
ORDER BY ga.id, gb.id
LIMIT 50;
-- result:
-- !result
SELECT v1, v2, id, v3, v4, v5, v6
FROM t1 FULL OUTER JOIN t2 USING(id)
        INNER JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        LEFT JOIN t5 USING(id)
        FULL OUTER JOIN t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;
-- result:
t1_1	t2_1	1	t3_1	None	t5_1	None
None	None	2	None	t4_2	None	t6_2
t1_3	None	3	t3_3	None	t5_3	None
None	None	4	None	t4_4	None	None
None	None	5	None	None	None	t6_5
None	None	6	None	t4_6	None	None
None	None	8	None	None	None	t6_8
None	None	40	None	t4_40	None	None
None	None	60	None	None	None	t6_60
-- !result
SELECT v3,
       CONCAT(v1, '-', v2) AS combined_val,
       v5,
       CASE WHEN id > 5 THEN 'HIGH' ELSE 'LOW' END AS id_category,
       v6,
       id * 10 AS id_x10,
       v4,
       id,  -- USING column at the end
       v1,
       v2,
       id + id + v4,
       id + id
FROM t1 FULL OUTER JOIN t2 USING(id)
        RIGHT JOIN t3 USING(id)
        FULL OUTER JOIN t4 USING(id)
        FULL OUTER JOIN t5 USING(id)
        LEFT JOIN t6 USING(id)
WHERE id BETWEEN 1 AND 10
ORDER BY id, v1
LIMIT 50;
-- result:
t3_1	t1_1-t2_1	t5_1	LOW	None	10	None	1	t1_1	t2_1	None	2
None	None	None	LOW	t6_2	20	t4_2	2	None	None	None	4
t3_3	None	t5_3	LOW	None	30	None	3	t1_3	None	None	6
None	None	None	LOW	None	40	t4_4	4	None	None	None	8
t3_5	None	None	LOW	t6_5	50	None	5	None	None	None	10
None	None	None	HIGH	None	60	t4_6	6	None	None	None	12
None	None	t5_7	HIGH	None	70	None	7	None	None	None	14
-- !result
DROP DATABASE test_join_using_comprehensive;
-- result:
-- !result