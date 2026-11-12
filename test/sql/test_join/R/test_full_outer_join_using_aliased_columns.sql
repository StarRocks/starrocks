-- name: test_full_outer_join_using_aliased_columns
DROP DATABASE IF EXISTS test_full_outer_join_using_aliased_columns_${uuid0};
-- result:
-- !result
CREATE DATABASE test_full_outer_join_using_aliased_columns_${uuid0};
-- result:
-- !result
USE test_full_outer_join_using_aliased_columns_${uuid0};
-- result:
-- !result
CREATE TABLE cell_1 (
    k INT,
    dt DATETIME,
    region VARCHAR(64),
    city VARCHAR(64),
    v BIGINT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE cell_2 (
    k INT,
    dt DATETIME,
    region VARCHAR(64),
    city VARCHAR(64),
    v BIGINT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO cell_1 VALUES
    (1, '2026-01-01 00:00:00', 'R1', 'C1', 10),
    (2, '2026-01-01 00:00:00', 'R2', 'C2', 20);
-- result:
-- !result
INSERT INTO cell_2 VALUES
    (1, '2026-01-01 00:00:00', 'R1', 'C1', 100),
    (3, '2026-01-01 00:00:00', 'R3', 'C3', 300);
-- result:
-- !result
CREATE VIEW view_1 AS
SELECT k, dt, max(region) AS region, max(city) AS city, sum(v) AS v FROM cell_1 GROUP BY k, dt;
-- result:
-- !result
CREATE VIEW view_2 AS
SELECT k, dt, max(region) AS region, max(city) AS city, sum(v) AS v FROM cell_2 GROUP BY k, dt;
-- result:
-- !result
SELECT dt, region, city, s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
USING (dt, region, city)
ORDER BY region, city;
-- result:
2026-01-01 00:00:00	R1	C1	10	100
2026-01-01 00:00:00	R2	C2	20	None
2026-01-01 00:00:00	R3	C3	None	300
-- !result
SELECT dt, region, city, s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
USING (dt, region, city)
ORDER BY 2, 3;
-- result:
2026-01-01 00:00:00	R1	C1	10	100
2026-01-01 00:00:00	R2	C2	20	None
2026-01-01 00:00:00	R3	C3	None	300
-- !result
SELECT coalesce(q1.dt, q2.dt) AS dt,
       coalesce(q1.region, q2.region) AS region,
       coalesce(q1.city, q2.city) AS city,
       s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
  ON q1.dt = q2.dt AND q1.region = q2.region AND q1.city = q2.city
ORDER BY region, city;
-- result:
2026-01-01 00:00:00	R1	C1	10	100
2026-01-01 00:00:00	R2	C2	20	None
2026-01-01 00:00:00	R3	C3	None	300
-- !result
DROP DATABASE test_full_outer_join_using_aliased_columns_${uuid0};
-- result:
-- !result