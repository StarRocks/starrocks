-- name: test_full_outer_join_using_aliased_columns
-- Test Point:
--   1. FULL OUTER JOIN ... USING(a, b, c) merges every USING column with its own
--      counterpart when a relation exposes only some of them under their own name
--      (here region/city come out of a view as max() aggregates), and the non-USING
--      columns keep their own values.
--   2. The same join with ORDER BY on an output ordinal plans instead of failing.
-- Method: compare the values against the equivalent ON + coalesce rewrite, which the
--         planner builds without synthesizing the merged columns.
-- Scope: RelationTransformer#buildFullOuterJoinUsingPlan, QueryTransformer#projectForOrder
DROP DATABASE IF EXISTS test_full_outer_join_using_aliased_columns_${uuid0};
CREATE DATABASE test_full_outer_join_using_aliased_columns_${uuid0};
USE test_full_outer_join_using_aliased_columns_${uuid0};

CREATE TABLE cell_1 (
    k INT,
    dt DATETIME,
    region VARCHAR(64),
    city VARCHAR(64),
    v BIGINT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE cell_2 (
    k INT,
    dt DATETIME,
    region VARCHAR(64),
    city VARCHAR(64),
    v BIGINT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES ("replication_num" = "1");

INSERT INTO cell_1 VALUES
    (1, '2026-01-01 00:00:00', 'R1', 'C1', 10),
    (2, '2026-01-01 00:00:00', 'R2', 'C2', 20);
INSERT INTO cell_2 VALUES
    (1, '2026-01-01 00:00:00', 'R1', 'C1', 100),
    (3, '2026-01-01 00:00:00', 'R3', 'C3', 300);

-- dt keeps its name through the view while region/city do not, which is what
-- desynchronizes a pairing that goes by column name.
CREATE VIEW view_1 AS
SELECT k, dt, max(region) AS region, max(city) AS city, sum(v) AS v FROM cell_1 GROUP BY k, dt;
CREATE VIEW view_2 AS
SELECT k, dt, max(region) AS region, max(city) AS city, sum(v) AS v FROM cell_2 GROUP BY k, dt;

SELECT dt, region, city, s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
USING (dt, region, city)
ORDER BY region, city;

SELECT dt, region, city, s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
USING (dt, region, city)
ORDER BY 2, 3;

-- the same join spelled with ON, whose merged columns the planner does not synthesize
SELECT coalesce(q1.dt, q2.dt) AS dt,
       coalesce(q1.region, q2.region) AS region,
       coalesce(q1.city, q2.city) AS city,
       s1, s2
FROM (SELECT dt, region, city, sum(v) AS s1 FROM view_1 GROUP BY dt, region, city) q1
FULL OUTER JOIN (SELECT dt, region, city, sum(v) AS s2 FROM view_2 GROUP BY dt, region, city) q2
  ON q1.dt = q2.dt AND q1.region = q2.region AND q1.city = q2.city
ORDER BY region, city;

DROP DATABASE test_full_outer_join_using_aliased_columns_${uuid0};
