-- name: test_agg_state_table_percentile_approx_weighted
CREATE TABLE t1 (
    c1 int,
    c2 double,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<double, double>,
    c13 struct<a bigint, b double>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
insert into t1 
    select generate_series, generate_series,  11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)
    from table(generate_series(1, 500, 3));
-- result:
-- !result
insert into t1 values
    (1, 1, 11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)),
    (2, 2, 22, 222, 2222, 22222, "222222", 2.2, "2024-09-02", "2024-09-02 11:00:00", [3, 4, 5], map(1, 511.2), row(200, 200)),
    (3, 3, 33, 333, 3333, 33333, "333333", 3.3,  "2024-09-03", "2024-09-03 00:00:00", [4, 1, 2], map(1, 666.6), row(300, 300)),
    (4, 4, 11, 444, 4444, 44444, "444444", 4.4, "2024-09-04", "2024-09-04 12:00:00", [7, 7, 5], map(1, 444.4), row(400, 400)),
    (5, null, null, null, null, null, null, null, null, null, null, null, null);
-- result:
-- !result
CREATE TABLE test_agg_state_percentile_approx_weighted(
  c1 VARCHAR(10),
  c2 percentile_approx_weighted(double, bigint, double),
  c3 percentile_approx_weighted(double, bigint, double),
  c4 percentile_approx_weighted(double, bigint, double),
  c5 percentile_approx_weighted(double, bigint, double),
  c6 percentile_approx_weighted(double, bigint, double),
  c11 percentile_approx_weighted(double, bigint, double),
  c12 percentile_approx_weighted(double, bigint, double),
  c13 percentile_approx_weighted(double, bigint, double),
  c14 percentile_approx_weighted(double, bigint, double)
)
AGGREGATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3;
-- result:
-- !result
INSERT INTO test_agg_state_percentile_approx_weighted
SELECT 
  c1,
  percentile_approx_weighted_state(c1, c1, 0.5),
  percentile_approx_weighted_state(c2, 1, 0.7),
  percentile_approx_weighted_state(c3, c1, 0.8, 10000),
  percentile_approx_weighted_state(c4, c1, 1),
  percentile_approx_weighted_state(c5, c1, 0.1),
  percentile_approx_weighted_state(c6, c1, 0.5),
  percentile_approx_weighted_state(c11[1], c1, 0.5),
  percentile_approx_weighted_state(c12[1], c12[2], 0.5),
  percentile_approx_weighted_state(c13.a, c13.b, 0.5)
FROM t1;
-- result:
-- !result
SELECT c1,
  percentile_approx_weighted_merge(c2),
  percentile_approx_weighted_merge(c3),
  percentile_approx_weighted_merge(c4),
  percentile_approx_weighted_merge(c5),
  percentile_approx_weighted_merge(c6),
  percentile_approx_weighted_merge(c11),
  percentile_approx_weighted_merge(c12),
  percentile_approx_weighted_merge(c13),
  percentile_approx_weighted_merge(c14)
FROM test_agg_state_percentile_approx_weighted
GROUP BY c1 ORDER BY c1 limit 10;
-- result:
1	1.0	1.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
10	10.0	10.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
100	100.0	100.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
103	103.0	103.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
106	106.0	106.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
109	109.0	109.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
112	112.0	112.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
115	115.0	115.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
118	118.0	118.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
121	121.0	121.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
-- !result
SELECT percentile_approx_weighted_merge(c2),
  percentile_approx_weighted_merge(c3),
  percentile_approx_weighted_merge(c4),
  percentile_approx_weighted_merge(c5),
  percentile_approx_weighted_merge(c6),
  percentile_approx_weighted_merge(c11),
  percentile_approx_weighted_merge(c12),
  percentile_approx_weighted_merge(c13),
  percentile_approx_weighted_merge(c14)
FROM test_agg_state_percentile_approx_weighted;
-- result:
353.83734130859375	346.5999755859375	11.0	444.0	1111.0	11111.0	1.0	None	100.0
-- !result
insert into t1 values
    (1, 1, 11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)),
    (2, 2, 22, 222, 2222, 22222, "222222", 2.2, "2024-09-02", "2024-09-02 11:00:00", [3, 4, 5], map(1, 511.2), row(200, 200)),
    (3, 3, 33, 333, 3333, 33333, "333333", 3.3,  "2024-09-03", "2024-09-03 00:00:00", [4, 1, 2], map(1, 666.6), row(300, 300)),
    (4, 4, 11, 444, 4444, 44444, "444444", 4.4, "2024-09-04", "2024-09-04 12:00:00", [7, 7, 5], map(1, 444.4), row(400, 400)),
    (5, null, null, null, null, null, null, null, null, null, null, null, null);
-- result:
-- !result
insert into t1 values
    (1, 1, 11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)),
    (2, 2, 22, 222, 2222, 22222, "222222", 2.2, "2024-09-02", "2024-09-02 11:00:00", [3, 4, 5], map(1, 511.2), row(200, 200)),
    (3, 3, 33, 333, 3333, 33333, "333333", 3.3,  "2024-09-03", "2024-09-03 00:00:00", [4, 1, 2], map(1, 666.6), row(300, 300)),
    (4, 4, 11, 444, 4444, 44444, "444444", 4.4, "2024-09-04", "2024-09-04 12:00:00", [7, 7, 5], map(1, 444.4), row(400, 400)),
    (5, null, null, null, null, null, null, null, null, null, null, null, null);
-- result:
-- !result
ALTER TABLE test_agg_state_percentile_approx_weighted COMPACT;
-- result:
-- !result
SELECT c1,
  percentile_approx_weighted_merge(c2),
  percentile_approx_weighted_merge(c3),
  percentile_approx_weighted_merge(c4),
  percentile_approx_weighted_merge(c5),
  percentile_approx_weighted_merge(c6),
  percentile_approx_weighted_merge(c11),
  percentile_approx_weighted_merge(c12),
  percentile_approx_weighted_merge(c13),
  percentile_approx_weighted_merge(c14)
FROM test_agg_state_percentile_approx_weighted
GROUP BY c1 ORDER BY c1 limit 10;
-- result:
1	1.0	1.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
10	10.0	10.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
100	100.0	100.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
103	103.0	103.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
106	106.0	106.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
109	109.0	109.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
112	112.0	112.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
115	115.0	115.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
118	118.0	118.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
121	121.0	121.0	11.0	111.0	1111.0	11111.0	1.0	None	100.0
-- !result
SELECT percentile_approx_weighted_merge(c2),
  percentile_approx_weighted_merge(c3),
  percentile_approx_weighted_merge(c4),
  percentile_approx_weighted_merge(c5),
  percentile_approx_weighted_merge(c6),
  percentile_approx_weighted_merge(c11),
  percentile_approx_weighted_merge(c12),
  percentile_approx_weighted_merge(c13),
  percentile_approx_weighted_merge(c14)
FROM test_agg_state_percentile_approx_weighted;
-- result:
353.83734130859375	346.5999755859375	11.0	444.0	1111.0	11111.0	1.0	None	100.0
-- !result