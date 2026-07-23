-- name: test_ds_theta_count_distinct
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));
-- result:
-- !result
select ds_theta_count_distinct(id) from t1;
-- result:
100215
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
-- result:
100215	100846	100	1
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age) from t1 group by dt order by 1, 2;
-- result:
100215	100846	100
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 group by dt order by 1, 2;
-- result:
100215	100846	100	1
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
select ds_theta_count_distinct(id) from t1;
-- result:
100215
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
-- result:
100215	100846	100	3
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age) from t1 group by dt order by 1, 2;
-- result:
1	1	1
1	1	1
100215	100846	100
-- !result
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 group by dt order by 1, 2;
-- result:
1	1	1	1
1	1	1	1
100215	100846	100	1
-- !result
select ds_theta_count_distinct(id, 12) from t1;
-- result:
100215
-- !result
[UC]select ds_theta_count_distinct(id, 14) from t1;
[UC]select ds_theta_count_distinct(id, 20) from t1;
CREATE TABLE t_theta_states (
  `id` bigint,
  `dt` varchar(10),
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 3;
-- result:
-- !result
INSERT INTO t_theta_states SELECT id, dt,
  ds_theta_count_distinct_state(id),
  ds_theta_count_distinct_state(province),
  ds_theta_count_distinct_state(age),
  ds_theta_count_distinct_state(dt) FROM t1;
-- result:
-- !result
[UC]SELECT DS_THETA_ACCUMULATE(id), DS_THETA_ACCUMULATE(province), DS_THETA_ACCUMULATE(age), DS_THETA_ACCUMULATE(dt) FROM t1;
[UC]SELECT DS_THETA_ACCUMULATE(id, 14) FROM t1;
[UC]SELECT dt, DS_THETA_ACCUMULATE(id), DS_THETA_ACCUMULATE(province, 14) FROM t1 GROUP BY dt ORDER BY 1 LIMIT 3;
[UC]SELECT DS_THETA_COMBINE(ds_id), DS_THETA_COMBINE(ds_province), DS_THETA_COMBINE(ds_age), DS_THETA_COMBINE(ds_dt) FROM t_theta_states;
SELECT DS_THETA_ESTIMATE(ds_id), DS_THETA_ESTIMATE(ds_province), DS_THETA_ESTIMATE(ds_age), DS_THETA_ESTIMATE(ds_dt) FROM t_theta_states;
-- result:
100215	100846	100	3
-- !result
select ds_theta_count_distinct(id, 10, "INVALID") from t1 order by 1, 2;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 7 to line 1, column 48. Detail message: No matching function with signature: ds_theta_count_distinct(bigint(20), tinyint(4), varchar).')
-- !result