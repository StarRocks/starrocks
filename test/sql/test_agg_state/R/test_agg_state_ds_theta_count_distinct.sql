-- name: test_agg_state_ds_theta_count_distinct
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
CREATE TABLE test_theta_sketch (
  dt VARCHAR(10),
  theta_id ds_theta_count_distinct(bigint not null),
  theta_province ds_theta_count_distinct(varchar),
  theta_age ds_theta_count_distinct(SMALLINT),
  theta_dt ds_theta_count_distinct(varchar not null)
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;
-- result:
-- !result
select id, province, from_binary(ds_theta_count_distinct_state(id), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_theta_count_distinct_state(province), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_theta_count_distinct_state(age), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_theta_count_distinct_state(dt), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select ds_theta_count_distinct_merge(theta_province) from test_theta_sketch;
-- result:
None
-- !result
select ds_theta_count_distinct_merge(theta_province) from test_theta_sketch group by dt order by 1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100));
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100));
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-25" from table(generate_series(1, 100));
-- result:
-- !result
insert into test_theta_sketch select dt, ds_theta_count_distinct_state(id), ds_theta_count_distinct_state(province), ds_theta_count_distinct_state(age), ds_theta_count_distinct_state(dt) from t1;
-- result:
-- !result
insert into test_theta_sketch select dt, ds_theta_count_distinct_state(id), ds_theta_count_distinct_state(province), ds_theta_count_distinct_state(age), ds_theta_count_distinct_state(dt) from t1;
-- result:
-- !result
insert into test_theta_sketch select dt, to_binary('YWJj', 'encode64'), to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64')  from t1 limit 1;
-- result:
-- !result
ALTER TABLE test_theta_sketch COMPACT;
-- result:
-- !result
select ds_theta_count_distinct_merge(theta_id), ds_theta_count_distinct_merge(theta_province), ds_theta_count_distinct_merge(theta_age), ds_theta_count_distinct_merge(theta_dt) from test_theta_sketch;
-- result:
100	100	100	2
-- !result
select dt, ds_theta_count_distinct_merge(theta_id), ds_theta_count_distinct_merge(theta_province), ds_theta_count_distinct_merge(theta_age), ds_theta_count_distinct_merge(theta_dt) from test_theta_sketch group by dt order by 1 limit 3;
-- result:
2024-07-24	100	100	100	1
2024-07-25	100	100	100	1
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
insert into test_theta_sketch select dt, ds_theta_count_distinct_state(id), ds_theta_count_distinct_state(province), ds_theta_count_distinct_state(age), ds_theta_count_distinct_state(dt) from t1;
-- result:
-- !result
select ds_theta_count_distinct_merge(theta_id), ds_theta_count_distinct_merge(theta_province), ds_theta_count_distinct_merge(theta_age), ds_theta_count_distinct_merge(theta_dt) from test_theta_sketch;
-- result:
100	102	100	3
-- !result
select dt, ds_theta_count_distinct_merge(theta_id), ds_theta_count_distinct_merge(theta_province), ds_theta_count_distinct_merge(theta_age), ds_theta_count_distinct_merge(theta_dt) from test_theta_sketch group by dt order by 1 limit 3;
-- result:
2024-07-22	1	1	1	1
2024-07-24	100	100	100	1
2024-07-25	100	101	100	1
-- !result