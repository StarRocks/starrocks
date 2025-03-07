-- name: test_agg_state_ds_hll_count_distinct
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
CREATE TABLE test_hll_sketch (
  dt VARCHAR(10),
  hll_id ds_hll_count_distinct(varchar not null, int),
  hll_province ds_hll_count_distinct(varchar, int),
  hll_age ds_hll_count_distinct(varchar, int),
  hll_dt ds_hll_count_distinct(varchar not null, int)
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;
-- result:
-- !result
select id, province, from_binary(ds_hll_count_distinct_state(id), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_hll_count_distinct_state(province), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_hll_count_distinct_state(age), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select id, province, from_binary(ds_hll_count_distinct_state(dt), 'hex') from t1 order by 1, 2 limit 3;
-- result:
-- !result
select ds_hll_count_distinct_merge(hll_province) from test_hll_sketch;
-- result:
None
-- !result
select ds_hll_count_distinct_merge(hll_province) from test_hll_sketch group by dt order by 1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
-- result:
-- !result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-25" from table(generate_series(1, 1000));
-- result:
-- !result
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
-- result:
-- !result
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id, 20), ds_hll_count_distinct_state(province, 19), ds_hll_count_distinct_state(age, 18), ds_hll_count_distinct_state(dt, 17) from t1;
-- result:
-- !result
insert into test_hll_sketch select dt, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64')  from t1;
-- result:
-- !result
insert into test_hll_sketch select dt, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64')  from t1;
-- result:
-- !result
insert into test_hll_sketch select dt, to_binary('YWJj', 'encode64'), to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64')  from t1;
-- result:
-- !result
ALTER TABLE test_hll_sketch COMPACT;
-- result:
-- !result
select ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch;
-- result:
1001	1001	101	3
-- !result
select dt, ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;
-- result:
2024-07-24	1001	1001	101	2
2024-07-25	1001	1001	101	2
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
-- result:
-- !result
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
-- result:
-- !result
select ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch;
-- result:
1001	1003	101	4
-- !result
select dt, ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;
-- result:
2024-07-22	1	1	1	1
2024-07-24	1001	1001	101	2
2024-07-25	1001	1002	101	2
-- !result