-- name: test_agg_state_ds_hll_count_distinct

CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
-- PARTITION BY (province, dt) 
DISTRIBUTED BY HASH(id) BUCKETS 4;

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

-- basic test for empty table
select id, province, from_binary(ds_hll_count_distinct_state(id), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(ds_hll_count_distinct_state(province), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(ds_hll_count_distinct_state(age), 'hex') from t1 order by 1, 2 limit 3;
select id, province, from_binary(ds_hll_count_distinct_state(dt), 'hex') from t1 order by 1, 2 limit 3;

select ds_hll_count_distinct_merge(hll_province) from test_hll_sketch;
select ds_hll_count_distinct_merge(hll_province) from test_hll_sketch group by dt order by 1;

-- first insert & test result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-25" from table(generate_series(1, 1000));
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
-- insert with different sketch size
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id, 20), ds_hll_count_distinct_state(province, 19), ds_hll_count_distinct_state(age, 18), ds_hll_count_distinct_state(dt, 17) from t1;
-- insert with binary value
insert into test_hll_sketch select dt, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64')  from t1;
insert into test_hll_sketch select dt, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64') , to_binary('AgEHEQMIAQQ9nPUc', 'encode64')  from t1;
-- insert with bad binary value!
insert into test_hll_sketch select dt, to_binary('YWJj', 'encode64'), to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64') , to_binary('YWJj', 'encode64')  from t1;
ALTER TABLE test_hll_sketch COMPACT;

-- query    
select ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch;
select dt, ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_hll_sketch select dt, ds_hll_count_distinct_state(id), ds_hll_count_distinct_state(province), ds_hll_count_distinct_state(age), ds_hll_count_distinct_state(dt) from t1;
select ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch;
select dt, ds_hll_count_distinct_merge(hll_id), ds_hll_count_distinct_merge(hll_province), ds_hll_count_distinct_merge(hll_age), ds_hll_count_distinct_merge(hll_dt) from test_hll_sketch group by dt order by 1 limit 3;