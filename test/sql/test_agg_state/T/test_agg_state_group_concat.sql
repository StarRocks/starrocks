-- name: test_agg_state_group_concat

CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
-- PARTITION BY (province, dt) 
DISTRIBUTED BY HASH(id) BUCKETS 4;

CREATE TABLE test_group_concat (
  dt VARCHAR(10),
  gc_id group_concat(varchar not null),
  gc_province group_concat(varchar, varchar),
  gc_age group_concat(varchar),
  gc_dt group_concat(varchar not null, varchar)
)
AGGREGATE KEY(dt)
PARTITION BY (dt) 
DISTRIBUTED BY HASH(dt) BUCKETS 4;

-- basic test for empty table
select id, province, from_binary(group_concat_state(cast(id as string))) from t1 order by 1, 2 limit 3;
select id, province, from_binary(group_concat_state(province), ',') from t1 order by 1, 2 limit 3;
select id, province, from_binary(group_concat_state(cast(age as string))) from t1 order by 1, 2 limit 3;
select id, province, from_binary(group_concat_state(dt), '#') from t1 order by 1, 2 limit 3;

select group_concat_merge(gc_province) from test_group_concat;
select group_concat_merge(gc_province) from test_group_concat group by dt order by 1;

-- first insert & test result
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_group_concat select dt, group_concat_state(cast(id as string)), group_concat_state(province, ","), group_concat_state(cast(age as string)), group_concat_state(dt, "#") from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
insert into test_group_concat select dt, group_concat_state(cast(id as string)), group_concat_state(province, ","), group_concat_state(cast(age as string)), group_concat_state(dt, "#") from t1;
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-25" from table(generate_series(1, 1000));
insert into test_group_concat select dt, group_concat_state(cast(id as string)), group_concat_state(province, ","), group_concat_state(cast(age as string)), group_concat_state(dt, "#") from t1;
ALTER TABLE test_group_concat COMPACT;

-- query    
[UC] select group_concat_merge(gc_id), group_concat_merge(gc_province), group_concat_merge(gc_age), group_concat_merge(gc_dt) from test_group_concat;
[UC] select dt, group_concat_merge(gc_id), group_concat_merge(gc_province), group_concat_merge(gc_age), group_concat_merge(gc_dt) from test_group_concat group by dt order by 1 limit 3;

-- second insert & test result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');
insert into test_group_concat select dt, group_concat_state(cast(id as string)), group_concat_state(province, ","), group_concat_state(cast(age as string)), group_concat_state(dt, "#") from t1;
[UC] select group_concat_merge(gc_id), group_concat_merge(gc_province), group_concat_merge(gc_age), group_concat_merge(gc_dt) from test_group_concat;
[UC] select dt, group_concat_merge(gc_id), group_concat_merge(gc_province), group_concat_merge(gc_age), group_concat_merge(gc_dt) from test_group_concat group by dt order by 1 limit 3;