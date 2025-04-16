-- name: test_ds_theta_count_distinct
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

-- init with 10w values
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));

select ds_theta_count_distinct(id) from t1;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age) from t1 group by dt order by 1, 2;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 group by dt order by 1, 2;

INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (3, 'c', 1, '2024-07-25'), (5, NULL, NULL, '2024-07-24');

select ds_theta_count_distinct(id) from t1;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 order by 1, 2;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age) from t1 group by dt order by 1, 2;
select ds_theta_count_distinct(id), ds_theta_count_distinct(province), ds_theta_count_distinct(age), ds_theta_count_distinct(dt) from t1 group by dt order by 1, 2;

-- bad cases
select ds_theta_count_distinct(id, 1)  from t1 order by 1, 2;
select ds_theta_count_distinct(id, 100)  from t1 order by 1, 2;
select ds_theta_count_distinct(id, 10, "INVALID") from t1 order by 1, 2;