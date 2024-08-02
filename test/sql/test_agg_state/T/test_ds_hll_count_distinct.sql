-- name: test_ds_hll_count_distinct
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

-- without params
select ds_hll_count_distinct(id), ds_hll_count_distinct(province), ds_hll_count_distinct(age), ds_hll_count_distinct(dt) from t1 order by 1, 2;

-- with 1 param
select ds_hll_count_distinct(id, 4), ds_hll_count_distinct(province, 4), ds_hll_count_distinct(age, 4), ds_hll_count_distinct(dt, 4) from t1 order by 1, 2;
select ds_hll_count_distinct(id, 10), ds_hll_count_distinct(province, 10), ds_hll_count_distinct(age, 10), ds_hll_count_distinct(dt, 10) from t1 order by 1, 2;
select ds_hll_count_distinct(id, 21), ds_hll_count_distinct(province, 21), ds_hll_count_distinct(age, 21), ds_hll_count_distinct(dt, 21) from t1 order by 1, 2;

-- with 2 params
select ds_hll_count_distinct(id, 10, "HLL_4"), ds_hll_count_distinct(province, 10, "HLL_4"), ds_hll_count_distinct(age, 10, "HLL_4"), ds_hll_count_distinct(dt, 10, "HLL_4") from t1 order by 1, 2;
select ds_hll_count_distinct(id, 10, "HLL_6"), ds_hll_count_distinct(province, 10, "HLL_6"), ds_hll_count_distinct(age, 10, "HLL_6"), ds_hll_count_distinct(dt, 10, "HLL_6") from t1 order by 1, 2;
select ds_hll_count_distinct(id, 10, "HLL_8"), ds_hll_count_distinct(province, 10, "HLL_8"), ds_hll_count_distinct(age, 10, "HLL_8"), ds_hll_count_distinct(dt, 10, "HLL_8") from t1 order by 1, 2;

INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');
select id, province, ds_hll_count_distinct(id), ds_hll_count_distinct(province), ds_hll_count_distinct(age), ds_hll_count_distinct(dt) from t1 group by 1, 2 order by 1, 2 limit 3;
select id, province, ds_hll_count_distinct(id, 10), ds_hll_count_distinct(province, 10), ds_hll_count_distinct(age, 10), ds_hll_count_distinct(dt, 10) from t1 group by 1, 2 order by 1, 2 limit 3;
select id, province, ds_hll_count_distinct(id, 10, "HLL_4"), ds_hll_count_distinct(province, 10, "HLL_4"), ds_hll_count_distinct(age, 10, "HLL_4"), ds_hll_count_distinct(dt, 10, "HLL_4") from t1 group by 1, 2 order by 1, 2 limit 3;

-- bad cases
select ds_hll_count_distinct(id, 1)  from t1 order by 1, 2;
select ds_hll_count_distinct(id, 100)  from t1 order by 1, 2;
select ds_hll_count_distinct(id, 10, "INVALID") from t1 order by 1, 2;