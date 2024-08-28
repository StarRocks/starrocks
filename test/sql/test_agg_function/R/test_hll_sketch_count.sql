-- name: test_ds_hll_count_distinct
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
select ds_hll_count_distinct(id), ds_hll_count_distinct(province), ds_hll_count_distinct(age), ds_hll_count_distinct(dt) from t1 order by 1, 2;
-- result:
100090	100140	100	1
-- !result
select ds_hll_count_distinct(id, 4), ds_hll_count_distinct(province, 4), ds_hll_count_distinct(age, 4), ds_hll_count_distinct(dt, 4) from t1 order by 1, 2;
-- result:
94302	83035	106	1
-- !result
select ds_hll_count_distinct(id, 10), ds_hll_count_distinct(province, 10), ds_hll_count_distinct(age, 10), ds_hll_count_distinct(dt, 10) from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll_count_distinct(id, 21), ds_hll_count_distinct(province, 21), ds_hll_count_distinct(age, 21), ds_hll_count_distinct(dt, 21) from t1 order by 1, 2;
-- result:
99995	100001	100	1
-- !result
select ds_hll_count_distinct(id, 10, "HLL_4"), ds_hll_count_distinct(province, 10, "HLL_4"), ds_hll_count_distinct(age, 10, "HLL_4"), ds_hll_count_distinct(dt, 10, "HLL_4") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll_count_distinct(id, 10, "HLL_6"), ds_hll_count_distinct(province, 10, "HLL_6"), ds_hll_count_distinct(age, 10, "HLL_6"), ds_hll_count_distinct(dt, 10, "HLL_6") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll_count_distinct(id, 10, "HLL_8"), ds_hll_count_distinct(province, 10, "HLL_8"), ds_hll_count_distinct(age, 10, "HLL_8"), ds_hll_count_distinct(dt, 10, "HLL_8") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');
-- result:
-- !result
select id, province, ds_hll_count_distinct(id), ds_hll_count_distinct(province), ds_hll_count_distinct(age), ds_hll_count_distinct(dt) from t1 group by 1, 2 order by 1, 2 limit 3;
-- result:
1	1	1	1	1	1
1	a	1	1	1	1
2	2	1	1	1	1
-- !result
select id, province, ds_hll_count_distinct(id, 10), ds_hll_count_distinct(province, 10), ds_hll_count_distinct(age, 10), ds_hll_count_distinct(dt, 10) from t1 group by 1, 2 order by 1, 2 limit 3;
-- result:
1	1	1	1	1	1
1	a	1	1	1	1
2	2	1	1	1	1
-- !result
select id, province, ds_hll_count_distinct(id, 10, "HLL_4"), ds_hll_count_distinct(province, 10, "HLL_4"), ds_hll_count_distinct(age, 10, "HLL_4"), ds_hll_count_distinct(dt, 10, "HLL_4") from t1 group by 1, 2 order by 1, 2 limit 3;
-- result:
1	1	1	1	1	1
1	a	1	1	1	1
2	2	1	1	1	1
-- !result
select ds_hll_count_distinct(id, 1)  from t1 order by 1, 2;
-- result:
E: (1064, "Getting analyzing error. Detail message: ds_hll_count_distinct second parameter'value should be between 4 and 21.")
-- !result
select ds_hll_count_distinct(id, 100)  from t1 order by 1, 2;
-- result:
E: (1064, "Getting analyzing error. Detail message: ds_hll_count_distinct second parameter'value should be between 4 and 21.")
-- !result
select ds_hll_count_distinct(id, 10, "INVALID") from t1 order by 1, 2;
-- result:
E: (1064, "Getting analyzing error. Detail message: ds_hll_count_distinct third  parameter'value should be in HLL_4/HLL_6/HLL_8.")
-- !result