-- name: test_datasketchs
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
select ds_hll(id), ds_hll(province), ds_hll(age), ds_hll(dt) from t1 order by 1, 2;
-- result:
100090	100140	100	1
-- !result
select ds_hll(id, 4), ds_hll(province, 4), ds_hll(age, 4), ds_hll(dt, 4) from t1 order by 1, 2;
-- result:
94302	83035	106	1
-- !result
select ds_hll(id, 10), ds_hll(province, 10), ds_hll(age, 10), ds_hll(dt, 10) from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll(id, 21), ds_hll(province, 21), ds_hll(age, 21), ds_hll(dt, 21) from t1 order by 1, 2;
-- result:
99995	100001	100	1
-- !result
select ds_hll(id, 10, "HLL_4"), ds_hll(province, 10, "HLL_4"), ds_hll(age, 10, "HLL_4"), ds_hll(dt, 10, "HLL_4") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll(id, 10, "HLL_6"), ds_hll(province, 10, "HLL_6"), ds_hll(age, 10, "HLL_6"), ds_hll(dt, 10, "HLL_6") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result
select ds_hll(id, 10, "HLL_8"), ds_hll(province, 10, "HLL_8"), ds_hll(age, 10, "HLL_8"), ds_hll(dt, 10, "HLL_8") from t1 order by 1, 2;
-- result:
99844	101905	96	1
-- !result

CREATE TABLE t2 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL 
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
-- result:
-- !result
insert into t2 select generate_series, generate_series, generate_series % 10, "2024-07-24" from table(generate_series(1, 100));
-- result:
-- !result
select ds_quantile(id), ds_quantile(age) from t2 order by 1, 2;
-- result:
[50]	[4]
-- !result
select ds_quantile(id, [0.1, 0.5, 0.9]), ds_quantile(age, [0.1, 0.5, 0.9]) from t2 order by 1, 2;
-- result:
[10,50,90]	[0,4,8]
-- !result
select ds_quantile(id, [0.1, 0.5, 0.9], 10000), ds_quantile(age, [0.1, 0.5, 0.9], 10000) from t2 order by 1, 2;
-- result:
[10,50,90]	[0,4,8]
-- !result

select ds_frequent(age, 1) from t1;
-- result:
[{"value":10,"count":1000,"lower_bound":1000,"upper_bound":1000}]
-- !result
select ds_frequent(age, 2, 21) from t1;
-- result:
[{"value":10,"count":1000,"lower_bound":1000,"upper_bound":1000},{"value":20,"count":1000,"lower_bound":1000,"upper_bound":1000}]
-- !result
select ds_frequent(age, 2, 21, 3) from t1;
-- result:
[{"value":10,"count":1000,"lower_bound":1000,"upper_bound":1000},{"value":20,"count":1000,"lower_bound":1000,"upper_bound":1000}]
-- !result

select ds_theta(id), ds_theta(province), ds_theta(age), ds_theta(dt) from t1 order by 1, 2;
-- result:
100215	100846	100	1
-- !result
