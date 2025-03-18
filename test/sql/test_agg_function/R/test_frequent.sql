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