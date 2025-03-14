-- name: test_quantile_sketch
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

insert into t2 select generate_series, generate_series, generate_series % 10, "2024-07-24" from table(generate_series(1, 100));

select ds_quantile(id), ds_quantile(age) from t2 order by 1, 2;

select ds_quantile(id, [0.1, 0.5, 0.9]), ds_quantile(age, [0.1, 0.5, 0.9]) from t2 order by 1, 2;

select ds_quantile(id, [0.1, 0.5, 0.9], 10000), ds_quantile(age, [0.1, 0.5, 0.9], 10000) from t2 order by 1, 2;