-- name: test_ds_hll
CREATE TABLE t1 (
  id BIGINT,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

CREATE TABLE t2 (
  `id` bigint,
  `dt` varchar(10),
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
DISTRIBUTED BY HASH(id) BUCKETS 3;
CREATE TABLE t3 (
  `id` bigint NOT NULL,
  `dt` varchar(10) NOT NULL,
  `ds_id` binary,
  `ds_province` binary,
  `ds_age` binary,
  `ds_dt` binary
) ENGINE=OLAP
PRIMARY KEY(id, dt)
DISTRIBUTED BY HASH(id) BUCKETS 3;


insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 1000));
INSERT INTO t1 VALUES (NULL, NULL, NULL, NULL), (NULL, 'a', 1, '2024-07-24'), (1, NULL, 1, '2024-07-24');
INSERT INTO t2 SELECT id, dt,
  ds_hll_count_distinct_state(id), 
  ds_hll_count_distinct_state(province, 10), 
  ds_hll_count_distinct_state(age, 20, "HLL_6"), 
  ds_hll_count_distinct_state(dt, 10, "HLL_8") FROM t1;
INSERT INTO t2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL), (NULL, 'a', to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64')), (1, NULL, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'));
INSERT INTO t3 SELECT * FROM t2 WHERE id IS NOT NULL AND dt IS NOT NULL;

select DS_HLL_COMBINE(id) from t1;
select DS_HLL_COMBINE(dt) from t1;
select DS_HLL_ESTIMATE(id) from t1;
select DS_HLL_ESTIMATE(dt) from t1;

[UC]SELECT dt, DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province, 20),  DS_HLL_ACCUMULATE(age, 12, "HLL_6"), DS_HLL_ACCUMULATE(dt) FROM t1 GROUP BY dt ORDER BY 1 limit 3;
[UC]SELECT id, DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province),  DS_HLL_ACCUMULATE(age), DS_HLL_ACCUMULATE(dt) FROM t1 GROUP BY id ORDER BY 1 limit 3;
[UC]SELECT DS_HLL_ACCUMULATE(id), DS_HLL_ACCUMULATE(province),  DS_HLL_ACCUMULATE(age), DS_HLL_ACCUMULATE(dt) FROM t1;
[UC]SELECT DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2;
[UC]SELECT dt, DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2 GROUP BY dt ORDER BY 1 limit 3;
[UC]SELECT id, DS_HLL_COMBINE(ds_id), DS_HLL_COMBINE(ds_province), DS_HLL_COMBINE(ds_age), DS_HLL_COMBINE(ds_dt) FROM t2 GROUP BY id ORDER BY 1 limit 3;
SELECT DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2;
SELECT dt, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2 GROUP BY dt ORDER BY 1 limit 3;
SELECT id, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2 GROUP BY id ORDER BY 1 limit 3;

SELECT DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3;
SELECT dt, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3 GROUP BY dt ORDER BY 1 limit 3;
SELECT id, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3 GROUP BY id ORDER BY 1 limit 3;


INSERT INTO t2 SELECT id, dt,
  ds_hll_count_distinct_state(id), 
  ds_hll_count_distinct_state(province, 10), 
  ds_hll_count_distinct_state(age, 20, "HLL_6"), 
  ds_hll_count_distinct_state(dt, 10, "HLL_8") FROM t1;
INSERT INTO t2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL), (NULL, 'a', to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64')), (1, NULL, to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'), to_binary('AgEHEQMIAQQ9nPUc', 'encode64'));
INSERT INTO t3 SELECT * FROM t2 WHERE id IS NOT NULL AND dt IS NOT NULL;

SELECT DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2;
SELECT dt, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2 GROUP BY dt ORDER BY 1 limit 3;
SELECT id, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t2 GROUP BY id ORDER BY 1 limit 3;

SELECT DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3;
SELECT dt, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3 GROUP BY dt ORDER BY 1 limit 3;
SELECT id, DS_HLL_ESTIMATE(ds_id), DS_HLL_ESTIMATE(ds_province), DS_HLL_ESTIMATE(ds_age), DS_HLL_ESTIMATE(ds_dt) FROM t3 GROUP BY id ORDER BY 1 limit 3;

