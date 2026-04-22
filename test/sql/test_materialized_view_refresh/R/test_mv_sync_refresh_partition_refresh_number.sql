-- name: test_mv_sync_refresh_partition_refresh_number
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE TABLE base (
  id int,
  dt date,
  val int
) DUPLICATE KEY(id, dt)
PARTITION BY RANGE(dt) (
  PARTITION p20240101 VALUES [('2024-01-01'), ('2024-01-02')),
  PARTITION p20240102 VALUES [('2024-01-02'), ('2024-01-03')),
  PARTITION p20240103 VALUES [('2024-01-03'), ('2024-01-04')),
  PARTITION p20240104 VALUES [('2024-01-04'), ('2024-01-05')),
  PARTITION p20240105 VALUES [('2024-01-05'), ('2024-01-06'))
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1');
-- result:
-- !result
INSERT INTO base VALUES
  (1, '2024-01-01', 10),
  (2, '2024-01-02', 20),
  (3, '2024-01-03', 30),
  (4, '2024-01-04', 40),
  (5, '2024-01-05', 50);
-- result:
-- !result
CREATE MATERIALIZED VIEW mv_batched
PARTITION BY dt
DISTRIBUTED BY HASH(dt) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES (
  'replication_num' = '1',
  'partition_refresh_number' = '2'
)
AS SELECT dt, sum(val) as sv FROM base GROUP BY dt;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW mv_batched WITH SYNC MODE;
-- result:
019db506-c527-74cb-ab0a-1def2e3a41cd
-- !result
function: wait_async_materialized_view_finish("db_${uuid0}", "mv_batched")
-- result:
None
-- !result
SELECT count(*) FROM mv_batched;
-- result:
5
-- !result
[UC]TASK_NAME_BATCHED=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'mv_batched';
-- result:
mv-74840
-- !result
SELECT count(1) FROM information_schema.task_runs WHERE TASK_NAME = '${TASK_NAME_BATCHED}' AND STATE = 'SUCCESS';
-- result:
3
-- !result
CREATE MATERIALIZED VIEW mv_unbatched
PARTITION BY dt
DISTRIBUTED BY HASH(dt) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES (
  'replication_num' = '1',
  'partition_refresh_number' = '-1'
)
AS SELECT dt, sum(val) as sv FROM base GROUP BY dt;
-- result:
-- !result
[UC]REFRESH MATERIALIZED VIEW mv_unbatched WITH SYNC MODE;
-- result:
019db506-e64f-726f-84d9-ab9587c799b1
-- !result
function: wait_async_materialized_view_finish("db_${uuid0}", "mv_unbatched")
-- result:
None
-- !result
SELECT count(*) FROM mv_unbatched;
-- result:
5
-- !result
[UC]TASK_NAME_UNBATCHED=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'mv_unbatched';
-- result:
mv-74885
-- !result
SELECT count(1) FROM information_schema.task_runs WHERE TASK_NAME = '${TASK_NAME_UNBATCHED}' AND STATE = 'SUCCESS';
-- result:
1
-- !result
drop database db_${uuid0} force;
-- result:
-- !result