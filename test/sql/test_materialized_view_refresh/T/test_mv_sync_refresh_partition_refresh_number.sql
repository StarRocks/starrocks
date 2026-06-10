-- name: test_mv_sync_refresh_partition_refresh_number
-- Test Point:
--   1. REFRESH MATERIALIZED VIEW ... WITH SYNC MODE must refresh ALL stale partitions in one SQL call
--      (externally atomic), regardless of partition_refresh_number.
--   2. Internally the sync refresh must honor partition_refresh_number by fanning out into multiple
--      task runs of at most N partitions each — not a single task run covering all partitions.
--   3. When partition_refresh_number = -1, internal batching is disabled and one task run covers all.
-- Method: Query MV row count (confirms atomic one-call completion) AND count SUCCESS task_runs
--         via information_schema.task_runs joined on TASK_NAME (confirms internal fan-out). With 5
--         stale partitions and partition_refresh_number=2, expect ceil(5/2) = 3 internal task runs.
-- Scope: MVPCTBasedRefreshProcessor.generateNextTaskRunIfNeeded,
--        MVPCTRefreshPartitioner.isGenerateNextTaskRun, TaskManager.executeTaskSync continuation loop.
-- Related: Issue#71974

create database db_${uuid0};
use db_${uuid0};

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

INSERT INTO base VALUES
  (1, '2024-01-01', 10),
  (2, '2024-01-02', 20),
  (3, '2024-01-03', 30),
  (4, '2024-01-04', 40),
  (5, '2024-01-05', 50);

-- Case A: partition_refresh_number = 2 with 5 stale partitions.
-- Expect sync refresh to (a) fully refresh all partitions in one call and (b) internally fan out
-- into 3 task runs (2 + 2 + 1).
CREATE MATERIALIZED VIEW mv_batched
PARTITION BY dt
DISTRIBUTED BY HASH(dt) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES (
  'replication_num' = '1',
  'partition_refresh_number' = '2'
)
AS SELECT dt, sum(val) as sv FROM base GROUP BY dt;

[UC]REFRESH MATERIALIZED VIEW mv_batched WITH SYNC MODE;
function: wait_async_materialized_view_finish("db_${uuid0}", "mv_batched")

-- (1) External contract: one sync call refreshes all 5 partitions.
SELECT count(*) FROM mv_batched;

-- (2) Internal contract: partition_refresh_number=2 triggers 3 successful internal task runs.
[UC]TASK_NAME_BATCHED=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'mv_batched';
SELECT count(1) FROM information_schema.task_runs WHERE TASK_NAME = '${TASK_NAME_BATCHED}' AND STATE = 'SUCCESS';

-- Case B: partition_refresh_number = -1 (disabled) — same 5 stale partitions but no internal batching.
CREATE MATERIALIZED VIEW mv_unbatched
PARTITION BY dt
DISTRIBUTED BY HASH(dt) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES (
  'replication_num' = '1',
  'partition_refresh_number' = '-1'
)
AS SELECT dt, sum(val) as sv FROM base GROUP BY dt;

[UC]REFRESH MATERIALIZED VIEW mv_unbatched WITH SYNC MODE;
function: wait_async_materialized_view_finish("db_${uuid0}", "mv_unbatched")

SELECT count(*) FROM mv_unbatched;

[UC]TASK_NAME_UNBATCHED=SELECT TASK_NAME FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'db_${uuid0}' AND TABLE_NAME = 'mv_unbatched';
SELECT count(1) FROM information_schema.task_runs WHERE TASK_NAME = '${TASK_NAME_UNBATCHED}' AND STATE = 'SUCCESS';

drop database db_${uuid0} force;
