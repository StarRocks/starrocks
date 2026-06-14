-- name: test_materialized_view_refresh_jobs
create database db_${uuid0};
use db_${uuid0};

CREATE TABLE base (dt date, val int) PARTITION BY date_trunc('day', dt);

INSERT INTO base VALUES
  ('2023-12-01', 100),
  ('2023-12-01', 200),
  ('2023-12-02', 300),
  ('2023-12-02', 400),
  ('2023-12-03', 500);

CREATE MATERIALIZED VIEW mv1 PARTITION BY dt
REFRESH DEFERRED MANUAL
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;

REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- Success-path deterministic columns.
function: assert_query_contains("SELECT REFRESH_STATE FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "SUCCESS")
function: assert_query_contains("SELECT REFRESH_TRIGGER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "MANUAL")
function: assert_query_contains("SELECT REFRESH_MODE FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "PCT")
function: assert_query_contains("SELECT RESOURCE_GROUP FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "default_mv_wg")
function: assert_query_contains("SELECT TABLE_SCHEMA FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "db_${uuid0}")
function: assert_query_contains("SELECT IMV_SOURCE_VERSION_RANGE, IMV_SOURCE_TIMESTAMP_RANGE, IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "{}")

-- Failure columns are NULL for a successful job.
function: assert_query_contains("SELECT ifnull(ERROR_CODE, 'NULL'), ifnull(FAILED_TASK_RUN_ID, 'NULL') FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "NULL")

-- Drill-down consistency: the job's JOB_ID matches its task_runs rows.
function: assert_query_contains("SELECT (count(*) >= 1) FROM information_schema.materialized_view_refresh_jobs j JOIN information_schema.task_runs t ON j.JOB_ID = t.JOB_ID WHERE j.TABLE_NAME='mv1'", "1")

-- RESOURCE_GROUP reflects the MV's configured resource_group property.
CREATE RESOURCE GROUP rg_${uuid0}
TO (user='u_${uuid0}')
WITH ('cpu_core_limit' = '1', 'mem_limit' = '10%');

CREATE MATERIALIZED VIEW mv2 PARTITION BY dt
REFRESH DEFERRED MANUAL
PROPERTIES ("resource_group" = "rg_${uuid0}")
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;

REFRESH MATERIALIZED VIEW mv2 WITH SYNC MODE;

function: assert_query_contains("SELECT RESOURCE_GROUP FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv2'", "rg_${uuid0}")

-- Failure-column population (FAILED_TASK_RUN_ID / FAILED_QUERY_ID / ERROR_CODE / ERROR_MESSAGE)
-- is covered deterministically by the FE unit test
-- MaterializedViewRefreshJobsSystemTableTest.testFailedRunColumnsComeFromLatestFailure; a live
-- FAILED refresh is not reproducible without flakiness here, so it is intentionally omitted.

DROP RESOURCE GROUP rg_${uuid0};
drop database db_${uuid0};
