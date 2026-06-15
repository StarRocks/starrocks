-- name: test_materialized_view_refresh_jobs
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
CREATE TABLE base (dt date, val int) PARTITION BY date_trunc('day', dt);
-- result:
-- !result
INSERT INTO base VALUES
  ('2023-12-01', 100),
  ('2023-12-01', 200),
  ('2023-12-02', 300),
  ('2023-12-02', 400),
  ('2023-12-03', 500);
-- result:
-- !result
CREATE MATERIALIZED VIEW mv1 PARTITION BY dt
REFRESH DEFERRED MANUAL
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;
-- result:
-- !result
-- Success-path deterministic columns.
function: assert_query_contains("SELECT REFRESH_STATE FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "SUCCESS")
-- result:
None
-- !result
function: assert_query_contains("SELECT REFRESH_TRIGGER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "MANUAL")
-- result:
None
-- !result
function: assert_query_contains("SELECT REFRESH_MODE FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "PCT")
-- result:
None
-- !result
function: assert_query_contains("SELECT RESOURCE_GROUP FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "default_mv_wg")
-- result:
None
-- !result
function: assert_query_contains("SELECT TABLE_SCHEMA FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "db_${uuid0}")
-- result:
None
-- !result
function: assert_query_contains("SELECT (IMV_SOURCE_VERSION_RANGE IS NULL AND IMV_SOURCE_TIMESTAMP_RANGE IS NULL AND IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP IS NULL) FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "1")
-- result:
None
-- !result
-- All failure/error columns are NULL for a successful job.
function: assert_query_contains("SELECT ifnull(ERROR_CODE, 'NULL'), ifnull(ERROR_MESSAGE, 'NULL'), ifnull(FAILED_TASK_RUN_ID, 'NULL'), ifnull(FAILED_QUERY_ID, 'NULL') FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "NULL")
-- result:
None
-- !result
-- Drill-down consistency: the job's JOB_ID matches its task_runs rows.
function: assert_query_contains("SELECT (count(*) >= 1) FROM information_schema.materialized_view_refresh_jobs j JOIN information_schema.task_runs t ON j.JOB_ID = t.JOB_ID WHERE j.TABLE_NAME='mv1'", "1")
-- result:
None
-- !result
-- Cross-check the shared columns against information_schema.materialized_views for the same MV.
function: assert_query_contains("SELECT (j.MATERIALIZED_VIEW_ID = m.MATERIALIZED_VIEW_ID AND j.TASK_ID = m.TASK_ID AND j.SUBMIT_TIME = m.LAST_REFRESH_START_TIME AND j.FINISH_TIME = m.LAST_REFRESH_FINISHED_TIME) FROM information_schema.materialized_view_refresh_jobs j JOIN information_schema.materialized_views m ON j.TABLE_SCHEMA = m.TABLE_SCHEMA AND j.TABLE_NAME = m.TABLE_NAME AND j.JOB_ID = m.LAST_REFRESH_JOB_ID WHERE j.TABLE_SCHEMA='db_${uuid0}' AND j.TABLE_NAME='mv1'", "1")
-- result:
None
-- !result
-- User, warehouse, and wall-clock duration columns are populated for a finished job.
function: assert_query_contains("SELECT (WAREHOUSE IS NOT NULL AND CREATOR IS NOT NULL AND SUBMIT_USER IS NOT NULL AND RUN_AS_USER IS NOT NULL AND DURATION_TIME >= 0) FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv1'", "1")
-- result:
None
-- !result
-- RESOURCE_GROUP reflects the MV's configured resource_group property.
CREATE RESOURCE GROUP rg_${uuid0}
TO (user='u_${uuid0}')
WITH ('cpu_core_limit' = '1', 'mem_limit' = '10%');
-- result:
-- !result
CREATE MATERIALIZED VIEW mv2 PARTITION BY dt
REFRESH DEFERRED MANUAL
PROPERTIES ("resource_group" = "rg_${uuid0}")
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv2 WITH SYNC MODE;
-- result:
-- !result
function: assert_query_contains("SELECT RESOURCE_GROUP FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv2'", "rg_${uuid0}")
-- result:
None
-- !result
-- Failure-column population (FAILED_TASK_RUN_ID / FAILED_QUERY_ID / ERROR_CODE / ERROR_MESSAGE)
-- is covered deterministically by the FE unit test
-- MaterializedViewRefreshJobsSystemTableTest.testFailedRunColumnsComeFromLatestFailure; a live
-- FAILED refresh is not reproducible without flakiness here, so it is intentionally omitted.
DROP RESOURCE GROUP rg_${uuid0};
-- result:
-- !result
drop database db_${uuid0};
-- result:
-- !result
