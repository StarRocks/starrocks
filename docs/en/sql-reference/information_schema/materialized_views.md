---
displayed_sidebar: docs
description: "materialized_views provides information about all materialized views."
---

# materialized_views

`materialized_views` provides information about all materialized views.

The following fields are provided in `materialized_views`:

| **Field**                            | **Description**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | ID of the materialized view.                                 |
| TABLE_SCHEMA                         | Database in which the materialized view resides.             |
| TABLE_NAME                           | Name of the materialized view.                               |
| REFRESH_TYPE                         | Refresh type of the materialized view. Valid values: `SYNC` (synchronous materialized view) and `ASYNC` (asynchronous materialized view, regardless of how the refresh is triggered). When the value is `SYNC`, all fields related to activation status and refresh are empty. See `REFRESH_TRIGGER` and `REFRESH_POLICY` for how an asynchronous materialized view is refreshed.  |
| IS_ACTIVE                            | Indicates whether the materialized view is active. Inactive materialized views cannot be refreshed or queried. |
| INACTIVE_REASON                      | The reason that the materialized view is inactive.           |
| PARTITION_TYPE                       | Type of partitioning strategy for the materialized view.     |
| TASK_ID                              | ID of the task responsible for refreshing the materialized view. |
| TASK_NAME                            | Name of the task responsible for refreshing the materialized view. |
| LAST_REFRESH_START_TIME              | Start time of the most recent refresh task.                  |
| LAST_REFRESH_FINISHED_TIME           | End time of the most recent refresh task.                    |
| LAST_REFRESH_DURATION                | Duration of the most recent refresh task.                    |
| LAST_REFRESH_STATE                   | State of the most recent refresh task.                       |
| LAST_REFRESH_FORCE_REFRESH           | Indicates whether the most recent refresh task was a force refresh. |
| LAST_REFRESH_START_PARTITION         | Starting partition for the most recent refresh task.         |
| LAST_REFRESH_END_PARTITION           | Ending partition for the most recent refresh task.           |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | Base table partitions involved in the most recent refresh task. |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | Materialized view partitions refreshed in the most recent refresh task. |
| LAST_REFRESH_ERROR_CODE              | Error code of the most recent refresh task.                  |
| LAST_REFRESH_ERROR_MESSAGE           | Error message of the most recent refresh task.               |
| TABLE_ROWS                           | Number of data rows in the materialized view, based on approximate background statistics. |
| MATERIALIZED_VIEW_DEFINITION         | SQL definition of the materialized view.                     |
| EXTRA_MESSAGE                        | Extra message of the materialized view.                      |
| QUERY_REWRITE_STATUS                 | Query rewrite status of the materialized view.               |
| CREATOR                              | Creator of the materialized view.                            |
| LAST_REFRESH_PROCESS_TIME            | Process time of the most recent refresh task.                |
| LAST_REFRESH_JOB_ID                  | Job ID of the most recent refresh task.                      |
| LAST_REFRESH_TIME                    | Time up to which base table updates are reflected in the materialized view. |
| WAREHOUSE                            | Name of the warehouse that the asynchronous materialized view uses for its refresh tasks. Empty in shared-nothing mode, or for synchronous (rollup) materialized views. |
| REFRESH_MODE                         | Configured refresh mode of the asynchronous materialized view. Valid values: `PCT` (partition change tracking, where only changed partitions are refreshed), `INCREMENTAL` (incremental view maintenance), and `AUTO`. Empty for synchronous materialized views. |
| REFRESH_TRIGGER                      | How a refresh is triggered. Valid values: `NONE` (synchronous materialized view), `MANUAL` (only via REFRESH MATERIALIZED VIEW), `SCHEDULED` (periodic, via an EVERY interval), and `ON_BASE_TABLE_CHANGE` (automatically when a base table loads or changes). |
| REFRESH_POLICY                       | Human-readable refresh policy. Valid values: `NONE`, `MANUAL`, `ON_BASE_TABLE_CHANGE`, or a schedule such as `START("yyyy-MM-dd HH:mm:ss") EVERY(INTERVAL n unit)` (the `START` clause is present only if a start time was defined). |
| RESOURCE_GROUP                       | Resource group used for the materialized view's refresh tasks (from the materialized view's `resource_group` property). Defaults to `default_mv_wg` when not set. |
| QUERY_REWRITE_STATUS_REASON          | The reason behind `QUERY_REWRITE_STATUS`. Valid values: `OK`, `MV_INACTIVE`, `QUERY_REWRITE_DISABLED`, `UNSUPPORTED_DEFINITION`, and `UNKNOWN`. |
| BASE_TABLE_REFRESH_VERSION_TIMES     | Per-base-table data version time, as a JSON object mapping each base table's `catalog.database.table` name to the latest data version time observed for it. This is the per-table detail behind `LAST_REFRESH_TIME` (their single maximum): external/data lake base tables report the partition source modified time, and OLAP (internal) base tables report the visible-version commit time. `{}` when no base table has a recorded time. |
