---
displayed_sidebar: "English"
---

# materialized_views

`materialized_views` provides information about all asynchronous materialized views.

The following fields are provided in `materialized_views`:

| **Field**                            | **Description**                                              |
| ------------------------------------ | ------------------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | ID of the materialized view.                                 |
| TABLE_SCHEMA                         | Database in which the materialized view resides.             |
| TABLE_NAME                           | Name of the materialized view.                               |
| REFRESH_TYPE                         | Refresh type of the materialized view. Valid values: `ROLLUP`, `ASYNC`, and `MANUAL`. |
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
