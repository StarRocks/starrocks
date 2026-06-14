---
displayed_sidebar: docs
description: "materialized_view_refresh_jobs provides job-level information about materialized view refreshes."
---

# materialized_view_refresh_jobs

`materialized_view_refresh_jobs` provides job-level information about materialized view refreshes.

A single refresh job may comprise multiple task runs (for example, partition-by-partition refresh batches); this view rolls those task runs up into one row per job. It shares its data source with [`task_runs`](./task_runs.md), so you can use `JOB_ID` to drill down into the individual task runs of a job (`SELECT * FROM information_schema.task_runs WHERE JOB_ID = '<job_id>'`), and job records are retained for the same window as `task_runs` history.

The following fields are provided in `materialized_view_refresh_jobs`:

| **Field**                          | **Description**                                              |
| ---------------------------------- | ------------------------------------------------------------ |
| JOB_ID                             | ID of the refresh job. All task runs of one refresh share this ID; use it to drill down into `task_runs.JOB_ID`. |
| MATERIALIZED_VIEW_ID               | ID of the materialized view.                                 |
| TABLE_SCHEMA                       | Database the materialized view belongs to.                   |
| TABLE_NAME                         | Name of the materialized view. `NULL` if the materialized view has been dropped. |
| TASK_ID                            | ID of the refresh task.                                      |
| WAREHOUSE                          | Warehouse used by the refresh job.                           |
| RESOURCE_GROUP                     | Resource group used by the refresh job. This is the materialized view's configured `session.resource_group`; returns `default_mv_wg` when not configured. |
| CREATOR                            | User who created the materialized view (the task owner).     |
| SUBMIT_USER                        | User who submitted the refresh job. For a manual refresh this is the user who issued it; for scheduled or base-table-change refreshes it is submitted by the system. |
| RUN_AS_USER                        | User identity the refresh runs as.                           |
| SUBMIT_TIME                        | Time when the job was submitted (the first task run's creation time). |
| REFRESH_STATE                      | State of the job, rolled up from the last task run. Valid values: `PENDING`, `RUNNING`, `FAILED`, `SUCCESS`, `MERGED`, and `SKIPPED`. |
| FINISH_TIME                        | Time when the job finished. `NULL` if the job has not finished. |
| DURATION_TIME                      | Wall-clock duration of the job, in seconds (the last task run's finish time minus the first task run's process-start time). `NULL` if the job has not finished. |
| REFRESH_TRIGGER                    | How the refresh is triggered, derived from the materialized view's refresh scheme. Valid values: `MANUAL`, `SCHEDULED`, `ON_BASE_TABLE_CHANGE`, and `NONE`. `UNKNOWN` if the materialized view has been dropped. |
| REFRESH_MODE                       | The materialized view's configured refresh mode. Valid values: `AUTO`, `PCT`, and `INCREMENTAL`. `NULL` if the materialized view has been dropped. |
| IMV_SOURCE_VERSION_RANGE           | JSON of the source version ranges consumed by an incremental refresh. Returns `{}` when not applicable. |
| IMV_SOURCE_TIMESTAMP_RANGE         | JSON of the source timestamp ranges consumed by an incremental refresh. Returns `{}` when not applicable. |
| IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP  | JSON of pinned source snapshot IDs for an incremental refresh. Returns `{}` when not applicable. |
| FAILED_TASK_RUN_ID                 | Task-run ID of the failed run within the job. `NULL` if no run failed. |
| FAILED_QUERY_ID                    | Query ID of the failed run. `NULL` if no run failed.         |
| ERROR_CODE                         | Error code of the failed run. `NULL` if no run failed.       |
| ERROR_MESSAGE                      | Error message of the failed run. `NULL` if no run failed.    |

:::note
This view has no persistent storage. Its rows are derived from `task_runs` at query time, so record retention follows the `task_runs` history settings.
:::
