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
| RESOURCE_GROUP                     | Resource group used by the refresh job. This is the materialized view's configured `resource_group` property; returns `default_mv_wg` when not configured. |
| CREATOR                            | User that created the materialized view (its create-user; the run identity is in RUN_AS_USER).     |
| SUBMIT_USER                        | User who submitted the refresh job. For a manual refresh this is the user who issued it; for scheduled or base-table-change refreshes it is submitted by the system. |
| RUN_AS_USER                        | User identity the refresh runs as. With creator-based authorization (the default, `mv_use_creator_based_authorization=true`) this is the materialized view's creator; with root-based authorization it is `'root'@'%'`. |
| SUBMIT_TIME                        | Time when the job was submitted (the first task run's creation time). |
| REFRESH_STATE                      | State of the job, rolled up from the last task run. Valid values: `PENDING`, `RUNNING`, `FAILED`, `SUCCESS`, and `SKIPPED`. |
| FINISH_TIME                        | Time when the job finished. `NULL` if the job has not finished. |
| DURATION_TIME                      | Wall-clock duration of the job, in seconds (the last task run's finish time minus the first task run's process-start time). `NULL` if the job has not finished. |
| REFRESH_TRIGGER                    | How this job was triggered. `MANUAL` for a manually-issued `REFRESH MATERIALIZED VIEW` (even when the materialized view's scheme is scheduled or automatic); otherwise the materialized view's configured scheme. Valid values: `MANUAL`, `SCHEDULED`, `ON_BASE_TABLE_CHANGE`, and `NONE`. `UNKNOWN` if the materialized view has been dropped and the job was not manual. |
| REFRESH_MODE                       | The materialized view's configured refresh mode. Valid values: `PCT`, `INCREMENTAL` and `AUTO`. `NULL` if the materialized view has been dropped. Note that this is the *configured* mode, not the mode a given refresh actually ran in: they differ for the bootstrap refresh and whenever INCREMENTAL planning fails and falls back to PCT. For the mode a refresh actually ran in, query `get_json_string(EXTRA_MESSAGE, '$.refreshMode')` on `information_schema.task_runs`. |
| IMV_SOURCE_VERSION_RANGE           | JSON of the source version ranges an incremental refresh worked from, one entry per base table. A range whose `start` equals its `end` means that base table had no change, so a refresh skipped because nothing changed reports such a range for every base table. Returns `NULL` for a non-incremental (PCT) refresh. |
| IMV_SOURCE_TIMESTAMP_RANGE         | JSON of the commit times of the same range endpoints as IMV_SOURCE_VERSION_RANGE. Returns `NULL` for a non-incremental (PCT) refresh, and also for an incremental refresh whose endpoint commit times could not be resolved. |
| IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP  | JSON of pinned source snapshot IDs, keyed by the base table's `<catalog>.<db>.<table>` name — the same key IMV_SOURCE_VERSION_RANGE and IMV_SOURCE_TIMESTAMP_RANGE use, so the three can be joined per base table. Populated on the baseline/PCT-path refresh; returns `NULL` on a pure incremental run or when no snapshot was pinned. |
| FAILED_TASK_RUN_ID                 | Task-run ID of the failed run within the job. `NULL` if no run failed. To drill down into `task_runs`, join on `FAILED_QUERY_ID = task_runs.QUERY_ID` (or on `JOB_ID`); `task_runs` does not expose a task-run-id column. |
| FAILED_QUERY_ID                    | Query ID of the failed run. `NULL` if no run failed.         |
| ERROR_CODE                         | Error code of the failed run. `NULL` if no run failed.       |
| ERROR_MESSAGE                      | Error message of the failed run. `NULL` if no run failed.    |
| EXECUTED_REFRESH_MODE | Which mode this refresh job actually used: `INCREMENTAL` or `PCT`. When the materialized view's `REFRESH_MODE` is `AUTO`, a single job can switch to `PCT` because a base table changed in a way incremental maintenance cannot express; comparing this column against `REFRESH_MODE` is how you see that a job fell back. A job that refreshed in more than one step reports the mode it finished on, and a job that failed reports the mode it was running in when it failed. NULL when the job never got as far as choosing a mode -- skipped because nothing needed refreshing, or failed before the choice -- and for a historical job that recorded none. |
| REFRESH_MODE_REASON | Why this job ran in `EXECUTED_REFRESH_MODE` instead of refreshing incrementally. Empty when no mode decision was made. One of: `NON_APPEND_ONLY_CHANGE` (a base table change that is not append-only -- a partition drop, a truncate, an overwrite, an external delete, or a row-level delete), `BASELINE_UNREACHABLE` (the recorded baseline is no longer an ancestor of the table head -- the snapshot expired, or the table was rolled back or replaced), `BASELINE_MISSING` (no baseline at all to read a delta from: the first refresh, or one after a metadata repair), `CHANGE_CAPTURE_DISABLED` (a version in the window was published while change data capture was off on that base table), `FORCE_REFRESH` (a forced refresh), `UNKNOWN` (a fallback none of the above classifies; the cause is in the frontend log, not in `ERROR_MESSAGE`, which stays empty when the fallback itself succeeded). |
| REFRESH_MODE_REASON_TABLE | The base table that drove the mode decision, as `catalog.database.table`. Empty when no single base table drove it: `FORCE_REFRESH` comes from the request rather than a table, and a reason the backend reports while reading changes identifies a tablet rather than a table. |

:::note
This view has no persistent storage. Its rows are derived from `task_runs` at query time, so record retention follows the `task_runs` history settings. Because each job is aggregated from its `task_runs` rows at query time, a job is only fully represented while all of its task runs are still within the `task_runs` history window; older jobs are not shown, and a job straddling the retention boundary may be partially aggregated (for example, its `SUBMIT_TIME` or `IMV_SOURCE_*` range may reflect only the retained runs).
:::
