---
displayed_sidebar: docs
description: "iceberg_maintenance_tasks provides information about Iceberg metadata maintenance tasks."
---

# iceberg_maintenance_tasks

`iceberg_maintenance_tasks` provides information about Iceberg metadata maintenance tasks, including both tasks triggered automatically by Iceberg metadata auto maintenance and tasks executed manually via `ALTER TABLE ... EXECUTE`.

The following fields are provided in `iceberg_maintenance_tasks`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| TASK_ID        | Unique ID of the maintenance task.                           |
| CATALOG_NAME   | Name of the catalog the table belongs to.                    |
| DATABASE_NAME  | Name of the database the table belongs to.                   |
| TABLE_NAME     | Name of the table.                                           |
| ACTION         | Maintenance action. Valid values: `expire_snapshots`, `remove_orphan_files`, and `rewrite_manifests`. |
| TRIGGER_REASON | How the task was triggered. Valid values: `schedule` (triggered by auto maintenance) and `manual` (executed via `ALTER TABLE ... EXECUTE`). |
| STMT           | The SQL statement for manually triggered tasks. NULL for automatically triggered tasks. |
| START_TIME     | Time when the task started.                                  |
| END_TIME       | Time when the task finished.                                 |
| DURATION_MS    | Duration of the task in milliseconds.                        |
| STATUS         | Status of the task. Valid values: `success` (changed table state), `skipped` (ran but had nothing to do), `failed`, and `partial` (some changes were applied before a failure, for example, part of the orphan files were already deleted). |
| FAILURE_REASON | Failure reason for failed tasks.                             |
| DETAILS        | JSON object with action-specific execution details: for `expire_snapshots`, snapshot counts before and after execution; for `rewrite_manifests`, manifest file counts and bytes before and after execution; for `remove_orphan_files`, the number of detected and removed orphan files and the reclaimed bytes. |

:::note
The task history is kept in the memory of the leader FE only, bounded by the FE configuration items `iceberg_maintenance_task_history_ttl_second` and `iceberg_maintenance_task_history_max_number`. It is lost when the FE restarts or the leader changes.
:::
