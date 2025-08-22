---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` provides information about tablet scheduling tasks on FE nodes.

The following fields are provided in `fe_tablet_schedules`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLET_ID       | The ID of the Tablet.                                        |
| TABLE_ID        | The ID of the table to which the Tablet belongs.             |
| PARTITION_ID    | The ID of the partition to which the Tablet belongs.         |
| TYPE            | The type of the task. Valid values: `REPAIR` and `BALANCE`.  |
| STATE           | The state of the task.                                       |
| SCHEDULE_REASON | The reason for the task.                                     |
| MEDIUM          | The storage medium where the Tablet is located.              |
| PRIORITY        | The current priority of the task.                            |
| ORIG_PRIORITY   | The original priority of the task.                           |
| LAST_PRIORITY_ADJUST_TIME | The time when the task's priority was last adjusted. |
| VISIBLE_VERSION | The version of the data that is visible to the Tablet.       |
| COMMITTED_VERSION | Tablet The version of the data that has been committed.    |
| SRC_BE_ID       | The ID of the BE where the source copy resides.              |
| SRC_PATH        | The path where the source copy resides.                      |
| DEST_BE_ID      | ID of the BE where the target copy is located.               |
| DEST_PATH       | Path where the target copy is located.                       |
| CREATE_TIME     | Task creation time.                                          |
| SCHEDULE_TIME   | The time when the task starts scheduling execution.          |
| FINISH_TIME     | Task completion time.                                        |
| CLONE_BYTES     | Size of the copy file in bytes.                              |
| CLONE_DURATION  | Copy elapsed time in seconds.                                |
| CLONE_RATE      | Copy rate in MB/s.                                           |
| FAILED_SCHEDULE_COUNT | The number of task scheduling failures.                |
| FAILED_RUNNING_COUNT | Number of task execution failures.                      |
| MSG             | Task-related information.                                    |
