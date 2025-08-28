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
| STATE           | The status of the task.                                      |
| SCHEDULE_REASON | The reason for the task scheduling.                          |
| MEDIUM          | The storage medium where the Tablet is located.              |
| PRIORITY        | The current priority of the task.                            |
| ORIG_PRIORITY   | The original priority of the task.                           |
| LAST_PRIORITY_ADJUST_TIME | The time when the task's priority was last adjusted. |
| VISIBLE_VERSION | The visible data version of the Tablet.                      |
| COMMITTED_VERSION | The committed data version of the Tablet.                  |
| SRC_BE_ID       | The ID of the BE where the source replica resides.           |
| SRC_PATH        | The path where the source replica resides.                   |
| DEST_BE_ID      | ID of the BE where the target replica is located.            |
| DEST_PATH       | Path where the target replica is located.                    |
| CREATE_TIME     | The time when the task was created.                          |
| SCHEDULE_TIME   | The time when the task starts scheduling execution.          |
| FINISH_TIME     | The time when the task was finished.                         |
| CLONE_BYTES     | Size of the file cloned in bytes.                            |
| CLONE_DURATION  | Time elapsed for Clone in seconds.                           |
| CLONE_RATE      | Clone rate in MB/s.                                          |
| FAILED_SCHEDULE_COUNT | The number of task scheduling failures.                |
| FAILED_RUNNING_COUNT | Number of task execution failures.                      |
| MSG             | Information on task scheduling and exection.                 |
