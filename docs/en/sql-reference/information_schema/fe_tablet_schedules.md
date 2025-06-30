---
displayed_sidebar: docs
---

# fe_tablet_schedules

`fe_tablet_schedules` provides information about tablet scheduling tasks on FE nodes.

The following fields are provided in `fe_tablet_schedules`:

| **Field**       | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| TABLE_ID        | ID of the table to which the tablet belongs.                 |
| PARTITION_ID    | ID of the partition to which the tablet belongs.             |
| TABLET_ID       | ID of the tablet.                                            |
| TYPE            | Type of the scheduling task (e.g., `CLONE`, `REPAIR`).       |
| PRIORITY        | Priority of the scheduling task.                             |
| STATE           | State of the scheduling task (e.g., `PENDING`, `RUNNING`, `FINISHED`). |
| TABLET_STATUS   | Status of the tablet.                                        |
| CREATE_TIME     | Creation time of the scheduling task.                        |
| SCHEDULE_TIME   | Time when the scheduling task was scheduled.                 |
| FINISH_TIME     | Finish time of the scheduling task.                          |
| CLONE_SRC       | Source BE ID for clone tasks.                                |
| CLONE_DEST      | Destination BE ID for clone tasks.                           |
| CLONE_BYTES     | Number of bytes cloned for clone tasks.                      |
| CLONE_DURATION  | Duration of the clone task (in seconds).                     |
| MSG             | Message related to the scheduling task.                      |
