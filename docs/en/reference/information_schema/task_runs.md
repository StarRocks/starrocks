---
displayed_sidebar: "English"
---

# task_runs

`task_runs` provides information about the execution of asynchronous tasks.

The following fields are provided in `task_runs`:

| **Field**     | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| QUERY_ID      | ID of the query.                                             |
| TASK_NAME     | Name of the task.                                            |
| CREATE_TIME   | Time when the task was created.                               |
| FINISH_TIME   | Time when the task finished.                                 |
| STATE         | State of the task. Valid values: `PENDING`, `RUNNING`, `FAILED`, and `SUCCESS`. |
| DATABASE      | Database where the task belongs.                             |
| DEFINITION    | SQL definition of the task.                                  |
| EXPIRE_TIME   | Time when the task expires.                                  |
| ERROR_CODE    | Error code of the task.                                      |
| ERROR_MESSAGE | Error message of the task.                                   |
| PROGRESS      | The progress of the task.                                    |
| EXTRA_MESSAGE | Extra message for the task, for example, the partition information in an asynchronous materialized view creation task. |
