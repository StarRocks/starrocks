---
displayed_sidebar: docs
---

# tasks

`tasks` provides information about asynchronous tasks.

The following fields are provided in `tasks`:

| **Field**   | **Description**                                              |
| ----------- | ------------------------------------------------------------ |
| TASK_NAME   | Name of the task.                                            |
| CREATE_TIME | Time when the task was created.                               |
| SCHEDULE    | Task schedule. If the task is regularly triggered, this field displays `START xxx EVERY xxx`. |
| DATABASE    | Database where the task belongs.                             |
| DEFINITION  | SQL definition of the task.                                  |
| EXPIRE_TIME | Time when the task expires.                                  |
