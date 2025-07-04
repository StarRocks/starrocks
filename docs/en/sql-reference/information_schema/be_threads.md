---
displayed_sidebar: docs
---

# be_threads

`be_threads` provides information about the threads running on each BE node.

The following fields are provided in `be_threads`:

| **Field**      | **Description**                                              |
| -------------- | ------------------------------------------------------------ |
| BE_ID          | ID of the BE node.                                           |
| GROUP          | Thread group name.                                           |
| NAME           | Thread name.                                                 |
| PTHREAD_ID     | Pthread ID.                                                  |
| TID            | Thread ID.                                                   |
| IDLE           | Indicates whether the thread is idle (`true`) or not (`false`). |
| FINISHED_TASKS | Number of tasks finished by the thread.                      |
| BOUND_CPUS     | CPUs to which the thread is bound.                           |
