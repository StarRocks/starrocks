---
displayed_sidebar: "English"
---

# be_cloud_native_compactions

`be_cloud_native_compactions` provides information on compaction transactions running on CNs (or BEs for v3.0) of a shared-data cluster. A compaction transaction is divided into multiple tasks on the tablet level, and each row in `be_cloud_native_compactions` corresponds to a task in the compaction transaction.

The following fields are provided in `be_cloud_native_compactions`:

| **Field**   | **Description**                                              |
| ----------- | ------------------------------------------------------------ |
| BE_ID       | ID of the CN (BE).                                           |
| TXN_ID      | ID of the compaction transaction. It can be duplicated because each compaction transaction may have multiple tasks. |
| TABLET_ID   | ID of the tablet the task corresponds to.                    |
| VERSION     | Version of the data that is input to the task.               |
| SKIPPED     | Whether the task is skipped.                                 |
| RUNS        | Number of task executions. A value greater than `1` indicates that retries have occurred. |
| START_TIME  | Task start time.                                             |
| FINISH_TIME | Task finish time. `NULL` is returned if the task is still in progress. |
| PROGRESS    | Percentage of progress, ranging from 0 to 100.               |
| STATUS      | Task status.                                                 |

