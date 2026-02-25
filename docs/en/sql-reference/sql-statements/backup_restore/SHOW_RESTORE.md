---
displayed_sidebar: docs
---

# SHOW RESTORE

Views the last RESTORE task in a specified database.

> **NOTE**
>
> Only the information of the last RESTORE task is saved in StarRocks.

## Syntax

```SQL
SHOW RESTORE [FROM <db_name>]
```

## Parameters

| **Parameter** | **Description**                                        |
| ------------- | ------------------------------------------------------ |
| db_name       | Name of the database that the RESTORE task belongs to. |

## Return

| **Return**           | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JobId                | Unique job ID.                                               |
| Label                | Name of the data snapshot.                                   |
| Timestamp            | Backup timestamp.                                            |
| DbName               | Name of the database that the RESTORE task belongs to.       |
| State                | Current state of the RESTORE task:<ul><li>PENDING: Initial state after submitting a job.</li><li>SNAPSHOTING: Executing the local snapshot.</li><li>DOWNLOAD: Submitting snapshot download task.</li><li>DOWNLOADING: Downloading the snapshot.</li><li>COMMIT: To commit the downloaded snapshot.</li><li>COMMITTING: Committing the downloaded snapshot.</li><li>FINISHED: RESTORE task finished.</li><li>CANCELLED: RESTORE task failed or cancelled.</li></ul> |
| AllowLoad            | If loading data is allowed during the RESTORE task.          |
| ReplicationNum       | Number of replicas to be restored.                           |
| RestoreObjs          | The restored objects (tables and partitions).                |
| CreateTime           | Task submission time.                                        |
| MetaPreparedTime     | Local metadata completion time.                              |
| SnapshotFinishedTime | Snapshot completion time.                                    |
| DownloadFinishedTime | Snapshot download completion time.                           |
| FinishedTime         | Task completion Time.                                        |
| UnfinishedTasks      | Unfinished subtask IDs in the SNAPSHOTTING, DOWNLOADING, and COMMITTING phases. |
| Progress             | The progress of snapshot downloading tasks.                  |
| TaskErrMsg           | Error messages.                                              |
| Status               | Status information.                                          |
| Timeout              | Task timeout. Unit: second.                                  |

## Examples

Example 1: Views the last RESTORE task in the database `example_db`.

```SQL
SHOW RESTORE FROM example_db;
```
