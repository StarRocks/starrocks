# SHOW BACKUP

## Description

Views the last BACKUP task in a specified database. For more information, see [data backup and restoration](../../../administration/Backup_and_restore.md).

> **NOTE**
>
> Only the information of the last BACKUP task is saved in StarRocks.

## Syntax

```SQL
SHOW BACKUP [FROM <db_name>]
```

## Parameters

| **Parameter** | **Description**                                       |
| ------------- | ----------------------------------------------------- |
| db_name       | Name of the database that the BACKUP task belongs to. |

## Return

| **Return**           | **Description**                                              |
| -------------------- | ------------------------------------------------------------ |
| JobId                | Unique job ID.                                               |
| SnapshotName         | Name of the data snapshot.                                   |
| DbName               | Name of the database that the BACKUP task belongs to.        |
| State                | Current state of the BACKUP task:<ul><li>PENDING: Initial state after submitting a job.</li><li>SNAPSHOTING: Creating snapshot.</li><li>UPLOAD_SNAPSHOT: Snapshot complete, ready for upload.</li><li>UPLOADING: Uploading snapshot.</li><li>SAVE_META: Creating local metadata files.</li><li>UPLOAD_INFO: Uploading metadata files and information of the BACKUP task.</li><li>FINISHED: BACKUP task finished.</li><li>CANCELLED: BACKUP task failed or cancelled.</li></ul> |
| BackupObjs           | Backed up objects.                                           |
| CreateTime           | Task submission time.                                        |
| SnapshotFinishedTime | Snapshot completion time.                                    |
| UploadFinishedTime   | Snapshot upload completion time.                             |
| FinishedTime         | Task completion Time.                                        |
| UnfinishedTasks      | Unfinished subtask IDs in the SNAPSHOTING and UPLOADING phases. |
| Progress             | The progress of snapshot uploading tasks.                             |
| TaskErrMsg           | Error messages.                                              |
| Status               | Status information.                                          |
| Timeout              | Task timeout. Unit: second.                                  |

## Examples

Example 1: Views the last BACKUP task in the database `example_db`.

```SQL
SHOW BACKUP FROM example_db;
```
