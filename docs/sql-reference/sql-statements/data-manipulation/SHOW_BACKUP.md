# SHOW BACKUP

## Description

This statement is used to view BACKUP tasks

## Syntax

```sql
SHOW BACKUP [FROM <db_name>]
```

Note:

```plain text
1. Only the last BACKUP task is saved in StarRocks.
2. Each column has the following meaning:
JobId:                  Unique job ID
SnapshotName:           Name of the backup
DbName:                 Owning database
State:                  Current Stage
PENDING:                Initial state after submitting a job
SNAPSHOTING:            in execution snapshot
UPLOAD_SNAPSHOT:        Snapshot complete, ready for upload
UPLOADING:              Snapshot uploading
SAVE_META:              Save job meta-information as a local file
UPLOAD_INFO:            Upload job meta information
FINISHED:               Job success
CANCELLED:              Job failure
BackupObjs:             Backed up tables and partitions
CreateTime:             Task Submission Time
SnapshotFinishedTime:   Snapshot Completion Time
UploadFinishedTime:     Snapshot upload completion time
FinishedTime:           Job end time
UnfinishedTasks:        In the SNAPSHOTING and UPLOADING phases, the unfinished subtask IDs are displayed
Status:                 Display failure information if job fails
Timeout:                Job timeout in seconds
```

## Examples

View example_ db Last BACKUP task.

```sql
SHOW BACKUP FROM example_db;
```
