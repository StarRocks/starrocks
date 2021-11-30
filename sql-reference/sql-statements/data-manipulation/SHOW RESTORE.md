# SHOW RESTORE

## description

This statement is used to view RESTORE task

Syntax:

```sql
SHOW RESTORE [FROM db_name]
```

Note：

```plain text
1. Only the latest RESTORE task is saved in StarRocks.
2. The meanings of each column are as follows:
JobId：                  unique job id
Label：                  name of the backup to restore
Timestamp：              the time version of the backup to restore
DbName：                 Owning Database
State：                  current stage
PENDING：                the initial status after submitting the job
SNAPSHOTING：            executing snapshot
DOWNLOAD：               the snapshot is completed and ready to download the snapshot in the repository.
DOWNLOADING：            snapshot downloading
COMMIT：                 the snapshot download is complete and ready to take effect
COMMITING：              in effect
FINISHED：               job succeeded
CANCELLED：              job failed
AllowLoad：              whether to allow import during recovery (currently not supported)
ReplicationNum：         Specifies the number of replicas to restore
RestoreJobs：            tables and partitions to restore
CreateTime：             ask submission time
MetaPreparedTime：       metadata preparation completion time
SnapshotFinishedTime：   snapshot completion time
DownloadFinishedTime：   snapshot download completion time
FinishedTime：           job end time
UnfinishedTasks：        unfinished subtask id is displayed in the SNAPSHOTING、DOWNLOADING and COMMITING  phases
Status：                 if the job fails, the failure information is displayed
Timeout：                job timeout, in seconds
```

## example

1. View example_ db the next RESTORE task.

```sql
SHOW RESTORE FROM example_db;
```

## keyword

SHOW, RESTORE
