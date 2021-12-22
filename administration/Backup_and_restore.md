# Backup and Recovery

StarRocks supports backing up the current data as a file to a remote storage system via the broker (the broker is an optional process in the StarRocks cluster, mainly used to support StarRocks to read and write files and directories on the remote storage, please refer to the [Broker Load documentation](../loading/BrokerLoad.md). The backup data can be restored from the remote storage system to any StarRocks cluster with the restore command. This feature supports periodic snapshot backup of data. It also allows migrating data between clusters.

To use this feature, you need to deploy a broker that corresponds to the remote storage system, such as HDFS. You can check the broker information by `SHOW BROKER;`.

## Principle Description

### Backup

Backup is to upload data from a specified table or partition to a remote repository in the form of a file stored by StarRocks. When a user submits a Backup request, the system executes the following operations internally:

1. Snapshot and Snapshot Upload
   * First, it takes a snapshot of the data in the specified table or partition . The backup is performed on the snapshot. After the snapshot, any operation made to the table no longer affects the backup result. The snapshot quickly produces a hard link to the current data file. Once the snapshot is complete, the snapshot files are uploaded one by one. Snapshot upload is done concurrently by each Backend.
  
2. Metadata Preparation and Upload
   * After the snapshot upload is completed, Frontend will write the corresponding metadata to a local file, and then upload this file to the remote repository through the broker.

### Restore

The restore operation requires specifying a backup that already exists in the remote repository and will restore the contents of this backup to the local cluster. When the user submits a Restore request, the system executes the following operations internally.

1. Create the corresponding metadata locally
   * This step first creates a structure such as a table partition in the local cluster that corresponds to the restore request. Once created, the table will be visible, but not accessible.

2. local snapshot
   * This step takes a snapshot of the table created in the previous step. The snapshot will be empty because the newly created table has no data. The purpose of this step is to generate the corresponding snapshot directory on Backend, which can be used to receive the snapshot files downloaded from the remote repository later.

3. download the snapshot
   * The snapshot file from the remote repository is downloaded to the corresponding snapshot directory generated in the previous step. This step is done concurrently by each Backend.

4. ready to use
   * After the snapshots are downloaded, the individual snapshots are mapped to the current local table metadata. These snapshots are then reloaded to be ready to use and complete the final recovery operation.

5. Overall Process
   * First create a cloud repository for both old and new clusters with the same `REPOSITORY` name. The BROKER Name can be checked in the cluster. Second, in the old cluster, prepare the tables that need to be migrated and backed up. Third, back up those tables to the cloud repository, and then Restore from the cloud repository to the new cluster.

   * The tables to be backed up and restored do not need to be created in advance in the new cluster, because they will be created automatically when the Restore operation is performed.

## Instructions

### Data Backup

Currently StarRocks supports full backups at the partition level. If you need to perform regular backups, plan the partitioning and bucketing wisely when creating tables. Later you can perform data backups regularly for each partition.

### Data Migration

To complete data migration, users can back up data to a remote repository and then restore data from the repository to another cluster. Because data backup is done in the form of snapshots, any data that is imported after the snapshot phase will not be backed up. Therefore, any data imported to the original cluster during the grace period needs to be imported again to the new cluster. It is recommended to perform parallel import to both clusters after the migration is completed. Complete data verification and then migrate the business to the new cluster.

## Notes

1. B Only ADMIN users can perform backup and recovery operations.
2. Only one active backup or recovery job can run in a Database.
3. Both backup and recovery can be performed at the partition level. For large volume data, it is recommended to perform the operations separately by partition to reduce retries after failures.
4. Backup and recovery operations are performed on the actual data files. When a table has too many tablets or minor versions, it takes a long time to back up or restore even if the total data volume is small. Users can estimate the job execution time by viewing the number of tablets and the number of file versions in each partition with `SHOW PARTITIONS FROM table_name;` and `SHOW TABLET FROM table_name;` respectively. The number of files has a great impact on the job execution time, so it is recommended to plan the partitioning and bucketing wisely.
5. When checking the job status with `SHOW BACKUP` or `SHOW RESTORE`, you may see an error message in the TaskErrMsg column. However, as long as the State column is not `CANCELLED`, the job is still running. Some tasks may be retried successfully. Some Task errors may cause the job to fail directly.
6. If the recovery job is an overwrite operation, i.e., it specifies to restore data to an existing table or partition, then the data overwritten on the current cluster may not be restored from the `COMMIT` phase. If the recovery job fails or is canceled at this point, it is possible that the previous data is corrupted and inaccessible. In this case, you can only recover the data by performing the recovery operation again and waiting for the job to complete. Therefore, try not to use overwrite to recover data unless it is confirmed that the current data is no longer in use. The overwrite operation checks whether the metadata of the snapshot is the same as the existing table or partition. If different, the recovery operation cannot be executed.
7. The snapshot data currently cannot be deleted directly by StarRocks.Users can manually delete the snapshot paths backed up in the remote storage system.

## Related Commands

The following commands are related to the backup and recovery operations.

CREATE REPOSITORY

* Create a remote repository path for backup or recovery. This command requires access to the remote storage system via Broker. Different Brokers need different parameters, see [Broker documentation](. /loading/BrokerLoad.md). The backup and recovery clusters need to create the same repository, including the repository path and repository name, which allows the  recovery cluster to read the backup snapshot of the backup cluster.

BACKUP

* Perform a backup operation.

SHOW BACKUP

View the execution status of the last backup job, including:

* JobId: The id of this backup job.
* SnapshotName: The user-specified name (Label) of this backup job.
* DbName: The Database for the backup job.
* State: The current phase of the backup job.
  * PENDING: The initial state of the job.
  * SNAPSHOTING: The snapshot operation is in progress.
  * UPLOAD_SNAPSHOT: The snapshot is ready to be uploaded.
  * UPLOADING: The snapshot is being uploaded.
  * SAVE_META: Metadata file is being generated locally.
  * UPLOAD_INFO: Uploading metadata file and job information.
  * FINISHED: Backup is complete.
  * CANCELLED: Backup failed or was cancelled.
* BackupObjs: A list of the tables and partitions involved in this backup.
* CreateTime: The time when the job was created.
* SnapshotFinishedTime: The completion time of the snapshot.
* UploadFinishedTime: The time when the snapshot was uploaded successfully.
* FinishedTime: The completion time of this job.
* UnfinishedTasks: There will be multiple subtasks in progress, such as SNAPSHOTTING, UPLOADING, etc. Here are the task ids of the incomplete subtasks.
* TaskErrMsg: If there is a subtask execution error, the error message will be displayed here.
* Status: The status that may occur when the job is running.
* Timeout: The timeout of the job, in "seconds".

SHOW SNAPSHOT

* View the backups that already exist in the remote repository.

  * Snapshot: The name (Label) of this backup specified at backup time.
  * Timestamp: The timestamp of the backup.
  * Status: Whether the backup is normal or not.

* If you specify the `where` clause after `SHOW SNAPSHOT`, you will see more detailed backup information.

  * Database: The corresponding Database.
  * Details:  A holistic view of the backup data directory.

RESTORE

Perform a restore operation.

SHOW RESTORE

View the execution of the most recent recovery job, including:

* JobId: The id of this recovery job.
* Label: The name (Label) of the backup in the repository.
* Timestamp: The timestamp of the backup in the repository.
* DbName: The Database corresponding to the recovery job.
* State: The current phase of the recovery job.

  * PENDING: The initial state of the job.
  * SNAPSHOTTING: Snapshot is being performed.
  * DOWNLOAD: Snapshot downloading task is being sent.
  * DOWNLOADING: Snapshot is being downloaded.
  * COMMIT: Preparing to validate a downloaded snapshot.
  * COMMITTING: The downloaded snapshot is being validated.
  * FINISHED: Recovery is complete.
  * CANCELLED: Recovery failed or was canceled.

* AllowLoad: Whether import is allowed during recovery.
* ReplicationNum: The number of replications.
* RestoreObjs: A list of the tables and partitions involved in this recovery job.
* CreateTime: The time when the job was created.
* MetaPreparedTime: The time when the local metadata generation is completed.
* SnapshotFinishedTime: Completion time of Local snapshot.
* DownloadFinishedTime: Completion time of remote snapshot download.
* FinishedTime: Completion time of this job.
* UnfinishedTasks: There will be multiple subtasks in progress, such as S SNAPSHOTTING, DOWNLOADING, COMMITTING, etc. Here are the task ids of the incomplete subtasks.
* TaskErrMsg: If there is a subtask execution error, the error message will be displayed here.
* Status: The status that may occur when the job is running.
* Timeout: The timeout of the job, in seconds.

CANCEL BACKUP

Cancel the running backup job.

CANCEL RESTORE

Cancel the running recovery job.

DROP REPOSITORY

Delete a remote repository that has been created. Deleting a repository only removes the mapping of that repository in StarRocks, not the actual repository data.
