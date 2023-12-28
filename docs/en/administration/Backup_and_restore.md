---
displayed_sidebar: "English"
---

# Back up and restore data

This topic describes how to back up and restore data in StarRocks, or migrate data to a new StarRocks cluster.

StarRocks supports backing up data as snapshots into a remote storage system, and restoring the data to any StarRocks clusters.

StarRocks supports the following remote storage systems:

- Apache™ Hadoop® (HDFS) cluster
- AWS S3
- Google GCS

> **NOTE**
>
> Shared-data StarRocks clusters do not support data BACKUP and RESTORE.

## Back up data

StarRocks supports FULL backup on the granularity level of database, table, or partition.

If you have stored a large amount of data in a table, we recommend that you back up and restore data by partition. This way, you can reduce the cost of retries in case of job failures. If you need to back up incremental data on a regular basis, you can strategize a [dynamic partitioning](../table_design/dynamic_partitioning.md) plan (by a certain time interval, for example) for your table, and back up only new partitions each time.

### Create a repository

Before backing up data, you need to create a repository, which is used to store data snapshots in a remote storage system. You can create multiple repositories in a StarRocks cluster. For detailed instructions, see [CREATE REPOSITORY](../sql-reference/sql-statements/data-definition/CREATE_REPOSITORY.md).

- Create a repository in HDFS

The following example creates a repository named `test_repo` in an HDFS cluster.

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "hdfs://<hdfs_host>:<hdfs_port>/repo_dir/backup"
PROPERTIES(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

- Create a repository in AWS S3

  You can choose IAM user-based credential (Access Key and Secret Key), Instance Profile, or Assumed Role as the credential method for accessing AWS S3.

  - The following example creates a repository named `test_repo` in the AWS S3 bucket `bucket_s3` using IAM user-based credential as the credential method.

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
      "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyyyyyyyyy",
      "aws.s3.endpoint" = "s3.us-east-1.amazonaws.com"
  );
  ```

  - The following example creates a repository named `test_repo` in the AWS S3 bucket `bucket_s3` using Instance Profile as the credential method.

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.region" = "us-east-1"
  );
  ```

  - The following example creates a repository named `test_repo` in the AWS S3 bucket `bucket_s3` using Assumed Role as the credential method.

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.use_instance_profile" = "true",
      "aws.s3.iam_role_arn" = "arn:aws:iam::xxxxxxxxxx:role/yyyyyyyy",
      "aws.s3.region" = "us-east-1"
  );
  ```

> **NOTE**
>
> StarRocks supports creating repositories in AWS S3 only according to the S3A protocol. Therefore, when you create repositories in AWS S3, you must replace `s3://` in the S3 URI you pass as a repository location in `ON LOCATION` with `s3a://`.

- Create a repository in Google GCS

The following example creates a repository named `test_repo` in the Google GCS bucket `bucket_gcs`.

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3a://bucket_gcs/backup"
PROPERTIES(
    "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxxxxx",
    "fs.s3a.secret.key" = "yyyyyyyyyyyyyyyyyyyy",
    "fs.s3a.endpoint" = "storage.googleapis.com"
);
```

> **NOTE**
>
> StarRocks supports creating repositories in Google GCS only according to the S3A protocol. Therefore, when you create repositories in Google GCS, you must replace the prefix in the GCS URI you pass as a repository location in `ON LOCATION` with `s3a://`.

After the repository is created, you can check the repository via [SHOW REPOSITORIES](../sql-reference/sql-statements/data-manipulation/SHOW_REPOSITORIES.md). After restoring data, you can delete the repository in StarRocks using [DROP REPOSITORY](../sql-reference/sql-statements/data-definition/DROP_REPOSITORY.md). However, data snapshots backed up in the remote storage system cannot be deleted through StarRocks. You need to delete them manually in the remote storage system.

### Back up a data snapshot

After the repository is created, you need to create a data snapshot and back up it in the remote repository. For detailed instructions, see [BACKUP](../sql-reference/sql-statements/data-definition/BACKUP.md).

The following example creates a data snapshot `sr_member_backup` for the table `sr_member` in the database `sr_hub` and backs up it in the repository `test_repo`.

```SQL
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

BACKUP is an asynchronous operation. You can check the status of a BACKUP job using [SHOW BACKUP](../sql-reference/sql-statements/data-manipulation/SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](../sql-reference/sql-statements/data-definition/CANCEL_BACKUP.md).

## Restore or migrate data

You can restore the data snapshot backed up in the remote storage system to the current or other StarRocks clusters to restore or migrate data.

### (Optional) Create a repository in the new cluster

To migrate data to another StarRocks cluster, you need to create a repository with the same **repository name** and **location** in the new cluster, otherwise you will not be able to view the previously backed up data snapshots. See [Create a repository](#create-a-repository) for details.

### Check the snapshot

Before restoring data, you can check the snapshots in a specified repository using [SHOW SNAPSHOT](../sql-reference/sql-statements/data-manipulation/SHOW_SNAPSHOT.md).

The following example checks the snapshot information in `test_repo`.

```Plain
mysql> SHOW SNAPSHOT ON test_repo;
+------------------+-------------------------+--------+
| Snapshot         | Timestamp               | Status |
+------------------+-------------------------+--------+
| sr_member_backup | 2023-02-07-14-45-53-143 | OK     |
+------------------+-------------------------+--------+
1 row in set (1.16 sec)
```

### Restore data via the snapshot

You can use the [RESTORE](../sql-reference/sql-statements/data-definition/RESTORE.md) statement to restore data snapshots in the remote storage system to the current or other StarRocks clusters.

The following example restores the data snapshot `sr_member_backup` in `test_repo` on the table `sr_member`. It only restores ONE data replica.

```SQL
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES (
    "backup_timestamp"="2023-02-07-14-45-53-143",
    "replication_num" = "1"
);
```

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](../sql-reference/sql-statements/data-manipulation/SHOW_RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](../sql-reference/sql-statements/data-definition/CANCEL_RESTORE.md).

## Configure BACKUP or RESTORE jobs

You can optimize the performance of BACKUP or RESTORE jobs by modifying the following configuration items in the BE configuration file **be.conf**:

| Configuration item      | Description                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| upload_worker_count     | The maximum number of threads for the upload tasks of BACKUP jobs on a BE node. Default: `1`. Increase the value of this configuration item to increase the concurrency of the upload task. |
| download_worker_count   | The maximum number of threads for the download tasks of RESTORE jobs on a BE node. Default: `1`. Increase the value of this configuration item to increase the concurrency of the download task. |
| max_download_speed_kbps | The upper limit of the download speed on a BE node. Default: `50000`. Unit: KB/s. Usually, the speed of the download tasks in RESTORE jobs will not exceed the default value. If this configuration is limiting the performance of RESTORE jobs, you can increase it according to your bandwidth.|

<!--

## Materialized view BACKUP and RESTORE

During a BACKUP or a RESTORE job of a table, StarRocks automatically backs up or restores its [Synchronous materialized view](../using_starrocks/Materialized_view-single_table.md).

From v3.2.0, StarRocks supports backing up and restoring [asynchronous materialized views](../using_starrocks/Materialized_view.md) when you back up and restore the database they reside in.

During BACKUP and RESTORE a database, StarRocks does as follows:

- **BACKUP**

1. Traverse the database to gather information on all tables and asynchronous materialized views.
2. Adjust the order of tables in the BACKUP and RESTORE queue, ensuring that the base tables of materialized views are positioned before the materialized views:
   - If the base table exists in the current database, StarRocks adds the table to the queue.
   - If the base table does not exist in the current database, StarRocks prints a warning log and proceeds with the BACKUP operation without blocking the process.
3. Execute the BACKUP task in the order of the queue.

- **RESTORE**

1. Restore the tables and materialized views in the order of the BACKUP and RESTORE queue.
2. Re-build the dependency between materialized views and their base tables, and re-submit the refresh task schedule.

Any error encountered throughout the RESTORE process will not block the process.

After RESTORE, you can check the status of the materialized view using [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md).

- If the materialized view is active, it can be used directly.
- If the materialized view is inactive, it might be because its base tables are not restored. After all the base tables are restored, you can use [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md) to re-activate the materialized view.

-->

## Usage notes

- Only users with the ADMIN privilege can back up or restore data.
- In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.
- Because BACKUP and RESTORE jobs occupy many resources of your StarRocks cluster, you can back up and restore your data while your StarRocks cluster is not heavily loaded.
- StarRocks does not support specifying data compression algorithm for data backup.
- Because data is backed up as snapshots, the data loaded upon snapshot generation is not included in the snapshot. Therefore, if you load data into the old cluster after the snapshot is generated and before the RESTORE job is completed, you also need to load the data into the cluster that data is restored into. It is recommended that you load data into both clusters in parallel for a period of time after the data migration is complete, and then migrate your application to the new cluster after verifying the correctness of the data and services.
- Before the RESTORE job is completed, you cannot operate the table to be restored.
- Primary Key tables cannot be restored to a StarRocks cluster earlier than v2.5.
- You do not need to create the table to be restored in the new cluster before restoring it. The RESTORE job automatically creates it.
- If there is an existing table that has a duplicated name with the table to be restored, StarRocks first checks whether or not the schema of the existing table matches that of the table to be restored. If the schemas match, StarRocks overwrites the existing table with the data in the snapshot. If the schema does not match, the RESTORE job fails. You can either rename the table to be restored using the keyword `AS`, or delete the existing table before restoring data.
- If the RESTORE job overwrites an existing database, table, or partition, the overwritten data cannot be restored after the job enters the COMMIT phase. If the RESTORE job fails or is canceled at this point, the data may be corrupted and inaccessible. In this case, you can only perform the RESTORE operation again and wait for the job to complete. Therefore, we recommend that you do not restore data by overwriting unless you are sure that the current data is no longer used. The overwrite operation first checks metadata consistency between the snapshot and the existing database, table, or partition. If an inconsistency is detected, the RESTORE operation cannot be performed.
- Currently, StarRocks does not support backing up and restoring logical views.
- Currently, StarRocks does not support backing up and restoring the configuration data related to user accounts, privileges, and resource groups.
- Currently, StarRocks does not support backing up and restoring the Colocate Join relationship among tables.
