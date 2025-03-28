---
displayed_sidebar: docs
---

# Back up and restore data

This topic describes how to back up and restore data in StarRocks, or migrate data to a new StarRocks cluster.

StarRocks supports backing up data as snapshots into a remote storage system and restoring the data to any StarRocks clusters.

From v3.4.0 onwards, StarRocks have enhanced the functionality of BACKUP and RESTORE by supporting more objects and refactoring the syntax for better flexibility.

StarRocks supports the following remote storage systems:

- Apache™ Hadoop® (HDFS) cluster
- AWS S3
- Google GCS
- MinIO

StarRocks supports backing up the following objects:

- Internal databases, tables (of all types and partitioning strategies), and partitions
- Metadata of external catalogs (supported from v3.4.0 onwards)
- Synchronous materialized views and asynchronous materialized views
- Logical views (supported from v3.4.0 onwards)
- User-defined functions (supported from v3.4.0 onwards)

> **NOTE**
>
> Shared-data StarRocks clusters do not support data BACKUP and RESTORE.

## Create a repository

Before backing up data, you need to create a repository, which is used to store data snapshots in a remote storage system. You can create multiple repositories in a StarRocks cluster. For detailed instructions, see [CREATE REPOSITORY](../../sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY.md).

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

  - The following example creates a repository named `test_repo` in the AWS S3 bucket `bucket_s3` using IAM user-based credentials as the credential method.

  ```SQL
  CREATE REPOSITORY test_repo
  WITH BROKER
  ON LOCATION "s3a://bucket_s3/backup"
  PROPERTIES(
      "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
      "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyyyyyyyyy",
      "aws.s3.region" = "us-east-1"
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
> - StarRocks supports creating repositories in Google GCS only according to the S3A protocol. Therefore, when you create repositories in Google GCS, you must replace the prefix in the GCS URI you pass as a repository location in `ON LOCATION` with `s3a://`.
> - Do not specify `https` in the endpoint address.

- Create a repository in MinIO

The following example creates a repository named `test_repo` in the MinIO bucket `bucket_minio`.

```SQL
CREATE REPOSITORY test_repo
WITH BROKER
ON LOCATION "s3://bucket_minio/backup"
PROPERTIES(
    "aws.s3.access_key" = "XXXXXXXXXXXXXXXXX",
    "aws.s3.secret_key" = "yyyyyyyyyyyyyyyyy",
    "aws.s3.endpoint" = "http://minio:9000"
);
```

After the repository is created, you can check the repository via [SHOW REPOSITORIES](../../sql-reference/sql-statements/backup_restore/SHOW_REPOSITORIES.md). After restoring data, you can delete the repository in StarRocks using [DROP REPOSITORY](../../sql-reference/sql-statements/backup_restore/DROP_REPOSITORY.md). However, data snapshots backed up in the remote storage system cannot be deleted through StarRocks. You need to delete them manually in the remote storage system.

## Back up data

After the repository is created, you need to create a data snapshot and back up it in the remote repository. For detailed instructions, see [BACKUP](../../sql-reference/sql-statements/backup_restore/BACKUP.md). BACKUP is an asynchronous operation. You can check the status of a BACKUP job using [SHOW BACKUP](../../sql-reference/sql-statements/backup_restore/SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](../../sql-reference/sql-statements/backup_restore/CANCEL_BACKUP.md).

StarRocks supports FULL backup on the granularity level of database, table, or partition.

If you have stored a large amount of data in a table, we recommend that you back up and restore data by partition. This way, you can reduce the cost of retries in case of job failures. If you need to back up incremental data on a regular basis, you can configure a [partitioning plan](../../table_design/data_distribution/Data_distribution.md#partitioning) for your table, and back up only new partitions each time.

### Back up database

Performing a full BACKUP on a database will back up all tables, synchronous and asynchronous materialized views, logical views, and UDFs within the database.

The following examples back up the database `sr_hub` in the snapshot `sr_hub_backup` and upload the snapshot to the repository `test_repo`.

```SQL
-- Supported from v3.4.0 onwards.
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup
TO test_repo;

-- Compatible with the syntax in earlier versions.
BACKUP SNAPSHOT sr_hub.sr_hub_backup
TO test_repo;
```

### Back up table

StarRocks supports backing up and restoring tables of all types and partitioning strategies. Performing a full BACKUP on a table will back up the table and the synchronous materialized views built on it.

The following examples back up the table `sr_member` from the database `sr_hub` in the snapshot `sr_member_backup` and upload the snapshot to the repository `test_repo`.

```SQL
-- Supported from v3.4.0 onwards.
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- Compatible with the syntax in earlier versions.
BACKUP SNAPSHOT sr_hub.sr_member_backup
TO test_repo
ON (sr_member);
```

The following example backs up two tables, `sr_member` and `sr_pmc`, from the database `sr_hub` in the snapshot `sr_core_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);
```

The following example backs up all tables from the database `sr_hub` in the snapshot `sr_all_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

### Back up partition

The following examples back up the partition `p1` of the table `sr_member` from the database `sr_hub` in the snapshot `sr_par_backup` and upload the snapshot to the repository `test_repo`.

```SQL
-- Supported from v3.4.0 onwards.
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- Compatible with the syntax in earlier versions.
BACKUP SNAPSHOT sr_hub.sr_par_backup
TO test_repo
ON (sr_member PARTITION (p1));
```

You can specify multiple partition names separated by commas (`,`) to back up partitions in batch.

### Back up materialized view

You do not need to manually back up synchronous materialized views because they will be backed up along with the BACKUP operation of the base table.

Asynchronous materialized views can be backed up along with the BACKUP operation of the database it belongs to. You can also manually back up them.

The following example backs up the materialized view `sr_mv1` from the database `sr_hub` in the snapshot `sr_mv1_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);
```

The following example backs up two materialized views, `sr_mv1` and `sr_mv2`, from the database `sr_hub` in the snapshot `sr_mv2_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);
```

The following example backs up all materialized views from the database `sr_hub` in the snapshot `sr_mv3_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

### Back up logical view

The following example backs up the logical view `sr_view1` from the database `sr_hub` in the snapshot `sr_view1_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);
```

The following example backs up two logical views, `sr_view1` and `sr_view2`, from the database `sr_hub` in the snapshot `sr_view2_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);
```

The following example backs up all logical views from the database `sr_hub` in the snapshot `sr_view3_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

### Back up UDF

The following example backs up the UDF `sr_udf1` from the database `sr_hub` in the snapshot `sr_udf1_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);
```

The following example backs up two UDFs, `sr_udf1` and `sr_udf2`, from the database `sr_hub` in the snapshot `sr_udf2_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);
```

The following example backs up all UDFs from the database `sr_hub` in the snapshot `sr_udf3_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
```

### Back up metadata of external catalog

The following example backs up the metadata of the external catalog `iceberg` in the snapshot `iceberg_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP EXTERNAL CATALOG (iceberg) SNAPSHOT iceberg_backup
TO test_repo;
```

The following example backs up the metadata of two external catalogs, `iceberg` and `hive`, in the snapshot `iceberg_hive_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP EXTERNAL CATALOGS (iceberg, hive) SNAPSHOT iceberg_hive_backup
TO test_repo;
```

The following example backs up the metadata of all external catalogs in the snapshot `all_catalog_backup` and upload the snapshot to the repository `test_repo`.

```SQL
BACKUP ALL EXTERNAL CATALOGS SNAPSHOT all_catalog_backup
TO test_repo;
```

To cancel the BACKUP operation on external catalogs, execute the following statement:

```SQL
CANCEL BACKUP FOR EXTERNAL CATALOG;
```

## Restore data

You can restore the data snapshot backed up in the remote storage system to the current or other StarRocks clusters to restore or migrate data.

**When you restore an object from a snapshot, you must specify the timestamp of the snapshot.**

Use the [RESTORE](../../sql-reference/sql-statements/backup_restore/RESTORE.md) statement to restore data snapshots in the remote storage system.

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](../../sql-reference/sql-statements/backup_restore/SHOW_RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](../../sql-reference/sql-statements/backup_restore/CANCEL_RESTORE.md).

### (Optional) Create a repository in the new cluster

To migrate data to another StarRocks cluster, you need to create a repository with the same **repository name** and **location** in the target cluster, otherwise, you will not be able to view the previously backed-up data snapshots. See [Create a repository](#create-a-repository) for details.

### Obtain snapshot timestamp

Before restoring data, you can check the snapshots in the repository to obtain the timestamps using [SHOW SNAPSHOT](../../sql-reference/sql-statements/backup_restore/SHOW_SNAPSHOT.md).

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

### Restore database

The following examples restore the database `sr_hub` in the snapshot `sr_hub_backup` to the database `sr_hub` in the target cluster. If the database does not exist in the snapshot, the system will return an error. If the database does not exist in the target cluster, the system will create it automatically.

```SQL
-- Supported from v3.4.0 onwards.
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");

-- Compatible with the syntax in earlier versions.
RESTORE SNAPSHOT sr_hub.sr_hub_backup
FROM `test_repo` 
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

The following example restore the database `sr_hub` in the snapshot `sr_hub_backup` to the database `sr_hub_new` in the target cluster. If the database `sr_hub` does not exist in the snapshot, the system will return an error. If the database `sr_hub_new` does not exist in the target cluster, the system will create it automatically.

```SQL
-- Supported from v3.4.0 onwards.
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub AS sr_hub_new
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

### Restore table

The following examples restore the table `sr_member` of the database `sr_hub` in the snapshot `sr_member_backup` to the table `sr_member` of the database `sr_hub` in the target cluster.

```SQL
-- Supported from v3.4.0 onwards.
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub 
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Compatible with the syntax in earlier versions.
RESTORE SNAPSHOT sr_hub.sr_member_backup
FROM test_repo
ON (sr_member)
PROPERTIES ("backup_timestamp"="2024-12-09-10-52-10-940");
```

The following examples restore the table `sr_member` of the database `sr_hub` in the snapshot `sr_member_backup` to the table `sr_member_new` of the database `sr_hub_new` in the target cluster.

```SQL
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub  AS sr_hub_new
ON (TABLE sr_member AS sr_member_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores two tables, `sr_member` and `sr_pmc`, of the database `sr_hub` in the snapshot `sr_core_backup` to two tables, `sr_member` and `sr_pmc`, of the database `sr_hub` in the target cluster.

```SQL
RESTORE SNAPSHOT sr_core_backup
FROM test_repo 
DATABASE sr_hub
ON (TABLE sr_member, TABLE sr_pmc) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores all tables from the database `sr_hub` in the snapshot `sr_all_backup`.

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (ALL TABLES);
```

The following example restores one of all tables from the database `sr_hub` in the snapshot `sr_all_backup`.

```SQL
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### Restore partition

The following examples restore the partition `p1` of the table `sr_member` in the snapshot `sr_par_backup` to the partition `p1` of the table `sr_member` in the target cluster.

```SQL
-- Supported from v3.4.0 onwards.
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Compatible with the syntax in earlier versions.
RESTORE SNAPSHOT sr_hub.sr_par_backup
FROM test_repo
ON (sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

You can specify multiple partition names separated by commas (`,`) to restore partitions in batch.

### Restore materialized view

The following example restores the materialized view `sr_mv1` from the database `sr_hub` in the snapshot `sr_mv1_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_mv1_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores two materialized views, `sr_mv1` and `sr_mv2`, from the database `sr_hub` in the snapshot `sr_mv2_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_mv2_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores all materialized views from the database `sr_hub` in the snapshot `sr_mv3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL MATERIALIZED VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores one of the materialized views from the database `sr_hub` in the snapshot `sr_mv3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

:::info

After RESTORE, you can check the status of the materialized view using [SHOW MATERIALIZED VIEWS](../../sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW.md).

- If the materialized view is active, it can be used directly.
- If the materialized view is inactive, it might be because its base tables are not restored. After all the base tables are restored, you can use [ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) to re-activate the materialized view.

:::

### Restore logical view

The following example restores the logical view `sr_view1` from the database `sr_hub` in the snapshot `sr_view1_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_view1_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores two logical views, `sr_view1` and `sr_view2`, from the database `sr_hub` in the snapshot `sr_view2_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_view2_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1, VIEW sr_view2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores all logical views from the database `sr_hub` in the snapshot `sr_view3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores one of all logical views from the database `sr_hub` in the snapshot `sr_view3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### Restore UDF

The following example restores the UDF `sr_udf1` from the database `sr_hub` in the snapshot `sr_udf1_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_udf1_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores two UDFs, `sr_udf1` and `sr_udf2`, from the database `sr_hub` in the snapshot `sr_udf2_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_udf2_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1, FUNCTION sr_udf2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores all UDFs from the database `sr_hub` in the snapshot `sr_udf3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL FUNCTIONS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores one of all UDFs from the database `sr_hub` in the snapshot `sr_udf3_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

### Restore metadata of external catalog

The following example restores the metadata of the external catalog `iceberg` in the snapshot `iceberg_backup` to the target cluster, and rename it as `iceberg_new`.

```SQL
RESTORE SNAPSHOT iceberg_backup
FROM test_repo
EXTERNAL CATALOG (iceberg AS iceberg_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores the metadata of two external catalogs, `iceberg` and `hive`, in the snapshot `iceberg_hive_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT iceberg_hive_backup
FROM test_repo 
EXTERNAL CATALOGS (iceberg, hive)
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

The following example restores the metadata of all external catalogs in the snapshot `all_catalog_backup` to the target cluster.

```SQL
RESTORE SNAPSHOT all_catalog_backup
FROM test_repo 
ALL EXTERNAL CATALOGS
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

To cancel the RESTORE operation on external catalogs, execute the following statement:

```SQL
CANCEL RESTORE FOR EXTERNAL CATALOG;
```

## Configure BACKUP or RESTORE jobs

You can optimize the performance of BACKUP or RESTORE jobs by modifying the following configuration items in the BE configuration file **be.conf**:

| Configuration item      | Description                                                                             |
| ----------------------- | -------------------------------------------------------------------------------- |
| make_snapshot_worker_count     | The maximum number of threads for the make snapshot tasks of BACKUP jobs on a BE node. Default: `5`. Increase the value of this configuration item to increase the concurrency of the make snapshot task. |
| release_snapshot_worker_count     | The maximum number of threads for the release snapshot tasks of failed BACKUP jobs on a BE node. Default: `5`. Increase the value of this configuration item to increase the concurrency of the release snapshot task. |
| upload_worker_count     | The maximum number of threads for the upload tasks of BACKUP jobs on a BE node. Default: `0`. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides. Increase the value of this configuration item to increase the concurrency of the upload task. |
| download_worker_count   | The maximum number of threads for the download tasks of RESTORE jobs on a BE node. Default: `0`. `0` indicates setting the value to the number of CPU cores on the machine where the BE resides. Increase the value of this configuration item to increase the concurrency of the download task. |

## Usage notes

- Performing backup and restore operations on global, database, table, and partition levels requires different privileges. For detailed information, see [Customize roles based on scenarios](../user_privs/User_privilege.md#customize-roles-based-on-scenarios).
- In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.
- Because BACKUP and RESTORE jobs occupy many resources of your StarRocks cluster, you can back up and restore your data while your StarRocks cluster is not heavily loaded.
- StarRocks does not support specifying data compression algorithms for data backup.
- Because data is backed up as snapshots, the data loaded upon snapshot generation is not included in the snapshot. Therefore, if you load data into the old cluster after the snapshot is generated and before the RESTORE job is completed, you also need to load the data into the cluster that data is restored into. It is recommended that you load data into both clusters in parallel for a period of time after the data migration is complete, and then migrate your application to the new cluster after verifying the correctness of the data and services.
- Before the RESTORE job is completed, you cannot operate the table to be restored.
- Primary Key tables cannot be restored to a StarRocks cluster earlier than v2.5.
- You do not need to create the table to be restored in the new cluster before restoring it. The RESTORE job automatically creates it.
- If there is an existing table that has a duplicated name with the table to be restored, StarRocks first checks whether or not the schema of the existing table matches that of the table to be restored. If the schemas match, StarRocks overwrites the existing table with the data in the snapshot. If the schema does not match, the RESTORE job fails. You can either rename the table to be restored using the keyword `AS`, or delete the existing table before restoring data.
- If the RESTORE job overwrites an existing database, table, or partition, the overwritten data cannot be restored after the job enters the COMMIT phase. If the RESTORE job fails or is canceled at this point, the data may be corrupted and inaccessible. In this case, you can only perform the RESTORE operation again and wait for the job to complete. Therefore, we recommend that you do not restore data by overwriting unless you are sure that the current data is no longer used. The overwrite operation first checks metadata consistency between the snapshot and the existing database, table, or partition. If an inconsistency is detected, the RESTORE operation cannot be performed.
- Currently, StarRocks does not support backing up and restoring the configuration data related to user accounts, privileges, and resource groups.
- Currently, StarRocks does not support backing up and restoring the Colocate Join relationship among tables.
