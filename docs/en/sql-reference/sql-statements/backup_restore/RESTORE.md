---
displayed_sidebar: docs
---

# RESTORE

## Description

Restores data to a specified database, table, or partition. Currently, StarRocks only supports restoring data to OLAP tables.

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](./SHOW_RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](./CANCEL_RESTORE.md).

> **CAUTION**
>
> - Only users with the ADMIN privilege can restore data.
> - In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.

## Syntax

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

## Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | Name of the database that the data is restored to.           |
| snapshot_name   | Name for the data snapshot.                                  |
| repository_name | Repository name.                                             |
| ON              | Name of the tables to restore. The whole database is restored if this parameter is not specified. |
| PARTITION       | Name of the partitions to be restored. The whole table is restored if this parameter is not specified. You can view the partition name using [SHOW PARTITIONS](../table_bucket_part_index/SHOW_PARTITIONS.md). |
| PROPERTIES      | Properties of the RESTORE operation. Valid keys:<ul><li>`backup_timestamp`: Backup timestamp. **Required**. You can view backup timestamps using [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md).</li><li>`replication_num`: Specify the number of replicas to be restored. Default: `3`.</li><li>`meta_version`: This parameter is only used as a temporary solution to restore the data backed up by the earlier version of StarRocks. The latest version of the backed up data already contains `meta version`, and you do not need to specify it.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Examples

Example 1: Restores the table `backup_tbl` in the snapshot `snapshot_label1` from the `example_repo` repository to the database `example_db`, and the backup timestamp is `2018-05-04-16-45-08`. Restores one replica.

```SQL
RESTORE SNAPSHOT example_db.snapshot_label1
FROM example_repo
ON ( backup_tbl )
PROPERTIES
(
    "backup_timestamp"="2018-05-04-16-45-08",
    "replication_num" = "1"
);
```

Example 2: Restores partitions `p1` and `p2` of table `backup_tbl` in `snapshot_label2` and table `backup_tbl2` from `example_repo` to database `example_db`, and rename `backup_tbl2` to `new_tbl`. The backup timestamp is `2018-05-04-17-11-01`. Restores three replicas by default.

```SQL
RESTORE SNAPSHOT example_db.snapshot_label2
FROM example_repo
ON(
    backup_tbl PARTITION (p1, p2),
    backup_tbl2 AS new_tbl
)
PROPERTIES
(
    "backup_timestamp"="2018-05-04-17-11-01"
);
```

## Usage notes

- Performing backup and restore operations on global, database, table, and partition levels requires different privileges.
- In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.
- Because BACKUP and RESTORE jobs occupy many resources of your StarRocks cluster, you can back up and restore your data while your StarRocks cluster is not heavily loaded.
- StarRocks does not support specifying data compression algorithms for data backup.
- Because data is backed up as snapshots, the data loaded upon snapshot generation is not included in the snapshot. Therefore, if you load data into the old cluster after the snapshot is generated and before the RESTORE job is completed, you also need to load the data into the cluster that data is restored into. It is recommended that you load data into both clusters in parallel for a period of time after the data migration is complete, and then migrate your application to the new cluster after verifying the correctness of the data and services.
- Before the RESTORE job is completed, you cannot operate the table to be restored.
- Primary Key tables cannot be restored to a StarRocks cluster earlier than v2.5.
- You do not need to create the table to be restored in the new cluster before restoring it. The RESTORE job automatically creates it.
- If there is an existing table that has a duplicated name with the table to be restored, StarRocks first checks whether or not the schema of the existing table matches that of the table to be restored. If the schemas match, StarRocks overwrites the existing table with the data in the snapshot. If the schema does not match, the RESTORE job fails. You can either rename the table to be restored using the keyword `AS`, or delete the existing table before restoring data.
- If the RESTORE job overwrites an existing database, table, or partition, the overwritten data cannot be restored after the job enters the COMMIT phase. If the RESTORE job fails or is canceled at this point, the data may be corrupted and inaccessible. In this case, you can only perform the RESTORE operation again and wait for the job to complete. Therefore, we recommend that you do not restore data by overwriting unless you are sure that the current data is no longer used. The overwrite operation first checks metadata consistency between the snapshot and the existing database, table, or partition. If an inconsistency is detected, the RESTORE operation cannot be performed.
- Currently, StarRocks does not support backing up and restoring logical views.
- Currently, StarRocks does not support backing up and restoring the configuration data related to user accounts, privileges, and resource groups.
- Currently, StarRocks does not support backing up and restoring the Colocate Join relationship among tables.
