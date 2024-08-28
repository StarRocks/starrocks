---
displayed_sidebar: docs
---

# BACKUP

## Description

Backs up data in a specified database, table, or partition. Currently, StarRocks only supports backing up data in OLAP tables.

BACKUP is an asynchronous operation. You can check the status of a BACKUP job status using [SHOW BACKUP](./SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](./CANCEL_BACKUP.md). You can view the snapshot information using [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md).

> **CAUTION**
>
> - Only users with the ADMIN privilege can back up data.
> - In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.
> - StarRocks does not support specifying data compression algorithm for data backup.

## Syntax

```SQL
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

## Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | Name of the database that stores the data to be backed up.   |
| snapshot_name   | Specify a name for the data snapshot. Globally unique.       |
| repository_name | Repository name. You can create a repository using [CREATE REPOSITORY](./CREATE_REPOSITORY.md). |
| ON              | Name of the tables to be backed up. The whole database is backed up if this parameter is not specified. |
| PARTITION       | Name of the partitions to be backed up. The whole table is backed up if this parameter is not specified. |
| PROPERTIES      | Properties of the data snapshot. Valid keys:`type`: Backup type. Currently, only full backup `FULL` is supported. Default: `FULL`.`timeout`: Task timeout. Unit: second. Default: `86400`. |

## Examples

Example 1: Backs up the database `example_db` to the repository `example_repo`.

```SQL
BACKUP SNAPSHOT example_db.snapshot_label1
TO example_repo
PROPERTIES ("type" = "full");
```

Example 2: Backs up the table `example_tbl` in `example_db` to `example_repo`.

```SQL
BACKUP SNAPSHOT example_db.snapshot_label2
TO example_repo
ON (example_tbl);
```

Example 2: Backs up the partitions `p1` and `p2` of `example_tbl` and the table `example_tbl2` in `example_db` to `example_repo`.

```SQL
BACKUP SNAPSHOT example_db.snapshot_label3
TO example_repo
ON(
    example_tbl PARTITION (p1, p2),
    example_tbl2
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
