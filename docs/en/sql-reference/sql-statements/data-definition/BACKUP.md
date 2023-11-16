---
displayed_sidebar: "English"
---

# BACKUP

## Description

Backs up data in a specified database, table, or partition. Currently, StarRocks only supports backing up data in OLAP tables. For more information, see [data backup and restoration](../../../administration/Backup_and_restore.md).

BACKUP is an asynchronous operation. You can check the status of a BACKUP job status using [SHOW BACKUP](../data-manipulation/SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](../data-definition/CANCEL_BACKUP.md). You can view the snapshot information using [SHOW SNAPSHOT](../data-manipulation/SHOW_SNAPSHOT.md).

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
| repository_name | Repository name. You can create a repository using [CREATE REPOSITORY](../data-definition/CREATE_REPOSITORY.md). |
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
