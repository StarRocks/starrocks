# RESTORE

## Description

Restores data to a specified database, table, or partition. Currently, StarRocks only supports restoring data to OLAP tables. For more information, see [data backup and restoration](../../../administration/Backup_and_restore.md).

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](../data-manipulation/SHOW%20RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](../data-definition/CANCEL%20RESTORE.md).

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
| ON              | Name of the tables to restored. The whole database is restored if this parameter is not specified. |
| PARTITION       | Name of the partitions to be restored. The whole table is restored if this parameter is not specified. You can view the partition name using [SHOW PARTITIONS](../data-manipulation/SHOW%20PARTITIONS.md). |
| PROPERTIES      | Properties of the RESTORE operation. Valid keys:<ul><li>`backup_timestamp`: Backup timestamp. **Required**. You can view backup timestamps using [SHOW SNAPSHOT](../data-manipulation/SHOW%20SNAPSHOT.md).</li><li>`replication_num`: Specify the number of replicas to be restored. Default: `3`.</li><li>`meta_version`: This parameter is only used as a temporary solution to restore the data backed up by the earlier version of StarRocks. The latest version of the backed up data already contains `meta version`, and you do not need to specify it.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Examples

Example 1: Restores the table `backup_tbl` in the snapshot `snapshot_label1` from `example_repo` repository to the database `example_db`, and the backup timestamp is `2018-05-04-16-45-08`. Restores one replica.

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
