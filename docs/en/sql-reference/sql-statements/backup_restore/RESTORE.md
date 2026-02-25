---
displayed_sidebar: docs
---

# RESTORE

StarRocks supports backing up and restoring the following objects:

- Internal databases, tables (of all types and partitioning strategies), and partitions
- Metadata of external catalogs (supported from v3.4.0 onwards)
- Synchronous materialized views and asynchronous materialized views
- Logical views (supported from v3.4.0 onwards)
- User-defined functions (supported from v3.4.0 onwards)

:::tip
For an overview of backup and restore please see the [backup and restore guide](../../../administration/management/Backup_and_restore.md).
:::

RESTORE is an asynchronous operation. You can check the status of a RESTORE job using [SHOW RESTORE](./SHOW_RESTORE.md), or cancel a RESTORE job using [CANCEL RESTORE](./CANCEL_RESTORE.md).

> **CAUTION**
>
> - Shared-data StarRocks clusters do not support data BACKUP and RESTORE.
> - In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.

## Privilege requirement

In versions earlier than v3.0, users with the `admin_priv` privilege can perform this operation. In v3.0 and later versions, to back up a specific object, users must have the REPOSITORY privilege at the System level and the EXPORT privilege for the corresponding table or all tables under the corresponding database. For example:

- Grant the role permission to export data from the specified table.

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_tbl;
    GRANT EXPORT ON TABLE <table_name> TO ROLE backup_tbl;
    ```

- Grant the role permission to export data from all tables under the specified data.

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup_db;
    GRANT EXPORT ON ALL TABLES IN DATABASE <database_name> TO ROLE backup_db;
    ```

- Grant the role permission to export data from all tables in all databases.

    ```SQL
    GRANT REPOSITORY ON SYSTEM TO ROLE backup;
    GRANT EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE backup;
    ```

## Syntax compatible with earlier versions

```SQL
RESTORE SNAPSHOT <db_name>.<snapshot_name>
FROM <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
    [ AS <table_alias>] [, ...] ) ]
PROPERTIES ("key"="value", ...)
```

### Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | Name of the database that the data is restored to.           |
| snapshot_name   | Name for the data snapshot.                                  |
| repository_name | Repository name.                                             |
| ON              | Name of the tables to restore. The whole database is restored if this parameter is not specified. |
| PARTITION       | Name of the partitions to be restored. The whole table is restored if this parameter is not specified. You can view the partition name using [SHOW PARTITIONS](../table_bucket_part_index/SHOW_PARTITIONS.md). |
| PROPERTIES      | Properties of the RESTORE operation. Valid keys:<ul><li>`backup_timestamp`: Backup timestamp. **Required**. You can view backup timestamps using [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md).</li><li>`replication_num`: Specify the number of replicas to be restored. Default: `3`.</li><li>`meta_version`: This parameter is only used as a temporary solution to restore the data backed up by the earlier version of StarRocks. The latest version of the backed up data already contains `meta version`, and you do not need to specify it.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Syntax supported from v3.4.0 onwards

```SQL
-- Restore external catalog metadata.
RESTORE SNAPSHOT [<db_name>.]<snapshot_name> FROM <repository_name>
{ ALL EXTERNAL CATALOGS | EXTERNAL CATALOG[S] <catalog_name>[, EXTERNAL CATALOG[S] <catalog_name> ...] [ AS <alias> ] }
[ DATABASE <db_name_in_snapshot> [AS <target_db>] ]
[ PROPERTIES ("key"="value" [, ...] ) ]

-- Restore databases, tables, partitions, materialized views, logical views, or UDFs.
RESTORE SNAPSHOT [<db_name>.]<snapshot_name> FROM <repository_name>
[ DATABASE <db_name_in_snapshot> [AS <target_db>] ]
[ ON ( restore_object [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]

restore_object ::= [
    { ALL TABLE[S]             | TABLE[S] <table_name>[, TABLE[S] <table_name> ...] [AS <alias>] } |
    { ALL MATERIALIZED VIEW[S] | MATERIALIZED VIEW[S] <mv_name>[, MATERIALIZED VIEW[S] <mv_name> ...] [AS <alias>] } |
    { ALL VIEW[S]              | VIEW[S] <view_name>[, VIEW[S] <view_name> ...] [AS <alias>] } |
    { ALL FUNCTION[S]          | FUNCTION[S] <udf_name>[, FUNCTION[S] <udf_name> ...] [AS <alias>] } |
     <table_name> PARTITION (<partition_name>[, ...]) [AS <alias>] ]
```

### Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name.        | Name of the database to which the object(s) or snapshot is restored in the target cluster. If the database does not exist, the system will create it. You can only specify either `AS <target_db>` or `<db_name>.`. |
| snapshot_name   | Name of the data snapshot.                                   |
| repository_name | Repository name.                                             |
| ALL EXTERNAL CATALOGS | Restores the metadata of all external catalogs.        |
| catalog_name    | Name of the external catalog(s) that need to be restored.    |
| DATABASE db_name_in_snapshot | Name of the database to which the object(s) or snapshot belongs when it was backed up in the source cluster. |
| AS target_db    | Name of the database to which the object(s) or snapshot is restored in the target cluster. If the database does not exist, the system will create it. You can only specify either `AS <target_db>` or `<db_name>.`. |
| ON              | The object to be restored. The whole database is restored if this parameter is not specified. |
| table_name      | Name of the table(s) to be restored.                        |
| mv_name         | Name of the materialized view(s) to be restored.            |
| view_name       | Name of the logical view(s) to be restored.                 |
| udf_name        | Name of the UDF(s) to be restored.                          |
| PARTITION       | Name of the partitions to be restored. The whole table is restored if this parameter is not specified. |
| AS alias        | Sets a new name for the object to be restored in the target cluster. |
| PROPERTIES      | Properties of the RESTORE operation. Valid keys:<ul><li>`backup_timestamp`: Backup timestamp. **Required**. You can view backup timestamps using [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md).</li><li>`replication_num`: Specify the number of replicas to be restored. Default: `3`.</li><li>`meta_version`: This parameter is only used as a temporary solution to restore the data backed up by the earlier version of StarRocks. The latest version of the backed up data already contains `meta version`, and you do not need to specify it.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Examples

### Examples with syntax compatible with earlier versions

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

### Examples with syntax supported from v3.4.0 onwards

Example 1: Restores a database.

```SQL
-- Restores the database with its original name.
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");

-- Restores the database with a new name.
RESTORE SNAPSHOT sr_hub_backup
FROM test_repo
DATABASE sr_hub AS sr_hub_new
PROPERTIES("backup_timestamp" = "2024-12-09-10-25-58-842");
```

Example 2: Restores the table(s) in the database.

```SQL
-- Restores one table with its original name.
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub 
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores one table with a new name.
RESTORE SNAPSHOT sr_member_backup
FROM test_repo 
DATABASE sr_hub  AS sr_hub_new
ON (TABLE sr_member AS sr_member_new) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores multiple tables.
RESTORE SNAPSHOT sr_core_backup
FROM test_repo 
DATABASE sr_hub
ON (TABLE sr_member, TABLE sr_pmc) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores all tables.
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (ALL TABLES);

-- Restores one from all tables.
RESTORE SNAPSHOT sr_all_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

Example 3: Restores the partition(s) in a table.

```SQL
-- Restores one partition.
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores multiple partitions.
RESTORE SNAPSHOT sr_par_backup
FROM test_repo
DATABASE sr_hub
ON (TABLE sr_member PARTITION (p1,p2)) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

Example 4: Restores the materialized view(s) in the database.

```SQL
-- Restores one materialized view.
RESTORE SNAPSHOT sr_mv1_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores multiple materialized views.
RESTORE SNAPSHOT sr_mv2_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores all materialized views.
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL MATERIALIZED VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores one of all materialized views.
RESTORE SNAPSHOT sr_mv3_backup
FROM test_repo
DATABASE sr_hub
ON (MATERIALIZED VIEW sr_mv1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

Example 5: Restores the logical view(s) in the database.

```SQL
-- Restores one logical view.
RESTORE SNAPSHOT sr_view1_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores multiple logical views.
RESTORE SNAPSHOT sr_view2_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1, VIEW sr_view2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores all logical views.
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL VIEWS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores one of all logical views.
RESTORE SNAPSHOT sr_view3_backup
FROM test_repo
DATABASE sr_hub
ON (VIEW sr_view1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
```

Example 6: Restores the UDF(s) in the database.

```SQL
-- Restores one UDF.
RESTORE SNAPSHOT sr_udf1_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores multiple UDFs.
RESTORE SNAPSHOT sr_udf2_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1, FUNCTION sr_udf2) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores all UDFs.
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (ALL FUNCTIONS) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");

-- Restores one of all UDFs.
RESTORE SNAPSHOT sr_udf3_backup
FROM test_repo
DATABASE sr_hub
ON (FUNCTION sr_udf1) 
PROPERTIES ("backup_timestamp" = "2024-12-09-10-52-10-940");
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
- Currently, StarRocks does not support backing up and restoring the configuration data related to user accounts, privileges, and resource groups.
- Currently, StarRocks does not support backing up and restoring the Colocate Join relationship among tables.
