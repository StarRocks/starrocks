---
displayed_sidebar: docs
---

# BACKUP

StarRocks supports backing up and restoring the following objects:

- Internal databases, tables (of all types and partitioning strategies), and partitions
- Metadata of external catalogs (supported from v3.4.0 onwards)
- Synchronous materialized views and asynchronous materialized views
- Logical views (supported from v3.4.0 onwards)
- User-defined functions (supported from v3.4.0 onwards)

:::tip
For an overview of backup and restore please see the [backup and restore guide](../../../administration/management/Backup_and_restore.md).
:::

BACKUP is an asynchronous operation. You can check the status of a BACKUP job status using [SHOW BACKUP](./SHOW_BACKUP.md), or cancel a BACKUP job using [CANCEL BACKUP](./CANCEL_BACKUP.md). You can view the snapshot information using [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md).

> **CAUTION**
>
> - Shared-data StarRocks clusters do not support data BACKUP and RESTORE.
> - In each database, only one running BACKUP or RESTORE job is allowed each time. Otherwise, StarRocks returns an error.
> - StarRocks does not support specifying data compression algorithm for data backup.

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
BACKUP SNAPSHOT <db_name>.<snapshot_name>
TO <repository_name>
[ ON ( <table_name> [ PARTITION ( <partition_name> [, ...] ) ]
       [, ...] ) ]
[ PROPERTIES ("key"="value" [, ...] ) ]
```

### Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| db_name         | Name of the database that stores the data to be backed up.   |
| snapshot_name   | Specify a name for the data snapshot. Globally unique.       |
| repository_name | Repository name. You can create a repository using [CREATE REPOSITORY](./CREATE_REPOSITORY.md). |
| ON              | Name of the tables to be backed up. The whole database is backed up if this parameter is not specified. |
| PARTITION       | Name of the partitions to be backed up. The whole table is backed up if this parameter is not specified. |
| PROPERTIES      | Properties of the data snapshot. Valid keys:<ul><li>`type`: Backup type. Currently, only full backup `FULL` is supported. Default: `FULL`.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Syntax supported from v3.4.0 onwards

```SQL
-- Back up external catalog metadata.
BACKUP { ALL EXTERNAL CATALOGS | EXTERNAL CATALOG[S] (<catalog_name> [, ...]) }
[ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ PROPERTIES ("key"="value" [, ...] ) ]

-- Back up databases, tables, partitions, materialized views, logical views, or UDFs.

BACKUP [ DATABASE <db_name> ] SNAPSHOT [<db_name>.]<snapshot_name>
TO <repository_name>
[ ON ( backup_object [, ...] )] 
[ PROPERTIES ("key"="value" [, ...] ) ]

backup_object ::= [
    { ALL TABLE[S]             | TABLE[S] <table_name>[, TABLE[S] <table_name> ...] } |
    { ALL MATERIALIZED VIEW[S] | MATERIALIZED VIEW[S] <mv_name>[, MATERIALIZED VIEW[S] <mv_name> ...] } |
    { ALL VIEW[S]              | VIEW[S] <view_name>[, VIEW[S] <view_name> ...] } |
    { ALL FUNCTION[S]          | FUNCTION[S] <udf_name>[, FUNCTION[S] <udf_name> ...] } |
     <table_name> PARTITION (<partition_name>[, ...]) ]
```

### Parameters

| **Parameter**   | **Description**                                              |
| --------------- | ------------------------------------------------------------ |
| ALL EXTERNAL CATALOGS | Backs up the metadata of all external catalogs.        |
| catalog_name    | Name of the external catalog(s) that need to be backed up.   |
| DATABASE db_name | Name of the database to which the object(s) or snapshot belongs. You can only specify either `DATABASE <db_name>` or `<db_name>.`. |
| db_name.        | Name of the database to which the object(s) or snapshot belongs. You can only specify either `DATABASE <db_name>` or `<db_name>.`. |
| snapshot_name   | Name of the data snapshot. Globally unique.                  |
| repository_name | Repository name. You can create a repository using [CREATE REPOSITORY](./CREATE_REPOSITORY.md). |
| ON              | The object to be backed up. The whole database is backed up if this parameter is not specified. |
| table_name      | Name of the table(s) to be backed up.                        |
| mv_name         | Name of the materialized view(s) to be backed up.            |
| view_name       | Name of the logical view(s) to be backed up.                 |
| udf_name        | Name of the UDF(s) to be backed up.                          |
| PARTITION       | Name of the partitions to be backed up. The whole table is backed up if this parameter is not specified. |
| PROPERTIES      | Properties of the data snapshot. Valid keys:<ul><li>`type`: Backup type. Currently, only full backup `FULL` is supported. Default: `FULL`.</li><li>`timeout`: Task timeout. Unit: second. Default: `86400`.</li></ul> |

## Examples

### Examples with syntax compatible with earlier versions

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

### Examples with syntax supported from v3.4.0 onwards

Example 1: Backs up a database.

```SQL
BACKUP DATABASE sr_hub SNAPSHOT sr_hub_backup TO test_repo;
```

Example 2: Backs up the table(s) in the database.

```SQL
-- Backs up one table.
BACKUP DATABASE sr_hub SNAPSHOT sr_member_backup
TO test_repo
ON (TABLE sr_member);

-- Backs up multiple tables.
BACKUP DATABASE sr_hub SNAPSHOT sr_core_backup
TO test_repo
ON (TABLE sr_member, TABLE sr_pmc);

-- Backs up all tables.
BACKUP DATABASE sr_hub SNAPSHOT sr_all_backup
TO test_repo
ON (ALL TABLES);
```

Example 3: Backs up the partition(s) in a table.

```SQL
-- Backs up one partition.
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1));

-- Backs up multiple partitions.
BACKUP DATABASE sr_hub SNAPSHOT sr_par_backup
TO test_repo
ON (TABLE sr_member PARTITION (p1,p2,p3));
```

Example 4: Backs up the materialized view(s) in the database.

```SQL
-- Backs up one materialized view.
BACKUP DATABASE sr_hub SNAPSHOT sr_mv1_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1);

-- Backs up multiple materialized views.
BACKUP DATABASE sr_hub SNAPSHOT sr_mv2_backup
TO test_repo
ON (MATERIALIZED VIEW sr_mv1, MATERIALIZED VIEW sr_mv2);

-- Backs up all materialized views.
BACKUP DATABASE sr_hub SNAPSHOT sr_mv3_backup
TO test_repo
ON (ALL MATERIALIZED VIEWS);
```

Example 5: Backs up the logical view(s) in the database.

```SQL
-- Backs up one logical view.
BACKUP DATABASE sr_hub SNAPSHOT sr_view1_backup
TO test_repo
ON (VIEW sr_view1);

-- Backs up multiple logical views.
BACKUP DATABASE sr_hub SNAPSHOT sr_view2_backup
TO test_repo
ON (VIEW sr_view1, VIEW sr_view2);

-- Backs up all logical views.
BACKUP DATABASE sr_hub SNAPSHOT sr_view3_backup
TO test_repo
ON (ALL VIEWS);
```

Example 6: Backs up the UDF(s) in the database.

```SQL
-- Backs up one UDF.
BACKUP DATABASE sr_hub SNAPSHOT sr_udf1_backup
TO test_repo
ON (FUNCTION sr_udf1);

-- Backs up multiple UDFs.
BACKUP DATABASE sr_hub SNAPSHOT sr_udf2_backup
TO test_repo
ON (FUNCTION sr_udf1, FUNCTION sr_udf2);

-- Backs up all UDFs.
BACKUP DATABASE sr_hub SNAPSHOT sr_udf3_backup
TO test_repo
ON (ALL FUNCTIONS);
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
