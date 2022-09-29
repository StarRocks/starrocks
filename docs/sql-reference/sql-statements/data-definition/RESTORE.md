# RESTORE

## description

1. RESTORE

  RESTORE statement is used to restore data previously backed up through BACKUP command to specified database. This command is asynchronous. After running the statement, users can check the progress through SHOW RESTORE command. It only supports the restoration of table with OLAP type.

Syntax:

```sql
RESTORE SNAPSHOT [db_name].{snapshot_name}
FROM `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)] [AS `tbl_alias`],
...
)
PROPERTIES ("key"="value", ...);
```

Note:

- Only one BACKUP or RESTORE task can be performed under the same database.
- Tables and partitions needed to be restored should be identified in ON clause. If no partition is specified, all partitions of the table will be restored by default. The specified tables and partitions must already exist in the backup warehouse.
- The backup tables in the warehouse could be restored to new tables through AS statement with the precondition that the new table name does not exist in the database before restoration. Partition names cannot be changed.
- Backup tables in the warehouse can be restored to replace tables with identical names in the database. But the table structures of the two must be consistent. Structures included: table name, column, partition, Rollup and etc.
- Part of the partitions in a table can be specified to be restored. The system will verify whether the Range of partitions is matched.
- PROPERTIES currently support the following attributes:
  - "backup_timestamp" = "2018-05-04-16-45-08": It specifies which backed-up version should be restored. It must be filled in. This information could be obtained through "SHOW SNAPSHOT ON repo;" statement.  
  - "replication_num" = "3": It specifies the number of table or partition replicas to be restored. Default number: 3. To restore existing tables or partitions, please make sure the number of replicas to be restored is the same as that of existing replicas.  Meanwhile, there must be enough hosts to accommodate multiple replicas.
  - "timeout" = "3600": Task timeout. Default time: one day. Unit: second.
  - "meta_version": Use specified meta_version to read the metadata before backup. Please note that this parameter is just a temporary solution and is only used to restore data backed up from the previous version of StarRocks. Backup data from the latest version already includes meta version and does not need to be specified.

## example

1. Restore table backup_tbl in backup snapshot_1 from example_repo to database example_ab1 with the time version of "2018-05-04-16-45-08". Restore it as 1 replica.

    ```sql
    RESTORE SNAPSHOT example_db1.`snapshot_1`
    FROM `example_repo`
    ON ( `backup_tbl` )
    PROPERTIES
    (
        "backup_timestamp"="2018-05-04-16-45-08",
        "replication_num" = "1"
    );
    ````

2. Restore partitions p1, p2 of table backup_tbl in backup snapshot_2 from example_repo as well as table backup_tbl2 to database example_db1. Rename backup_tbl2 as new_tbl. Time version: "2018-05-04-17-11-01". Restore to 3 replicas by default.  

    ```sql
    RESTORE SNAPSHOT example_db1.`snapshot_2`
    FROM `example_repo`
    ON
    (
    `backup_tbl` PARTITION (`p1`, `p2`),
    `backup_tbl2` AS `new_tbl`
    )
    PROPERTIES
    (
        "backup_timestamp"="2018-05-04-17-11-01"
    );
    ```
