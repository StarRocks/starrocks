# BACKUP

## description

This statement is used to backup data under the specified database. This command is an asynchronous operation. After successful submission, you can check progress through the SHOW BACKUP command. Only tables of OLAP type are backed up.

Syntax:

```sql
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)],
...
)
PROPERTIES ("key"="value", ...);
```

Note:

1. Only one ongoing BACKUP or RESTORE task under the same database.

2. The ON clause identifies the tables and partitions that need to be backed up. If no partition is specified, all partitions of the table are backed up by default.

3. PROPERTIES currently supports the following attributes:

  "type" = "full": means that this is a full update (default).

  "timeout" = "3600": Task timeout: one day by default. Unit: second.

## example

1. Conduct full backup of the table example_tbl under example_db to the repository example_repo:

    ```sql
    BACKUP SNAPSHOT example_db.snapshot_label1
    TO example_repo
    ON (example_tbl)
    PROPERTIES ("type" = "full");
    ```

2. Conduct full backup of the P1 and P2 partitions of table example_tbl, and table example_tbl2 under example_db, to repository example_repo:

    ```sql
    BACKUP SNAPSHOT example_db.snapshot_label2
    TO example_repo
    ON
    (
    example_tbl PARTITION (p1,p2),
    example_tbl2
    );
    ```
