---
displayed_sidebar: docs
---

# RECOVER

Recovers a database, table, or partition that was dropped by using the DROP command. The dropped database, table, or partition can be recovered within the period specified by the FE parameter `catalog_trash_expire_second` (1 day by default).

Data deleted by using [TRUNCATE TABLE](../table_bucket_part_index/TRUNCATE_TABLE.md) cannot be recovered.

## Syntax

1. Recover a database.

    ```sql
    RECOVER DATABASE <db_name>
    ```

2. Recover a table.

    ```sql
    RECOVER TABLE [<db_name>.]<table_name>
    ```

3. Recover a partition.

    ```sql
    RECOVER PARTITION <partition_name> FROM [<db_name>.]<table_name>
    ```

Note:

1. This command can only recover metadata deleted some time ago (1 day by default). You can change the duration by adjusting the FE parameter `catalog_trash_expire_second`.
2. If the metadata is deleted with identical metadata created, the previous one will not be recovered.

## Examples

1. Recover database `example_db`.

    ```sql
    RECOVER DATABASE example_db;
    ```

2. Recover table `example_tbl`.

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. Recover partition `p1` in the `example_tbl` table.

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```
