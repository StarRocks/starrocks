# RECOVER

## Description

This statement is used to recover deleted database, table or partition.

Syntax:

1. Recover database

    ```sql
    RECOVER DATABASE <db_name>
    ```

2. Recover database

    ```sql
    RECOVER TABLE [<db_name>.]<table_name>
    ```

3. Recover partition

    ```sql
    RECOVER PARTITION partition_name FROM [<db_name>.]<table_name>
    ```

Note:

1. It can only recover meta-information deleted some time ago. The default time: one day. (You can change it through parameter configuration catalog_trash_expire_second in fe.conf. )
2. If the meta-information is deleted with an identical meta-information created, the previous one will not be recovered.

## Examples

1. Recover database named example_db

    ```sql
    RECOVER DATABASE example_db;
    ```

2. Recover table name example_tbl

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. Recover partition named p1 in the example_tbl table

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```
