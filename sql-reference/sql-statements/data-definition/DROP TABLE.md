# DROP TABLE

## description

This statement is used to delete a table.

Syntax:

```sql
DROP TABLE [IF EXISTS] [FORCE] [db_name.]table_name ;
```

Note:

1. After executing DROP TABLE for a while, you can use RECOVER statement to restore the dropped table. See RECOVER statement for more detail.
2. If DROP Table FORCE is executed, the table will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database.  Generally this operation is not recommended.

## example

1. Drop a table.

    ```sql
    DROP TABLE my_table;
    ```

2. If it exists, then drop the table on the specified database.

    ```sql
    DROP TABLE IF EXISTS example_db.my_table;
    ```

## keyword

DROP,TABLE
