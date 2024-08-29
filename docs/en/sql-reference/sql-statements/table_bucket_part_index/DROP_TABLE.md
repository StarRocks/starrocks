---
displayed_sidebar: docs
---

# DROP TABLE

## Description

This statement is used to delete a table.

## Syntax

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db_name.]table_name [FORCE]
```

Note:

- After a table is dropped, you can use the [RECOVER](../backup_restore/RECOVER.md) statement to restore the table within a specified period (1 day by default).
- If DROP Table FORCE is executed, the table will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database. Generally, this operation is not recommended.
- Once dropped, temporary tables cannot be restored using RECOVER.

## Examples

1. Drop a table.

    ```sql
    DROP TABLE my_table;
    ```

2. If it exists, then drop the table on the specified database.

    ```sql
    DROP TABLE IF EXISTS example_db.my_table;
    ```

3. Force to drop the table and clear its data on disk.

    ```sql
    DROP TABLE my_table FORCE;
    ```

## References

- [CREATE TABLE](CREATE_TABLE.md)
- [SHOW TABLES](SHOW_TABLES.md)
- [SHOW CREATE TABLE](SHOW_CREATE_TABLE.md)
- [ALTER TABLE](ALTER_TABLE.md)
- [SHOW ALTER TABLE](SHOW_ALTER.md)
