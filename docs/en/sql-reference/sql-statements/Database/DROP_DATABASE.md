---
displayed_sidebar: docs
---

# DROP DATABASE

## Description

Drops a database in StarRocks.

> **NOTE**
>
> This operation requires the DROP privilege on the destination database.

## Syntax

```sql
DROP DATABASE [IF EXISTS] <db_name> [FORCE]
```

Note:

<<<<<<< HEAD:docs/en/sql-reference/sql-statements/data-definition/DROP_DATABASE.md
1. After executing DROP DATABASE for a while, you can restore the dropped database through RECOVER statement. See RECOVER statement for more detail.
2. If DROP DATABASE FORCE is executed, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database.  Generally this operation is not recommended.
=======
- After executing DROP DATABASE to drop a database, you can restore the dropped database by using the [RECOVER](../backup_restore/RECOVER.md) statement within a specified retention period (the default retention period spans one day), but the pipes (supported from v3.2 onwards) that have been dropped along with the database cannot be recovered.
- If you execute `DROP DATABASE FORCE` to drop a database, the database is deleted directly without any checks on whether there are unfinished activities in it and cannot be recovered. Generally, this operation is not recommended.
- If you drop a database, all pipes (supported from v3.2 onwards) that belong to the database are dropped along with the database.
>>>>>>> e06217c368 ([Doc] Ref docs (#50111)):docs/en/sql-reference/sql-statements/Database/DROP_DATABASE.md

## Examples

1. Drop database db_text.

    ```sql
    DROP DATABASE db_test;
    ```

## References

- [CREATE DATABASE](CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
