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

1. After executing DROP DATABASE for a while, you can restore the dropped database through RECOVER statement. See RECOVER statement for more detail.
2. If DROP DATABASE FORCE is executed, the database will be deleted directly and cannot be recovered without checking whether there are unfinished activities in the database.  Generally this operation is not recommended.

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
