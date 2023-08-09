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

- [CREATE DATABASE](../data-definition/CREATE%20DATABASE.md)
- [SHOW CREATE DATABASE](../data-manipulation/SHOW%20CREATE%20DATABASE.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [SHOW DATABASES](../data-manipulation/SHOW%20DATABASES.md)
