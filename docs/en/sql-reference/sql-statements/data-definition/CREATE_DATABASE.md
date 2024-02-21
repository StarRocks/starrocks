---
displayed_sidebar: "English"
---

# CREATE DATABASE

## Description

This statement is used to create databases.

:::tip

This operation requires the CREATE DATABASE privilege on the target catalog. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>
```

`db_name`: the name of the database to create. For the naming conventions, see [System limits](../../../reference/System_limit.md).

## Examples

1. Create database `db_test`.

    ```sql
    CREATE DATABASE db_test;
    ```

## References

- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md)
- [DESC](../Utility/DESCRIBE.md)
- [DROP DATABASE](../data-definition/DROP_DATABASE.md)
