---
displayed_sidebar: docs
---

# CREATE DATABASE

## Description

このステートメントはデータベースを作成するために使用されます。

## Syntax

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>
```

## Examples

1. データベース `db_test` を作成します。

    ```sql
    CREATE DATABASE db_test;
    ```

## References

- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md) 
- [USE](../data-definition/USE.md) 
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md) 
- [DESC](../Utility/DESCRIBE.md) 
- [DROP DATABASE](../data-definition/DROP_DATABASE.md) 