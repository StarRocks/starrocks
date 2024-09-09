---
displayed_sidebar: docs
---

# SHOW INDEX

## Description

This statement is used to show information related to index in a table. It currently only supports bitmap index.

:::tip

This operation does not require privileges.

:::

## Syntax

```sql
SHOW INDEX[ES] FROM [db_name.]table_name [FROM database]
Or
SHOW KEY[S] FROM [db_name.]table_name [FROM database]
```

## Examples

1. Show all indexes under the specified table_name:

    ```sql
    SHOW INDEX FROM example_db.table_name;
    ```
