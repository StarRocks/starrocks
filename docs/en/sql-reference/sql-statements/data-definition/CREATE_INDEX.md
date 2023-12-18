---
displayed_sidebar: "English"
---

# CREATE INDEX

## Description

This statement is used to create indexes.

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala']
```

Note:

1. Only support bitmap index in the current version.
2. Create BITMAP index only in a single column.

## Examples

1. Create bitmap index for `siteid` on `table1`.

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```
