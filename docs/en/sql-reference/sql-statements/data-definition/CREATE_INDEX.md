---
displayed_sidebar: "English"
---

# CREATE INDEX

## Description

This statement is used to create indexes. You can use this statement to create oly Bitmap indexes. For usage notes and scenarios of Bitmap indexes, see [Bitmap index](../../../table_design/indexes/Bitmap_index.md).

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT'balabala']
```

Note:

1. For the naming conventions of indexes, see [System limits](../../../reference/System_limit.md).
2. One column can have only one BITMAP index. If a column already has an index, you cannot create one more index on it.

## Examples

1. Create bitmap index for `siteid` on `table1`.

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```
