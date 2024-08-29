---
displayed_sidebar: docs
---

# CREATE INDEX

## Description

This statement is used to create indexes.

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
CREATE INDEX index_name ON table_name (column [, ...],) [USING BITMAP] [COMMENT 'balabala']
```

Note:

<<<<<<< HEAD:docs/en/sql-reference/sql-statements/data-definition/CREATE_INDEX.md
1. Only support bitmap index in the current version.
2. Create BITMAP index only in a single column.
=======
1. For the naming conventions of indexes, see [System limits](../../System_limit.md).
2. One column can have only one BITMAP index. If a column already has an index, you cannot create one more index on it.
>>>>>>> e06217c368 ([Doc] Ref docs (#50111)):docs/en/sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md

## Examples

1. Create bitmap index for `siteid` on `table1`.

    ```sql
    CREATE INDEX index_name ON table1 (siteid) USING BITMAP COMMENT 'balabala';
    ```
