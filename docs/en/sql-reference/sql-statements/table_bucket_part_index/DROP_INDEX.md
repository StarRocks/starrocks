---
displayed_sidebar: docs
---

# DROP INDEX

## Description

This statement is used to drop a specified index on a table. Currently, only bitmap index is supported in this version.

:::tip

This operation requires the ALTER privilege on the target table. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

Syntax:

```sql
DROP INDEX index_name ON [db_name.]table_name
```
