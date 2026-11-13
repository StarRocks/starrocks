---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW INDEX is used to show information related to index in a table."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW INDEX

SHOW INDEX is used to show information related to index in a table. It currently only supports bitmap index.

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
