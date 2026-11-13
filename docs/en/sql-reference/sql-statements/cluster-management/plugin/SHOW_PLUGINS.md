---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "SHOW PLUGINS is used to view the installed plugins."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# SHOW PLUGINS

SHOW PLUGINS is used to view the installed plugins.

:::tip

This operation requires the SYSTEM-level PLUGIN privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
SHOW PLUGINS
```

This command will display all built-in plugins and custom plugins.

## Examples

1. Show the installed plugins:

    ```sql
    SHOW PLUGINS;
    ```
