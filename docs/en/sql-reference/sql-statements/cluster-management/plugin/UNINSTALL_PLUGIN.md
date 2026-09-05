---
displayed_sidebar: docs
description: "UNINSTALL PLUGIN is used to uninstall a plugin."
---

# UNINSTALL PLUGIN

UNINSTALL PLUGIN is used to uninstall a plugin.

:::tip

This operation requires the SYSTEM-level PLUGIN privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```SQL
UNINSTALL PLUGIN [IF EXISTS] <plugin_name>
```

**IF EXISTS**: If specified, the statement succeeds silently when the plugin does not exist instead of returning an error.

plugin_name can be viewed through SHOW PLUGINS command

Only non-builtin plugins can be uninstalled.

## Examples

1. Uninstall a plugin:

    ```SQL
    UNINSTALL PLUGIN auditdemo;
    ```

2. Uninstall a plugin if it exists:

    ```SQL
    UNINSTALL PLUGIN IF EXISTS auditdemo;
    ```
