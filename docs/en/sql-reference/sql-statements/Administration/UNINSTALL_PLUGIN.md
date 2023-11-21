---
displayed_sidebar: "English"
---

# UNINSTALL PLUGIN

## Description

This statement is used to uninstall a plugin.

Syntax:

```SQL
UNINSTALL PLUGIN <plugin_name>
```

plugin_name can be viewed through SHOW PLUGINS command

Only non-builtin plugins can be uninstalled.

## Examples

1. Uninstall a plugin:

    ```SQL
    UNINSTALL PLUGIN auditdemo;
    ```
