---
displayed_sidebar: docs
description: "INSTALL PLUGIN is used to install a plugin."
---

# INSTALL PLUGIN

INSTALL PLUGIN is used to install a plugin.

:::tip

This operation requires the SYSTEM-level PLUGIN privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
INSTALL PLUGIN [IF NOT EXISTS] FROM [source] [PROPERTIES ("key"="value", ...)]
```

**IF NOT EXISTS**: If specified, the statement succeeds silently when the plugin is already installed instead of returning an error.

3 types of sources are supported:

```plain text
1. An absolute path that directs to a zip file
2. An absolute path that directs to a plugin directory 
3. A http or https download link that directs to a zip file
```

PROPERTIES supports setting some configurations of plugins, such as setting the  md5sum value of the zip file, etc.

## Examples

1. Install a plugin from local zip file:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo.zip";
    ```

2. Install a plugin from local inpath:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo/";
    ```

3. Download and install a plugin:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
    ```

4. Download and install a plugin. Meanwhile, set the md5sum value of the zip file:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570");
    ```

5. Install a plugin if it is not already installed:

    ```sql
    INSTALL PLUGIN IF NOT EXISTS FROM "/home/users/starrocks/auditdemo.zip";
    ```
