---
displayed_sidebar: "English"
---

# ADMIN SET CONFIG

## Description

This statement is used to set configuration items for the cluster (Currently, only FE dynamic configuration items can be set using this command). You can view these configuration items using the [ADMIN SHOW FRONTEND CONFIG](ADMIN_SET_CONFIG.md) command.

The configurations will be restored to the default values in the `fe.conf` file after the FE restarts. Therefore, we recommend that you also modify the configuration items in `fe.conf` to prevent the loss of modifications.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value")
```

## Examples

1. Set `disable_balance` to `true`.

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```
