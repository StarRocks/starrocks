---
displayed_sidebar: docs
description: "ADMIN SET CONFIG sets FE dynamic configuration items for the StarRocks cluster at runtime."
products: [oss, enterprise, cloud, anywhere]
---

# ADMIN SET CONFIG

ADMIN SET CONFIG is used to set configuration items for the cluster (Currently, only FE dynamic configuration items can be set using this command). You can view these configuration items using the [ADMIN SHOW FRONTEND CONFIG](ADMIN_SHOW_CONFIG.md) command.

The configurations will be restored to the default values in the `fe.conf` file after the FE restarts. Therefore, we recommend that you also modify the configuration items in `fe.conf` to prevent the loss of modifications. From v3.5.0 onwards, you can add `WITH PERSISTENT` to the statement so that the new value is also written into the `fe.conf` file of each FE and is retained after the FE restarts. The statement with `WITH PERSISTENT` fails if `fe.conf` is not writable or the FE runs in a container.

The statement applies the change to the FE that you are connected to first, and then sends it to each of the other FEs. From v3.5.2 onwards, FEs that are offline are skipped, so they keep the old value after the statement succeeds. In earlier versions, an offline FE causes the statement to report an error. If the change fails on some FEs, the statement reports an error that names them, but the change is not rolled back on the FEs where it has already been applied. Connect to each FE that was skipped or failed, check the value with ADMIN SHOW FRONTEND CONFIG, and set it again if needed.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value") [WITH PERSISTENT]
```

## Examples

1. Set `disable_balance` to `true`.

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```
