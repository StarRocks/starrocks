# ADMIN SET CONFIG

## Description

This statement is used to set the configuration items for the cluster (Currently it only supports the setting of FE configuration items.). Settable configuration items can be viewed through ADMIN SHOW FRONTEND CONFIG command.

After setting, configuration items will be restored to be configuration or default values on fe.cof once FE reboots. Thus, it is recommended that fe.conf be modified after setting to avoid the loss of previous modification.

Syntax:

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value")
```

## Examples

1. Set 'disable_balance' as true

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```
