# ADMIN SET CONFIG

## Description

This statement is used to set configuration items for the cluster (Currently, only FE configuration items can be set using this command). You can view these configuration items using the ADMIN SHOW FRONTEND CONFIG command.

The configurations will be restored to the default values in `fe.cof` after the FE reboots. Therefore, we recommended that you also modify the configuration items in `fe.conf` to prevent the loss of modifications.

## Syntax

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value")
```

## Examples

1. Set 'disable_balance' to true.

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```
