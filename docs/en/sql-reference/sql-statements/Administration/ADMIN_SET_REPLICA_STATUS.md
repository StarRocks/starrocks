---
displayed_sidebar: "English"
---

# ADMIN SET REPLICA STATUS

## Description

This statement is used to set the status of the specified replicas.

This command currently only used to manually set the status of some replicas to BAD or OK, allowing the system to automatically repair these replicas.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET REPLICA STATUS
PROPERTIES ("key" = "value", ...)
```

The following attributes are currently supported:

"table_id': required. Specify a Tablet Id.

"backend_id": required. Specify a Backend Id.

"status": required. Specify the status. Currently only "bad" and "ok" are supported.

If the specified replica does not exist or its status is bad, the replica will be ignored.

Note:

Replicas set to Bad status may be dropped immediately, please proceed with caution.

## Examples

1. Set the replica status of tablet 10003 on BE 10001 to bad.

    ```sql
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");
    ```

2. Set the replica status of tablet 10003 on BE 10001 to ok.

    ```sql
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10003", "backend_id" = "10001", "status" = "ok");
    ```
