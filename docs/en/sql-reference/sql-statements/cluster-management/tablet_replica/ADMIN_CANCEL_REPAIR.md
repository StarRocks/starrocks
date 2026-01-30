---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

Cancels the prioritized schedule of repairing operations on specified tables or partitions. This statement only indicates that the system will no longer repair sharding replicas of specified tables or partitions with high priority. It still repairs these copies by default scheduling.

ADMIN CANCEL REPAIR applies only to native tables in shared-nothing clusters.

For detailed instructions, see [Manage Replica](../../../../administration/management/resource_management/Replica.md).

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

## Examples

1. Cancel the high-priority repairing schedule for the partition `p1` in the native table `tbl1`.

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
