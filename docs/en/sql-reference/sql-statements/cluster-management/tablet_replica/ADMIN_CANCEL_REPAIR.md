---
displayed_sidebar: docs
---

# ADMIN CANCEL REPAIR

<<<<<<< HEAD
ADMIN CANCEL REPAIR is used to cancel repairing specified tables or partitions with high priority.
=======
Cancels the prioritized schedule of repairing operations on specified tables or partitions. This statement only indicates that the system will no longer repair sharding replicas of specified tables or partitions with high priority. It still repairs these copies by default scheduling.

ADMIN CANCEL REPAIR applies only to native tables in shared-nothing clusters.
>>>>>>> 9d6586f1d4 ([Doc] Remove Problematic Links (#69829))

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)]
```

Note
>
> This statement only indicates that the system will no longer repair sharding replicas of specified tables or partitions with high priority. It still repairs these copies by default scheduling.

## Examples

1. Cancel high priority repair

    ```sql
    ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);
    ```
