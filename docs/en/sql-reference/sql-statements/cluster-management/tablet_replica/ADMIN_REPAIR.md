---
displayed_sidebar: docs
---

# ADMIN REPAIR

Attempts to repair the specified table or partitions.

For native tables in shared-nothing clusters, this statement attempts to prioritize scheduling the replica repairing operation.

For cloud-native tables in shared-data clusters, it attempts to rollback to a historical available version when metadata or data files are lost. Please note that **this may result in the loss of the latest data for some tablets**.

For detailed instructions, see [Manage Replica](../../../../administration/management/resource_management/Replica.md).

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN REPAIR TABLE table_name [PARTITION (p1,...)] [PROPERTIES ("key" = "value", ...)]
```

Note:

1. This statement only means that the system attempts to repair sharding replicas of specified tables or partitions with high priority without the guarantee that the repair will be successful. Users can view the repair status through ADMIN SHOW REPLICA STATUS command.
2. The default timeout is 14400 seconds (4 hours). Timeout means the system will not repair sharding replicas of specified tables or partitions with high priority. In case of timeout, the command needs to be used again for the intended settings.
3. You can set repair behavior by specifying `PROPERTIES` in the statement. **Currently, only cloud-native tables in shared-data clusters support `PROPERTIES`**.

**PROPERTIES**

- `enforce_consistent_version`: Whether to enforce all tablets in a partition to roll back to a consistent version. Default: `true`. If this item is set to `true`, the system will search for a consistent version in the history that is valid for all tablets to perform the rollback, ensuring data version alignment across the partition. If it is set to `false`, each tablet in the partition is allowed to rollback to its latest available valid version. The versions of different tablets may be inconsistent, but this maximizes data preservation.
- `allow_empty_tablet_recovery`: Whether to allow recovery by creating empty tablets. Default: `false`. This item takes effect only when `enforce_consistent_version` is `false`. If this item is set to `true`, when metadata is missing for all versions of some tablets but valid metadata exists for at least one tablet, the system attempts to create empty tablets to fill the missing versions. If metadata for all versions of all tablets is lost, recovery is impossible.

## Examples

1. Attempt to repair specified tables

    ```sql
    ADMIN REPAIR TABLE tbl1;
    ```

2. Attempt to fix specified partition

    ```sql
    ADMIN REPAIR TABLE tbl1 PARTITION (p1, p2);
    ```

3. Attempt to repair a shared-data table, allowing inconsistent versions and creating empty tablets for recovery

    ```sql
    ADMIN REPAIR TABLE cloud_tbl PROPERTIES (
        "enforce_consistent_version" = "false",
        "allow_empty_tablet_recovery" = "true"
    );
    ```
