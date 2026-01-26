---
displayed_sidebar: docs
---

# ADMIN REPAIR

ADMIN REPAIR is used to attempt to repair the specified table or partitions.

For shared-nothing tables, it attempts to prioritize scheduling replica repair.

For shared-data tables, it attempts to rollback to a historical available version when metadata or data files are lost. (Note: This may result in the loss of the latest data for some tablets.)

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
3. Supports setting repair behavior via `PROPERTIES`, currently only supported for shared-data tables.

**PROPERTIES**

`enforce_consistent_version`: Whether to enforce all tablets in a partition to rollback to a consistent version. Defaults to `true`. If set to `true`, the system will search for a unified version in the history that is valid for all tablets to perform the rollback, ensuring data version alignment across the partition. If set to `false`, each tablet in the partition is allowed to rollback to its latest available valid version. The versions of different tablets may be inconsistent, but this maximizes data preservation.
`allow_empty_tablet_recovery`: Whether to allow recovery by creating empty tablets. Defaults to `false`. Only effective when `enforce_consistent_version` is `false`. When metadata is missing for all versions of some tablets but valid metadata exists for at least one tablet, if this parameter is `true`, the system attempts to create empty tablets to fill the missing versions. If metadata for all versions of all tablets is lost, recovery is impossible.

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
