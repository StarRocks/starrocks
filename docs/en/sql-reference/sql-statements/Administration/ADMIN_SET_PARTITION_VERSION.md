---
displayed_sidebar: "English"
---

# ADMIN SET PARTITION VERSION

## Description

Manually setting the partition version is a dangerous operation and is only used when there is a problem with the cluster metadata. Under normal circumstances, the partition version will automatically be consistent with the tablet version.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET TABLE table_name PARTITION (partition_name | partition_id) TO VERSION xxx;
```

Note:

1. For non-partitioned tables, partition_name is the same as the table name.
2. For random distribution tables, partition_id needs to be used to specify the partition.

## Examples

1. Set the version of table t1 to 10. t1 is a non-partitioned table.

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) TO VERSION 10;
    ```

2. Set the p1 partition version of partitioned table t2 to 10

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) TO VERSION 10;
    ```

3. Set the partition whose id is 123456 version to 10. t3 is a random distribution table.

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') TO VERSION 10;
    ```
