---
displayed_sidebar: docs
---

# ADMIN SET PARTITION VERSION

## Description

Sets a partition to a specific data version.

Note that manually setting the partition version is a high-risk operation and is recommended only when problems occur in the cluster metadata. In normal circumstances, the version of a partition is consistent with that of the tablets within.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET TABLE <table_name> PARTITION ( <partition_name> | <partition_id> ) 
VERSION TO <version>
```

## Parameters

- `table_name`: The name of the table to which the partition belongs.
- `partition_name`: The name of the partition. You need to specify a partition using either `partition_name` or `partition_id`. For non-partitioned tables, `partition_name` is the same as the table name.
- `partition_id`: The ID of the partition. You need to specify a partition using either `partition_name` or `partition_id`. For tables with the random bucketing strategy, you can only use `partition_id` to specify the partition.
- `version`: The version you want to set for the partition.

## Examples

1. Set the version of the non-partitioned table `t1` to `10`.

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) VERSION TO 10;
    ```

2. Set the version of partition `p1` in table `t2` to `10`.

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) VERSION TO 10;
    ```

3. Set the version of the partition whose ID is `123456` to `10`. `t3` is a table with the random bucketing strategy.

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') VERSION TO 10;
    ```
