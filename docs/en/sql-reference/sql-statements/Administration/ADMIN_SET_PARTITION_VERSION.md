---
displayed_sidebar: "English"
---

# ADMIN SET PARTITION VERSION

## Description

Sets a partition to a specific data version.

Note that manually setting the partition version is a critical operation and is recommended only when problems occur in the cluster metadata. In normal circumstances, the version of a partition is automatically set consistent with that of the tablet within.

:::tip

This operation requires the SYSTEM-level OPERATE privilege. You can follow the instructions in [GRANT](../account-management/GRANT.md) to grant this privilege.

:::

## Syntax

```sql
ADMIN SET TABLE <table_name> PARTITION ( <partition_name> | <partition_id> ) 
TO VERSION <version>
```

## Parameters

- `table_name`: The name of the table to which the partition belongs.
- `partition_name`: The name of the partition. You need to specify a partition using either `partition_name` or `partition_id`. For non-partitioned tables, `partition_name` is the same as the table name.
- `partition_id`: The ID of the partition. You need to specify a partition using either `partition_name` or `partition_id`. For tables with the random bucketing strategy, you can only use `partition_id` to specify the partition.
- `version`: The version of the partition to set.

## Examples

1. Set the version of the non-partitioned table `t1` to `10`.

    ```sql
    ADMIN SET TABLE t1 PARTITION(t1) TO VERSION 10;
    ```

2. Set the version of the partition `p1` from the table `t2` to `10`.

    ```sql
    ADMIN SET TABLE t2 PARTITION(p1) TO VERSION 10;
    ```

3. Set the version of the partition whose ID is `123456` to `10`. `t3` is a table with the random bucketing strategy.

    ```sql
    ADMIN SET TABLE t3 PARTITION('123456') TO VERSION 10;
    ```
