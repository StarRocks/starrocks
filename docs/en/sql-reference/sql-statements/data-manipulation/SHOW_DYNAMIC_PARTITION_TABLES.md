---
displayed_sidebar: "English"
---

# SHOW DYNAMIC PARTITION TABLES

## Description

This statement is used to display the status of all the partitioned tables for which dynamic partitioning properties are configured in a database.

## Syntax

```sql
SHOW DYNAMIC PARTITION TABLES FROM <db_name>
```

This statement returns the following fields:

- TableName: the name of the table.
- Enable: whether dynamic partitioning is enabled.
- TimeUnit: the time granularity for the partitions.
- Start: the starting offset of dynamic partitioning.
- End: the end offset of dynamic partitioning.
- Prefix: the prefix of the partition name.
- Buckets: the number of buckets per partition.
- ReplicationNum: the number of replicas for the table.
- StartOf
- LastUpdateTime: the time when the table was last updated.
- LastSchedulerTime: the time when data in the table was last scheduled.
- State: the status of the table.
- LastCreatePartitionMsg: the message for the latest partition creation operation.
- LastDropPartitionMsg: the message for the latest partition dropping operation.

## Examples

Display the status of all the partitioned tables for which dynamic partitioning properties are configured in `db_test`.

```sql
SHOW DYNAMIC PARTITION TABLES FROM db_test;
```
