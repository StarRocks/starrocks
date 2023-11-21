---
displayed_sidebar: "English"
---

# SHOW DELETE

## Description

This statement is used to show historical DELETE tasks that are successfully performed on Duplicate Key tables in the current database. For more information about data deletion, see [DELETE](DELETE.md).

## Syntax

```sql
SHOW DELETE [FROM <db_name>]
```

`db_name`: the database name, optional. If this parameter is not specified, the current database is used by default.

Return fields:

- TableName: the table from which data is deleted.
- PartitionName: the partition from which data is deleted. If the table is a non-partitioned table, `*` is displayed.
- CreateTime: the time when the DELETE task was created.
- DeleteCondition: the specified DELETE condition.
- State: the status of the DELETE task.

## Examples

Show all historical DELETE tasks of `database`.

```sql
SHOW DELETE FROM database;

+------------+---------------+---------------------+-----------------+----------+
| TableName  | PartitionName | CreateTime          | DeleteCondition | State    |
+------------+---------------+---------------------+-----------------+----------+
| mail_merge | *             | 2023-03-14 10:39:03 | name EQ "Peter" | FINISHED |
+------------+---------------+---------------------+-----------------+----------+
```
