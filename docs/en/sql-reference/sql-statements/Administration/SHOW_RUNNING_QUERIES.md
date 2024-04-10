---
displayed_sidebar: "English"
---

# SHOW RUNNING QUERIES

## Description

Shows the information of all queries that are running or pending in the query queue. This feature is supported from v3.1.4 onwards.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SHOW RUNNING QUERIES
```

## Return

- `QueryId`: The ID of the query.
- `ResourceGroupId`: The ID of the resource group that the query hit. When there is no hit on a user-defined resource group, it will be displayed as "-".
- `StartTime`: The start time of the query.
- `PendingTimeout`: The time when the PENDING query will time out in the queue.
- `QueryTimeout`: The time when the query times out.
- `State`: The queue state of the query, where "PENDING" indicates it is in the queue, and "RUNNING" indicates it is currently executing.
- `Slots`: The logical resource quantity requested by the query, currently fixed at `1`.
- `Frontend`: The FE node that initiated the query.
- `FeStartTime`: The start time of the FE node that initiated the query.

## Example

```Plain
MySQL [(none)]> SHOW RUNNING QUERIES;
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| QueryId                              | ResourceGroupId | StartTime           | PendingTimeout      | QueryTimeout        |   State   | Slots | Frontend                        | FeStartTime         |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| a46f68c6-3b49-11ee-8b43-00163e10863a | -               | 2023-08-15 16:56:37 | 2023-08-15 17:01:37 | 2023-08-15 17:01:37 |  RUNNING  | 1     | 127.00.00.01_9010_1692069711535 | 2023-08-15 16:37:03 |
| a6935989-3b49-11ee-935a-00163e13bca3 | 12003           | 2023-08-15 16:56:40 | 2023-08-15 17:01:40 | 2023-08-15 17:01:40 |  RUNNING  | 1     | 127.00.00.02_9010_1692069658426 | 2023-08-15 16:37:03 |
| a7b5e137-3b49-11ee-8b43-00163e10863a | 12003           | 2023-08-15 16:56:42 | 2023-08-15 17:01:42 | 2023-08-15 17:01:42 |  PENDING  | 1     | 127.00.00.03_9010_1692069711535 | 2023-08-15 16:37:03 |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
```
