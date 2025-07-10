---
displayed_sidebar: docs
---

# SHOW PROCESSLIST

SHOW PROCESSLIST lists the operations currently being performed by threads executing within the server. The current version of StarRocks only supports listing queries.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
SHOW [FULL] PROCESSLIST
```

## Parameter

| Parameter | Required | Description                                                                                                                      |
| --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| FULL      | No       | If you specify this parameter, the complete SQL statement will be displayed. Otherwise, only the first 100 characters of the statement are displayed. |

## Return

| Return              | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| Id                  | Connection ID.                                               |
| User                | The name of the user who runs the operation.                 |
| Host                | The hostname of the client which runs the operation.         |
| Db                  | The name of the database where the operation is executed.    |
| Command             | The type of the command.                                     |
| ConnectionStartTime | Time when the connection starts.                             |
| Time                | The time (in second) since the operation has entered the current state. |
| State               | The state of the operation.                                  |
| Info                | The command that the operation is executing.                 |
| IsPending           | Whether the query is pending in the queue. Valid values: `true` and `false`. |
| Warehouse           | The name of the warehouse where the query is executed.       |
| CNGroup             | The name of the compute node group where the query is executed. |

## Usage note

If the current user is `root`, this statement lists the operations of all users in the cluster. Otherwise, only operations of the current user are listed.

The `IsPending`, `Warehouse`, and `CNGroup` fields provide additional information about query execution in warehouse environments:

- `IsPending`: Shows whether a query is waiting in the queue (`true`) or actively executing (`false`)
- `Warehouse`: Displays the warehouse name where the query is being executed
- `CNGroup`: Shows the compute node group name responsible for executing the query

## Example

Example 1: lists the operations state via the user `root`.

```Plain
SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+-----------+-----------+---------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info             | IsPending  | Warehouse | CNGroup |
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+-----------+-----------+---------+
|  0   | root | x.x.x.x:xxxx        | tpcds | Query   | 2022-10-09 19:58:25 |    0 | OK    | SHOW PROCESSLIST | false      | default   |         |
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+-----------+-----------+---------+
```
