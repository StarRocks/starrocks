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
| Server              | Server ID.                                                   |
| Id                  | Connection ID.                                               |
| User                | The name of the user who runs the operation.                 |
| Host                | The hostname of the client which runs the operation.         |
| Db                  | The name of the database where the operation is executed.    |
| Command             | The type of the command.                                     |
| ConnectionStartTime | Time when the connection starts.                             |
| Time                | The time (in second) since the operation has entered the current state. |
| State               | The state of the operation.                                  |
| Info                | The command that the operation is executing.                 |
<<<<<<< HEAD
=======
| IsPending           | Whether the query is pending in the queue. Valid values: `true` and `false`. |
| Warehouse           | The name of the warehouse where the query is executed.       |
| CNGroup             | The name of the compute node group where the query is run.   |
| Catalog             | The name of the catalog.                                     |
| QueryId             | The qury ID if the Command is "Query".                       |
>>>>>>> 49c0466fd0 ([Doc] add server column (#75672))

## Usage note

If the current user is `root`, this statement lists the operations of all users in the cluster. Otherwise, only operations of the current user are listed.

## Example

Example 1: lists the operations state via the user `root`.

<<<<<<< HEAD
```Plain
SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info             |
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+
|  0   | root | x.x.x.x:xxxx        | tpcds | Query   | 2022-10-09 19:58:25 |    0 | OK    | SHOW PROCESSLIST |
+------+------+---------------------+-------+---------+---------------------+------+-------+------------------+
=======
```SQL
SHOW PROCESSLIST\G

*************************** 6. row ***************************
         ServerName: starrocks-fe_9010_1782850099498
                 Id: 33560554
               User: root
               Host: 172.18.0.4:54818
                 Db:
            Command: Query
ConnectionStartTime: 2026-07-02 01:21:14
               Time: 0
              State: OK
               Info: show processlist
          IsPending: false
          Warehouse: default_warehouse
            CNGroup:
            Catalog: default_catalog
            QueryId: 019f1eb3-246c-7899-ab3e-40a018645bba
>>>>>>> 49c0466fd0 ([Doc] add server column (#75672))
```
