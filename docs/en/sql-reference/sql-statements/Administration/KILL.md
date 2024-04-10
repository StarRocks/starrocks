---
displayed_sidebar: "English"
---

# KILL

## Description

Terminates a connection or a query currently being performed by threads executing within StarRocks.

:::tip

This operation does not require privileges.

:::

## Syntax

```SQL
KILL [ CONNECTION | QUERY ] <processlist_id>
```

## Parameters

| **Parameter**            | **Description**                                              |
| ------------------------ | ------------------------------------------------------------ |
| Modifier:<ul><li>CONNECTION</li><li>QUERY</li></ul> | <ul><li>With a `CONNECTION` modifier, the KILL statement terminates the connection associated with the given `processlist_id`, after terminating any statement the connection is executing.</li><li>With a `QUERY` modifier, the KILL statement terminates the statement the connection is currently executing, but leaves the connection itself intact.</li><li>If no modifier is present, the default is `CONNECTION`.</li></ul> |
| processlist_id           | The ID of the thread you want to terminate. You can get the IDs of the threads that are being executed using [SHOW PROCESSLIST](../Administration/SHOW_PROCESSLIST.md). |

## Examples

```Plain
mysql> SHOW FULL PROCESSLIST;
+------+------+---------------------+--------+---------+---------------------+------+-------+-----------------------+-----------+
| Id   | User | Host                | Db     | Command | ConnectionStartTime | Time | State | Info                  | IsPending |
+------+------+---------------------+--------+---------+---------------------+------+-------+-----------------------+-----------+
|   20 | root | xxx.xx.xxx.xx:xxxxx | sr_hub | Query   | 2023-01-05 16:30:19 |    0 | OK    | show full processlist | false     |
+------+------+---------------------+--------+---------+---------------------+------+-------+-----------------------+-----------+
1 row in set (0.01 sec)

mysql> KILL 20;
Query OK, 0 rows affected (0.00 sec)
```
