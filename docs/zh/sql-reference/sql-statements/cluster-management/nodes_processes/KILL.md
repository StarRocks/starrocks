---
displayed_sidebar: docs
---

# KILL

## 功能

终止当前集群内的正在执行的指定连接或者查询。

:::tip

该操作不需要权限。

:::

## 语法

```SQL
KILL [ CONNECTION | QUERY ] <processlist_id>
```

## 参数说明

| **参数**                | **说明**                                                     |
| ----------------------- | ------------------------------------------------------------ |
| 修饰符：<ul><li>CONNECTION</li><li>QUERY</li></ul> | <ul><li>使用 `CONNECTION` 修饰符，KILL 语句首先终止与 `processlist_id` 关联的连接下正在执行的语句，然后终止该连接。</li><li>使用 `QUERY` 修饰符，KILL 语句仅终止与 `processlist_id` 关联的连接下正在执行的语句，不会终止该连接。</li><li>如不指定修饰符，默认值为 `CONNECTION`。</li></ul> |
| processlist_id          | 需要终止的线程的 ID。 您可以通过 [SHOW PROCESSLIST](SHOW_PROCESSLIST.md) 获取正在执行的线程的 ID。 |

## 示例

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
