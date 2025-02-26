---
displayed_sidebar: docs
---

# KILL

## 説明

StarRocks 内で実行中のスレッドによって現在実行されている接続またはクエリを終了します。

:::tip

この操作には特権は必要ありません。

:::

## 構文

```SQL
KILL [ CONNECTION | QUERY ] <processlist_id>
```

## パラメーター

| **パラメーター**            | **説明**                                              |
| ------------------------ | ------------------------------------------------------------ |
| 修飾子:<ul><li>CONNECTION</li><li>QUERY</li></ul> | <ul><li>`CONNECTION` 修飾子を使用すると、KILL ステートメントは指定された `processlist_id` に関連付けられた接続を終了し、その接続が実行しているステートメントを終了します。</li><li>`QUERY` 修飾子を使用すると、KILL ステートメントは接続が現在実行しているステートメントを終了しますが、接続自体はそのまま残ります。</li><li>修飾子が指定されていない場合、デフォルトは `CONNECTION` です。</li></ul> |
| processlist_id           | 終了したいスレッドの ID です。実行中のスレッドの ID は [SHOW PROCESSLIST](SHOW_PROCESSLIST.md) を使用して取得できます。 |

## 例

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