---
displayed_sidebar: docs
sidebar_position: 30
---

# クエリ管理

## ユーザー接続数

`Property` はユーザー単位で設定されます。Client と FE 間の最大接続数を設定するには、以下のコマンドを使用します。

```sql
ALTER USER '<username>' SET PROPERTIES ("key"="value", ...)
```

ユーザーのプロパティには、ユーザーに割り当てられたリソースが含まれます。ここで設定されるプロパティは `user_identity` ではなくユーザーに対するものです。つまり、`CREATE USER` ステートメントで `jack'@'%` と `jack'@'192.%` の2つのユーザーが作成された場合、`ALTER USER SET PROPERTIES` ステートメントはユーザー `jack` に対して機能し、`jack'@'%` や `jack'@'192.%` には機能しません。

例 1:

```sql
-- ユーザー `jack` の最大接続数を1000に変更
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");

-- root ユーザーの接続制限を確認
SHOW PROPERTY FOR 'root'; 
```

## クエリ関連のセッション変数

セッション変数は 'key' = 'value' で設定でき、現在のセッションでの並行性、メモリ、その他のクエリパラメータを制限できます。例えば：

- parallel_fragment_exec_instance_num

  クエリの並行性で、デフォルト値は1です。各 BE 上のフラグメントインスタンスの数を示します。クエリパフォーマンスを向上させるために、BE の CPU コア数の半分に設定できます。

- query_mem_limit

  各 BE ノードでのクエリのメモリ制限で、メモリ不足を報告するクエリの際に調整できます。

- load_mem_limit

  インポートのメモリ制限で、メモリ不足を報告するインポートジョブの際に調整できます。

例 2:

```sql
set parallel_fragment_exec_instance_num  = 8; 
set query_mem_limit  = 137438953472;
```

## データベースストレージの容量クォータ

データベースストレージの容量クォータはデフォルトで無制限です。`alter database` を使用してクォータ値を変更できます。

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

クォータの単位は: B/K/KB/M/MB/G/GB/T/TB/P/PB

例 3:

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

## クエリの終了

特定の接続でクエリを終了するには、以下のコマンドを使用します。

```sql
kill connection_id;
```

`connection_id` は `show processlist;` または `select connection_id();` で確認できます。

```plain text
 show processlist;
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
| Id   | User       | Host                | Cluster         | Db            | Command | Time | State | Info |
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
|    1 | starrocksmgr | 172.26.34.147:56208 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  129 | root       | 172.26.92.139:54818 | default_cluster |               | Query   |    0 |       |      |
|  114 | test       | 172.26.34.147:57974 | default_cluster | ssb_100g      | Query   |    3 |       |      |
|    3 | starrocksmgr | 172.26.34.147:57268 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  100 | root       | 172.26.34.147:58472 | default_cluster | ssb_100       | Sleep   |  637 |       |      |
|  117 | starrocksmgr | 172.26.34.147:33790 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|    6 | starrocksmgr | 172.26.34.147:57632 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  119 | starrocksmgr | 172.26.34.147:33804 | default_cluster | starrocks_monitor | Sleep   |    8 |       |      |
|  111 | root       | 172.26.92.139:55472 | default_cluster |               | Sleep   | 2758 |       |      |
+------+------------+---------------------+-----------------+---------------+---------+------+-------+------+
9 rows in set (0.00 sec)

mysql> select connection_id();
+-----------------+
| CONNECTION_ID() |
+-----------------+
|              98 |
+-----------------+


mysql> kill 114;
Query OK, 0 rows affected (0.02 sec)

```