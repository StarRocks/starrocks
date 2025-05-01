---
displayed_sidebar: docs
---

# Query queues

このトピックでは、StarRocks におけるクエリキューの管理方法について説明します。

バージョン 2.5 から、StarRocks はクエリキューをサポートしています。クエリキューが有効になると、同時実行の閾値やリソース制限に達した場合に、StarRocks は自動的に受信クエリをキューに入れ、過負荷の悪化を回避します。保留中のクエリは、実行を開始するのに十分な計算リソースが利用可能になるまでキューで待機します。

CPU 使用率、メモリ使用率、クエリの同時実行数に閾値を設定して、クエリキューをトリガーできます。

## Enable query queues

クエリキューはデフォルトでは無効になっています。INSERT ロード、SELECT クエリ、および統計クエリに対して、対応するグローバルセッション変数を設定することでクエリキューを有効にできます。

- ロードタスクのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_load = true;
```

- SELECT クエリのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_select = true;
```

- 統計クエリのクエリキューを有効にする:

```SQL
SET GLOBAL enable_query_queue_statistic = true;
```

## Specify resource thresholds

次のグローバルセッション変数を使用して、クエリキューをトリガーする閾値を設定できます。

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | BE 上の同時実行クエリの上限。この値が `0` より大きく設定された場合にのみ有効です。 |
| query_queue_mem_used_pct_limit      | 0           | BE 上のメモリ使用率の上限。この値が `0` より大きく設定された場合にのみ有効です。範囲: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | BE 上の CPU 使用率のパーミル（CPU 使用率 * 1000）の上限。この値が `0` より大きく設定された場合にのみ有効です。範囲: [0, 1000] |

> **NOTE**
>
> デフォルトでは、BE は 1 秒間隔でリソース使用状況を FE に報告します。この間隔は、BE の設定項目 `report_resource_usage_interval_ms` を設定することで変更できます。

## Configure query queues

次のグローバルセッション変数を使用して、クエリキューの容量とキュー内のクエリの最大タイムアウトを設定できます。

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024        | キュー内のクエリの上限。この閾値に達すると、受信クエリは拒否されます。この値が `0` より大きく設定された場合にのみ有効です。 |
| query_queue_pending_timeout_second | 300         | キュー内の保留中のクエリの最大タイムアウト。この閾値に達すると、対応するクエリは拒否されます。単位: 秒。 |

## View query queue statistics

次の方法でクエリキューの統計を確認できます。

- [ SHOW PROC](../sql-reference/sql-statements/Administration/SHOW_PROC.md) を使用して、BE ノードで実行中のクエリ数、メモリ使用量、および CPU 使用量を確認します。

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

- [ SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW_PROCESSLIST.md) を使用して、クエリがキューにあるかどうかを確認します（`IsPending` が `true` の場合）。

```Plain
mysql> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

- FE の監査ログファイル **fe.audit.log** を確認します。フィールド `PendingTimeMs` は、クエリがキューにある時間を示し、その単位はミリ秒です。

- 次の監視メトリクスを確認します。

| **Metrics**                      | **Description**                                |
| -------------------------------- | ---------------------------------------------- |
| starrocks_fe_query_queue_pending | キュー内の保留中のクエリの数。                  |
| starrocks_fe_query_queue_total   | キューに入ったクエリの総数。                    |
| starrocks_fe_query_queue_timeout | キュー内のタイムアウトクエリの数。              |