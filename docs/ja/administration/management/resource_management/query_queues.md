---
displayed_sidebar: docs
sidebar_position: 20
---

# クエリキュー

このトピックでは、StarRocks におけるクエリキューの管理方法について説明します。

v2.5 から、StarRocks はクエリキューをサポートしています。クエリキューが有効になると、同時実行のしきい値やリソース制限に達した場合に、StarRocks は自動的に受信クエリをキューに入れ、過負荷の悪化を避けます。保留中のクエリは、実行を開始するのに十分な計算リソースが利用可能になるまでキューで待機します。

クエリキューには 2 つのバージョンがあります。

- **Query Queue v1**: クエリの同時実行数、BE のメモリ使用率、および BE の CPU 使用率に基づいてキューイングをトリガーします。このトピックの既存のクエリキュー設定と動作は v1 に属します。v3.1.4 以降、v1 はリソースグループレベルでのクエリキュー設定をサポートしています。
- **Query Queue v2**: v3.3 以降でサポートされています。v2 は各 Query が消費する BE リソースを見積もり、BE リソースを論理 slot として表現し、各 Query が必要とする slot 数に基づいてキューイングとスケジューリングを行います。

## Query Queue v1

Query Queue v1 では、CPU 使用率、メモリ使用率、クエリの同時実行数にしきい値を設定して、クエリキューをトリガーできます。

**ロードマップ**:

| Version | Global query queue | Resource group-level query queue | Collective concurrency management | Dynamic concurrency adjustment  |
| ------  | ------------------ | -------------------------------- | --------------------------------- | ------------------------------- |
| v2.5    | ✅                 | ❌                                | ❌                                | ❌                              |
| v3.1.4  | ✅                 | ✅                                | ✅                                | ✅                              |

### Query Queue v1 を有効にする

クエリキューはデフォルトで無効になっています。INSERT ロード、SELECT クエリ、および統計クエリに対して、対応するグローバルセッション変数を設定することで、グローバルまたはリソースグループレベルのクエリキューを有効にできます。

#### グローバルクエリキューを有効にする

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

#### リソースグループレベルのクエリキューを有効にする

v3.1.4 以降、StarRocks はリソースグループレベルでのクエリキューの設定をサポートしています。

リソースグループレベルのクエリキューを有効にするには、上記のグローバルセッション変数に加えて `enable_group_level_query_queue` を設定する必要があります。

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

### リソースしきい値を指定する

#### グローバルクエリキューのリソースしきい値を指定する

次のグローバルセッション変数を使用して、クエリキューをトリガーするしきい値を設定できます。

| **Variable**                        | **Default** | **Description**                                              |
| ----------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0           | BE 上の同時クエリの上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。 |
| query_queue_mem_used_pct_limit      | 0           | BE 上のメモリ使用率の上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。範囲: [0, 1] |
| query_queue_cpu_used_permille_limit | 0           | BE 上の CPU 使用率のパーミル (CPU 使用率 * 1000) の上限。`0` より大きく設定された場合にのみ有効になります。`0` に設定すると、制限がないことを示します。範囲: [0, 1000] |

:::note

これら 3 つのしきい値パラメータは Query Queue v1 にのみ適用されます。Query Queue v2 を有効にすると、`query_queue_concurrency_limit`、`query_queue_mem_used_pct_limit`、および `query_queue_cpu_used_permille_limit` によるキューイングのトリガーはサポートされません。

:::

> **NOTE**
>
> デフォルトでは、BE は 1 秒間隔でリソース使用状況を FE に報告します。この間隔は、BE の設定項目 `report_resource_usage_interval_ms` を設定することで変更できます。

#### リソースグループレベルのクエリキューのリソースしきい値を指定する

v3.1.4 以降、リソースグループを作成する際に、個別の同時実行制限 (`concurrency_limit`) と CPU コア制限 (`max_cpu_cores`) を設定できます。クエリが開始されると、グローバルまたはリソースグループレベルのいずれかでリソース消費がリソースしきい値を超えた場合、すべてのリソース消費がしきい値内に収まるまでクエリはキューに入れられます。

| **Variable**        | **Default** | **Description**                                              |
| ------------------- | ----------- | ------------------------------------------------------------ |
| concurrency_limit   | 0           | 単一の BE ノード上のリソースグループの同時実行制限。`0` より大きく設定された場合にのみ有効になります。 |
| max_cpu_cores       | 0           | 単一の BE ノード上のこのリソースグループの CPU コア制限。`0` より大きく設定された場合にのみ有効になります。範囲: [0, `avg_be_cpu_cores`]、ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数を表します。 |

各 BE ノードのリソースグループごとのリソース使用情報を表示するには、[View Resource Group Usage Information](./resource_group.md#view-resource-group-usage-information) を参照してください。

#### クエリの同時実行数を管理する

実行中のクエリの数 (`num_running_queries`) がグローバルまたはリソースグループの `concurrency_limit` を超えると、受信クエリはキューに入れられます。`num_running_queries` を取得する方法は、バージョン &lt; v3.1.4 と &ge; v3.1.4 の間で異なります。

- バージョン &lt; v3.1.4 では、`num_running_queries` は `report_resource_usage_interval_ms` で指定された間隔で BEs によって報告されます。したがって、`num_running_queries` の変化の特定に遅延が生じる可能性があります。たとえば、BEs によって報告された `num_running_queries` がグローバルまたはリソースグループの `concurrency_limit` を超えていない場合でも、次の報告前に受信クエリが到着して `concurrency_limit` を超えた場合、これらの受信クエリはキューで待機せずに実行されます。

- バージョン &ge; v3.1.4 では、すべての実行中のクエリは Leader FE によって集中的に管理されます。各 Follower FE はクエリの開始または終了時に Leader FE に通知し、StarRocks が `concurrency_limit` を超えるクエリの急増に対応できるようにします。

### Query Queue v1 を設定する

クエリキューの容量とキュー内のクエリの最大タイムアウトを次のグローバルセッション変数を使用して設定できます。

| **Variable**                       | **Default** | **Description**                                              |
| ---------------------------------- | ----------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024        | キュー内のクエリの上限。このしきい値に達すると、受信クエリは拒否されます。`0` より大きく設定された場合にのみ有効になります。 |
| query_queue_pending_timeout_second | 300         | キュー内の保留中のクエリの最大タイムアウト。このしきい値に達すると、対応するクエリは拒否されます。単位: 秒。 |

### クエリ同時実行数の動的調整を設定する

バージョン v3.1.4 以降、クエリキューによって管理され、Pipeline Engine によって実行されるクエリに対して、StarRocks は現在の実行中のクエリの数 `num_running_queries`、フラグメントの数 `num_fragments`、およびクエリの同時実行数 `pipeline_dop` に基づいて、受信クエリのクエリの同時実行数 `pipeline_dop` を動的に調整できます。これにより、スケジューリングのオーバーヘッドを最小限に抑えながらクエリの同時実行数を動的に制御し、最適な BE リソースの利用を確保できます。フラグメントとクエリの同時実行数 `pipeline_dop` についての詳細は、[Query Management - Adjusting Query Concurrency](./Query_management.md) を参照してください。

クエリキューの下での各クエリに対して、StarRocks はドライバーの概念を維持します。これは、単一の BE 上のクエリの同時フラグメントを表します。その論理値 `num_drivers` は、単一の BE 上のそのクエリのすべてのフラグメントの総同時実行数を表し、`num_fragments * pipeline_dop` に等しいです。新しいクエリが到着すると、StarRocks は次のルールに基づいてクエリの同時実行数 `pipeline_dop` を調整します。

- 実行中のドライバーの数 `num_drivers` がクエリの同時ドライバーの低水位制限 `query_queue_driver_low_water` を超えるほど、クエリの同時実行数 `pipeline_dop` は低く調整されます。
- StarRocks は、クエリの同時ドライバーの高水位制限 `query_queue_driver_high_water` を下回るように実行中のドライバーの数 `num_drivers` を抑制します。

クエリの同時実行数 `pipeline_dop` の動的調整を次のグローバルセッション変数を使用して設定できます。

| **Variable**                  | **Default** | **Description**                                             |
| ----------------------------- | ----------- | ----------------------------------------------------------- |
| query_queue_driver_high_water | -1          | クエリの同時ドライバーの高水位制限。非負の値に設定された場合にのみ有効になります。`0` に設定すると、`avg_be_cpu_cores * 16` に相当します。ここで `avg_be_cpu_cores` はすべての BE ノードの平均 CPU コア数を表します。`0` より大きい値に設定すると、その値が直接使用されます。 |
| query_queue_driver_low_water  | -1          | クエリの同時ドライバーの低水位制限。非負の値に設定された場合にのみ有効になります。`0` に設定すると、`avg_be_cpu_cores * 8` に相当します。`0` より大きい値に設定すると、その値が直接使用されます。 |

## Query Queue v2

v3.3 以降、StarRocks は Query Queue v2 をサポートしています。v2 は、クエリの同時実行数、BE のメモリ使用率、または BE の CPU 使用率の固定しきい値に基づいてキューイングをトリガーしません。代わりに、各 Query が必要とする BE リソースを見積もり、論理 slot に基づいてキューイングとスケジューリングを行います。

### Query Queue v2 を設定する

Query Queue v2 は FE 設定項目で有効化および調整します。`enable_query_queue_v2` の変更を有効にするには、FE ノードの再起動が必要です。

| 設定項目 | デフォルト | 説明 |
| -------- | ---------- | ---- |
| `enable_query_queue_v2` | `false` (v3.3 から v4.0)<br />`true` (v4.1 以降) | Query Queue v2 を有効にするかどうか。`true` に設定すると、StarRocks は v2 の slot ベースのクエリスケジューリングメカニズムを使用します。 |
| `query_queue_v2_concurrency_level` | `4` | Query Queue v2 がクラスタ全体の slot 総数を計算するときに使用する論理同時実行レベル。値が大きいほど、システムが受け入れられる Query が増えます。これは相対的な調整パラメータです。 |

:::note

`query_queue_concurrency_limit`、`query_queue_mem_used_pct_limit`、および `query_queue_cpu_used_permille_limit` は Query Queue v1 にのみ適用されます。Query Queue v2 を有効にすると、これらのパラメータは有効になりません。

:::

### リソース slot

Query Queue v2 は BE リソースを論理 slot として表現します。

- **クラスタ全体の slot 総数**: StarRocks はクラスタ全体に対して論理的な slot 総数を設定します。この総数は BE 数と BE CPU Core 数に正の相関があり、`query_queue_v2_concurrency_level` の影響も受けます。
- **Query が必要とする slot 数**: StarRocks は各 Query が必要とする slot 数を見積もります。見積もりは、統計情報、クエリの複雑度、Fragment 数、複雑なオペレーターの入力および出力データ量の推定、DOP などに基づきます。

### キューイングロジック

ある Query が必要とする slot 数が現在の残り slot 数を超える場合、その Query はキューで待機します。Query Queue v2 は、必要な slot 数が少ない Query を優先的に満たすことで、小さいクエリが先にリソースを取得できるようにし、大きいクエリがキューの先頭に長時間とどまって後続の小さいクエリをブロックするヘッドオブラインブロッキング (Head-of-line blocking) を避けます。

キューイングロジック全体は FE 上で完了します。これには、クラスタ全体の slot 総数の設定、Query が必要とする slot 数の見積もり、およびどの Query の slot 要求を優先的に満たすかの決定が含まれます。Query Queue v2 は、BE の実際のリソース使用状況に基づいてスケジューリングを行いません。

## Monitor query queues

次の方法を使用して、クエリキューに関連する情報を表示できます。

### SHOW PROC

[SHOW PROC](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md) を使用して、BE ノードでの実行中のクエリの数、メモリおよび CPU 使用率を確認できます。

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

[SHOW PROCESSLIST](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST.md) を使用して、クエリがキューに入っているかどうか (`IsPending` が `true` の場合) を確認できます。

```Plain
mysql> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

### FE audit log

FE の監査ログファイル **fe.audit.log** を確認できます。フィールド `PendingTimeMs` は、クエリがキューで待機していた時間を示し、その単位はミリ秒です。

### Monitoring metrics

[Monitor and Alert](../monitoring/Monitor_and_Alert.md) 機能を使用して、StarRocks のクエリキューのメトリクスを取得できます。次の FE メトリクスは、各 FE ノードの統計データから導出されます。

| Metric                                          | Unit | Type    | Description                                                    |
| ----------------------------------------------- | ---- | ------- | -------------------------------------------------------------- |
| starrocks_fe_query_queue_pending                | Count | Instantaneous | 現在キューにあるクエリの数。                                  |
| starrocks_fe_query_queue_total                  | Count | Instantaneous | キューに入れられたクエリの総数 (現在実行中のものを含む)。      |
| starrocks_fe_query_queue_timeout                | Count | Instantaneous | キュー内でタイムアウトしたクエリの総数。                      |
| starrocks_fe_resource_group_query_queue_total   | Count | Instantaneous | このリソースグループでキューに入れられたクエリの総数 (現在実行中のものを含む)。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |
| starrocks_fe_resource_group_query_queue_pending | Count | Instantaneous | このリソースグループのキューに現在あるクエリの数。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |
| starrocks_fe_resource_group_query_queue_timeout | Count | Instantaneous | このリソースグループのキュー内でタイムアウトしたクエリの数。`name` ラベルはリソースグループの名前を示します。このメトリクスは v3.1.4 以降でサポートされています。 |

### SHOW RUNNING QUERIES

v3.1.4 以降、StarRocks は SQL ステートメント `SHOW RUNNING QUERIES` をサポートしており、各クエリのキュー情報を表示するために使用されます。各フィールドの意味は次のとおりです。

- `QueryId`: クエリの ID。
- `ResourceGroupId`: クエリがヒットしたリソースグループの ID。ユーザー定義のリソースグループにヒットしない場合は "-" と表示されます。
- `StartTime`: クエリの開始時間。
- `PendingTimeout`: キュー内で PENDING クエリがタイムアウトする時間。
- `QueryTimeout`: クエリがタイムアウトする時間。
- `State`: クエリのキュー状態。"PENDING" はキューに入っていることを示し、"RUNNING" は現在実行中であることを示します。
- `Slots`: クエリによって要求される論理リソース量。Query Queue v1 では通常 `1` です。Query Queue v2 では、その Query について見積もられた slot 数です。
- `Frontend`: クエリを開始した FE ノード。
- `FeStartTime`: クエリを開始した FE ノードの開始時間。

例:

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
