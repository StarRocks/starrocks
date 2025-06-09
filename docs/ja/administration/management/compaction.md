---
displayed_sidebar: docs
---

# 共有データクラスタの Compaction

このトピックでは、StarRocks 共有データクラスタにおける Compaction の管理方法について説明します。

## 概要

StarRocks の各データロード操作は、新しいバージョンのデータファイルを生成します。Compaction は、異なるバージョンのデータファイルをより大きなファイルにマージし、小さなファイルの数を減らし、クエリ効率を向上させます。

## Compaction スコア

### 概要

*Compaction スコア* は、パーティション内のデータファイルのマージ状況を反映します。スコアが高いほどマージの進捗が低いことを示し、パーティションには未マージのデータファイルバージョンが多くなります。FE は各パーティションの Compaction スコア情報を保持しており、Max Compaction スコア（パーティション内のすべてのタブレットの中で最も高いスコア）を含みます。

パーティションの Max Compaction スコアが FE パラメータ `lake_compaction_score_selector_min_score`（デフォルト: 10）を下回る場合、そのパーティションの Compaction は完了と見なされます。Max Compaction スコアが 100 を超えると、Compaction 状態が不健康であることを示します。スコアが FE パラメータ `lake_ingest_slowdown_threshold`（デフォルト: 100）を超えると、システムはそのパーティションのデータロードトランザクションのコミットを遅らせます。`lake_compaction_score_upper_bound`（デフォルト: 2000）を超えると、システムはそのパーティションのインポートトランザクションのロードを停止します。

### 計算ルール

通常、各データファイルは Compaction スコアに 1 を寄与します。例えば、1 つのタブレットと最初のロード操作から生成された 10 個のデータファイルを持つパーティションの場合、そのパーティションの Max Compaction スコアは 10 です。タブレット内のトランザクションによって生成されたすべてのデータファイルは Rowset としてグループ化されます。

スコア計算中、タブレットの Rowset はサイズごとにグループ化され、ファイル数が最も多いグループがタブレットの Compaction スコアを決定します。

例えば、タブレットが 7 回のロード操作を受け、Rowset のサイズが 100 MB, 100 MB, 100 MB, 10 MB, 10 MB, 10 MB, 10 MB である場合、システムは 3 つの 100 MB Rowset を 1 つのグループにし、4 つの 10 MB Rowset を別のグループにします。Compaction スコアは、ファイル数が多いグループに基づいて計算されます。この場合、2 番目のグループがより大きな Compaction スコアを持ちます。Compaction はスコアの高いグループを優先するため、最初の Compaction 後の Rowset の分布は 100 MB, 100 MB, 100 MB, 40 MB となります。

## Compaction ワークフロー

共有データクラスタでは、StarRocks は新しい FE 制御の Compaction メカニズムを導入しています。

1. スコア計算: Leader FE ノードがトランザクションの公開結果に基づいてパーティションの Compaction スコアを計算し、保存します。
2. 候補選択: FE は Max Compaction スコアが最も高いパーティションを Compaction 候補として選択します。
3. タスク生成: FE は選択されたパーティションの Compaction トランザクションを開始し、タブレットレベルのサブタスクを生成し、FE パラメータ `lake_compaction_max_tasks` で設定された制限に達するまで Compute Nodes (CN) に配信します。
4. サブタスク実行: CN はバックグラウンドで Compaction サブタスクを実行します。CN ごとの同時サブタスク数は CN パラメータ `compact_threads` によって制御されます。
5. 結果収集: FE はサブタスクの結果を集約し、Compaction トランザクションをコミットします。
6. 公開: FE は正常にコミットされた Compaction トランザクションを公開します。

## Compaction の管理

### Compaction スコアの表示

- 特定のテーブル内のパーティションの Compaction スコアを表示するには、SHOW PROC ステートメントを使用します。

  ```Plain
  SHOW PROC '/dbs/<database_name>/<table_name>/partitions'
  ```

  例:

  ```Plain
  mysql> SHOW PROC '/dbs/load_benchmark/store_sales/partitions';
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | PartitionId | PartitionName | CompactVersion | VisibleVersion | NextVersion | State  | PartitionKey | Range | DistributionKey              | Buckets | DataSize | RowCount  | CacheTTL | AsyncWrite | AvgCS | P50CS | MaxCS |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  | 38028       | store_sales   | 913            | 921            | 923         | NORMAL |              |       | ss_item_sk, ss_ticket_number | 64      | 15.6GB   | 273857126 | 2592000  | false      | 10.00 | 10.00 | 10.00 |
  +-------------+---------------+----------------+----------------+-------------+--------+--------------+-------+------------------------------+---------+----------+-----------+----------+------------+-------+-------+-------+
  1 row in set (0.20 sec)
  ```

- システム定義ビュー `information_schema.partitions_meta` をクエリすることで、パーティションの Compaction スコアを表示することもできます。

  例:

  ```Plain
  mysql> SELECT * FROM information_schema.partitions_meta ORDER BY Max_CS LIMIT 10;
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  | DB_NAME      | TABLE_NAME                 | PARTITION_NAME             | PARTITION_ID | COMPACT_VERSION | VISIBLE_VERSION | VISIBLE_VERSION_TIME | NEXT_VERSION | PARTITION_KEY | PARTITION_VALUE | DISTRIBUTION_KEY                        | BUCKETS | REPLICATION_NUM | STORAGE_MEDIUM | COOLDOWN_TIME       | LAST_CONSISTENCY_CHECK_TIME | IS_IN_MEMORY | IS_TEMP | DATA_SIZE | ROW_COUNT  | ENABLE_DATACACHE | AVG_CS   | P50_CS | MAX_CS | STORAGE_PATH                                                      |
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  | tpcds_1t     | call_center                | call_center                |        11905 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | cc_call_center_sk                       |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 12.3KB    |         42 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11906/11905 |
  | tpcds_1t     | web_returns                | web_returns                |        12030 |               3 |               3 | 2024-03-17 08:40:48  |            4 |               |                 | wr_item_sk, wr_order_number             |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 3.5GB     |   71997522 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/12031/12030 |
  | tpcds_1t     | warehouse                  | warehouse                  |        11847 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | w_warehouse_sk                          |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 4.2KB     |         20 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11848/11847 |
  | tpcds_1t     | ship_mode                  | ship_mode                  |        11851 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | sm_ship_mode_sk                         |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 1.7KB     |         20 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11852/11851 |
  | tpcds_1t     | customer_address           | customer_address           |        11790 |               0 |               2 | 2024-03-17 08:32:19  |            3 |               |                 | ca_address_sk                           |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 120.9MB   |    6000000 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11791/11790 |
  | tpcds_1t     | time_dim                   | time_dim                   |        11855 |               0 |               2 | 2024-03-17 08:30:48  |            3 |               |                 | t_time_sk                               |      16 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 864.7KB   |      86400 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11856/11855 |
  | tpcds_1t     | web_sales                  | web_sales                  |        12049 |               3 |               3 | 2024-03-17 10:14:20  |            4 |               |                 | ws_item_sk, ws_order_number             |     128 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 47.7GB    |  720000376 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/12050/12049 |
  | tpcds_1t     | store                      | store                      |        11901 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | s_store_sk                              |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 95.6KB    |       1002 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11902/11901 |
  | tpcds_1t     | web_site                   | web_site                   |        11928 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | web_site_sk                             |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 13.4KB    |         54 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11929/11928 |
  | tpcds_1t     | household_demographics     | household_demographics     |        11932 |               0 |               2 | 2024-03-17 08:30:47  |            3 |               |                 | hd_demo_sk                              |       1 |               1 | HDD            | 9999-12-31 23:59:59 | NULL                        |            0 |       0 | 2.1KB     |       7200 |                0 |        0 |      0 |      0 | s3://XXX/536a3c77-52c3-485a-8217-781734a970b1/db10328/11933/11932 |
  +--------------+----------------------------+----------------------------+--------------+-----------------+-----------------+----------------------+--------------+---------------+-----------------+-----------------------------------------+---------+-----------------+----------------+---------------------+-----------------------------+--------------+---------+-----------+------------+------------------+----------+--------+--------+-------------------------------------------------------------------+
  ```

次の 2 つのメトリクスに注目してください。

- `AvgCS`: パーティション内のすべてのタブレットの平均 Compaction スコア。
- `MaxCS`: パーティション内のすべてのタブレットの中で最大の Compaction スコア。

### Compaction タスクの表示

新しいデータがシステムにロードされると、FE は異なる CN ノードで実行される Compaction タスクを常にスケジュールします。まず、FE 上の Compaction タスクの一般的なステータスを表示し、次に CN 上の各タスクの実行詳細を表示します。

#### Compaction タスクの一般的なステータスの表示

SHOW PROC ステートメントを使用して、Compaction タスクの一般的なステータスを表示できます。

```SQL
SHOW PROC '/compactions';
```

例:

```Plain
mysql> SHOW PROC '/compactions';
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
| Partition           | TxnID | StartTime           | CommitTime          | FinishTime          | Error | Profile                                                                                                            |
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
| ssb.lineorder.43026 | 51053 | 2024-09-24 19:15:16 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43027 | 51052 | 2024-09-24 19:15:16 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43025 | 51047 | 2024-09-24 19:15:15 | NULL                | NULL                | NULL  | NULL                                                                                                               |
| ssb.lineorder.43026 | 51046 | 2024-09-24 19:15:04 | 2024-09-24 19:15:06 | 2024-09-24 19:15:06 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
| ssb.lineorder.43027 | 51045 | 2024-09-24 19:15:04 | 2024-09-24 19:15:06 | 2024-09-24 19:15:06 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
| ssb.lineorder.43029 | 51044 | 2024-09-24 19:15:03 | 2024-09-24 19:15:05 | 2024-09-24 19:15:05 | NULL  | {"sub_task_count":1,"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"in_queue_sec":0} |
+---------------------+-------+---------------------+---------------------+---------------------+-------+--------------------------------------------------------------------------------------------------------------------+
```

次のフィールドが返されます。

- `Partition`: Compaction タスクが属するパーティション。
- `TxnID`: Compaction タスクに割り当てられたトランザクション ID。
- `StartTime`: Compaction タスクが開始された時間。`NULL` はタスクがまだ開始されていないことを示します。
- `CommitTime`: Compaction タスクがデータをコミットした時間。`NULL` はデータがまだコミットされていないことを示します。
- `FinishTime`: Compaction タスクがデータを公開した時間。`NULL` はデータがまだ公開されていないことを示します。
- `Error`: Compaction タスクのエラーメッセージ（ある場合）。
- `Profile`: (v3.2.12 および v3.3.4 からサポート) Compaction タスクの完了後のプロファイル。
  - `sub_task_count`: パーティション内のサブタスク（タブレットに相当）の数。
  - `read_local_sec`: ローカルキャッシュからデータを読み取るすべてのサブタスクの合計時間。単位: 秒。
  - `read_local_mb`: ローカルキャッシュからすべてのサブタスクによって読み取られたデータの合計サイズ。単位: MB。
  - `read_remote_sec`: リモートストレージからデータを読み取るすべてのサブタスクの合計時間。単位: 秒。
  - `read_remote_mb`: リモートストレージからすべてのサブタスクによって読み取られたデータの合計サイズ。単位: MB。
  - `in_queue_sec`: キューに滞在するすべてのサブタスクの合計時間。単位: 秒。

#### Compaction タスクの実行詳細の表示

各 Compaction タスクは複数のサブタスクに分割され、それぞれがタブレットに対応します。システム定義ビュー `information_schema.be_cloud_native_compactions` をクエリすることで、各サブタスクの実行詳細を表示できます。

例:

```Plain
mysql> SELECT * FROM information_schema.be_cloud_native_compactions;
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| BE_ID | TXN_ID | TABLET_ID | VERSION | SKIPPED | RUNS | START_TIME          | FINISH_TIME | PROGRESS | STATUS | PROFILE                                                                                                                                                                                         |
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 10001 |  51047 |     43034 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51048 |     43032 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":32,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51049 |     43033 |      12 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       82 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51051 |     43038 |       9 |       0 |    1 | 2024-09-24 19:15:15 | NULL        |       84 |        | {"read_local_sec":0,"read_local_mb":31,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":1900,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0} |
| 10001 |  51052 |     43036 |      12 |       0 |    0 | NULL                | NULL        |        0 |        |                                                                                                                                                                                                 |
| 10001 |  51053 |     43035 |      12 |       0 |    1 | 2024-09-24 19:15:16 | NULL        |        2 |        | {"read_local_sec":0,"read_local_mb":1,"read_remote_sec":0,"read_remote_mb":0,"read_remote_count":0,"read_local_count":100,"segment_init_sec":0,"column_iterator_init_sec":0,"in_queue_sec":0}   |
+-------+--------+-----------+---------+---------+------+---------------------+-------------+----------+--------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

次のフィールドが返されます。

- `BE_ID`: CN の ID。
- `TXN_ID`: サブタスクが属するトランザクションの ID。
- `TABLET_ID`: サブタスクが属するタブレットの ID。
- `VERSION`: タブレットのバージョン。
- `RUNS`: サブタスクが実行された回数。
- `START_TIME`: サブタスクが開始された時間。
- `FINISH_TIME`: サブタスクが終了した時間。
- `PROGRESS`: タブレットの Compaction 進捗率。
- `STATUS`: サブタスクのステータス。エラーがある場合、このフィールドにエラーメッセージが返されます。
- `PROFILE`: (v3.2.12 および v3.3.4 からサポート) サブタスクの実行時プロファイル。
  - `read_local_sec`: サブタスクがローカルキャッシュからデータを読み取るのにかかる時間。単位: 秒。
  - `read_local_mb`: サブタスクがローカルキャッシュから読み取ったデータのサイズ。単位: MB。
  - `read_remote_sec`: サブタスクがリモートストレージからデータを読み取るのにかかる時間。単位: 秒。
  - `read_remote_mb`: サブタスクがリモートストレージから読み取ったデータのサイズ。単位: MB。
  - `read_local_count`: サブタスクがローカルキャッシュからデータを読み取った回数。
  - `read_remote_count`: サブタスクがリモートストレージからデータを読み取った回数。
  - `in_queue_sec`: サブタスクがキューに滞在する時間。単位: 秒。

### Compaction タスクの設定

これらの FE および CN (BE) パラメータを使用して Compaction タスクを設定できます。

#### FE パラメータ

次の FE パラメータを動的に設定できます。

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");
```

##### lake_compaction_max_tasks

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタで許可される同時 Compaction タスクの最大数。この項目を `-1` に設定すると、同時タスク数を適応的に計算します。つまり、生存している CN ノードの数に 16 を掛けた数です。この値を `0` に設定すると Compaction が無効になります。
- 導入バージョン: v3.1.0

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_disable_tables" = "11111;22222");
```

##### lake_compaction_disable_tables

- デフォルト：""
- タイプ：String
- 単位：-
- 変更可能：はい
- 説明：特定のテーブルの Compaction を無効にします。これにより、開始された Compaction には影響しません。この項目の値はテーブル ID です。複数の値は ';' で区切られます。
- 導入バージョン：v3.2.7

#### CN パラメータ

次の CN パラメータを動的に設定できます。

```SQL
UPDATE information_schema.be_configs SET VALUE = 8 
WHERE name = "compact_threads";
```

##### compact_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時 Compaction タスクに使用されるスレッドの最大数。この設定は v3.1.7 および v3.2.2 以降で動的に変更可能になりました。
- 導入バージョン: v3.0.0

> **NOTE**
>
> 本番環境では、`compact_threads` を BE/CN CPU コア数の 25% に設定することをお勧めします。

##### max_cumulative_compaction_num_singleton_deltas

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 単一の Cumulative Compaction でマージできるセグメントの最大数。Compaction 中に OOM が発生した場合、この値を減らすことができます。
- 導入バージョン: -

> **NOTE**
>
> 本番環境では、Compaction タスクを加速し、リソース消費を削減するために `max_cumulative_compaction_num_singleton_deltas` を `100` に設定することをお勧めします。

##### lake_pk_compaction_max_input_rowsets

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタ内の主キーテーブルの Compaction タスクで許可される入力 Rowset の最大数。このパラメータのデフォルト値は、v3.2.4 および v3.1.10 から `5` から `1000` に、v3.3.1 および v3.2.9 から `500` に変更されました。主キーテーブルに対して Sized-tiered Compaction ポリシーが有効になった後（`enable_pk_size_tiered_compaction_strategy` を `true` に設定することで）、StarRocks は各 Compaction の Rowset 数を制限して書き込み増幅を削減する必要がなくなります。したがって、このパラメータのデフォルト値が増加しました。
- 導入バージョン: v3.1.8, v3.2.3

### Compaction タスクの手動トリガー

```SQL
-- テーブル全体の Compaction をトリガーします。
ALTER TABLE <table_name> COMPACT;
-- 特定のパーティションの Compaction をトリガーします。
ALTER TABLE <table_name> COMPACT <partition_name>;
-- 複数のパーティションの Compaction をトリガーします。
ALTER TABLE <table_name> COMPACT (<partition_name>, <partition_name>, ...);
```

### Compaction タスクのキャンセル

タスクのトランザクション ID を使用して Compaction タスクを手動でキャンセルできます。

```SQL
CANCEL COMPACTION WHERE TXN_ID = <TXN_ID>;
```

> **NOTE**
>
> - CANCEL COMPACTION ステートメントは Leader FE ノードから送信する必要があります。
> - CANCEL COMPACTION ステートメントは、コミットされていないトランザクションにのみ適用されます。つまり、`SHOW PROC '/compactions'` の戻り値で `CommitTime` が NULL であるトランザクションです。
> - CANCEL COMPACTION は非同期プロセスです。`SHOW PROC '/compactions'` を実行して、タスクがキャンセルされたかどうかを確認できます。

## ベストプラクティス

Compaction はクエリパフォーマンスにとって重要であるため、テーブルとパーティションのデータマージ状況を定期的に監視することをお勧めします。以下はベストプラクティスとガイドラインです。

- ロード間の時間間隔を増やし（10 秒未満の間隔を避ける）、ロードごとのバッチサイズを増やします（100 行未満のデータバッチサイズを避ける）。
- CN 上の並列 Compaction ワーカースレッドの数を調整してタスクの実行を加速します。本番環境では、`compact_threads` を BE/CN CPU コア数の 25% に設定することをお勧めします。クラスタがアイドル状態（例えば、Compaction のみを実行し、クエリを処理していない場合）では、この値を一時的に 50% に増やし、タスク完了後に 25% に戻すことができます。
- `show proc '/compactions'` および `select * from information_schema.be_cloud_native_compactions;` を使用して Compaction タスクのステータスを監視します。
- Compaction スコアを監視し、それに基づいてアラートを設定します。StarRocks の組み込み Grafana 監視テンプレートにはこのメトリクスが含まれています。
- Compaction 中のリソース消費、特にメモリ使用量に注意を払います。Grafana 監視テンプレートにもこのメトリクスが含まれています。
```