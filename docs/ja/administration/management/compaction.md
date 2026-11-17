---
sidebar_position: 120
displayed_sidebar: docs
description: "StarRocks 共有データクラスタで Compaction を管理・監視し、クエリ効率を向上させる方法。"
products: [oss, enterprise, anywhere]
---

# 共有データクラスタの Compaction

このトピックでは、StarRocks 共有データクラスタにおける compaction の管理方法について説明します。

## 概要

StarRocks の各データロード操作は、新しいバージョンのデータファイルを生成します。Compaction は異なるバージョンのデータファイルをより大きなファイルにマージし、小さなファイルの数を減らし、クエリ効率を向上させます。

## Compaction スコア

### 概要

*Compaction スコア* は、パーティション内のデータファイルのマージ状況を反映します。スコアが高いほどマージの進捗が低いことを示し、パーティションには未マージのデータファイルバージョンが多く存在します。FE は各パーティションの Compaction スコア情報を保持しており、Max Compaction スコア（パーティション内のすべてのタブレットの中で最も高いスコア）を含みます。

パーティションの Max Compaction スコアが FE パラメータ `lake_compaction_score_selector_min_score`（デフォルト: 10）を下回る場合、通常はそのパーティションが自動 Compaction の候補から外れます。ただし、すべての削除マークが除去されたことを意味しません。Max Compaction スコアが 100 を超えると、compaction の状態が不健全であることを示します。スコアが FE パラメータ `lake_ingest_slowdown_threshold`（デフォルト: 100）を超えると、システムはそのパーティションのデータロードトランザクションのコミットを遅らせます。`lake_compaction_score_upper_bound`（デフォルト: 2000）を超えると、そのパーティションのインポートトランザクションを拒否します。

### 計算ルール

通常、各データファイルは Compaction スコアに 1 を寄与します。例えば、あるパーティションに 1 つのタブレットと最初のロード操作から生成された 10 個のデータファイルがある場合、そのパーティションの Max Compaction スコアは 10 です。タブレット内のトランザクションによって生成されたすべてのデータファイルは Rowset としてグループ化されます。

スコア計算中、タブレットの Rowset はサイズごとにグループ化され、ファイル数が最も多いグループがタブレットの Compaction スコアを決定します。

例えば、あるタブレットが 7 回のロード操作を受け、Rowset のサイズが 100 MB, 100 MB, 100 MB, 10 MB, 10 MB, 10 MB, 10 MB であるとします。計算中、システムは 3 つの 100 MB Rowset を 1 つのグループにし、4 つの 10 MB Rowset を別のグループにします。Compaction スコアはファイル数が多いグループに基づいて計算されます。この場合、2 番目のグループがより大きな compaction スコアを持ちます。compaction はスコアの高いグループを優先するため、最初の compaction 後の Rowset の分布は 100 MB, 100 MB, 100 MB, 40 MB となります。

## Compaction ワークフロー

共有データクラスタでは、StarRocks は新しい FE 制御の compaction メカニズムを導入しています。

1. スコア計算: Leader FE ノードがトランザクション公開結果に基づいてパーティションの Compaction スコアを計算し、保存します。
2. 候補選択: FE は Max Compaction スコアが最も高いパーティションを compaction 候補として選択します。
3. タスク生成: FE は選択されたパーティションの compaction トランザクションを開始し、タブレットレベルのサブタスクを生成し、FE パラメータ `lake_compaction_max_tasks` で設定された制限に達するまで Compute Node (CN) に配布します。
4. サブタスク実行: CN はバックグラウンドで compaction サブタスクを実行します。CN ごとの同時サブタスク数は CN パラメータ `compact_threads` によって制御されます。
5. 結果収集: FE はサブタスクの結果を集約し、compaction トランザクションをコミットします。
6. 公開: FE は正常にコミットされた compaction トランザクションを公開します。

## 主キーテーブルの Base Compaction {/* #primary-key-base-compaction */}

この節では、共有データクラスタの主キーテーブルに対する Base Compaction を説明します。通常の Cumulative Compaction は size-tiered ポリシーで Rowset を選択し、主に小さいファイルをマージします。Base Compaction はタブレット内のすべての Rowset を候補とし、削除行数が多い Rowset を優先的に書き換えて、無効なデータを除去し Delete Vector を縮小します。そのため、既存の主キーが継続的に更新または削除される場合、小さいファイルのマージが続いていても Base Compaction が必要になることがあります。

### バージョン別のサポート {/* #primary-key-base-compaction-versions */}

| バージョン | 自動 PK Base Compaction | 手動 PK Base Compaction | 共有データ Base Compaction の時間帯と最小間隔の制御 |
| --- | --- | --- | --- |
| v4.1.0～v4.1.3 | 非対応。 | PK Base Compaction は非対応。通常の Compaction は手動で実行できます。 | 非対応。 |
| v4.1.4～v4.1.6 | タスク配布後に削除率または削除行数を確認します。自動トリガー専用のスイッチはありません。 | 自動トリガーの削除しきい値に達していなくても実行できます。 | 非対応。 |
| v4.1.7 以降の 4.1 リリース | `lake_pk_compaction_base_enable_delete_trigger` で制御します。デフォルトは `false` です。 | スイッチと削除しきい値に関係なく実行できます。 | 非対応。 |
| v4.2 | 同じスイッチで制御します。デフォルトは `false` です。 | 実行可能。自動 Base Compaction の時間帯と最小間隔の制限も回避します。 | 対応。[実行時間の制御](#base-compaction-time-controls)を参照してください。 |

PK Base Compaction は v4.1.4 からサポートされています。v4.1.7 で追加された `lake_pk_compaction_base_enable_delete_trigger` は、**削除量による自動 Base Compaction だけを制御します**。手動 Base Compaction や通常の Cumulative Compaction を無効にするものではありません。

### 自動トリガー {/* #automatic-base-compaction */}

自動 Compaction は引き続き FE がパーティション単位でスケジュールし、デフォルトでは Compaction スコアで候補を選択します。タスクが CN に配布された後、CN が各タブレットに Base Compaction を選択するかを判断します。削除しきい値に達しても、それだけで FE が新しいタスクをスケジュールするわけではなく、即時実行は保証されません。

v4.1.7 以降では、以下の判定を有効にするため、まず CN パラメータ `lake_pk_compaction_base_enable_delete_trigger` を `true` に設定します。v4.1.4～v4.1.6 にはこのスイッチがなく、以下の条件を直接確認します。

- タブレット全体の削除率 `sum(num_dels) / sum(num_rows)` が `lake_pk_compaction_base_delete_ratio_threshold`（デフォルト `0.5`）以上である。
- タブレットの合計削除行数 `sum(num_dels)` が `lake_pk_compaction_base_delete_rows_threshold`（デフォルト `10000000`）以上である。

**いずれか一方**の条件を満たすと Base Compaction の選択対象になります。統計はタブレット内の現在のすべての Rowset を対象とし、更新で置き換えられた古い行の削除マークも含みます。削除行がないタブレットには通常の Compaction 選択を使用します。[実行時間の制御](#base-compaction-time-controls)をサポートするバージョンでは、自動 Base Compaction は時間帯と最小間隔の条件も満たす必要があります。

例えば、v4.1.7 以降では、削除量による自動トリガーを動的に有効化できます。

```sql
UPDATE information_schema.be_configs
SET VALUE = 'true'
WHERE NAME = 'lake_pk_compaction_base_enable_delete_trigger';
```

パラメータの定義は [BE パラメータリファレンス](../configuration/BE_parameters/shared_lake_other.md#lake_pk_compaction_base_enable_delete_trigger)を参照してください。更新が継続する大きなタブレットでは、Compaction 後すぐに絶対削除行数のしきい値へ再到達することがあります。有効化する前に、繰り返しのデータ書き換えによるリソース消費を評価してください。

### 手動トリガー {/* #manual-base-compaction */}

対象のテーブルまたはパーティションに `ALTER TABLE ... COMPACT` を実行すると、手動 Compaction を要求できます。`BASE` キーワードを追加する必要はありません。SQL の例は [Compaction タスクの手動トリガー](#compaction-タスクの手動トリガー)を参照してください。

- v4.1.4 以降では、削除行のあるタブレットに対し、自動トリガーの削除しきい値に達していなくても手動要求で Base Compaction を選択できます。
- v4.1.7 以降では、`lake_pk_compaction_base_enable_delete_trigger = false` の場合も手動 Base Compaction を実行できます。
- 実行時間の制御をサポートするバージョンでは、手動要求は自動 Base Compaction の禁止時間帯と最小間隔も回避します。
- 削除行がないタブレットは通常の Compaction 選択にフォールバックするため、対象となる入力がない場合もあります。

手動要求は非同期です。SQL が成功しても、要求が送信されたことを示すだけで、Compaction の完了を示すものではありません。スケジューリングとリソースの制限は引き続き適用され、1 回の要求でタブレット全体が Compaction されるとは限りません。

### 入力の選択とリソース消費 {/* #base-compaction-inputs */}

Base Compaction は削除率ではなく、Rowset の**絶対削除行数の降順**で入力を選択します。すべての Rowset が候補となるため、予算内であれば削除行のない Rowset もマージされます。

1 回の選択は、`lake_pk_compaction_max_input_rowsets`（デフォルト `500`）と、保持データ量の推定予算 `max(update_compaction_result_bytes, タブレットの総データ量 × update_compaction_ratio_threshold)` によって制限されます。後者の 2 つのパラメータのデフォルトは、それぞれ `1 GiB` と `0.5` です。Rowset 単位で選択するため、最後の Rowset によって推定値が予算を超える場合があります。また、この予算は実際の読み取りバイト数の上限ではありません。自動と手動の両方に入力制限が適用されるため、1 回の実行ですべての削除マークが除去されるとは限りません。

### 実行時間の制御 {/* #base-compaction-time-controls */}

以下の制御は v4.2 からサポートされています。**4.1 系列では、共有データテーブルに対するこれらの制限はサポートされていません**。

- テーブルプロパティ [`base_compaction_forbidden_time_ranges`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#ベースコンパクションの禁止) は、**自動 Base Compaction を禁止する時間帯**を指定します。それ以外の時間帯は許可されます。4.1 系列の FE は、共有データテーブルへのこのプロパティの設定を拒否します。
- CN パラメータ [`base_compaction_interval_seconds_since_last_operation`](../configuration/BE_parameters/stats_storage.md#base_compaction_interval_seconds_since_last_operation) は、同じタブレットで Base Compaction が成功した後、次の自動 Base Compaction を選択するまでの最小間隔を指定します。デフォルトは `86400` 秒です。毎日定時に実行する周期ではありません。過去の Base Compaction 時刻が記録されていない場合、または値が `0` 以下の場合は、間隔を制限しません。このパラメータは v4.1 より前から存在しますが、4.1 系列の共有データ PK Base Compaction は制限しません。

例えば、これらの制御をサポートするバージョンでのみ、毎日 08:00 以上 21:00 未満の自動 Base Compaction を禁止できます。

```sql
ALTER TABLE example_db.pk_table SET (
    "base_compaction_forbidden_time_ranges" = "* 8-20 * * *"
);
```

これらの制限は自動 Base Compaction だけに適用されます。通常の Cumulative Compaction は継続でき、手動の `ALTER TABLE ... COMPACT` は制限を回避します。

### 実行結果の確認 {/* #verify-base-compaction */}

[Compaction タスクの表示](#compaction-タスクの表示)で説明する `SHOW PROC '/compactions'` と `information_schema.be_cloud_native_compactions` を使用し、非同期タスクの状態と実行の詳細を確認します。Compaction スコアはスケジューリング用の指標であり、それだけですべての削除マークが除去されたとは判断できません。

Base Compaction は CN の CPU、メモリ、およびオブジェクトストレージの読み書きリソースを消費します。まず 1 つのパーティションで手動実行し、リソース消費を確認することを推奨します。Compaction 後も保持ポリシーによって過去のデータバージョンが残る場合があります。これらの回収は AutoVacuum と [`lake_autovacuum_grace_period_minutes`](../configuration/FE_parameters/shared_lake_other.md#lake_autovacuum_grace_period_minutes) にも依存し、タスク完了時にオブジェクトストレージの領域がすべて即時解放されるわけではありません。

## Compaction の管理

### Compaction スコアの表示

- 特定のテーブル内のパーティションの compaction スコアを表示するには、SHOW PROC ステートメントを使用します。通常、`MaxCS` フィールドに注目するだけで十分です。デフォルトのスケジュール設定では、`MaxCS` が 10 未満のパーティションは通常、自動 Compaction の候補から外れますが、すべての削除マークが除去されたことを意味しません。`MaxCS` が 100 を超える場合、Compaction スコアは比較的高いです。`MaxCS` が 500 を超える場合、Compaction スコアは非常に高く、手動での介入が必要になることがあります。

  ```Plain
  SHOW PARTITIONS FROM <table_name>
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

- システム定義ビュー `information_schema.partitions_meta` をクエリすることで、パーティションの compaction スコアを表示することもできます。

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

### Compaction タスクの表示

新しいデータがシステムにロードされると、FE は異なる CN ノードで実行される compaction タスクを継続的にスケジュールします。まず、FE 上の compaction タスクの一般的なステータスを表示し、次に CN 上の各タスクの実行詳細を表示できます。

#### Compaction タスクの一般的なステータスの表示

SHOW PROC ステートメントを使用して、compaction タスクの一般的なステータスを表示できます。

```SQL
SHOW PROC '/compactions';
```

例:

```Plain
mysql> SHOW PROC '/compactions';
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Partition           | TxnID | StartTime           | CommitTime          | FinishTime          | Error | Profile                                                                                                                                                                                                              |
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ssb.lineorder.10081 | 15    | 2026-01-10 03:29:07 | 2026-01-10 03:29:11 | 2026-01-10 03:29:12 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":219,"write_remote_sec":4,"in_queue_sec":18,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
| ssb.lineorder.10068 | 16    | 2026-01-10 03:29:07 | 2026-01-10 03:29:13 | 2026-01-10 03:29:14 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":218,"write_remote_sec":4,"in_queue_sec":38,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
| ssb.lineorder.10055 | 20    | 2026-01-10 03:29:11 | 2026-01-10 03:29:15 | 2026-01-10 03:29:17 | NULL  | {"sub_task_count":12,"read_local_sec":0,"read_local_mb":218,"read_remote_sec":0,"read_remote_mb":0,"read_segment_count":120,"write_segment_count":12,"write_segment_mb":218,"write_remote_sec":4,"in_queue_sec":23,"score_before":{"avg":10.0,"p50":10.0,"max":10.0},"score_after":{"avg":8.0,"p50":8.0,"max":8.0},"partial_success":false} |
+---------------------+-------+---------------------+---------------------+---------------------+-------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

返されるフィールドは次のとおりです。

- `Partition`: Compaction タスクが属するパーティション。
- `TxnID`: Compaction タスクに割り当てられたトランザクション ID。
- `StartTime`: Compaction タスクが開始される時間。`NULL` はタスクがまだ開始されていないことを示します。
- `CommitTime`: Compaction タスクがデータをコミットする時間。`NULL` はデータがまだコミットされていないことを示します。
- `FinishTime`: Compaction タスクがデータを公開する時間。`NULL` はデータがまだ公開されていないことを示します。
- `Error`: Compaction タスクのエラーメッセージ（ある場合）。
- `Profile`: (v3.2.12 および v3.3.4 からサポート) Compaction タスクが終了した後のプロファイル。
  - `sub_task_count`: パーティション内のサブタスク（タブレットに相当）の数。
  - `read_local_sec`: ローカルキャッシュからデータを読み取るすべてのサブタスクの合計時間。単位: 秒。
  - `read_local_mb`: ローカルキャッシュからすべてのサブタスクによって読み取られたデータの合計サイズ。単位: MB。
  - `read_remote_sec`: リモートストレージからデータを読み取るすべてのサブタスクの合計時間。単位: 秒。
  - `read_remote_mb`: リモートストレージからすべてのサブタスクによって読み取られたデータの合計サイズ。単位: MB。
  - `read_segment_count`: すべてのサブタスクによって読み取られたファイルの総数。
  - `write_segment_count`: すべてのサブタスクによって生成された新しいファイルの総数。
  - `write_segment_mb`: すべてのサブタスクによって生成された新しいファイルの合計サイズ。単位: MB。
  - `write_remote_sec`: リモートストレージにデータを書き込むすべてのサブタスクの合計時間。単位: 秒。
  - `in_queue_sec`: キューに滞在するすべてのサブタスクの合計時間。単位: 秒。
  - `score_before`: Compaction 前のパーティションの Compaction Score。`avg`、`p50`、`max` フィールドを含みます。
  - `score_after`: Compaction 後のパーティションの Compaction Score。`avg`、`p50`、`max` フィールドを含みます。
  - `partial_success`: Compaction ジョブが部分的に成功したかどうか（一部のタブレットが失敗）。

#### Compaction タスクの実行詳細の表示

`information_schema.be_cloud_native_compactions` の各行は、1 つの Tablet Compaction 実行単位を表します。
Tablet Parallel Compaction が有効な場合、各 Parallel Subtask は、同じ Transaction ID と Tablet ID、異なる
`SUBTASK_ID` を持つ別々の行として返されます。Parallel Compaction の merged context は実行単位ではなく
集約用オブジェクトであるため、返されません。

例:

```Plain
mysql> SELECT BE_ID, TXN_ID, TABLET_ID, VERSION, SKIPPED, RUNS, START_TIME, FINISH_TIME,
              PROGRESS, STATUS, PROFILE
       FROM information_schema.be_cloud_native_compactions;
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

返されるフィールドは次のとおりです。

- `BE_ID`: CN の ID。
- `TXN_ID`: サブタスクが属するトランザクションの ID。
- `TABLET_ID`: サブタスクが属するタブレットの ID。
- `VERSION`: タブレットのバージョン。
- `RUNS`: サブタスクが実行された回数。
- `START_TIME`: サブタスクが開始される時間。
- `FINISH_TIME`: サブタスクが終了する時間。
- `PROGRESS`: タブレットの compaction 進捗状況（パーセンテージ）。
- `STATUS`: サブタスクのステータス。エラーメッセージがある場合、このフィールドに返されます。
- `PROFILE`: (v3.2.12 および v3.3.4 からサポート) サブタスクの実行時プロファイル。
  - `read_local_sec`: ローカルキャッシュからデータを読み取るサブタスクの時間消費。単位: 秒。
  - `read_local_mb`: サブタスクによってローカルキャッシュから読み取られたデータのサイズ。単位: MB。
  - `read_remote_sec`: リモートストレージからデータを読み取るサブタスクの時間消費。単位: 秒。
  - `read_remote_mb`: サブタスクによってリモートストレージから読み取られたデータのサイズ。単位: MB。
  - `read_local_count`: サブタスクがローカルキャッシュからデータを読み取った回数。
  - `read_remote_count`: サブタスクがリモートストレージからデータを読み取った回数。
  - `in_queue_sec`: サブタスクがキューに滞在する時間。単位: 秒。
- `SUBTASK_ID`: 0 から始まる Parallel Subtask ID です。通常の非 Parallel Tablet Task では `NULL` です。
  `BE_ID`、`TXN_ID`、`TABLET_ID` が同じで `SUBTASK_ID` が異なる行は、同じ Tablet Compaction の Parallel
  実行単位です。

`PROFILE` のフルチェーンフィールドは、正確に時間を照合できるようナノ秒単位で記録されます。

- プロファイルの状態:
  - `profile_final`: タスクの実行中は `false`、完了後は `true` です。実行中のプロファイルでは、現在の
    attempt の経過時間を `task_total_ns` に加え、executor に入った後は現在の executor の経過時間も
    `task_execute_ns` に加えます。その他のフェーズタイマーは現在のスコープを抜けたときに更新されるため、
    実行中の `task_unaccounted_ns` は一時的に大きくなる場合があります。
- タスク全体の時間:
  - `queue_wait_ns`: タスクが CN の Compaction キューに入ってから worker が開始するまでの時間です。
    `task_total_ns` には含まれません。
  - `task_prepare_ns`: 入力 Rowset の選択、Tablet メタデータのロード、および Compaction Task の構築に
    使用した時間です。
  - `task_execute_ns`: Horizontal、Vertical、または Index Compaction executor 内の wall time です。
    実行中のプロファイルでは、現在の executor の経過時間も含まれます。
  - `task_total_ns`: CN worker がタスクの準備を開始してから、プロファイルのスナップショットまたはタスク
    の完了までの wall time です。
  - `task_accounted_ns`: 重複しない最上位フェーズの合計です。
  - `task_unaccounted_ns`: `task_total_ns - task_accounted_ns` です。値が大きい場合は、未計測のフェーズ、
    または scheduler/runtime のオーバーヘッドがあることを示します。
- 重複しない実行フェーズ: `input_prepare_ns`、`reader_prepare_ns`、`reader_open_ns`、
  `reader_get_next_ns`、`reader_close_ns`、`chunk_transform_ns`、`writer_create_ns`、`writer_open_ns`、
  `writer_write_ns`、`writer_flush_ns`、`writer_finish_ns`、`writer_close_ns`、`mask_io_ns`、
  `txn_log_build_ns`、`pk_sst_merge_ns`、`txn_log_write_ns`、`preload_compaction_state_ns`、
  `tablet_write_log_ns`。
- Reader 内部の詳細: `read_remote_ns`、`read_local_ns`、`create_segment_iter_ns`、`segment_init_ns`、
  `column_iterator_init_ns`、`block_load_ns`、`block_fetch_ns`、`block_seek_ns`、`decompress_ns`、
  `decode_dict_ns`、および Delete Vector/Filter の各タイマーです。これらは Reader フェーズに含まれるため、
  `task_accounted_ns` に再度加算しないでください。
- Vertical Compaction の詳細: `column_group_count`、`vertical_key_group_ns`、`vertical_value_group_ns`。
  Column Group の時間は Reader/Writer フェーズと重複する診断値であり、加算対象ではありません。

タスクが完了すると、同じ Tablet 単位のプロファイルが Tablet ID、Transaction ID、ステータス、Table ID、
Partition ID とともに CN INFO ログに書き込まれます。タスクが `be_cloud_native_compactions` から消えた後も、
個別タスクの詳細を追跡できます。

### Compaction タスクの設定

これらの FE および CN (BE) パラメータを使用して compaction タスクを設定できます。

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
- 説明: 共有データクラスタで許可される同時 Compaction タスクの最大数。この項目を `-1` に設定すると、同時タスク数を適応的に計算します。つまり、生存している CN ノードの数に 16 を掛けた数です。この値を `0` に設定すると、compaction が無効になります。
- 導入バージョン: v3.1.0

```SQL
ADMIN SET FRONTEND CONFIG ("lake_compaction_disable_ids" = "11111;22222");
```

##### lake_compaction_disable_ids

- デフォルト：""
- タイプ：String
- 単位：-
- 変更可能：はい
- 説明：特定のテーブルまたはパーティションの compaction を無効にします。これにより、すでに開始された compaction には影響しません。この項目の値はテーブル ID またはパーティション ID です。複数の値は ';' で区切られます。v3.4.5 および v3.5.0 より前のバージョンでは、この項目の名前は `lake_compaction_disable_tables` で、テーブル ID のみをサポートします。旧名は引き続きエイリアスとして使用できます。
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
- 説明: 同時 compaction タスクに使用されるスレッドの最大数。この設定は v3.1.7 および v3.2.2 以降で動的に変更可能になりました。
- 導入バージョン: v3.0.0

> **NOTE**
>
> 本番環境では、`compact_threads` を BE/CN CPU コア数の 25% に設定することを推奨します。

##### max_cumulative_compaction_num_singleton_deltas

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 単一の Cumulative Compaction でマージできるセグメントの最大数。compaction 中に OOM が発生した場合、この値を減らすことができます。
- 導入バージョン: -

> **NOTE**
>
> 本番環境では、`max_cumulative_compaction_num_singleton_deltas` を `100` に設定して、compaction タスクを加速し、そのリソース消費を減らすことを推奨します。

##### lake_pk_compaction_max_input_rowsets

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタ内の Primary Key テーブル compaction タスクで許可される入力 rowset の最大数。このパラメータのデフォルト値は v3.2.4 および v3.1.10 から `5` から `1000` に変更され、v3.3.1 および v3.2.9 から `500` に変更されました。Primary Key テーブルに対して Sized-tiered Compaction ポリシーが有効になった後（`enable_pk_size_tiered_compaction_strategy` を `true` に設定することで）、StarRocks は書き込み増幅を減らすために各 compaction の rowset 数を制限する必要がなくなります。したがって、このパラメータのデフォルト値が増加しました。
- 導入バージョン: v3.1.8, v3.2.3

> **NOTE**
>
> 本番環境では、`max_cumulative_compaction_num_singleton_deltas` を `100` に設定して、Compaction タスクを加速し、そのリソース消費を減らすことを推奨します。

##### lake_pk_compaction_max_rowsets_per_level

- デフォルト: 20
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタ内の Primary Key テーブルで、あるサイズレベルの compaction 待ちの rowset がこの数に達すると、そのレベルのスコアが最高でなくてもマージされます。Size-tiered Compaction ポリシーは各ラウンドでスコアが最も高いレベルのみをマージします。ロードが継続している間は、最新の小さな rowset のレベルが毎回最高スコアとなるため、より大きな rowset（以前の compaction の出力や中規模のロード）からなるレベルは増え続けます。その結果、クエリが読み取る segment 数が増え、compaction の完了直後に compaction スコアのスパイクとして現れます。選択されていないレベルの compaction 待ちの rowset がこの数に達し、かつそれらが選択済みの rowset と同じ compaction タスクに収まる場合、そのレベルはその compaction タスクでマージされます。`0` に設定するとこの動作は無効になります。
- 導入バージョン: -

### Compaction タスクの手動トリガー

以下のステートメントは手動要求を非同期に送信します。共有データ PK Base Compaction は v4.1.4 からサポートされています。v4.1.7 以降では、自動削除トリガーのスイッチを `false` にしても手動要求には影響しません。削除行がないタブレットは通常の Compaction 選択にフォールバックします。詳細は[主キーテーブルの Base Compaction](#primary-key-base-compaction)を参照してください。

```SQL
-- テーブル全体の compaction をトリガーします。
ALTER TABLE <table_name> COMPACT;
-- 特定のパーティションの compaction をトリガーします。
ALTER TABLE <table_name> COMPACT <partition_name>;
-- 複数のパーティションの compaction をトリガーします。
ALTER TABLE <table_name> COMPACT (<partition_name>, <partition_name>, ...);
```

### Compaction タスクのキャンセル

タスクのトランザクション ID を使用して、compaction タスクを手動でキャンセルできます。

```SQL
CANCEL COMPACTION WHERE TXN_ID = <TXN_ID>;
```

> **NOTE**
>
> - CANCEL COMPACTION ステートメントは Leader FE ノードから送信する必要があります。
> - CANCEL COMPACTION ステートメントは、コミットされていないトランザクションにのみ適用されます。つまり、`SHOW PROC '/compactions'` の返り値で `CommitTime` が NULL であるものです。
> - CANCEL COMPACTION は非同期プロセスです。タスクがキャンセルされたかどうかは `SHOW PROC '/compactions'` を実行して確認できます。

## ベストプラクティス

Compaction はクエリパフォーマンスにとって重要であるため、テーブルおよびパーティションのデータマージ状況を定期的に監視することを推奨します。以下はベストプラクティスとガイドラインです。

- ロード間の時間間隔を増やし（10 秒未満の間隔を避ける）、ロードごとのバッチサイズを増やします（100 行未満のバッチサイズを避ける）。
- CN 上の並列 compaction ワーカースレッドの数を調整して、タスクの実行を加速します。本番環境では、`compact_threads` を BE/CN CPU コア数の 25% に設定することを推奨します。
- `show proc '/compactions'` および `select * from information_schema.be_cloud_native_compactions;` を使用して Compaction タスクのステータスを監視します。
- Compaction スコアを監視し、それに基づいてアラートを設定します。StarRocks の組み込み Grafana 監視テンプレートにはこのメトリックが含まれています。
- Compaction 中のリソース消費、特にメモリ使用量に注意を払います。Grafana 監視テンプレートにもこのメトリックが含まれています。

## トラブルシューティング

### クエリの遅延

共有データクラスタでタイムリーでない Compaction によって引き起こされるクエリの遅延を特定するには、SQL プロファイルの単一の Fragment 内で `PhySegmentsCount` を `TabletCount` で割った値を確認します。これが数十以上の大きな値である場合、タイムリーでない Compaction がクエリの遅延の原因である可能性があります。

`PhySegmentsCount` は共有データスキャンでのみ提供されます。共有なしクラスタ、または `PhySegmentsCount` を含まない旧バージョンのプロファイルでは、代わりに単一の Fragment 内で `SegmentsReadCount` を `TabletCount` で割った値を使用します。共有データスキャンと共有なしスキャンの両方で、Tablet 内並列スキャンにより、複数の split reader が `SegmentsReadCount` で同じセグメントを重複してカウントする可能性があります。`PhySegmentsCount` がある場合は優先して使用してください。どちらのアーキテクチャでも、並列スキャンで代替カウンターを使用する場合、比率が高いことだけでは Compaction の問題があると判断できません。

### クラスタ内の高い Max Compaction スコア

1. `ADMIN SHOW FRONTEND CONFIG LIKE "%lake_compaction%"` および `SELECT * FROM information_schema.be_configs WHERE name = "compact_threads"` を使用して、Compaction 関連のパラメータが合理的な範囲内にあるかどうかを確認します。
2. `SHOW PROC '/compactions'` を使用して Compaction が停止しているかどうかを確認します。
   - `CommitTime` が NULL のままである場合、Compaction が停止している理由をシステムビュー `information_schema.be_cloud_native_compactions` で確認します。
   - `FinishTime` が NULL のままである場合、`TxnID` を使用して Leader FE ログで公開失敗の理由を検索します。
3. `SHOW PROC '/compactions'` を使用して compaction が遅れているかどうかを確認します。
   - `sub_task_count` が大きすぎる場合（`SHOW PARTITIONS` を使用してこのパーティション内の各タブレットのサイズを確認）、テーブルが不適切に作成されている可能性があります。
   - `read_remote_mb` が大きすぎる場合（読み取ったデータの合計の 30% を超える）、サーバーディスクサイズを確認し、`SHOW BACKENDS` を通じて `DataCacheMetrics` フィールドでキャッシュクォータを確認します。
   - `write_remote_sec` が大きすぎる場合（Compaction 全体の時間の 90% を超える）、リモートストレージへの書き込みが遅すぎる可能性があります。`single upload latency` および `multi upload latency` というキーワードを持つ共有データ専用の監視メトリックを確認することで確認できます。
   - `in_queue_sec` が大きすぎる場合（タブレットごとの平均待機時間が 60 秒を超える）、パラメータ設定が不合理であるか、他の実行中の Compaction が遅すぎる可能性があります。
