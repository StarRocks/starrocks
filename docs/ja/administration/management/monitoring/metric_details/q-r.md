---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - r"
---

# メトリクス q から r

## `query_cache_capacity`

- 説明: クエリキャッシュの容量。

## `query_cache_hit_count`

- 単位: カウント
- 説明: クエリキャッシュのヒット数。

## `query_cache_hit_ratio`

- 単位: -
- 説明: クエリキャッシュのヒット率。

## `query_cache_lookup_count`

- 単位: カウント
- 説明: クエリキャッシュのルックアップ総数。

## `query_cache_usage`

- 単位: バイト
- 説明: 現在のクエリキャッシュ使用量。

## `query_cache_usage_ratio`

- 単位: -
- 説明: 現在のクエリキャッシュ使用率。

## `query_mem_bytes`

- 単位: バイト
- 説明: クエリによって使用されるメモリ。

## `query_scan_bytes`

- 単位: バイト
- 説明: スキャンされたバイトの総数。

## `query_scan_bytes_per_second`

- 単位: バイト/秒
- 説明: 1秒あたりのスキャンされたバイトの推定レート。

## `query_scan_rows`

- 単位: カウント
- 説明: スキャンされた行の総数。

## `ranger_audit_init_failed`

- 単位: -
- タイプ: Gauge
- 説明: この FE が Ranger 監査の初期化に失敗したかどうか。`1` の場合、認可は引き続き動作しますが、監査設定を修正して再起動するまで、この FE は Ranger 監査イベントを一切出力しません。失敗した監査先は FE ログに記録されます。それ以外は `0` です。よくある原因は、FE に同梱されていない監査先を設定していることです。FE で利用できる Ranger 監査先は `file` と `solr` のみです。

## `readable_blocks_total (Deprecated)`

## `recycle_bin_database_num`

- 単位: 個
- 説明: FE カタログのリサイクルビンに現在保持されているデータベースの数。

## `recycle_bin_partition_num`

- 単位: 個
- 説明: FE カタログのリサイクルビンに現在保持されているパーティションの数。

## `recycle_bin_table_num`

- 単位: 個
- 説明: FE カタログのリサイクルビンに現在保持されているテーブルの数。

## `resource_group_bigquery_count`

- 単位: カウント
- 説明: 各リソースグループで、大規模クエリの制限をトリガーしたクエリの数。これは瞬時値です。

## `resource_group_concurrency_overflow_count`

- 単位: カウント
- 説明: 各リソースグループで、同時実行制限をトリガーしたクエリの数。これは瞬時値です。

## `resource_group_connector_scan_use_ratio (Deprecated)`

- 単位: -
- 説明: 各リソースグループが使用する外部テーブルスキャン・スレッドのタイムスライスの、すべてのリソースグループが使用する合計に対する比率。これは、2つのメトリクス取得間の時間間隔における平均値です。

## `resource_group_cpu_limit_ratio`

- 単位: -
- 説明: 各リソースグループのCPUコア制限の、すべてのリソースグループの合計CPUコア制限に対する比率。これは瞬時値です。

## `resource_group_cpu_use_ratio (Deprecated)`

- 単位: -
- 説明: 各リソースグループが使用するパイプライン・スレッドのタイムスライスの、すべてのリソースグループが使用する合計に対する比率。これは、2つのメトリクス取得間の時間間隔における平均値です。

## `resource_group_driver_queue_level_accu_time_ns`

- 単位: ナノ秒
- 説明: 各リソースグループのパイプライン実行キューで、各レベルの累積実行時間をそのレベルの係数で割った値。`level` ラベルはレベル (0〜7) を表し、Driver は実行時間が長くなるほど上位のレベルに移ります。次の Driver は、この値が最も小さい空でないレベルから取り出されます。レベルが空から空でない状態に変わると、この値は直近にスケジュールされたレベルの値以上に引き上げられます。 数値範囲を超えないように、必要に応じて全レベルとスケジューリングの基準値から共通の時刻原点を差し引きます。このゲージはその際に減少することがありますが、累積待機時間とスケジュール回数のカウンターは維持されます。

## `resource_group_driver_queue_level_schedule_count`

- 単位: カウント
- 説明: 各リソースグループのパイプライン実行キューで、各レベルから取り出された Driver の累積数。実行が残っている旧バージョンのリソースグループも含み、その削除後も最終的な累積値を保持します。同じ BE/CN プロセス内でリソースグループを更新、削除、または同じ名前で再作成しても、累積値は維持されます。

## `resource_group_driver_queue_level_wait_time_ns`

- 単位: ナノ秒
- 説明: 各リソースグループのパイプライン実行キューで、各レベルから取り出された Driver がレディキューで待機した累積時間。スケジュール回数のカウンターと同様に、実行が残っている旧バージョンも含み、同じ BE/CN プロセス内での更新、削除、同じ名前での再作成後も累積値を保持します。この値の増分を `resource_group_driver_queue_level_schedule_count` の増分で割ると、そのレベルの平均待機時間になります。

## `resource_group_inuse_cpu_cores`

- 単位: カウント
- 説明: 各リソースグループで現在使用されているCPUコアの推定数。これは、2つのメトリクス取得間の時間間隔における平均値です。

## `resource_group_mem_inuse_bytes`

- 単位: バイト
- 説明: 各リソースグループが現在使用しているメモリ量（バイト単位）。これは瞬間的な値です。

## `resource_group_mem_limit_bytes`

- 単位: バイト
- 説明: 各リソースグループのメモリ制限（バイト単位）。これは瞬間的な値です。

## `resource_group_running_queries`

- 単位: カウント
- 説明: 各リソースグループで現在実行中のクエリ数。これは瞬間的な値です。

## `resource_group_scan_use_ratio (Deprecated)`

- 単位: -
- 説明: 各リソースグループが使用する内部テーブルスキャン・スレッドのタイムスライスの、全リソースグループが使用する合計に対する比率。これは2つのメトリック取得間の時間間隔における平均値です。

## `resource_group_total_queries`

- 単位: カウント
- 説明: 各リソースグループで実行されたクエリの総数（現在実行中のものを含む）。これは瞬間的な値です。

## `result_block_queue_count`

- 単位: カウント
- 説明: 結果ブロックキュー内の結果数。

## `result_buffer_block_count`

- 単位: カウント
- 説明: 結果バッファ内のブロック数。

## `routine_load_task_count`

- 単位: カウント
- 説明: 現在実行中のルーチンロードタスクの数。

## `rowset_count_generated_and_in_use`

- 単位: カウント
- 説明: 現在使用中のrowset IDの数。

## `rowset_metadata_mem_bytes`

- 単位: バイト
- 説明: rowsetメタデータの総バイト数。

## `running_base_compaction_task_num`

- 単位: カウント
- 説明: 実行中のベースコンパクションタスクの総数。

## `running_cumulative_compaction_task_num`

- 単位: カウント
- 説明: 実行中の累積コンパクションの総数。

## `running_update_compaction_task_num`

- 単位: カウント
- 説明: 現在実行中のプライマリキーテーブルコンパクションタスクの総数。

