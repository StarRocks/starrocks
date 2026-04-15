---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - z"
---

# メトリクス q から z

:::note

マテリアライズドビューと共有データクラスターのメトリクスは、対応するセクションで詳しく説明されています。

- [非同期マテリアライズドビューのメトリクス](../metrics-materialized_view.md)
- [共有データダッシュボードのメトリクスとStarletダッシュボードのメトリクス](../metrics-shared-data.md)

StarRocksクラスターの監視サービスを構築する方法の詳細については、以下を参照してください。[監視とアラート](../Monitor_and_Alert.md)。

:::

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

## `readable_blocks_total (Deprecated)`

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

## `schema_change_mem_bytes`

- 単位: バイト
- 説明: スキーマ変更に使用されるメモリ。

## `segment_flush_queue_count`

- 単位: カウント
- 説明: セグメントフラッシュスレッドプール内のキューに入れられたタスクの数。

## `segment_metadata_mem_bytes`

- 単位: バイト
- 説明: セグメントメタデータが使用するメモリ。

## `segment_read`

- 単位: カウント
- 説明: セグメント読み取りの総数。

## `segment_replicate_queue_count`

- 単位: カウント
- 説明: セグメントレプリケートスレッドプール内のキューに入れられたタスク数。

## `segment_zonemap_mem_bytes`

- 単位: バイト
- 説明: セグメントゾーンマップが使用するメモリ。

## `short_key_index_mem_bytes`

- 単位: バイト
- 説明: ショートキーインデックスが使用するメモリ。

## `small_file_cache_count`

- 単位: カウント
- 説明: 小さなファイルキャッシュの数。

## `snmp`

- 単位: -
- 説明: `/proc/net/snmp`によって返されるメトリクス。

## `starrocks_be_clone_task_copy_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: BEノードのクローンタスクによってコピーされた合計ファイルサイズ。INTER_NODEとINTRA_NODEの両方のタイプを含む。

## `starrocks_be_clone_task_copy_duration_ms`

- 単位: ミリ秒
- タイプ: 累積
- 説明: BEノードのクローンタスクによって消費されたコピーの合計時間。INTER_NODEとINTRA_NODEの両方のタイプを含む。

## `starrocks_be_exec_state_report_active_threads`

- 単位: カウント
- タイプ: 瞬間
- 説明: フラグメントインスタンスの実行ステータスを報告するスレッドプールで実行中のタスクの数。

## `starrocks_be_exec_state_report_queue_count`

- 単位: カウント
- タイプ: 瞬間
- 説明: フラグメントインスタンスの実行ステータスを報告するスレッドプールでキューに入れられているタスクの数（最大1000）。

## `starrocks_be_exec_state_report_running_threads`

- 単位: カウント
- タイプ: 瞬間
- 説明: フラグメントインスタンスの実行ステータスを報告するスレッドプールのスレッド数（最小1、最大2）。

## `starrocks_be_exec_state_report_threadpool_size`

- 単位: カウント
- タイプ: 瞬間
- 説明: フラグメントインスタンスの実行ステータスを報告するスレッドプールの最大スレッド数（デフォルトは2）。

## `starrocks_be_files_scan_num_bytes_read`

- 単位: バイト
- 説明: 外部ストレージから読み取られた合計バイト数。ラベル: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_files_read`

- 単位: カウント
- 説明: 外部ストレージから読み取られたファイルの数（CSV、Parquet、ORC、JSON、Avro）。ラベル: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_raw_rows_read`

- 単位: カウント
- 説明: 形式検証と述語フィルタリングの前に外部ストレージから読み取られた合計生行数。ラベル: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_rows_return`

- 単位: カウント
- 説明: 述語フィルタリング後に返された行数。ラベル: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_valid_rows_read`

- 単位: カウント
- 説明: 読み取られた有効な行数（無効な形式の行を除く）。ラベル: `file_format`, `scan_type`。

## `starrocks_be_mem_pool_mem_limit_bytes`

- 単位: バイト
- タイプ: 瞬間
- 説明: 各メモリプールのメモリ制限（バイト単位）。

## `starrocks_be_mem_pool_mem_usage_bytes`

- 単位: バイト
- タイプ: 瞬間
- 説明: 各メモリプールで現在使用されている合計メモリ（バイト単位）。

## `starrocks_be_mem_pool_mem_usage_ratio`

- 単位: -
- タイプ: 瞬間
- 説明: メモリプールのメモリ使用量とメモリプールのメモリ制限の比率。

## `starrocks_be_mem_pool_workgroup_count`

- 単位: カウント
- タイプ: 瞬間値
- 説明: 各メモリプールに割り当てられたリソースグループの数。

## `starrocks_be_pipe_prepare_pool_queue_len`

- 単位: カウント
- タイプ: 瞬間値
- 説明: パイプライン準備スレッドプールタスクキューの長さの瞬間値。

## `starrocks_be_priority_exec_state_report_active_threads`

- 単位: カウント
- タイプ: 瞬間値
- 説明: Fragmentインスタンスの最終実行状態を報告するスレッドプールで実行されているタスクの数。

## `starrocks_be_priority_exec_state_report_queue_count`

- 単位: カウント
- タイプ: 瞬間値
- 説明: Fragmentインスタンスの最終実行ステータスを報告するスレッドプールでキューに入れられたタスクの数（最大2147483647）。

## `starrocks_be_priority_exec_state_report_running_threads`

- 単位: カウント
- タイプ: 瞬間値
- 説明: Fragmentインスタンスの最終実行ステータスを報告するスレッドプールのスレッド数（最小1、最大2）。

## `starrocks_be_priority_exec_state_report_threadpool_size`

- 単位: カウント
- タイプ: 瞬間値
- 説明: Fragmentインスタンスの最終実行ステータスを報告するスレッドプールの最大スレッド数（デフォルトは2）。

## `starrocks_be_resource_group_cpu_limit_ratio`

- 単位: -
- タイプ: 瞬間値
- 説明: リソースグループのCPUクォータ比率の瞬間値。

## `starrocks_be_resource_group_cpu_use_ratio`

- 単位: -
- タイプ: 平均
- 説明: リソースグループが使用したCPU時間とすべてのリソースグループのCPU時間の比率。

## `starrocks_be_resource_group_mem_inuse_bytes`

- 単位: バイト
- タイプ: 瞬間値
- 説明: リソースグループのメモリ使用量の瞬間値。

## `starrocks_be_resource_group_mem_limit_bytes`

- 単位: バイト
- タイプ: 瞬間値
- 説明: リソースグループのメモリクォータの瞬間値。

## `starrocks_be_segment_file_not_found_total`

- 単位: カウント
- 説明: セグメントオープン中にセグメントファイルが見つからなかった（ファイルが見つからない）合計回数。継続的に増加する値は、データ損失またはストレージの不整合を示している可能性があります。

## `starrocks_be_staros_shard_info_fallback_total`

- 単位: カウント
- タイプ: 累積
- 説明: 共有データモード専用。BE の StarOSWorker が、ローカルキャッシュに必要な shard 情報が存在しない（クエリ／コンパクション／lake 操作がその shard を参照する前に、FE から当該 BE へ shard がプッシュされていない）ために、starmgr に対して実際に発行した RPC（`g_starlet->get_shard_info()`）の総数。starlet の ready チェックが通過し、RPC が実際にディスパッチされた場合のみカウントされます。starlet が未 ready のタイムアウトは含まれません。正常時はほぼゼロに近い値となることが期待されます。継続的または増加傾向のレートは、FE 側のタスク／ノード選択がまだ shard を保持していない BE に処理をスケジュールしている、あるいは FE 側からの shard プッシュの伝搬が遅延していることを示す強いシグナルです。推奨アラート: BE ごとの 5 分間ウィンドウでのレートが高い場合。

## `starrocks_be_staros_shard_info_fallback_failed_total`

- 単位: カウント
- タイプ: 累積
- 説明: 共有データモード専用。`starrocks_be_staros_shard_info_fallback_total` のうち、starmgr RPC が非 OK ステータスを返したサブセット。`failed_total / fallback_total` の比率を用いることで、starmgr の一時的なエラーと、正常に成功したフォールバックを区別してアラートを設定できます。

## `starrocks_fe_clone_task_copy_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: クラスター内のクローンタスクによってコピーされたファイルの合計サイズ（INTER_NODEタイプとINTRA_NODEタイプの両方を含む）。

## `starrocks_fe_clone_task_copy_duration_ms`

- 単位: ミリ秒
- タイプ: 累積
- 説明: クラスター内のクローンタスクによって消費されたコピーの合計時間（INTER_NODEタイプとINTRA_NODEタイプの両方を含む）。

## `starrocks_fe_clone_task_success`

- 単位: カウント
- タイプ: 累積
- 説明: クラスター内で正常に実行されたクローンタスクの数。

## `starrocks_fe_clone_task_total`

- 単位: カウント
- タイプ: 累積
- 説明: クラスター内のクローンタスクの総数。

## `starrocks_fe_last_finished_job_timestamp`

- 単位: ms
- タイプ: 瞬間
- 説明: 特定のウェアハウスにおける最後のクエリまたはロードの終了時間を示します。シェアードナッシングクラスターの場合、この項目はデフォルトのウェアハウスのみを監視します。

## `starrocks_fe_memory_usage`

- 単位: バイトまたはカウント
- タイプ: 瞬間
- 説明: 特定のウェアハウスにおける様々なモジュールのメモリ統計を示します。シェアードナッシングクラスターの場合、この項目はデフォルトのウェアハウスのみを監視します。

## `starrocks_fe_meta_log_count`

- 単位: カウント
- タイプ: 瞬間
- 説明: チェックポイントなしの編集ログの数。`100000` の範囲内の値は妥当と見なされます。

## `starrocks_fe_publish_version_daemon_loop_total`

- 単位: カウント
- タイプ: 累積
- 説明: このFEノードでの `publish-version-daemon` ループ実行の総数。

以下のメトリクスは、トランザクションの異なるフェーズにおけるレイテンシ分布を提供する `summary` タイプのメトリクスです。これらのメトリクスは、リーダーFEノードによってのみ報告されます。

各メトリクスには以下の出力が含まれます。

- **クォンタイル**: 異なるパーセンタイル境界でのレイテンシ値。これらは `quantile` ラベルを介して公開され、`0.75`、`0.95`、`0.98`、`0.99`、および `0.999` の値を持つことができます。
- **`<metric_name>_sum`**: このフェーズで費やされた累積時間の合計。例: `starrocks_fe_txn_total_latency_ms_sum`。
- **`<metric_name>_count`**: このフェーズで記録されたトランザクションの総数。例: `starrocks_fe_txn_total_latency_ms_count`。

すべてのトランザクションメトリクスは以下のラベルを共有します。

- `type`: トランザクションをロードジョブのソースタイプ（例: `all`、`stream_load`、`routine_load`）で分類します。これにより、トランザクション全体のパフォーマンスと特定のロードタイプのパフォーマンスの両方を監視できます。報告されるグループはFEパラメータで設定できます。[`txn_latency_metric_report_groups`](../../FE_configuration.md#txn_latency_metric_report_groups)。
- `is_leader`: 報告元のFEノードがリーダーであるかどうかを示します。リーダーFE (`is_leader="true"`) のみが実際のメトリクス値を報告します。フォロワーは `is_leader="false"` となり、データは報告しません。

## `starrocks_fe_query_resource_group`

- 単位: カウント
- タイプ: 累積
- 説明: 特定のリソースグループで実行されたクエリの総数を示します。

## `starrocks_fe_query_resource_group`

- 単位: カウント
- タイプ: 累積
- 説明: 各リソースグループのクエリ数。

## `starrocks_fe_query_resource_group_err`

- 単位: カウント
- タイプ: 累積
- 説明: 特定のリソースグループで失敗したクエリの数を示します。

## `starrocks_fe_query_resource_group_err`

- 単位: カウント
- タイプ: 累積
- 説明: 各リソースグループの不正なクエリ数。

## `starrocks_fe_query_resource_group_latency`

- 単位: ms
- タイプ: 累積
- 説明: 特定のリソースグループ下のクエリのレイテンシ統計を示します。

## `starrocks_fe_query_resource_group_latency`

- 単位: 秒
- タイプ: 平均
- 説明: 各リソースグループのクエリレイテンシパーセンタイル。

## `starrocks_fe_routine_load_error_rows`

- 単位: カウント
- 説明: すべてのルーチンロードジョブによるデータロード中に発生したエラー行の総数。

## `starrocks_fe_routine_load_jobs`

- 単位: カウント
- 説明: 異なる状態にあるルーチンロードジョブの総数。例:

  ```plaintext
  starrocks_fe_routine_load_jobs{state="NEED_SCHEDULE"} 0
  starrocks_fe_routine_load_jobs{state="RUNNING"} 1
  starrocks_fe_routine_load_jobs{state="PAUSED"} 0
  starrocks_fe_routine_load_jobs{state="STOPPED"} 0
  starrocks_fe_routine_load_jobs{state="CANCELLED"} 1
  starrocks_fe_routine_load_jobs{state="UNSTABLE"} 0
  ```

## `starrocks_fe_routine_load_max_lag_of_partition`

- 単位: -
- 説明: 各ルーチンロードジョブのKafkaパーティションオフセットラグの最大値。これは、FE設定 `enable_routine_load_lag_metrics` が `true` に設定され、オフセットラグがFE設定 `min_routine_load_lag_for_metrics` 以上の場合にのみ収集されます。デフォルトでは、`enable_routine_load_lag_metrics` は `false` であり、`min_routine_load_lag_for_metrics` は `10000` です。

## `starrocks_fe_routine_load_max_lag_time_of_partition`

- 単位: 秒
- 説明: 各ルーチンロードジョブのKafkaパーティションオフセットタイムスタンプラグの最大値。これは、FE設定 `enable_routine_load_lag_time_metrics` が `true` に設定されている場合にのみ収集されます。デフォルトでは、`enable_routine_load_lag_time_metrics` は `false` です。

## `starrocks_fe_routine_load_paused`

- 単位: カウント
- 説明: ルーチンロードジョブが一時停止された総回数。

## `starrocks_fe_routine_load_receive_bytes`

- 単位: バイト
- 説明: すべてのルーチンロードジョブによってロードされたデータの総量。

## `starrocks_fe_routine_load_rows`

- 単位: カウント
- 説明: すべてのルーチンロードジョブによってロードされた行の総数。

## `starrocks_fe_safe_mode`

- 単位: -
- タイプ: 瞬間
- 説明: セーフモードが有効になっているかどうかを示します。有効な値: `0` (無効) および `1` (有効)。セーフモードが有効な場合、クラスターはロードリクエストを受け入れなくなります。

## `starrocks_fe_scheduled_pending_tablet_num`

- 単位: カウント
- タイプ: 瞬間
- 説明: FEによってスケジュールされた、BALANCEタイプとREPAIRタイプの両方を含む、保留状態のクローンタスクの数。

## `starrocks_fe_scheduled_running_tablet_num`

- 単位: カウント
- タイプ: 瞬間
- 説明: FEによってスケジュールされた、BALANCEタイプとREPAIRタイプの両方を含む、実行状態のクローンタスクの数。

## `starrocks_fe_slow_lock_held_time_ms`

- 単位: ミリ秒
- タイプ: サマリー
- 説明: スローロックが検出された際のロック保持時間 (ミリ秒単位) を追跡するヒストグラム。このメトリックは、ロック待機時間が `slow_lock_threshold_ms` 設定パラメータを超えたときに更新されます。スローロックイベントが検出された際、すべてのロック所有者の中で最大のロック保持時間を追跡します。各メトリックには、分位値 (0.75, 0.95, 0.98, 0.99, 0.999)、`_sum`、および `_count` の出力が含まれます。注: このメトリックは、待機時間がしきい値を超えると更新されますが、所有者が操作を完了してロックを解放するまで保持時間が増加し続ける可能性があるため、高い競合下での正確なロック保持時間を反映しない場合があります。ただし、デッドロックが発生した場合でもこのメトリックは更新されます。

## `starrocks_fe_slow_lock_wait_time_ms`

- 単位: ミリ秒
- タイプ: サマリー
- 説明: スローロックが検出された際のロック待機時間 (ミリ秒単位) を追跡するヒストグラム。このメトリックは、ロック待機時間が `slow_lock_threshold_ms` 設定パラメータを超えたときに更新されます。ロック競合シナリオにおいて、スレッドがロックを取得するためにどれだけ長く待機するかを正確に追跡します。各メトリックには、分位値 (0.75, 0.95, 0.98, 0.99, 0.999)、`_sum`、および `_count` の出力が含まれます。このメトリックは正確な待機時間測定を提供します。注: このメトリックはデッドロックが発生した場合には更新できないため、デッドロック状況の検出には使用できません。

## `starrocks_fe_sql_block_hit_count`

- 単位: カウント
- 説明: ブラックリストに登録されたSQLがインターセプトされた回数。

## `starrocks_fe_tablet_max_compaction_score`

- 単位: カウント
- タイプ: 瞬間
- 説明: 各BEノードにおける最高のコンパクションスコアを示します。

## `starrocks_fe_tablet_num`

- 単位: カウント
- タイプ: 瞬間的
- 説明: 各BEノード上のタブレット数を示します。

## `starrocks_fe_txn_publish_ack_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: トランザクションが`VISIBLE`としてマークされたときの、`ready-to-finish`時間から最終的な`finish`時間までの最終確認レイテンシ。このメトリックには、トランザクションが完了準備ができた後の最終確認ステップが含まれます。

## `starrocks_fe_txn_publish_can_finish_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: `publish`タスクの完了から`canTxnFinish()`が最初にtrueを返す瞬間までのレイテンシで、`publish version finish`時間から`ready-to-finish`時間までで測定されます。

## `starrocks_fe_txn_publish_execute_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: `publish`タスクのアクティブな実行時間で、タスクが選択されてから完了するまでです。このメトリックは、トランザクションの変更を可視化するために実際に費やされた時間を表します。

## `starrocks_fe_txn_publish_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: `publish`フェーズのレイテンシで、`commit`時間から`finish`時間までです。これは、コミットされたトランザクションがクエリに対して可視になるまでの期間です。`schedule`、`execute`、`can_finish`、および`ack`のサブフェーズの合計です。

## `starrocks_fe_txn_publish_schedule_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: トランザクションがコミットされてから公開されるのを待つ時間で、`commit`時間から公開タスクが選択されるまで測定されます。このメトリックは、`publish`パイプラインにおけるスケジューリング遅延またはキューイング時間を反映します。

## `starrocks_fe_txn_total_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: トランザクションが完了するまでの合計レイテンシで、`prepare`時間から`finish`時間まで測定されます。このメトリックは、トランザクションの完全なエンドツーエンドの期間を表します。

## `starrocks_fe_txn_write_latency_ms`

- 単位: ms
- タイプ: 概要
- 説明: トランザクションの`write`フェーズのレイテンシで、`prepare`時間から`commit`時間までです。このメトリックは、トランザクションが公開準備が整う前のデータ書き込みおよび準備段階のパフォーマンスを分離します。

## `starrocks_fe_unfinished_backup_job`

- 単位: カウント
- タイプ: 瞬間的
- 説明: 特定のウェアハウスで実行中のBACKUPタスクの数を示します。共有なしクラスターの場合、この項目はデフォルトのウェアハウスのみを監視します。共有データクラスターの場合、この値は常に`0`です。

## `starrocks_fe_unfinished_query`

- 単位: カウント
- タイプ: 瞬間的
- 説明: 特定のウェアハウスで現在実行中のクエリの数を示します。共有なしクラスターの場合、この項目はデフォルトのウェアハウスのみを監視します。

## `starrocks_fe_unfinished_restore_job`

- 単位: カウント
- タイプ: 瞬間的
- 説明: 特定のウェアハウスで実行中のRESTOREタスクの数を示します。共有なしクラスターの場合、この項目はデフォルトのウェアハウスのみを監視します。共有データクラスターの場合、この値は常に`0`です。

## `storage_page_cache_mem_bytes`

- 単位: バイト
- 説明: ストレージページキャッシュによって使用されるメモリ。

## `stream_load`

- 単位: -
- 説明: ロードされた行の合計と受信バイト数。

## `stream_load_pipe_count`

- 単位: カウント
- 説明: 現在実行中のストリームロードタスクの数。

## `streaming_load_bytes`

- 単位: バイト
- 説明: ストリームロードによってロードされた合計バイト数。

## `streaming_load_current_processing`

- 単位: カウント
- 説明: 現在実行中のStream Loadタスクの数。

## `streaming_load_duration_ms`

- 単位: ms
- 説明: Stream Loadに費やされた合計時間。

## `streaming_load_requests_total`

- 単位: カウント
- 説明: Stream Loadリクエストの合計数。

##### SPLIT

## `tablet_base_max_compaction_score`

- 単位: -
- 説明: このBE内のタブレットの最高ベースコンパクションスコア。

## `tablet_cumulative_max_compaction_score`

- 単位: -
- 説明: このBE内のタブレットの最高累積コンパクションスコア。

## `tablet_metadata_mem_bytes`

- 単位: バイト
- 説明: タブレットメタデータが使用するメモリ。

## `tablet_schema_mem_bytes`

- 単位: バイト
- 説明: タブレットスキーマが使用するメモリ。

## `tablet_update_max_compaction_score`

- 単位: -
- 説明: 現在のBEにおけるプライマリキーテーブル内のタブレットの最高コンパクションスコア。

## `thrift_connections_total`

- 単位: カウント
- 説明: thrift接続の合計数（完了した接続を含む）。

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- 単位: カウント
- 説明: 現在開かれているthriftクライアントの数。

## `thrift_used_clients`

- 単位: カウント
- 説明: 現在使用中のthriftクライアントの数。

## `total_column_pool_bytes (Deprecated)`

## `transaction_streaming_load_bytes`

- 単位: バイト
- 説明: トランザクションロードの合計ロードバイト数。

## `transaction_streaming_load_current_processing`

- 単位: カウント
- 説明: 現在実行中のトランザクションStream Loadタスクの数。

## `transaction_streaming_load_duration_ms`

- 単位: ms
- 説明: Stream Loadトランザクションインターフェースに費やされた合計時間。

## `transaction_streaming_load_requests_total`

- 単位: カウント
- 説明: トランザクションロードリクエストの合計数。

## `txn_request`

- 単位: -
- 説明: BEGIN、COMMIT、ROLLBACK、EXECのトランザクションリクエスト。

## `uint8_column_pool_bytes`

- 単位: バイト
- 説明: UINT8カラムプールが使用するバイト数。

## `unused_rowsets_count`

- 単位: カウント
- 説明: 未使用の行セットの合計数。これらの行セットは後で再利用されます。

## `update_apply_queue_count`

- 単位: カウント
- 説明: プライマリキーテーブルトランザクションAPPLYスレッドプール内のキューイングされたタスク数。

## `update_compaction_duration_us`

- 単位: us
- Description: Primary Keyテーブルのコンパクションに費やされた合計時間。

## `update_compaction_outputs_bytes_total`

- Unit: バイト
- Description: Primary Keyテーブルのコンパクションによって書き込まれた合計バイト数。

## `update_compaction_outputs_total`

- Unit: カウント
- Description: Primary Keyテーブルのコンパクションの合計数。

## `update_compaction_task_byte_per_second`

- Unit: バイト/秒
- Description: Primary Keyテーブルのコンパクションの推定レート。

## `update_compaction_task_cost_time_ns`

- Unit: ns
- Description: Primary Keyテーブルのコンパクションに費やされた合計時間。

## `update_del_vector_bytes_total`

- Unit: バイト
- Description: Primary KeyテーブルでDELETEベクトルをキャッシュするために使用された合計メモリ。

## `update_del_vector_deletes_new`

- Unit: カウント
- Description: Primary Keyテーブルで使用された新しく生成されたDELETEベクトルの合計数。

## `update_del_vector_deletes_total (Deprecated)`

## `update_del_vector_dels_num (Deprecated)`

## `update_del_vector_num`

- Unit: カウント
- Description: Primary Keyテーブル内のDELETEベクトルキャッシュアイテムの数。

## `update_mem_bytes`

- Unit: バイト
- Description: Primary KeyテーブルのAPPLYタスクとPrimary Keyインデックスによって使用されるメモリ。

## `update_primary_index_bytes_total`

- Unit: バイト
- Description: Primary Keyインデックスの合計メモリコスト。

## `update_primary_index_num`

- Unit: カウント
- Description: メモリにキャッシュされたPrimary Keyインデックスの数。

## `update_rowset_commit_apply_duration_us`

- Unit: us
- Description: Primary KeyテーブルのAPPLYタスクに費やされた合計時間。

## `update_rowset_commit_apply_total`

- Unit: カウント
- Description: Primary KeyテーブルのCOMMITおよびAPPLYの合計数。

## `update_rowset_commit_request_failed`

- Unit: カウント
- Description: Primary Keyテーブルでの失敗した行セットCOMMITリクエストの合計数。

## `update_rowset_commit_request_total`

- Unit: カウント
- Description: Primary Keyテーブルでの行セットCOMMITリクエストの合計数。

## `wait_base_compaction_task_num`

- Unit: カウント
- Description: 実行を待機しているベースコンパクションタスクの数。

## `wait_cumulative_compaction_task_num`

- Unit: カウント
- Description: 実行を待機している累積コンパクションタスクの数。

## `writable_blocks_total (Deprecated)`
