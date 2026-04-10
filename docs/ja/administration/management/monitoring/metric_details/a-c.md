---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical a - c"
---

# メトリクス a から c

:::note

マテリアライズドビューと共有データクラスターのメトリクスは、対応するセクションで詳しく説明されています。

- [非同期マテリアライズドビューのメトリクス](../metrics-materialized_view.md)
- [共有データダッシュボードのメトリクス、およびStarletダッシュボードのメトリクス](../metrics-shared-data.md)

StarRocksクラスターの監視サービスを構築する方法の詳細については、以下を参照してください。[監視とアラート](../Monitor_and_Alert.md)。

:::

## `active_scan_context_count`

- 単位: カウント
- 説明: Flink/Spark SQLによって作成されたスキャンタスクの総数。

## `async_delta_writer_queue_count`

- 単位: カウント
- 説明: タブレットデルタライタースレッドプール内のキューに入れられたタスク数。

## `base_compaction_task_byte_per_second`

- 単位: バイト/秒
- 説明: ベースコンパクションの推定レート。

## `base_compaction_task_cost_time_ms`

- 単位: ミリ秒
- 説明: ベースコンパクションに費やされた合計時間。

## `be_base_compaction_bytes_per_second`

- 単位: バイト/秒
- タイプ: 平均
- 説明: BEのベースコンパクション速度。

## `be_base_compaction_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのベースコンパクション失敗。

## `be_base_compaction_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのベースコンパクションリクエスト。

## `be_base_compaction_rowsets_per_second`

- 単位: カウント
- タイプ: 平均
- 説明: BEのrowsetのベースコンパクション速度。

## `be_broker_count`

- 単位: カウント
- タイプ: 平均
- 説明: ブローカーの数。

## `be_brpc_endpoint_count`

- 単位: カウント
- タイプ: 平均
- 説明: bRPCにおけるStubCacheの数。

## `be_bytes_read_per_second`

- 単位: バイト/秒
- タイプ: 平均
- 説明: BEの読み取り速度。

## `be_bytes_written_per_second`

- 単位: バイト/秒
- タイプ: 平均
- 説明: BEの書き込み速度。

## `be_clone_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEクローン失敗。

## `be_clone_total_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのクローンリクエスト。

## `be_create_rollup_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのマテリアライズドビュー作成失敗。

## `be_create_rollup_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのマテリアライズドビュー作成リクエスト。

## `be_create_tablet_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのタブレット作成失敗。

## `be_create_tablet_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのタブレット作成リクエスト。

## `be_cumulative_compaction_bytes_per_second`

- 単位: バイト/秒
- タイプ: 平均
- 説明: BEの累積コンパクション速度。

## `be_cumulative_compaction_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEの累積コンパクション失敗。

## `be_cumulative_compaction_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEの累積コンパクションリクエスト。

## `be_cumulative_compaction_rowsets_per_second`

- 単位: カウント
- タイプ: 平均
- 説明: BEのロウセットの累積コンパクション速度。

## `be_delete_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEの削除失敗。

## `be_delete_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEの削除リクエスト。

## `be_finish_task_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのタスク失敗。

## `be_finish_task_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのタスク完了リクエスト。

## `be_fragment_endpoint_count`

- 単位: カウント
- タイプ: 平均
- 説明: BEデータストリームの数。

## `be_fragment_request_latency_avg`

- 単位: ミリ秒
- タイプ: 平均
- 説明: フラグメントリクエストのレイテンシ。

## `be_fragment_requests_per_second`

- 単位: カウント/秒
- タイプ: 平均
- 説明: フラグメントリクエストの数。

## `be_http_request_latency_avg`

- 単位: ミリ秒
- タイプ: 平均
- 説明: HTTPリクエストのレイテンシ。

## `be_http_request_send_bytes_per_second`

- 単位: バイト/秒
- タイプ: 平均
- 説明: HTTPリクエストで送信されたバイト数。

## `be_http_requests_per_second`

- 単位: カウント/秒
- タイプ: 平均
- 説明: HTTPリクエストの数。

## `be_publish_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのバージョンリリース失敗。

## `be_publish_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのバージョン公開リクエスト。

## `be_report_disk_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのディスクレポート失敗。

## `be_report_disk_requests`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのディスクレポートリクエスト。

## `be_report_tables_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのテーブルレポート失敗。

## `be_report_tablet_failed`

- 単位: カウント/秒
- タイプ: 平均
- 説明: BEのタブレットレポート失敗。

## `be_report_tablet_requests`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのタブレットレポートリクエスト。

## `be_report_tablets_requests`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのタブレットレポートリクエスト。

## `be_report_task_failed`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのタスクレポート失敗。

## `be_report_task_requests`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのタスクレポートリクエスト。

## `be_schema_change_failed`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのスキーマ変更失敗。

## `be_schema_change_requests`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのスキーマ変更レポートリクエスト。

## `be_storage_migrate_requests`

- 単位: カウント/秒
- 種類: 平均
- 説明: BEのマイグレーションリクエスト。

## `binary_column_pool_bytes`

- 単位: バイト
- 説明: BINARYカラムプールが使用するメモリ。

## `bitmap_index_mem_bytes`

- 単位: バイト
- 説明: ビットマップインデックスが使用するメモリ。

## `block_cache_hit_bytes`

- 単位: バイト
- 種類: カウンター
- 説明: ブロックキャッシュヒットの累積バイト数。現在、外部テーブルのキャッシュヒットバイトのみがカウントされています。

## `block_cache_miss_bytes`

- 単位: バイト
- 種類: カウンター
- 説明: ブロックキャッシュミスの累積バイト数。現在、外部テーブルのキャッシュミスバイトのみがカウントされています。

## `blocks_created_total (Deprecated)`

## `blocks_deleted_total (Deprecated)`

## `blocks_open_reading (Deprecated)`

## `blocks_open_writing (Deprecated)`

## `bloom_filter_index_mem_bytes`

- 単位: バイト
- 説明: ブルームフィルターインデックスが使用するメモリ。

## `broker_count`

- 単位: カウント
- 説明: 作成されたファイルシステムブローカーの総数（ホストアドレス別）。

## `brpc_endpoint_stub_count`

- 単位: カウント
- 説明: bRPCスタブの総数（アドレス別）。

## `builtin_inverted_index_mem_bytes`

- 単位: バイト
- 説明: 組み込みの転置インデックスが使用するメモリ。

## `bytes_read_total (Deprecated)`

## `bytes_written_total`

- 単位: バイト
- 説明: 書き込まれた総バイト数（セクター書き込み * 512）。

## `central_column_pool_bytes (Deprecated)`

## `chunk_allocator_mem_bytes`

- 単位: バイト
- 説明: チャンクアロケーターが使用するメモリ。

## `chunk_pool_local_core_alloc_count`

- 単位: カウント
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `chunk_pool_other_core_alloc_count`

- 単位: カウント
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `chunk_pool_system_alloc_cost_ns`

- 単位: ns
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `chunk_pool_system_alloc_count`

- 単位: カウント
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `chunk_pool_system_free_cost_ns`

- 単位: ns
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `chunk_pool_system_free_count`

- 単位: カウント
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

## `clone_mem_bytes`

- 単位: バイト
- 説明: レプリカクローンに使用されるメモリ。

## `column_metadata_mem_bytes`

- 単位: バイト
- 説明: カラムメタデータが使用するメモリ。

## `column_partial_update_apply_duration_us`

- 単位: us
- 説明: カラムのAPPLYタスク（カラムモード）の部分更新に費やされた合計時間。

## `column_partial_update_apply_total`

- 単位: カウント
- 説明: カラムごとの部分更新に対するAPPLYの合計数（カラムモード）

## `column_pool_mem_bytes`

- 単位: バイト
- 説明: カラムプールが使用するメモリ。

## `column_zonemap_index_mem_bytes`

- 単位: バイト
- 説明: カラムゾーンマップが使用するメモリ。

## `compaction_bytes_total`

- 単位: バイト
- 説明: ベースコンパクションと累積コンパクションからの合計マージ済みバイト数。

## `compaction_deltas_total`

- 単位: カウント
- 説明: ベースコンパクションと累積コンパクションからの合計マージ済み行セット数。

## `compaction_mem_bytes`

- 単位: バイト
- 説明: コンパクションが使用するメモリ。

## `consistency_mem_bytes`

- 単位: バイト
- 説明: レプリカ整合性チェックが使用するメモリ。

## `cpu`

- 単位: -
- 説明: `/proc/stat`によって返されるCPU使用率情報。

## `cpu_guest`

- 単位: -
- タイプ: 平均
- 説明: cpu_guest使用率。

## `cpu_idle`

- 単位: -
- 種類: 平均
- 説明: cpu_idle 使用率。

## `cpu_iowait`

- 単位: -
- 種類: 平均
- 説明: cpu_iowait 使用率。

## `cpu_irq`

- 単位: -
- 種類: 平均
- 説明: cpu_irq 使用率。

## `cpu_nice`

- 単位: -
- 種類: 平均
- 説明: cpu_nice 使用率。

## `cpu_softirq`

- 単位: -
- 種類: 平均
- 説明: cpu_softirq 使用率。

## `cpu_steal`

- 単位: -
- 種類: 平均
- 説明: cpu_steal 使用率。

## `cpu_system`

- 単位: -
- 種類: 平均
- 説明: cpu_system 使用率。

## `cpu_user`

- 単位: -
- 種類: 平均
- 説明: cpu_user 使用率。

## `cpu_util`

- 単位: -
- 種類: 平均
- 説明: CPU 使用率。

## `cumulative_compaction_task_byte_per_second`

- 単位: バイト/秒
- 説明: 累積コンパクション中に処理されたバイトのレート。

## `cumulative_compaction_task_cost_time_ms`

- 単位: ミリ秒
- 説明: 累積コンパクションに費やされた合計時間。
