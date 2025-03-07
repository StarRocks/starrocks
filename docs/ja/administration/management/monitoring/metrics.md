---
displayed_sidebar: docs
---

# 一般的なモニタリングメトリクス

このトピックでは、StarRocks の重要な一般メトリクスを紹介します。

マテリアライズドビューや共有データクラスタに特化したメトリクスについては、該当するセクションを参照してください。

- [非同期マテリアライズドビューのメトリクス](./metrics-materialized_view.md)
- [共有データダッシュボードメトリクス、および Starlet ダッシュボードメトリクス](./metrics-shared-data.md)

StarRocks クラスタのモニタリングサービスの構築方法については、[モニタリングとアラート](./Monitor_and_Alert.md)を参照してください。

## メトリック項目

### be_broker_count

- 単位: Count
- タイプ: Average
- 説明: ブローカーの数。

### be_brpc_endpoint_count

- 単位: Count
- タイプ: Average
- 説明: bRPC の StubCache の数。

### be_bytes_read_per_second

- 単位: Bytes/s
- タイプ: Average
- 説明: BE の読み取り速度。

### be_bytes_written_per_second

- 単位: Bytes/s
- タイプ: Average
- 説明: BE の書き込み速度。

### be_base_compaction_bytes_per_second

- 単位: Bytes/s
- タイプ: Average
- 説明: BE のベース Compaction 速度。

### be_cumulative_compaction_bytes_per_second

- 単位: Bytes/s
- タイプ: Average
- 説明: BE の累積 Compaction 速度。

### be_base_compaction_rowsets_per_second

- 単位: Count
- タイプ: Average
- 説明: BE のベース Compaction の rowset 速度。

### be_cumulative_compaction_rowsets_per_second

- 単位: Count
- タイプ: Average
- 説明: BE の累積 Compaction の rowset 速度。

### be_base_compaction_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のベース Compaction の失敗。

### be_clone_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のクローン失敗。

### be_create_rollup_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のマテリアライズドビュー作成失敗。

### be_create_tablet_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE の tablet 作成失敗。

### be_cumulative_compaction_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE の累積 Compaction の失敗。

### be_delete_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE の削除失敗。

### be_finish_task_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のタスク失敗。

### be_publish_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のバージョンリリース失敗。

### be_report_tables_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のテーブルレポート失敗。

### be_report_disk_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のディスクレポート失敗。

### be_report_tablet_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE の tablet レポート失敗。

### be_report_task_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE のタスクレポート失敗。

### be_schema_change_failed

- 単位: Count/s
- タイプ: Average
- 説明: BE の schema change 失敗。

### be_base_compaction_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のベース Compaction リクエスト。

### be_clone_total_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のクローンリクエスト。

### be_create_rollup_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のマテリアライズドビュー作成リクエスト。

### be_create_tablet_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の tablet 作成リクエスト。

### be_cumulative_compaction_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の累積 Compaction リクエスト。

### be_delete_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の削除リクエスト。

### be_finish_task_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のタスク完了リクエスト。

### be_publish_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のバージョン公開リクエスト。

### be_report_tablets_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の tablet レポートリクエスト。

### be_report_disk_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のディスクレポートリクエスト。

### be_report_tablet_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の tablet レポートリクエスト。

### be_report_task_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE のタスクレポートリクエスト。

### be_schema_change_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の schema change レポートリクエスト。

### be_storage_migrate_requests

- 単位: Count/s
- タイプ: Average
- 説明: BE の移行リクエスト。

### be_fragment_endpoint_count

- 単位: Count
- タイプ: Average
- 説明: BE DataStream の数。

### be_fragment_request_latency_avg

- 単位: ms
- タイプ: Average
- 説明: フラグメントリクエストのレイテンシー。

### be_fragment_requests_per_second

- 単位: Count/s
- タイプ: Average
- 説明: フラグメントリクエストの数。

### be_http_request_latency_avg

- 単位: ms
- タイプ: Average
- 説明: HTTP リクエストのレイテンシー。

### be_http_requests_per_second

- 単位: Count/s
- タイプ: Average
- 説明: HTTP リクエストの数。

### be_http_request_send_bytes_per_second

- 単位: Bytes/s
- タイプ: Average
- 説明: HTTP リクエストで送信されたバイト数。

### fe_connections_per_second

- 単位: Count/s
- タイプ: Average
- 説明: FE の新しい接続率。

### fe_connection_total

- 単位: Count
- タイプ: Cumulative
- 説明: FE の接続の総数。

### fe_edit_log_read

- 単位: Count/s
- タイプ: Average
- 説明: FE 編集ログの読み取り速度。

### fe_edit_log_size_bytes

- 単位: Bytes/s
- タイプ: Average
- 説明: FE 編集ログのサイズ。

### fe_edit_log_write

- 単位: Bytes/s
- タイプ: Average
- 説明: FE 編集ログの書き込み速度。

### fe_checkpoint_push_per_second

- 単位: Count/s
- タイプ: Average
- 説明: FE チェックポイントの数。

### fe_pending_hadoop_load_job

- 単位: Count
- タイプ: Average
- 説明: 保留中の hadoop ジョブの数。

### fe_committed_hadoop_load_job

- 単位: Count
- タイプ: Average
- 説明: コミットされた hadoop ジョブの数。

### fe_loading_hadoop_load_job

- 単位: Count
- タイプ: Average
- 説明: ロード中の hadoop ジョブの数。

### fe_finished_hadoop_load_job

- 単位: Count
- タイプ: Average
- 説明: 完了した hadoop ジョブの数。

### fe_cancelled_hadoop_load_job

- 単位: Count
- タイプ: Average
- 説明: キャンセルされた hadoop ジョブの数。

### fe_pending_insert_load_job

- 単位: Count
- タイプ: Average
- 説明: 保留中の insert ジョブの数。

### fe_loading_insert_load_job

- 単位: Count
- タイプ: Average
- 説明: ロード中の insert ジョブの数。

### fe_committed_insert_load_job

- 単位: Count
- タイプ: Average
- 説明: コミットされた insert ジョブの数。

### fe_finished_insert_load_job

- 単位: Count
- タイプ: Average
- 説明: 完了した insert ジョブの数。

### fe_cancelled_insert_load_job

- 単位: Count
- タイプ: Average
- 説明: キャンセルされた insert ジョブの数。

### fe_pending_broker_load_job

- 単位: Count
- タイプ: Average
- 説明: 保留中の broker ジョブの数。

### fe_loading_broker_load_job

- 単位: Count
- タイプ: Average
- 説明: ロード中の broker ジョブの数。

### fe_committed_broker_load_job

- 単位: Count
- タイプ: Average
- 説明: コミットされた broker ジョブの数。

### fe_finished_broker_load_job

- 単位: Count
- タイプ: Average
- 説明: 完了した broker ジョブの数。

### fe_cancelled_broker_load_job

- 単位: Count
- タイプ: Average
- 説明: キャンセルされた broker ジョブの数。

### fe_pending_delete_load_job

- 単位: Count
- タイプ: Average
- 説明: 保留中の delete ジョブの数。

### fe_loading_delete_load_job

- 単位: Count
- タイプ: Average
- 説明: ロード中の delete ジョブの数。

### fe_committed_delete_load_job

- 単位: Count
- タイプ: Average
- 説明: コミットされた delete ジョブの数。

### fe_finished_delete_load_job

- 単位: Count
- タイプ: Average
- 説明: 完了した delete ジョブの数。

### fe_cancelled_delete_load_job

- 単位: Count
- タイプ: Average
- 説明: キャンセルされた delete ジョブの数。

### fe_rollup_running_alter_job

- 単位: Count
- タイプ: Average
- 説明: rollup で作成されたジョブの数。

### fe_schema_change_running_job

- 単位: Count
- タイプ: Average
- 説明: schema change のジョブの数。

### cpu_util

- 単位: -
- タイプ: Average
- 説明: CPU 使用率。

### cpu_system

- 単位: -
- タイプ: Average
- 説明: cpu_system 使用率。

### cpu_user

- 単位: -
- タイプ: Average
- 説明: cpu_user 使用率。

### cpu_idle

- 単位: -
- タイプ: Average
- 説明: cpu_idle 使用率。

### cpu_guest

- 単位: -
- タイプ: Average
- 説明: cpu_guest 使用率。

### cpu_iowait

- 単位: -
- タイプ: Average
- 説明: cpu_iowait 使用率。

### cpu_irq

- 単位: -
- タイプ: Average
- 説明: cpu_irq 使用率。

### cpu_nice

- 単位: -
- タイプ: Average
- 説明: cpu_nice 使用率。

### cpu_softirq

- 単位: -
- タイプ: Average
- 説明: cpu_softirq 使用率。

### cpu_steal

- 単位: -
- タイプ: Average
- 説明: cpu_steal 使用率。

### disk_free

- 単位: Bytes
- タイプ: Average
- 説明: 空きディスク容量。

### disk_io_svctm

- 単位: ms
- タイプ: Average
- 説明: ディスク IO サービス時間。

### disk_io_util

- 単位: -
- タイプ: Average
- 説明: ディスク使用率。

### disk_used

- 単位: Bytes
- タイプ: Average
- 説明: 使用済みディスク容量。

### encryption_keys_created

- 単位: Count
- タイプ: Cumulative
- 説明: ファイル暗号化のために作成されたファイル暗号化キーの数。

### encryption_keys_unwrapped

- 単位: Count
- タイプ: Cumulative
- 説明: ファイル復号化のためにアンラップされた暗号化メタの数。

### encryption_keys_in_cache

- 単位: Count
- タイプ: Instantaneous
- 説明: 現在キーキャッシュにある暗号化キーの数。

### starrocks_fe_meta_log_count

- 単位: Count
- タイプ: Instantaneous
- 説明: チェックポイントのない Edit Log の数。`100000` 以内の値が合理的とされます。

### starrocks_fe_query_resource_group

- 単位: Count
- タイプ: Cumulative
- 説明: 各リソースグループのクエリ数。

### starrocks_fe_query_resource_group_latency

- 単位: Seconds
- タイプ: Average
- 説明: 各リソースグループのクエリレイテンシーのパーセンタイル。

### starrocks_fe_query_resource_group_err

- 単位: Count
- タイプ: Cumulative
- 説明: 各リソースグループの誤ったクエリ数。

### starrocks_be_resource_group_cpu_limit_ratio

- 単位: -
- タイプ: Instantaneous
- 説明: リソースグループの CPU クォータ比率の瞬時値。

### starrocks_be_resource_group_cpu_use_ratio

- 単位: -
- タイプ: Average
- 説明: リソースグループが使用した CPU 時間の比率。

### starrocks_be_resource_group_mem_limit_bytes

- 単位: Bytes
- タイプ: Instantaneous
- 説明: リソースグループのメモリクォータの瞬時値。

### starrocks_be_resource_group_mem_allocated_bytes

- 単位: Bytes
- タイプ: Instantaneous
- 説明: リソースグループのメモリ使用量の瞬時値。

### starrocks_be_pipe_prepare_pool_queue_len

- 単位: Count
- タイプ: Instantaneous
- 説明: パイプライン準備スレッドプールタスクキューの長さの瞬時値。

### starrocks_fe_safe_mode

- 単位: -
- タイプ: Instantaneous
- 説明: セーフモードが有効かどうかを示します。有効な値: `0` (無効) と `1` (有効)。セーフモードが有効になると、クラスタはロードリクエストを受け付けなくなります。

### starrocks_fe_unfinished_backup_job

- 単位: Count
- タイプ: Instantaneous
- 説明: 特定のウェアハウスで実行中の BACKUP タスクの数を示します。共有なしクラスタの場合、この項目はデフォルトのウェアハウスのみを監視します。共有データクラスタの場合、この値は常に `0` です。

### starrocks_fe_unfinished_restore_job

- 単位: Count
- タイプ: Instantaneous
- 説明: 特定のウェアハウスで実行中の RESTORE タスクの数を示します。共有なしクラスタの場合、この項目はデフォルトのウェアハウスのみを監視します。共有データクラスタの場合、この値は常に `0` です。

### starrocks_fe_memory

- 単位: Bytes または Count
- タイプ: Instantaneous
- 説明: 特定のウェアハウスのさまざまなモジュールのメモリ統計を示します。共有なしクラスタの場合、この項目はデフォルトのウェアハウスのみを監視します。

### starrocks_fe_unfinished_query

- 単位: Count
- タイプ: Instantaneous
- 説明: 特定のウェアハウスで現在実行中のクエリの数を示します。共有なしクラスタの場合、この項目はデフォルトのウェアハウスのみを監視します。

### starrocks_fe_last_finished_job_timestamp

- 単位: ms
- タイプ: Instantaneous
- 説明: 特定のウェアハウスでの最後のクエリまたはロードの終了時間を示します。共有なしクラスタの場合、この項目はデフォルトのウェアハウスのみを監視します。

### starrocks_fe_query_resource_group

- 単位: Count
- タイプ: Cumulative
- 説明: 特定のリソースグループで実行されたクエリの総数を示します。

### starrocks_fe_query_resource_group_err

- 単位: Count
- タイプ: Cumulative
- 説明: 特定のリソースグループで失敗したクエリの数を示します。

### starrocks_fe_query_resource_group_latency

- 単位: ms
- タイプ: Cumulative
- 説明: 特定のリソースグループでのクエリのレイテンシー統計を示します。

### starrocks_fe_tablet_num

- 単位: Count
- タイプ: Instantaneous
- 説明: 各 BE ノード上の tablet の数を示します。

### starrocks_fe_tablet_max_compaction_score

- 単位: Count
- タイプ: Instantaneous
- 説明: 各 BE ノード上の最高の Compaction Score を示します。

### update_compaction_outputs_total

- 単位: Count
- 説明: 主キーテーブルの Compaction の総数。

### update_del_vector_bytes_total

- 単位: Bytes
- 説明: 主キーテーブルで DELETE ベクトルをキャッシュするために使用されるメモリの総量。

### push_request_duration_us

- 単位: us
- 説明: Spark Load に費やされた総時間。

### writable_blocks_total (Deprecated)

### disks_data_used_capacity

- 説明: 各ディスクの使用済み容量（ストレージパスで表される）。

### query_scan_rows

- 単位: Count
- 説明: スキャンされた行の総数。

### update_primary_index_num

- 単位: Count
- 説明: メモリにキャッシュされた主キーインデックスの数。

### result_buffer_block_count

- 単位: Count
- 説明: 結果バッファ内のブロック数。

### query_scan_bytes

- 単位: Bytes
- 説明: スキャンされたバイトの総数。

### disk_reads_completed

- 単位: Count
- 説明: 正常に完了したディスク読み取りの数。

### query_cache_hit_count

- 単位: Count
- 説明: クエリキャッシュのヒット数。

### jemalloc_resident_bytes

- 単位: Bytes
- 説明: アロケータによってマッピングされた物理的に存在するデータページの最大バイト数で、アロケータメタデータ、アクティブな割り当てをサポートするページ、および未使用のダーティページを含みます。

### blocks_open_writing (Deprecated)

### disk_io_time_weigthed

- 単位: ms
- 説明: I/O に費やされた加重時間。

### update_compaction_task_byte_per_second

- 単位: Bytes/s
- 説明: 主キーテーブルの Compaction の推定速度。

### blocks_open_reading (Deprecated)

### tablet_update_max_compaction_score

- 単位: -
- 説明: 現在の BE における主キーテーブルの最高の Compaction スコア。

### segment_read

- 単位: Count
- 説明: セグメント読み取りの総数。

### disk_io_time_ms

- 単位: ms
- 説明: I/O に費やされた時間。

### load_mem_bytes

- 単位: Bytes
- 説明: データロードのメモリコスト。

### delta_column_group_get_non_pk_total

- 単位: Count
- 説明: デルタカラムグループを取得する総回数（非主キーテーブルのみ）。

### query_scan_bytes_per_second

- 単位: Bytes/s
- 説明: 秒あたりのスキャンされたバイトの推定速度。

### active_scan_context_count

- 単位: Count
- 説明: Flink/Spark SQL によって作成されたスキャンタスクの総数。

### fd_num_limit

- 単位: Count
- 説明: ファイルディスクリプタの最大数。

### update_compaction_task_cost_time_ns

- 単位: ns
- 説明: 主キーテーブルの Compaction に費やされた総時間。

### delta_column_group_get_hit_cache

- 単位: Count
- 説明: デルタカラムグループキャッシュのヒット総数（主キーテーブルのみ）。

### data_stream_receiver_count

- 単位: Count
- 説明: BE で Exchange レシーバーとして機能するインスタンスの累積数。

### bytes_written_total

- 単位: Bytes
- 説明: 書き込まれた総バイト数（セクター書き込み * 512）。

### transaction_streaming_load_bytes

- 単位: Bytes
- 説明: トランザクションロードの総ロードバイト数。

### running_cumulative_compaction_task_num

- 単位: Count
- 説明: 実行中の累積 Compaction の総数。

### transaction_streaming_load_requests_total

- 単位: Count
- 説明: トランザクションロードリクエストの総数。

### cpu

- 単位: -
- 説明: `/proc/stat` によって返される CPU 使用情報。

### update_del_vector_num

- 単位: Count
- 説明: 主キーテーブルにおける DELETE ベクターキャッシュアイテムの数。

### disks_avail_capacity

- 説明: 特定のディスクの利用可能容量。

### clone_mem_bytes

- 単位: Bytes
- 説明: レプリカクローンに使用されるメモリ。

### fragment_requests_total

- 単位: Count
- 説明: BE で実行中のフラグメントインスタンスの総数（非パイプラインエンジン用）。

### disk_write_time_ms

- 単位: ms
- 説明: ディスク書き込みに費やされた時間。単位: ms。

### schema_change_mem_bytes

- 単位: Bytes
- 説明: Schema Change に使用されるメモリ。

### thrift_connections_total

- 単位: Count
- 説明: thrift 接続の総数（完了した接続を含む）。

### thrift_opened_clients

- 単位: Count
- 説明: 現在開かれている thrift クライアントの数。

### thrift_used_clients

- 単位: Count
- 説明: 現在使用中の thrift クライアントの数。

### thrift_current_connections (Deprecated)

### disk_bytes_read

- 単位: Bytes
- 説明: ディスクから読み取られた総バイト数（セクター読み取り * 512）。

### base_compaction_task_cost_time_ms

- 単位: ms
- 説明: ベース Compaction に費やされた総時間。

### update_primary_index_bytes_total

- 単位: Bytes
- 説明: 主キーインデックスのメモリコストの総量。

### compaction_deltas_total

- 単位: Count
- 説明: ベース Compaction と累積 Compaction からマージされた rowset の総数。

### max_network_receive_bytes_rate

- 単位: Bytes
- 説明: ネットワークを介して受信された総バイト数（すべてのネットワークインターフェースの最大値）。

### max_network_send_bytes_rate

- 単位: Bytes
- 説明: ネットワークを介して送信された総バイト数（すべてのネットワークインターフェースの最大値）。

### chunk_allocator_mem_bytes

- 単位: Bytes
- 説明: チャンクアロケータによって使用されるメモリ。

### query_cache_usage_ratio

- 単位: -
- 説明: 現在のクエリキャッシュ使用率。

### process_fd_num_limit_soft

- 単位: Count
- 説明: ファイルディスクリプタの最大数のソフトリミット。この項目はソフトリミットを示します。ハードリミットは `ulimit` コマンドを使用して設定できます。

### query_cache_hit_ratio

- 単位: -
- 説明: クエリキャッシュのヒット率。

### tablet_metadata_mem_bytes

- 単位: Bytes
- 説明: tablet メタデータによって使用されるメモリ。

### jemalloc_retained_bytes

- 単位: Bytes
- 説明: アロケータメタデータ、アクティブな割り当てをサポートするページ、および未使用のダーティページを含む、オペレーティングシステムに返されることなく保持された仮想メモリマッピングの総バイト数。

### unused_rowsets_count

- 単位: Count
- 説明: 未使用の rowset の総数。これらの rowset は後で回収されます。

### update_rowset_commit_apply_total

- 単位: Count
- 説明: 主キーテーブルの COMMIT と APPLY の総数。

### segment_zonemap_mem_bytes

- 単位: Bytes
- 説明: セグメントゾーンマップによって使用されるメモリ。

### base_compaction_task_byte_per_second

- 単位: Bytes/s
- 説明: ベース Compaction の推定速度。

### transaction_streaming_load_duration_ms

- 単位: ms
- 説明: Stream Load トランザクションインターフェースに費やされた総時間。

### memory_pool_bytes_total

- 単位: Bytes
- 説明: メモリプールによって使用されるメモリ。

### short_key_index_mem_bytes

- 単位: Bytes
- 説明: ショートキーインデックスによって使用されるメモリ。

### disk_bytes_written

- 単位: Bytes
- 説明: ディスクに書き込まれた総バイト数。

### segment_flush_queue_count

- 単位: Count
- 説明: セグメントフラッシュスレッドプールにおけるキュータスクの数。

### jemalloc_metadata_bytes

- 単位: Bytes
- 説明: メタデータに専用のバイト数の合計で、ブートストラップに敏感なアロケータメタデータ構造と内部割り当てに使用されるベース割り当てを含みます。この項目には透過的な巨大ページの使用は含まれていません。

### network_send_bytes

- 単位: Bytes
- 説明: ネットワークを介して送信されたバイト数。

### update_mem_bytes

- 単位: Bytes
- 説明: 主キーテーブルの APPLY タスクと主キーインデックスによって使用されるメモリ。

### blocks_created_total (Deprecated)

### query_cache_usage

- 単位: Bytes
- 説明: 現在のクエリキャッシュ使用量。

### memtable_flush_duration_us

- 単位: us
- 説明: memtable フラッシュに費やされた総時間。

### push_request_write_bytes

- 単位: Bytes
- 説明: Spark Load を介して書き込まれた総バイト数。

### jemalloc_active_bytes

- 単位: Bytes
- 説明: アプリケーションによって割り当てられたアクティブページの総バイト数。

### load_rpc_threadpool_size

- 単位: Count
- 説明: Routine Load とテーブル関数を介したロードを処理するために使用される RPC スレッドプールの現在のサイズ。デフォルト値は 10 で、最大値は 1000 です。この値はスレッドプールの使用状況に基づいて動的に調整されます。

### publish_version_queue_count

- 単位: Count
- 説明: Publish Version スレッドプールにおけるキュータスクの数。

### chunk_pool_system_free_count

- 単位: Count
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### chunk_pool_system_free_cost_ns

- 単位: ns
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### chunk_pool_system_alloc_count

- 単位: Count
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### chunk_pool_system_alloc_cost_ns

- 単位: ns
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### chunk_pool_local_core_alloc_count

- 単位: Count
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### chunk_pool_other_core_alloc_count

- 単位: Count
- 説明: メモリチャンクの割り当て/キャッシュメトリクス。

### fragment_endpoint_count

- 単位: Count
- 説明: BE で Exchange 送信者として機能するインスタンスの累積数。

### disk_sync_total (Deprecated)

### max_disk_io_util_percent

- 単位: -
- 説明: 最大ディスク I/O 利用率のパーセンテージ。

### network_receive_bytes

- 単位: Bytes
- 説明: ネットワークを介して受信された総バイト数。

### storage_page_cache_mem_bytes

- 単位: Bytes
- 説明: ストレージページキャッシュによって使用されるメモリ。

### jit_cache_mem_bytes

- 単位: Bytes
- 説明: jit コンパイルされた関数キャッシュによって使用されるメモリ。

### column_partial_update_apply_total

- 単位: Count
- 説明: 部分更新のためのカラムの APPLY タスクの総数（カラムモード）。

### disk_writes_completed

- 単位: Count
- 説明: 正常に完了したディスク書き込みの総数。

### memtable_flush_total

- 単位: Count
- 説明: memtable フラッシュの総数。

### page_cache_hit_count

- 単位: Count
- 説明: ストレージページキャッシュのヒット総数。

### bytes_read_total (Deprecated)

### update_rowset_commit_request_total

- 単位: Count
- 説明: 主キーテーブルにおける rowset COMMIT リクエストの総数。

### tablet_cumulative_max_compaction_score

- 単位: -
- 説明: この BE における tablet の最高累積 Compaction スコア。

### column_zonemap_index_mem_bytes

- 単位: Bytes
- 説明: カラムゾーンマップによって使用されるメモリ。

### push_request_write_bytes_per_second

- 単位: Bytes/s
- 説明: Spark Load のデータ書き込み速度。

### process_thread_num

- 単位: Count
- 説明: このプロセスのスレッドの総数。

### query_cache_lookup_count

- 単位: Count
- 説明: クエリキャッシュのルックアップ総数。

### http_requests_total (Deprecated)

### http_request_send_bytes (Deprecated)

### cumulative_compaction_task_cost_time_ms

- 単位: ms
- 説明: 累積 Compaction に費やされた総時間。

### column_metadata_mem_bytes

- 単位: Bytes
- 説明: カラムメタデータによって使用されるメモリ。

### plan_fragment_count

- 単位: Count
- 説明: 現在実行中のクエリプランフラグメントの数。

### page_cache_lookup_count

- 単位: Count
- 説明: ストレージページキャッシュのルックアップ総数。

### query_mem_bytes

- 単位: Bytes
- 説明: クエリによって使用されるメモリ。

### load_channel_count

- 単位: Count
- 説明: ロードチャネルの総数。

### push_request_write_rows

- 単位: Count
- 説明: Spark Load を介して書き込まれた総行数。

### running_base_compaction_task_num

- 単位: Count
- 説明: 実行中のベース Compaction タスクの総数。

### fragment_request_duration_us

- 単位: us
- 説明: フラグメントインスタンスの累積実行時間（非パイプラインエンジン用）。

### process_mem_bytes

- 単位: Bytes
- 説明: このプロセスによって使用されるメモリ。

### broker_count

- 単位: Count
- 説明: 作成されたファイルシステムブローカーの総数（ホストアドレスごと）。

### segment_replicate_queue_count

- 単位: Count
- 説明: セグメントレプリケートスレッドプールにおけるキュータスクの数。

### brpc_endpoint_stub_count

- 単位: Count
- 説明: bRPC スタブの総数（アドレスごと）。

### readable_blocks_total (Deprecated)

### delta_column_group_get_non_pk_hit_cache

- 単位: Count
- 説明: デルタカラムグループキャッシュのヒット総数（非主キーテーブル用）。

### update_del_vector_deletes_new

- 単位: Count
- 説明: 主キーテーブルで使用される新しく生成された DELETE ベクトルの総数。

### compaction_bytes_total

- 単位: Bytes
- 説明: ベース Compaction と累積 Compaction からマージされた総バイト数。

### segment_metadata_mem_bytes

- 単位: Bytes
- 説明: セグメントメタデータによって使用されるメモリ。

### column_partial_update_apply_duration_us

- 単位: us
- 説明: カラムの部分更新の APPLY タスクに費やされた総時間（カラムモード）。

### bloom_filter_index_mem_bytes

- 単位: Bytes
- 説明: Bloomfilter インデックスによって使用されるメモリ。

### routine_load_task_count

- 単位: Count
- 説明: 現在実行中の Routine Load タスクの数。

### delta_column_group_get_total

- 単位: Count
- 説明: デルタカラムグループを取得する総回数（主キーテーブル用）。

### disk_read_time_ms

- 単位: ms
- 説明: ディスクからの読み取りに費やされた時間。単位: ms。

### update_compaction_outputs_bytes_total

- 単位: Bytes
- 説明: 主キーテーブルの Compaction によって書き込まれた総バイト数。

### memtable_flush_queue_count

- 単位: Count
- 説明: memtable フラッシュスレッドプールにおけるキュータスクの数。

### query_cache_capacity

- 説明: クエリキャッシュの容量。

### streaming_load_duration_ms

- 単位: ms
- 説明: Stream Load に費やされた総時間。

### streaming_load_current_processing

- 単位: Count
- 説明: 現在実行中の Stream Load タスクの数。

### streaming_load_bytes

- 単位: Bytes
- 説明: Stream Load によってロードされた総バイト数。

### load_rows

- 単位: Count
- 説明: ロードされた総行数。

### load_bytes

- 単位: Bytes
- 説明: ロードされた総バイト数。

### meta_request_total

- 単位: Count
- 説明: メタデータの読み取り/書き込みリクエストの総数。

### meta_request_duration

- 単位: us
- 説明: メタデータの読み取り/書き込みに費やされた総時間。

### tablet_base_max_compaction_score

- 単位: -
- 説明: この BE における tablet の最高ベース Compaction スコア。

### page_cache_capacity

- 説明: ストレージページキャッシュの容量。

### cumulative_compaction_task_byte_per_second

- 単位: Bytes/s
- 説明: 累積 Compaction 中に処理されたバイトの速度。

### stream_load

- 単位: -
- 説明: ロードされた総行数と受信されたバイト数。

### stream_load_pipe_count

- 単位: Count
- 説明: 現在実行中の Stream Load タスクの数。

### binary_column_pool_bytes

- 単位: Bytes
- 説明: BINARY カラムプールによって使用されるメモリ。

### int16_column_pool_bytes

- 単位: Bytes
- 説明: INT16 カラムプールによって使用されるメモリ。

### decimal_column_pool_bytes

- 単位: Bytes
- 説明: DECIMAL カラムプールによって使用されるメモリ。

### double_column_pool_bytes

- 単位: Bytes
- 説明: DOUBLE カラムプールによって使用されるメモリ。

### int128_column_pool_bytes

- 単位: Bytes
- 説明: INT128 カラムプールによって使用されるメモリ。

### date_column_pool_bytes

- 単位: Bytes
- 説明: DATE カラムプールによって使用されるメモリ。

### int64_column_pool_bytes

- 単位: Bytes
- 説明: INT64 カラムプールによって使用されるメモリ。

### int8_column_pool_bytes

- 単位: Bytes
- 説明: INT8 カラムプールによって使用されるメモリ。

### datetime_column_pool_bytes

- 単位: Bytes
- 説明: DATETIME カラムプールによって使用されるメモリ。

### int32_column_pool_bytes

- 単位: Bytes
- 説明: INT32 カラムプールによって使用されるメモリ。

### float_column_pool_bytes

- 単位: Bytes
- 説明: FLOAT カラムプールによって使用されるメモリ。

### uint8_column_pool_bytes

- 単位: Bytes
- 説明: UINT8 カラムプールによって使用されるバイト数。

### column_pool_mem_bytes

- 単位: Bytes
- 説明: カラムプールによって使用されるメモリ。

### local_column_pool_bytes (Deprecated)

### total_column_pool_bytes (Deprecated)

### central_column_pool_bytes (Deprecated)

### wait_base_compaction_task_num

- 単位: Count
- 説明: 実行待ちのベース Compaction タスクの数。

### wait_cumulative_compaction_task_num

- 単位: Count
- 説明: 実行待ちの累積 Compaction タスクの数。

### jemalloc_allocated_bytes

- 単位: Bytes
- 説明: アプリケーションによって割り当てられたバイト数の合計。

### pk_index_compaction_queue_count

- 単位: Count
- 説明: 主キーインデックス Compaction スレッドプールにおけるキュータスクの数。

### disks_total_capacity

- 説明: ディスクの総容量。

### disks_state

- 単位: -
- 説明: 各ディスクの状態。`1` はディスクが使用中であることを示し、`0` は使用されていないことを示します。

### update_del_vector_deletes_total (Deprecated)

### update_del_vector_dels_num (Deprecated)

### result_block_queue_count

- 単位: Count
- 説明: 結果ブロックキュー内の結果の数。

### engine_requests_total

- 単位: Count
- 説明: BE と FE 間のすべてのタイプのリクエストの総数（CREATE TABLE、Publish Version、tablet クローンを含む）。

### snmp

- 単位: -
- 説明: `/proc/net/snmp` によって返されるメトリクス。

### compaction_mem_bytes

- 単位: Bytes
- 説明: Compaction によって使用されるメモリ。

### txn_request

- 単位: -
- 説明: BEGIN、COMMIT、ROLLBACK、EXEC のトランザクションリクエスト。

### small_file_cache_count

- 単位: Count
- 説明: 小さなファイルキャッシュの数。

### rowset_metadata_mem_bytes

- 単位: Bytes
- 説明: rowset メタデータのバイト数の合計。

### update_apply_queue_count

- 単位: Count
- 説明: 主キーテーブルトランザクション APPLY スレッドプールにおけるキュータスクの数。

### async_delta_writer_queue_count

- 単位: Count
- 説明: tablet デルタライタースレッドプールにおけるキュータスクの数。

### update_compaction_duration_us

- 単位: us
- 説明: 主キーテーブルの Compaction に費やされた総時間。

### transaction_streaming_load_current_processing

- 単位: Count
- 説明: 現在実行中のトランザクション Stream Load タスクの数。

### ordinal_index_mem_bytes

- 単位: Bytes
- 説明: オーディナルインデックスによって使用されるメモリ。

### consistency_mem_bytes

- 単位: Bytes
- 説明: レプリカの整合性チェックによって使用されるメモリ。

### tablet_schema_mem_bytes

- 単位: Bytes
- 説明: tablet スキーマによって使用されるメモリ。

### fd_num_used

- 単位: Count
- 説明: 現在使用中のファイルディスクリプタの数。

### running_update_compaction_task_num

- 単位: Count
- 説明: 現在実行中の主キーテーブル Compaction タスクの総数。

### rowset_count_generated_and_in_use

- 単位: Count
- 説明: 現在使用中の rowset ID の数。

### bitmap_index_mem_bytes

- 単位: Bytes
- 説明: ビットマップインデックスによって使用されるメモリ。

### update_rowset_commit_apply_duration_us

- 単位: us
- 説明: 主キーテーブルの APPLY タスクに費やされた総時間。

### process_fd_num_used

- 単位: Count
- 説明: この BE プロセスで現在使用中のファイルディスクリプタの数。

### network_send_packets

- 単位: Count
- 説明: ネットワークを介して送信されたパケットの総数。

### network_receive_packets

- 単位: Count
- 説明: ネットワークを介して受信されたパケットの総数。

### metadata_mem_bytes (Deprecated)

### push_requests_total

- 単位: Count
- 説明: 成功および失敗した Spark Load リクエストの総数。

### blocks_deleted_total (Deprecated)

### jemalloc_mapped_bytes

- 単位: Bytes
- 説明: アロケータによってマッピングされたアクティブなエクステントの総バイト数。

### process_fd_num_limit_hard

- 単位: Count
- 説明: ファイルディスクリプタの最大数のハードリミット。

### jemalloc_metadata_thp

- 単位: Count
- 説明: メタデータに使用される透過的巨大ページの数。

### update_rowset_commit_request_failed

- 単位: Count
- 説明: 主キーテーブルにおける失敗した rowset COMMIT リクエストの総数。

### streaming_load_requests_total

- 単位: Count
- 説明: Stream Load リクエストの総数。

### resource_group_running_queries

- 単位: Count
- 説明: 各リソースグループで現在実行中のクエリの数。これは瞬時値です。

### resource_group_total_queries

- 単位: Count
- 説明: 各リソースグループで実行されたクエリの総数（現在実行中のものを含む）。これは瞬時値です。

### resource_group_bigquery_count

- 単位: Count
- 説明: 各リソースグループで大規模クエリの制限を超えたクエリの数。これは瞬時値です。

### resource_group_concurrency_overflow_count

- 単位: Count
- 説明: 各リソースグループで同時実行制限を超えたクエリの数。これは瞬時値です。

### resource_group_mem_limit_bytes

- 単位: Bytes
- 説明: 各リソースグループのメモリ制限（バイト単位）。これは瞬時値です。

### resource_group_mem_inuse_bytes

- 単位: Bytes
- 説明: 各リソースグループで現在使用中のメモリ（バイト単位）。これは瞬時値です。

### resource_group_cpu_limit_ratio

- 単位: -
- 説明: 各リソースグループの CPU コア制限の比率と、すべてのリソースグループの合計 CPU コア制限の比率。これは瞬時値です。

### resource_group_inuse_cpu_cores

- 単位: Count
- 説明: 各リソースグループで現在使用中の CPU コアの推定数。これはメトリクス取得間の時間間隔にわたる平均値です。

### resource_group_cpu_use_ratio (Deprecated)

- 単位: -
- 説明: 各リソースグループで使用されたパイプラインスレッド時間スライスの比率と、すべてのリソースグループで使用された合計の比率。これはメトリクス取得間の時間間隔にわたる平均値です。

### resource_group_connector_scan_use_ratio (Deprecated)

- 単位: -
- 説明: 各リソースグループで使用された外部テーブルスキャンスレッド時間スライスの比率と、すべてのリソースグループで使用された合計の比率。これはメトリクス取得間の時間間隔にわたる平均値です。

### resource_group_scan_use_ratio (Deprecated)

- 単位: -
- 説明: 各リソースグループで使用された内部テーブルスキャンスレッド時間スライスの比率と、すべてのリソースグループで使用された合計の比率。これはメトリクス取得間の時間間隔にわたる平均値です。

### pipe_poller_block_queue_len

- 単位: Count
- 説明: パイプラインエンジンにおける PipelineDriverPoller のブロックキューの現在の長さ。

### pip_query_ctx_cnt

- 単位: Count
- 説明: BE で現在実行中のクエリの総数。

### pipe_driver_schedule_count

- 単位: Count
- 説明: BE におけるパイプラインエグゼキュータのドライバースケジューリング回数の累積数。

### pipe_scan_executor_queuing

- 単位: Count
- 説明: スキャンオペレーターによって開始された保留中の非同期 I/O タスクの現在の数。

### pipe_driver_queue_len

- 単位: Count
- 説明: BE でスケジューリングを待っている準備完了キュー内の準備完了ドライバーの現在の数。

### pipe_driver_execution_time

- 説明: PipelineDriver エグゼキュータが PipelineDrivers を処理するのに費やした累積時間。

### pipe_prepare_pool_queue_len

- 単位: Count
- 説明: パイプライン PREPARE スレッドプールにおけるキュータスクの数。これは瞬時値です。

### starrocks_fe_routine_load_jobs

- 単位: Count
- 説明: 異なる状態の Routine Load ジョブの総数。例えば:

  ```plaintext
  starrocks_fe_routine_load_jobs{state="NEED_SCHEDULE"} 0
  starrocks_fe_routine_load_jobs{state="RUNNING"} 1
  starrocks_fe_routine_load_jobs{state="PAUSED"} 0
  starrocks_fe_routine_load_jobs{state="STOPPED"} 0
  starrocks_fe_routine_load_jobs{state="CANCELLED"} 1
  starrocks_fe_routine_load_jobs{state="UNSTABLE"} 0
  ```

### starrocks_fe_routine_load_paused

- 単位: Count
- 説明: Routine Load ジョブが一時停止された総回数。

### starrocks_fe_routine_load_rows

- 単位: Count
- 説明: すべての Routine Load ジョブによってロードされた総行数。

### starrocks_fe_routine_load_receive_bytes

- 単位: Byte
- 説明: すべての Routine Load ジョブによってロードされたデータの総量。

### starrocks_fe_routine_load_error_rows

- 単位: Count
- 説明: すべての Routine Load ジョブによるデータロード中に発生したエラーロウの総数。