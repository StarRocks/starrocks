---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical i - p"
---

# メトリクス iからpまで

:::note

マテリアライズドビューと共有データクラスターのメトリクスについては、対応するセクションで詳しく説明しています。

- [非同期マテリアライズドビューのメトリクスに関するメトリクス](../metrics-materialized_view.md)
- [共有データダッシュボードのメトリクス、およびスターレットダッシュボードのメトリクスに関するメトリクス](../metrics-shared-data.md)

StarRocksクラスターの監視サービスを構築する方法の詳細については、以下を参照してください。[監視とアラート](../Monitor_and_Alert.md)。

:::

## `iceberg_compaction_duration_ms_total`

- 単位: ミリ秒
- 種類: 累積
- ラベル: `compaction_type`（`manual` または `auto`）
- 説明: Icebergコンパクションタスクの実行に費やされた合計時間。

## `iceberg_compaction_input_files_total`

- 単位: カウント
- タイプ: 累積
- ラベル: `compaction_type` (`manual` または `auto`)
- 説明: Icebergコンパクションタスクによって読み取られたデータファイルの総数。

## `iceberg_compaction_output_files_total`

- 単位: 数
- タイプ: 累積
- ラベル: `compaction_type` (`manual` または `auto`)
- 説明: Icebergコンパクションタスクによって生成されたデータファイルの総数。

## `iceberg_compaction_removed_delete_files_total`

- 単位: カウント
- 種類: 累積
- ラベル: `compaction_type` (`manual` または `auto`)
- 説明: Icebergの手動コンパクションタスクによって削除された削除ファイルの総数。

## `iceberg_compaction_total`

- 単位: 数
- タイプ: 累積
- ラベル: `compaction_type` (`manual` または `auto`)
- 説明: Icebergコンパクション (`rewrite_data_files`) タスクの総数。

## `iceberg_delete_bytes`

- 単位: バイト
- 種類: 累積
- ラベル: `delete_type` (`position` または `metadata`)
- 説明: Iceberg `DELETE` タスクから削除された合計バイト数。`metadata` 削除の場合、これは削除されたデータファイルのサイズを表します。`position` 削除の場合、これは作成された位置削除ファイルのサイズを表します。

## `iceberg_delete_duration_ms_total`

- 単位: ミリ秒
- タイプ: 累積
- ラベル: `delete_type` (`position`または`metadata`)
- 説明: Iceberg `DELETE` タスクの合計実行時間（ミリ秒）。各タスクの実行時間は、終了後に加算されます。`delete_type` は、2つの削除方法を区別します。

## `iceberg_delete_rows`

- 単位: 行
- タイプ: 累積
- ラベル: `delete_type` (`position` または `metadata`)
- 説明: Iceberg `DELETE` タスクから削除された行の合計数。`metadata` 削除の場合、これは削除されたデータファイル内の行数を表します。`position` 削除の場合、これは作成された位置削除の数を表します。

## `iceberg_delete_total`

- 単位: カウント
- タイプ: 累積
- ラベル:
  - `status` (`success` または `failed`)
  - `reason` (`none`、`timeout`、`oom`、`access_denied`、`unknown`)
  - `delete_type` (`position` または `metadata`)
- 説明: Icebergテーブルをターゲットとする`DELETE`タスクの合計数。このメトリックは、各タスクの終了後、成功または失敗にかかわらず1ずつ増加します。`delete_type`は、2つの削除方法を区別します: `position` (位置削除ファイルを生成する) と `metadata` (メタデータレベルの削除)。

## `iceberg_metadata_table_query_total`

- 単位: カウント
- タイプ: 累積
- ラベル: `metadata_table` (`refs`、`history`、`metadata_log_entries`、`snapshots`、`manifests`、`files`、`partitions`、または `properties`)
- 説明: IcebergメタデータテーブルにアクセスするSQLクエリの合計数。各クエリは、アクセスされているメタデータテーブルを識別する`metadata_table`ラベルの下でカウントされます。

## `iceberg_time_travel_query_total`

- 単位: カウント
- タイプ: 累積
- ラベル: カテゴリ化されたシリーズの`time_travel_type` (`branch`、`tag`、`snapshot`、または `timestamp`)。
- 説明: Icebergタイムトラベルクエリの合計数。ラベルなしのシリーズは、各タイムトラベルクエリを1回カウントします。ラベル付きのシリーズは、クエリで使用される各異なるタイムトラベルタイプをカウントします。`snapshot`は`FOR VERSION AS OF <snapshot_id>`を意味し、`branch`と`tag`は`FOR VERSION AS OF <reference_name>`を意味し、`timestamp`は`FOR TIMESTAMP AS OF ...`を意味します。

## `iceberg_write_bytes`

- 単位: バイト
- タイプ: 累積
- ラベル: `write_type` (`insert`、`overwrite`、または `ctas`)
- 説明: Iceberg書き込みタスク (`INSERT`、`INSERT OVERWRITE`、`CTAS`) から書き込まれたバイトの合計。これは、Icebergテーブルに書き込まれたデータファイルの合計サイズを表します。`write_type`は操作タイプを区別します。

## `iceberg_write_duration_ms_total`

- 単位: ミリ秒
- タイプ: 累積
- ラベル: `write_type` (`insert`、`overwrite`、または `ctas`)
- 説明: Iceberg書き込みタスク (`INSERT`、`INSERT OVERWRITE`、`CTAS`) の合計実行時間 (ミリ秒)。各タスクの実行時間は、終了後に加算されます。`write_type`は操作タイプを区別します。

## `iceberg_write_files`

- 単位: カウント
- タイプ: 累積
- ラベル: `write_type` (`insert`、`overwrite`、または `ctas`)
- 説明: Iceberg書き込みタスク (`INSERT`、`INSERT OVERWRITE`、`CTAS`) からIcebergに書き込まれたデータファイルの合計数。これは、Icebergテーブルに書き込まれたデータファイルの数を表します。`write_type`は操作タイプを区別します。

## `iceberg_write_rows`

- 単位: 行
- タイプ: 累積
- ラベル: `write_type` (`insert`、`overwrite`、または `ctas`)
- 説明: Iceberg書き込みタスク (`INSERT`、`INSERT OVERWRITE`、`CTAS`) から書き込まれた行の合計。これは、Icebergテーブルに書き込まれた行数を表します。`write_type`は操作タイプを区別します。

## `iceberg_write_total`

- 単位: カウント
- タイプ: 累積
- ラベル:
  - `status` (`success` または `failed`)
  - `reason` (`none`、`timeout`、`oom`、`access_denied`、`unknown`)
  - `write_type` (`insert`、`overwrite`、または `ctas`)
- 説明: Icebergテーブルをターゲットとする`INSERT`、`INSERT OVERWRITE`、または`CTAS`タスクの合計数。このメトリックは、各タスクの終了後、成功または失敗にかかわらず1ずつ増加します。`write_type`は操作タイプを区別します。

## `int128_column_pool_bytes`

- 単位: バイト
- 説明: INT128カラムプールによって使用されるメモリ。

## `int16_column_pool_bytes`

- 単位: バイト
- 説明: INT16カラムプールによって使用されるメモリ。

## `int32_column_pool_bytes`

- 単位: バイト
- 説明: INT32カラムプールによって使用されるメモリ。

## `int64_column_pool_bytes`

- 単位: バイト
- 説明: INT64カラムプールによって使用されるメモリ。

## `int8_column_pool_bytes`

- 単位: バイト
- 説明: INT8カラムプールによって使用されるメモリ。

## `jemalloc_active_bytes`

- 単位: バイト
- 説明: アプリケーションによって割り当てられたアクティブなページ内の合計バイト数。

## `jemalloc_allocated_bytes`

- 単位: バイト
- 説明: アプリケーションによって割り当てられた合計バイト数。

## `jemalloc_mapped_bytes`

- 単位: バイト
- 説明: アロケータによってマップされたアクティブなエクステント内の合計バイト数。

## `jemalloc_metadata_bytes`

- 単位: バイト
- 説明: メタデータに割り当てられた合計バイト数。これには、ブートストラップに敏感なアロケータメタデータ構造に使用される基本割り当てと内部割り当てが含まれます。透過的ヒュージページの使用はこの項目には含まれません。

## `jemalloc_metadata_thp`

- 単位: カウント
- 説明: メタデータに使用される透過的ヒュージページの数。

## `jemalloc_resident_bytes`

- 単位: バイト
- 説明: アロケータによってマップされた物理的に常駐するデータページ内の最大バイト数。これには、アロケータメタデータに割り当てられたすべてのページ、アクティブな割り当てをバックアップするページ、および未使用のダーティページが含まれます。

## `jemalloc_retained_bytes`

- 単位: バイト
- 説明: munmap(2)のような操作を介してオペレーティングシステムに返されるのではなく、保持された仮想メモリマッピングの合計バイト数。

## `jit_cache_mem_bytes`

- 単位: バイト
- 説明: JITコンパイルされた関数キャッシュによって使用されるメモリ。

## `load_bytes`

- 単位: バイト
- 説明: ロードされた合計バイト数。

## `load_channel_count`

- 単位: カウント
- 説明: ロードチャネルの総数。

## `load_mem_bytes`

- 単位: バイト
- 説明: データロードのメモリコスト。

## `load_rows`

- 単位: カウント
- 説明: ロードされた合計行数。

## `load_rpc_threadpool_size`

- 単位: カウント
- 説明: RPCスレッドプールの現在のサイズ。これは、ルーチンロードとテーブル関数を介したロードの処理に使用されます。デフォルト値は10で、最大値は1000です。この値は、スレッドプールの使用状況に基づいて動的に調整されます。

## `local_column_pool_bytes (Deprecated)`

## `max_disk_io_util_percent`

- 単位: -
- 説明: ディスクI/O使用率の最大パーセンテージ。

## `max_network_receive_bytes_rate`

- 単位: バイト
- 説明: ネットワーク経由で受信した合計バイト数（すべてのネットワークインターフェースの中で最大値）。

## `max_network_send_bytes_rate`

- 単位: バイト
- 説明: ネットワーク経由で送信された合計バイト数（すべてのネットワークインターフェースの中で最大値）。

## `memory_pool_bytes_total`

- 単位: バイト
- 説明: メモリプールによって使用されたメモリ。

## `memtable_flush_duration_us`

- 単位: us
- 説明: メムテーブルフラッシュに費やされた合計時間。

## `memtable_flush_queue_count`

- 単位: カウント
- 説明: メムテーブルフラッシュスレッドプール内のキューに入れられたタスク数。

## `memtable_flush_total`

- 単位: カウント
- 説明: メムテーブルフラッシュの合計数。

## `merge_commit_append_pipe`

- 単位: マイクロ秒
- タイプ: サマリー
- 説明: マージコミット中にストリームロードパイプにデータを追加するのに費やされた時間。

## `merge_commit_fail_total`

- 単位: カウント
- タイプ: 累積
- 説明: 失敗したマージコミットリクエスト。

## `merge_commit_pending`

- 単位: マイクロ秒
- タイプ: サマリー
- 説明: マージコミットタスクが実行前に保留中のキューで待機する時間。

## `merge_commit_pending_bytes`

- 単位: バイト
- タイプ: 瞬間
- 説明: 保留中のマージコミットタスクによって保持されているデータの合計バイト数。

## `merge_commit_pending_total`

- 単位: カウント
- タイプ: 瞬間
- 説明: 現在実行キューで待機中のマージコミットタスク。

## `merge_commit_register_pipe_total`

- 単位: カウント
- タイプ: 累積
- 説明: マージコミット操作のために登録されたストリームロードパイプ。

## `merge_commit_request`

- 単位: マイクロ秒
- タイプ: サマリー
- 説明: マージコミットリクエストのエンドツーエンド処理レイテンシー。

## `merge_commit_request_bytes`

- 単位: バイト
- タイプ: 累積
- 説明: マージコミットリクエスト全体で受信されたデータの合計バイト数。

## `merge_commit_request_total`

- 単位: カウント
- タイプ: 累積
- 説明: BEによって受信されたマージコミットリクエストの合計数。

## `merge_commit_send_rpc_total`

- 単位: カウント
- タイプ: 累積
- 説明: マージコミット操作を開始するためにFEに送信されたRPCリクエスト。

## `merge_commit_success_total`

- 単位: カウント
- タイプ: 累積
- 説明: 正常に完了したマージコミットリクエスト。

## `merge_commit_unregister_pipe_total`

- 単位: カウント
- タイプ: 累積
- 説明: マージコミット操作から登録解除されたストリームロードパイプ。

レイテンシーメトリクスは、`merge_commit_request_latency_99` や `merge_commit_request_latency_90` のようなパーセンタイル系列を公開し、マイクロ秒で報告されます。エンドツーエンドのレイテンシーは以下に従います。

`merge_commit_request = merge_commit_pending + merge_commit_wait_plan + merge_commit_append_pipe + merge_commit_wait_finish`

> **注記**: v3.4.11、v3.5.12、v4.0.4より前では、これらのレイテンシーメトリクスはナノ秒で報告されていました。

## `merge_commit_wait_finish`

- 単位: マイクロ秒
- タイプ: サマリー
- 説明: マージコミットロード操作が完了するのを待機した時間。

## `merge_commit_wait_plan`

- 単位: マイクロ秒
- タイプ: サマリー
- 説明: RPCリクエストとストリームロードパイプが利用可能になるのを待機する時間の合計レイテンシー。

## `meta_request_duration`

- 単位: us
- 説明: メタ読み取り/書き込みの合計時間。

## `meta_request_total`

- 単位: カウント
- 説明: メタ読み取り/書き込みリクエストの合計数。

## `metadata_mem_bytes (Deprecated)`

## `network_receive_bytes`

- 単位: バイト
- 説明: ネットワーク経由で受信した合計バイト数。

## `network_receive_packets`

- 単位: カウント
- 説明: ネットワーク経由で受信したパケットの合計数。

## `network_send_bytes`

- 単位: バイト
- 説明: ネットワーク経由で送信されたバイト数。

## `network_send_packets`

- 単位: カウント
- 説明: ネットワーク経由で送信されたパケットの合計数。

## `ordinal_index_mem_bytes`

- 単位: バイト
- 説明: 順序インデックスによって使用されるメモリ。

## `page_cache_capacity`

- 説明: ストレージページキャッシュの容量。

## `page_cache_hit_count`

- 単位: カウント
- 説明: ストレージページキャッシュでのヒットの合計数。

## `page_cache_insert_count`

- 単位: カウント
- 説明: ストレージページキャッシュでの挿入操作の合計数。

## `page_cache_insert_evict_count`

- 単位: カウント
- 説明: 容量制約により挿入操作中に削除されたキャッシュエントリの合計数。

## `page_cache_lookup_count`

- 単位: カウント
- 説明: ストレージページキャッシュのルックアップの合計数。

## `page_cache_release_evict_count`

- 単位: カウント
- 説明: キャッシュ使用量が容量を超えた際に、解放操作中に削除されたキャッシュエントリの合計数。

## `pip_query_ctx_cnt`

- 単位: カウント
- 説明: BEで現在実行中のクエリの総数。

## `pipe_driver_execution_time`

- 説明: PipelineDriverエグゼキュータがPipelineDriverの処理に費やした累積時間。

## `pipe_driver_queue_len`

- 単位: カウント
- 説明: BEでスケジューリングを待機している準備完了キュー内の準備完了ドライバーの現在の数。

## `pipe_driver_schedule_count`

- 単位: カウント
- 説明: BEのパイプラインエグゼキュータのドライバーのスケジューリング時間の累積数。

## `pipe_poller_block_queue_len`

- 単位: カウント
- 説明: パイプラインエンジン内のPipelineDriverPollerのブロックキューの現在の長さ。

## `pipe_prepare_pool_queue_len`

- 単位: カウント
- 説明: パイプラインPREPAREスレッドプール内のキューに入れられたタスク数。これは瞬間的な値です。

## `pipe_scan_executor_queuing`

- 単位: カウント
- 説明: スキャンオペレータによって起動された保留中の非同期I/Oタスクの現在の数。

## `pk_index_compaction_queue_count`

- 単位: カウント
- 説明: プライマリキーインデックス圧縮スレッドプール内のキューに入れられたタスク数。

## `pk_index_sst_read_error_total`

- タイプ: カウンター
- 単位: カウント
- 説明: レイクプライマリキー永続インデックスにおけるSSTファイル読み取り失敗の総数。SSTマルチゲット（読み取り）操作が失敗した場合に増加します。

## `pk_index_sst_write_error_total`

- タイプ: カウンター
- 単位: カウント
- 説明: レイクプライマリキー永続インデックスにおけるSSTファイル書き込み失敗の総数。SSTファイルの構築が失敗した場合に増加します。

## `plan_fragment_count`

- 単位: カウント
- 説明: 現在実行中のクエリプランフラグメントの数。

## `process_fd_num_limit_hard`

- 単位: カウント
- 説明: ファイルディスクリプタの最大数のハードリミット。

## `process_fd_num_limit_soft`

- 単位: カウント
- 説明: ファイルディスクリプタの最大数のソフトリミット。この項目はソフトリミットを示します。ハードリミットは`ulimit`コマンドを使用して設定できます。

## `process_fd_num_used`

- 単位: カウント
- 説明: このBEプロセスで現在使用中のファイルディスクリプタの数。

## `process_mem_bytes`

- 単位: バイト
- 説明: このプロセスが使用するメモリ。

## `process_thread_num`

- 単位: カウント
- 説明: このプロセス内のスレッドの総数。

## `publish_version_queue_count`

- 単位: カウント
- 説明: パブリッシュバージョン スレッドプール内のキューに入れられたタスク数。

## `push_request_duration_us`

- 単位: us
- 説明: Spark Loadに費やされた合計時間。

## `push_request_write_bytes`

- 単位: バイト
- 説明: Spark Loadを介して書き込まれた合計バイト数。

## `push_request_write_bytes_per_second`

- 単位: バイト/秒
- 説明: Spark Loadのデータ書き込みレート。

## `push_request_write_rows`

- 単位: 数
- 説明: Spark Load経由で書き込まれた行の合計。

## `push_requests_total`

- 単位: 数
- 説明: 成功および失敗したSpark Loadリクエストの合計数。
