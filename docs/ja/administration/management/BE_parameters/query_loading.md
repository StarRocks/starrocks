---
displayed_sidebar: docs
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 設定

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

---

このトピックでは、以下の種類のFE構成について紹介します：
- [クエリ](#クエリエンジン)
- [ロードとアンロード](#ロードとアンロード)

## クエリエンジン

### dictionary_speculate_min_chunk_size

- デフォルト: 10000
- タイプ: Int
- 単位: Rows
- 変更可能: No
- 説明: StringColumnWriter および DictColumnWriter が辞書エンコーディングの推測を開始するために使用する最小行数（チャンクサイズ）。受信カラム（または蓄積されたバッファに加えた受信行）のサイズが `dictionary_speculate_min_chunk_size` 以上であれば、ライターは即座に推測を実行してエンコーディング（DICT、PLAIN、または BIT_SHUFFLE のいずれか）を設定し、さらに行をバッファリングしません。推測では文字列カラムに対して `dictionary_encoding_ratio`、数値/非文字列カラムに対して `dictionary_encoding_ratio_for_non_string_column` を使用して辞書エンコーディングが有利かどうかを判断します。また、カラムのバイトサイズが大きく（UINT32_MAX 以上）なる場合は、`BinaryColumn<uint32_t>` のオーバーフローを避けるために即時に推測が行われます。
- 導入バージョン: v3.2.0

### disable_storage_page_cache

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: PageCache を無効にするかどうかを制御するブール値。
  - PageCache が有効な場合、StarRocks は最近スキャンされたデータをキャッシュします。
  - PageCache は、類似のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。
  - `true` は PageCache を無効にすることを示します。
  - この項目のデフォルト値は StarRocks v2.4 以降、`true` から `false` に変更されました。
- 導入バージョン: -

### enable_bitmap_index_memory_page_cache

- デフォルト: true 
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Bitmap インデックスのメモリキャッシュを有効にするかどうか。Bitmap インデックスを使用してポイントクエリを高速化したい場合は、メモリキャッシュを使用することを推奨します。
- 導入バージョン: v3.1

### enable_compaction_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位: 
- 変更可能: はい
- 説明: Flat JSON データのコンパクションを有効にするかどうか。
- 導入バージョン: v3.3.3

### enable_json_flat

- デフォルト: false
- タイプ: Boolean
- 単位: 
- 変更可能: はい
- 説明: Flat JSON 機能を有効にするかどうか。 この機能を有効にすると、新しくロードされた JSON データが自動的にフラット化され、JSON クエリパフォーマンスが向上します。
- 導入バージョン: v3.3.0

### enable_lazy_dynamic_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位: 
- 変更可能: はい
- 説明: クエリが読み取りプロセスで Flat JSON スキーマを見逃した場合に Lazy Dynamic Flat JSON を有効にするかどうか。この項目が `true` に設定されている場合、StarRocks は Flat JSON 操作を読み取りプロセスではなく計算プロセスに延期します。
- 導入バージョン: v3.3.3

### enable_ordinal_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: オーディナルインデックスのメモリキャッシュを有効にするかどうか。オーディナルインデックスは行IDからデータページの位置へのマッピングであり、スキャンを高速化するために使用できる。
- 導入バージョン: -

### enable_string_prefix_zonemap

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 文字列（CHAR/VARCHAR）列に対して、前置長に基づくゾーンマップを有効にするかどうか。非キー列では、最小値/最大値は `string_prefix_zonemap_prefix_len` で指定した長さに切り詰められます。
- 導入バージョン: -

### enable_zonemap_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ゾーンマップインデックスのメモリーキャッシュを有効にするかどうか。ゾーンマップインデックスを使用してスキャンを高速化したい場合は、メモリキャッシュを使用することを推奨します。
- 導入バージョン: -

### exchg_node_buffer_size_bytes

- デフォルト: 10485760
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 各クエリの交換ノードの受信側の最大バッファサイズ。この設定項目はソフトリミットです。データが過剰な速度で受信側に送信されると、バックプレッシャーがトリガーされます。
- 導入バージョン: -

### exec_state_report_max_threads

- デフォルト: 2
- タイプ: Int
- 単位: スレッド
- 変更可能: はい
- 説明: exec-state-report スレッドプールの最大スレッド数。このプールは `ExecStateReporter` が通常優先度の実行状態レポート（フラグメント完了やエラーステータスなど）を BE から FE へ非同期で RPC 送信するために使用されます。起動時の実際のプールサイズは `max(1, exec_state_report_max_threads)` になります。このコンフィグを実行時に変更すると、全エグゼキュータセット（共有・専有）のプールに対して `update_max_threads` が呼び出されます。プールのタスクキューサイズは 1000 固定です。高並行クエリ実行時に実行状態レポートが遅延または消失する場合は値を増やしてください。対応する高優先度プールは `priority_exec_state_report_max_threads` で制御します。
- 導入バージョン: v4.1.0, v4.0.8, v3.5.15

### file_descriptor_cache_capacity

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: キャッシュできるファイルディスクリプタの数。
- 導入バージョン: -

### flamegraph_tool_dir

- デフォルト: `${STARROCKS_HOME}/bin/flamegraph`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: フレームグラフツールのディレクトリ。このディレクトリには、プロファイルデータからフレームグラフを生成するための pprof、stackcollapse-go.pl、flamegraph.pl スクリプトが含まれている必要があります。
- 導入バージョン: -

### fragment_pool_queue_size

- デフォルト: 2048
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 各 BE ノードで処理できるクエリ数の上限。
- 導入バージョン: -

### fragment_pool_thread_num_max

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クエリに使用される最大スレッド数。
- 導入バージョン: -

### fragment_pool_thread_num_min

- デフォルト: 64
- タイプ: Int
- 単位: 分
- 変更可能: いいえ
- 説明: クエリに使用される最小スレッド数。
- 導入バージョン: -

### hdfs_client_enable_hedged_read

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: ヘッジドリード機能を有効にするかどうかを指定します。
- 導入バージョン: v3.0

### hdfs_client_hedged_read_threadpool_size

- デフォルト: 128
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: HDFS クライアントのヘッジドリードスレッドプールのサイズを指定します。スレッドプールのサイズは、HDFS クライアントでヘッジドリードを実行するために専用されるスレッドの数を制限します。これは、HDFS クラスターの **hdfs-site.xml** ファイルの `dfs.client.hedged.read.threadpool.size` パラメータに相当します。
- 導入バージョン: v3.0

### hdfs_client_hedged_read_threshold_millis

- デフォルト: 2500
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: ヘッジドリードを開始する前に待機するミリ秒数を指定します。たとえば、このパラメータを `30` に設定した場合、ブロックからの読み取りが 30 ミリ秒以内に返されない場合、HDFS クライアントはすぐに別のブロックレプリカに対して新しい読み取りを開始します。これは、HDFS クラスターの **hdfs-site.xml** ファイルの `dfs.client.hedged.read.threshold.millis` パラメータに相当します。
- 導入バージョン: v3.0

### io_coalesce_adaptive_lazy_active

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 述語の選択性に基づいて、述語列と非述語列の I/O を結合するかどうかを適応的に決定します。
- 導入バージョン: v3.2

### jit_lru_cache_size

- デフォルト: 0
- タイプ: Int
- 単位: Bytes
- 変更可能: はい
- 説明: JIT コンパイルのための LRU キャッシュサイズ。0 より大きい場合、キャッシュの実際のサイズを表します。0 以下に設定されている場合、システムは `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` の式を使用してキャッシュを適応的に設定します (ノードの `mem_limit` は 16 GB 以上でなければなりません)。
- 導入バージョン: -

### json_flat_column_max

- デフォルト: 100
- タイプ: Int
- 単位: 
- 変更可能: はい
- 説明: Flat JSON によって抽出できるサブフィールドの最大数。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

### json_flat_create_zonemap

- デフォルト: true
- タイプ: Boolean
- 単位: 
- 変更可能: はい
- 説明: フラット化された JSON のサブカラムに対してゾーンマップを作成するかどうか。`enable_json_flat` が `true` の場合にのみ有効です。
- 導入バージョン: -

### json_flat_null_factor

- デフォルト: 0.3
- タイプ: Double
- 単位: 
- 変更可能: はい
- 説明: Flat JSON のために抽出する NULL 値の割合。NULL 値の割合がこのしきい値を超える列は抽出されません。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

### json_flat_sparsity_factor

- デフォルト: 0.3
- タイプ: Double
- 単位: 
- 変更可能: はい
- 説明: Flat JSON の同じ名前を持つ列の割合。この値より低い場合、抽出は行われません。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

### lake_tablet_ignore_invalid_delete_predicate

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: カラム名が変更された後に論理削除によって重複キーのテーブルの tablet rowset メタデータに導入される可能性のある無効な delete predicate を無視するかどうかを制御するブール値。
- 導入バージョン: v4.0

### max_hdfs_file_handle

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 開くことができる HDFS ファイルディスクリプタの最大数。
- 導入バージョン: -

### max_memory_sink_batch_count

- デフォルト: 20
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: スキャンキャッシュバッチの最大数。
- 導入バージョン: -

### max_pushdown_conditions_per_column

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各列でプッシュダウンを許可する条件の最大数。この制限を超えると、述語はストレージレイヤーにプッシュダウンされません。
- 導入バージョン: -

### max_scan_key_num

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各クエリによってセグメント化される最大スキャンキー数。
- 導入バージョン: -

### min_file_descriptor_number

- デフォルト: 60000
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE プロセスの最小ファイルディスクリプタ数。
- 導入バージョン: -

### object_storage_connect_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: オブジェクトストレージとのソケット接続を確立するためのタイムアウト期間。`-1` は SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

### object_storage_request_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: オブジェクトストレージとの HTTP 接続を確立するためのタイムアウト期間。`-1` は SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

### parquet_late_materialization_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: パフォーマンスを向上させるために Parquet リーダーの後期実体化を有効にするかどうかを制御するブール値。`true` は後期実体化を有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

### parquet_page_index_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: パフォーマンスを向上させるために Parquet ファイルのページインデックスを有効にするかどうかを制御するブール値。`true` はページインデックスを有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: v3.3

### parquet_reader_bloom_filter_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: パフォーマンスを向上させるために Parquet ファイルのブルームフィルターを有効にするかどうかを制御するブール値。`true` はブルームフィルタを有効にすることを示し、`false` は無効にすることを示す。システム変数 `enable_parquet_reader_bloom_filter` を使用して、セッションレベルでこの動作を制御することもできます。Parquet におけるブルームフィルタは、**各行グループ内のカラムレベルで管理されます**。Parquet ファイルに特定の列に対するブルームフィルタが含まれている場合、クエリはそれらの列に対する述語を使用して行グループを効率的にスキップすることができます。
- 導入バージョン: v3.5

### path_gc_check_step

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 連続してスキャンできる最大ファイル数。
- 導入バージョン: -

### path_gc_check_step_interval_ms

- デフォルト: 10
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: ファイルスキャン間の時間間隔。
- 導入バージョン: -

### path_scan_interval_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: GC が期限切れデータをクリーンアップする時間間隔。
- 導入バージョン: -

### pipeline_connector_scan_thread_num_per_cpu

- デフォルト: 8
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BE ノードの CPU コアごとに Pipeline Connector に割り当てられるスキャンスレッドの数。この設定は v3.1.7 以降、動的に変更されました。
- 導入バージョン: -

### pipeline_poller_timeout_guard_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 変更可能: はい
- 説明: この項目が `0` より大きい値に設定されている場合、ドライバがポーラの 1 回のディスパッチに `pipeline_poller_timeout_guard_ms` 以上の時間がかかると、ドライバとオペレータの情報が出力される。
- 導入バージョン: -

### pipeline_prepare_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Pipeline 実行エンジンの PREPARE Fragment スレッドプールの最大キュー長。
- 導入バージョン: -

### pipeline_prepare_thread_pool_thread_num

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Pipeline 実行エンジン PREPARE Fragment スレッドプールのスレッド数。`0` はシステムの VCPU コア数と同じであることを示す。
- 導入バージョン: -

### pipeline_prepare_timeout_guard_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 変更可能: はい
- 説明: この項目が `0` より大きい値に設定されている場合、PREPARE 処理中にプランの Fragment が `pipeline_prepare_timeout_guard_ms` を超えると、プランの Fragment のスタックトレースが出力される。
- 導入バージョン: -

### pipeline_scan_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Pipeline 実行エンジンの SCAN スレッドプールの最大タスクキュー長。
- 導入バージョン: -

### priority_exec_state_report_max_threads

- デフォルト: 2
- タイプ: Int
- 単位: スレッド
- 変更可能: はい
- 説明: 高優先度 exec-state-report スレッドプールの最大スレッド数。このプールは `ExecStateReporter` が高優先度の実行状態レポート（緊急なフラグメント失敗など）を BE から FE へ非同期で RPC 送信するために使用されます。通常のプールとは異なり、このプールのタスクキューはサイズ無制限です。起動時の実際のプールサイズは `max(1, priority_exec_state_report_max_threads)` になります。このコンフィグを実行時に変更すると、全エグゼキュータセット（共有・専有）の優先度プールに対して `update_max_threads` が呼び出されます。通常プールは `exec_state_report_max_threads` で制御します。
- 導入バージョン: v4.1.0, v4.0.8, v3.5.15

### query_cache_capacity

- デフォルト: 536870912
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: BE のクエリキャッシュのサイズ。デフォルトサイズは 512 MB です。サイズは 4 MB 未満にすることはできません。BE のメモリ容量が期待するクエリキャッシュサイズを提供するのに不十分な場合、BE のメモリ容量を増やすことができます。
- 導入バージョン: -

### query_pool_spill_mem_limit_threshold

- デフォルト: 1.0
- タイプ: Double
- 単位: -
- 変更可能: いいえ
- 説明: 自動スピリングが有効な場合、すべてのクエリのメモリ使用量が `query_pool memory limit * query_pool_spill_mem_limit_threshold` を超えると、中間結果のスピリングがトリガーされます。
- 導入バージョン: v3.2.7

### result_buffer_cancelled_interval_time

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BufferControlBlock がデータを解放するまでの待機時間。
- 導入バージョン: -

### scan_context_gc_interval_min

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 変更可能: はい
- 説明: スキャンコンテキストをクリーンアップする時間間隔。
- 導入バージョン: -

### scanner_row_num

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各スキャンで各スキャンスレッドが返す最大行数。
- 導入バージョン: -

### scanner_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ストレージエンジンがサポートするスキャンタスクの数。
- 導入バージョン: -

### scanner_thread_pool_thread_num

- デフォルト: 48
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ストレージエンジンが同時ストレージボリュームスキャンに使用するスレッドの数。すべてのスレッドはスレッドプールで管理されます。
- 導入バージョン: -

### string_prefix_zonemap_prefix_len

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: `enable_string_prefix_zonemap` が有効な場合に、文字列ゾーンマップの最小値/最大値に使用する前置長。
- 導入バージョン: -

## ロードとアンロード

### clear_transaction_task_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: トランザクションをクリアするために使用されるスレッドの数。
- 導入バージョン: -

### column_mode_partial_update_insert_batch_size

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 挿入行を処理する際の列モード部分更新におけるバッチサイズ。この項目が `0` または負の数値に設定されている場合、無限ループを回避するため `1` に制限されます。この項目は各バッチで処理される新規挿入行の数を制御します。大きな値は書き込みパフォーマンスを向上させますが、より多くのメモリを消費します。
- 導入バージョン: v3.5.10, v4.0.2

### enable_load_spill_parallel_merge

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 単一タブレット内で並列スピルマージを有効にするかどうかを指定します。これを有効にすると、データロード中のスピルマージのパフォーマンスが向上します。
- 導入バージョン: -

### enable_parallel_memtable_finalize

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Lake テーブル（ストレージとコンピューティングの分離モード）へのデータロード時に並列 MemTable Finalize を有効にするかどうかを指定します。有効にすると、MemTable の Finalize 操作（ソート/集計）が書き込みスレッドからフラッシュスレッドに移動され、前の MemTable が並列で Finalize およびフラッシュされている間、書き込みスレッドは新しい MemTable へのデータ挿入を継続できます。これにより、CPU 集約型の Finalize 操作と I/O 集約型のフラッシュ操作をオーバーラップさせることで、ロードスループットを大幅に向上させることができます。注意: 自動インクリメント列を埋める必要がある場合、自動インクリメント ID の割り当ては MemTable がフラッシュに送信される前に完了する必要があるため、この最適化は自動的に無効になります。
- 導入バージョン: -

### allow_list_object_for_random_bucketing_on_cache_miss

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: random bucketing のサイズ判定で Lake metadata がキャッシュミスした際に、object-storage LIST フォールバックを許可するかを制御します。`true` の場合は metadata ファイル LIST にフォールバックして base size を計算します（従来動作、推定がより正確）。`false` の場合は LIST をスキップして `base_size = 0` を使用し、LIST object リクエストを削減しますが、推定精度低下により immutable 判定がやや遅れる可能性があります。
- 導入バージョン: 4.1.0, 4.0.7, 3.5.15 

### enable_stream_load_verbose_log

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Stream Load ジョブの HTTP リクエストとレスポンスをログに記録するかどうかを指定します。
- 導入バージョン: v2.5.17, v3.0.9, v3.1.6, v3.2.1

### flush_thread_num_per_store

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各ストアで MemTable をフラッシュするために使用されるスレッドの数。
- 導入バージョン: -

### lake_flush_thread_num_per_store

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データモードで各ストアで MemTable をフラッシュするために使用されるスレッドの数。
  この値が `0` に設定されている場合、システムは CPU コア数の 2 倍を値として使用します。
  この値が `0` 未満に設定されている場合、システムはその絶対値と CPU コア数の積を値として使用します。
- 導入バージョン: 3.1.12, 3.2.7

### load_data_reserve_hours

- デフォルト: 4
- タイプ: Int
- 単位: 時間
- 変更可能: いいえ
- 説明: 小規模ロードによって生成されたファイルの予約時間。
- 導入バージョン: -

### load_error_log_reserve_hours

- デフォルト: 48
- タイプ: Int
- 単位: 時間
- 変更可能: はい
- 説明: データロードログが保持される時間。
- 導入バージョン: -

### load_process_max_memory_limit_bytes

- デフォルト: 107374182400
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースの最大サイズ制限。
- 導入バージョン: -

### load_spill_memory_usage_per_merge

- デフォルト: 1073741824
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: スピルマージ中のマージ操作ごとの最大メモリ使用量。デフォルトは 1 GB (1073741824 バイト) です。このパラメータは、データロードのスピルマージ中に個々のマージタスクのメモリ消費を制御し、過度のメモリ使用を防ぎます。
- 導入バージョン: -

### max_consumer_num_per_group

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: Routine Load のコンシューマーグループ内の最大コンシューマー数。
- 導入バージョン: -

### max_runnings_transactions_per_txn_map

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各パーティションで同時に実行できるトランザクションの最大数。
- 導入バージョン: -

### number_tablet_writer_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: インポート用の tablet writer のスレッド数，Stream Load、Broker Load、Insert などに使用されます。パラメータが 0 以下に設定されている場合、システムは CPU コア数の半分（最小で 16）を使用します。パラメータが 0 より大きい値に設定されている場合、システムはその値を使用します。この設定は v3.1.7 以降、動的に変更されました。
- 導入バージョン: -

### push_worker_count_high_priority

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: HIGH 優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

### push_worker_count_normal_priority

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: NORMAL 優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

### streaming_load_max_batch_size_mb

- デフォルト: 100
- タイプ: Int
- 単位: MB
- 変更可能: はい
- 説明: StarRocks にストリーミングできる JSON ファイルの最大サイズ。
- 導入バージョン: -

### streaming_load_max_mb

- デフォルト: 102400
- タイプ: Int
- 単位: MB
- 変更可能: はい
- 説明: StarRocks にストリーミングできるファイルの最大サイズ。v3.0 以降、デフォルト値は `10240` から `102400` に変更されました。
- 導入バージョン: -

### streaming_load_rpc_max_alive_time_sec

- デフォルト: 1200
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: Stream Load の RPC タイムアウト。
- 導入バージョン: -

### transaction_publish_version_thread_pool_idle_time_ms

- デフォルト: 60000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: スレッドが Publish Version スレッドプールによって再利用されるまでの Idle 時間。
- 導入バージョン: -

### transaction_publish_version_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: バージョンを公開するために使用される最大スレッド数。この値が `0` 以下に設定されている場合、システムは CPU コア数を値として使用し、インポートの同時実行が高いが固定スレッド数しか使用されない場合にスレッドリソースが不足するのを回避します。v2.5 以降、デフォルト値は `8` から `0` に変更されました。
- 導入バージョン: -

### write_buffer_size

- デフォルト: 104857600
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: メモリ内の MemTable のバッファサイズ。この設定項目はフラッシュをトリガーするしきい値です。
- 導入バージョン: -

### broker_write_timeout_seconds

- デフォルト: 30
- タイプ: int
- 単位: Seconds
- 変更可能: No
- 説明: バックエンドの broker 操作で書き込み/IO RPC に使用されるタイムアウト（秒）。この値は 1000 を掛けられてミリ秒タイムアウトとなり、BrokerFileSystem および BrokerServiceConnection インスタンス（例：ファイルエクスポートやスナップショットのアップロード/ダウンロード）にデフォルトの timeout_ms として渡されます。broker やネットワークが遅い場合、または大きなファイルを転送する場合は早期タイムアウトを避けるために増やしてください；減らすと broker RPC が早期に失敗する可能性があります。この値は common/config に定義され、プロセス起動時に適用されます（動的リロード不可）。
- 導入バージョン: v3.2.0

### enable_load_channel_rpc_async

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、load-channel の open RPC（例: PTabletWriterOpen）の処理が BRPC ワーカーから専用のスレッドプールへオフロードされます。リクエストハンドラは ChannelOpenTask を生成して内部 `_async_rpc_pool` に投入し、`LoadChannelMgr::_open` をインラインで実行しません。これにより BRPC スレッド内の作業量とブロッキングが減少し、`load_channel_rpc_thread_pool_num` と `load_channel_rpc_thread_pool_queue_size` で同時実行性を調整できるようになります。スレッドプールへの投入が失敗する（プールが満杯またはシャットダウン済み）と、リクエストはキャンセルされエラー状態が返されます。プールは `LoadChannelMgr::close()` でシャットダウンされるため、有効化する際は容量とライフサイクルを考慮し、リクエストの拒否や処理遅延を避けるようにしてください。
- 導入バージョン: v3.5.0

### enable_load_segment_parallel

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: 有効にすると、rowset セグメントの読み込みと rowset レベルの読み取りが StarRocks のバックグラウンドスレッドプール（ExecEnv::load_segment_thread_pool と ExecEnv::load_rowset_thread_pool）を使って並行して実行されます。Rowset::load_segments および TabletReader::get_segment_iterators は各セグメントまたは各 rowset のタスクをこれらのプールにサブミットし、サブミッションに失敗した場合は直列読み込みにフォールバックして警告をログに出します。大きな rowset に対する読み取り／ロードレイテンシを削減できますが、CPU/IO の並列度とメモリプレッシャーが増加します。注意: 並列ロードはセグメントの読み込み完了順序を変える可能性があり、そのため部分コンパクションを防ぎます（コードは `_parallel_load` をチェックし、有効時は部分コンパクションを無効化します）。セグメント順序に依存する操作への影響を考慮してください。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

### enable_streaming_load_thread_pool

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: ストリーミングロード用のスキャナを専用の streaming load スレッドプールに送るかどうかを制御します。有効で、かつクエリが `TLoadJobType::STREAM_LOAD` の LOAD の場合、ConnectorScanNode はスキャナタスクを `streaming_load_thread_pool`（INT32_MAX スレッドおよびキューサイズで構成され、事実上無制限）に送信します。無効にすると、スキャナは一般的な `thread_pool` とその `PriorityThreadPool` 提出ロジック（優先度計算、try_offer/offer の振る舞い）を使用します。有効にすると通常のクエリ実行からストリーミングロードの作業を分離して干渉を減らせますが、専用プールは事実上無制限であるため、トラフィックが多いと同時スレッド数やリソース使用量が増える可能性があります。このオプションはデフォルトでオンになっており、通常は変更を必要としません。
- 導入バージョン: v3.2.0

### es_http_timeout_ms

- デフォルト: 5000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: No
- 説明: Elasticsearch の scroll リクエストに対して ESScanReader 内の ES ネットワーククライアントが使用する HTTP 接続タイムアウト（ミリ秒）。この値は次の scroll POST を送信する前に `network_client.set_timeout_ms()` を介して適用され、スクロール処理中にクライアントが ES の応答を待つ時間を制御します。ネットワークが遅い場合や大きなクエリで早期タイムアウトを回避するためにこの値を増やし、応答しない ES ノードに対しては早めに失敗させたい場合は値を小さくしてください。この設定はスクロールコンテキストのキープアライブ期間を制御する `es_scroll_keepalive` を補完します。
- 導入バージョン: v3.2.0

### es_index_max_result_window

- デフォルト: 10000
- タイプ: Int
- 単位: Documents
- 変更可能: No
- 説明: StarRocks が単一バッチで Elasticsearch に要求する最大ドキュメント数を制限します。ES リーダーの `KEY_BATCH_SIZE` を構築する際、StarRocks は ES リクエストバッチサイズを min(`es_index_max_result_window`, `chunk_size`) に設定します。ES リクエストが Elasticsearch のインデックス設定 `index.max_result_window` を超えると、Elasticsearch は HTTP 400 (Bad Request) を返します。大きなインデックスをスキャンする場合はこの値を調整するか、Elasticsearch 側で `index.max_result_window` を増やしてより大きな単一リクエストを許可してください。
- 導入バージョン: v3.2.0

### load_channel_rpc_thread_pool_num

- デフォルト: -1
- タイプ: Int
- 単位: Threads
- 変更可能: はい
- 説明: load-channel 非同期 RPC スレッドプールの最大スレッド数。`<= 0`（デフォルト `-1`）に設定するとプールサイズは自動的に CPU コア数（CpuInfo::num_cores()）に設定されます。設定された値は ThreadPoolBuilder の max threads として使われ、プールの最小スレッド数は min(5, max_threads) に設定されます。プールのキューサイズは `load_channel_rpc_thread_pool_queue_size` によって別途制御されます。この設定は、load RPC の処理を同期から非同期に切り替えた後も動作が互換となるように、async RPC プールサイズを brpc ワーカーのデフォルト（`brpc_num_threads`）に合わせるために導入されました。ランタイムでこの設定を変更すると ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(...) がトリガーされます。
- 導入バージョン: v3.5.0

### load_channel_rpc_thread_pool_queue_size

- デフォルト: 1024000
- タイプ: int
- 単位: Count
- 変更可能: いいえ
- 説明: LoadChannelMgr によって作成される Load チャネル RPC スレッドプールの保留タスク最大キューサイズを設定します。このスレッドプールは `enable_load_channel_rpc_async` が有効なときに非同期の `open` リクエストを実行し、プールサイズは `load_channel_rpc_thread_pool_num` と組になっています。大きなデフォルト値（1024000）は、同期処理から非同期処理への切り替え後も動作が保たれるように brpc ワーカーのデフォルトに合わせたものです。キューが満杯になると ThreadPool::submit() は失敗し、到着した open RPC はエラーでキャンセルされ、呼び出し元は拒否を受け取ります。大量の同時 `open` リクエストをバッファしたい場合はこの値を増やしてください；逆に小さくするとバックプレッシャーが強まり、負荷時に拒否が増える可能性があります。
- 導入バージョン: v3.5.0

### load_diagnose_rpc_timeout_profile_threshold_ms

- デフォルト: 60000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: ロード RPC がタイムアウトしたとき（エラーに "[E1008]Reached timeout" が含まれる）かつ `enable_load_diagnose` が true の場合、フルプロファイリング診断を要求するかどうかを制御する閾値です。リクエスト単位の RPC タイムアウト `_rpc_timeout_ms` が `load_diagnose_rpc_timeout_profile_threshold_ms` より大きい場合、その診断でプロファイリングが有効になります。より小さい `_rpc_timeout_ms` の場合、リアルタイム／短タイムアウトのロードで頻繁な重い診断が発生しないよう、20 回のタイムアウトにつき 1 回だけプロファイリングをサンプリングします。この値は送信される `PLoadDiagnoseRequest` の `profile` フラグに影響します。スタックトレースの振る舞いは `load_diagnose_rpc_timeout_stack_trace_threshold_ms`、送信タイムアウトは `load_diagnose_send_rpc_timeout_ms` によって別途制御されます。
- 導入バージョン: v3.5.0

### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- デフォルト: 600000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: 長時間実行されている load RPC に対してリモートのスタックトレースを要求するかどうかを決定する閾値（ミリ秒）。load RPC がタイムアウトエラーでタイムアウトし、有効な RPC タイムアウト（_rpc_timeout_ms）がこの値を超える場合、`OlapTableSink`/`NodeChannel` はターゲット BE への `load_diagnose` RPC に `stack_trace=true` を含め、BE がデバッグ用のスタックトレースを返せるようにします。`LocalTabletsChannel::SecondaryReplicasWaiter` は、セカンダリレプリカの待機がこの間隔を超えた場合に、プライマリからベストエフォートのスタックトレース診断をトリガーします。この動作は `enable_load_diagnose` を必要とし、診断 RPC のタイムアウトには `load_diagnose_send_rpc_timeout_ms` を使用します；プロファイリングは `load_diagnose_rpc_timeout_profile_threshold_ms` によって別途制御されます。この値を下げると、スタックトレース要求がより積極的になります。
- 導入バージョン: v3.5.0

### load_fp_brpc_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: `node_channel_set_brpc_timeout` の fail point がトリガーされたときに OlapTableSink が使用するチャネルごとの brpc RPC タイムアウトを上書きします。正の値に設定すると、NodeChannel は内部の `_rpc_timeout_ms` をこの値（ミリ秒）に設定し、open/add-chunk/cancel RPC が短いタイムアウトを使うようになり、"[E1008]Reached timeout" エラーを発生させる brpc タイムアウトのシミュレーションが可能になります。デフォルト（`-1`）は上書きを無効にします。この値の変更はテストとフォールトインジェクションを目的としており、小さい値は偽のタイムアウトを発生させてロード診断をトリガーする可能性があります（`enable_load_diagnose`、`load_diagnose_rpc_timeout_profile_threshold_ms`、`load_diagnose_rpc_timeout_stack_trace_threshold_ms`、`load_diagnose_send_rpc_timeout_ms` を参照）。
- 導入バージョン: v3.5.0

### load_fp_tablets_channel_add_chunk_block_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: 有効にすると（正のミリ秒値に設定すると）このフェイルポイント設定は load 処理中に TabletsChannel::add_chunk を指定した時間だけスリープさせます。BRPC のタイムアウトエラー（例: "[E1008]Reached timeout"）をシミュレートしたり、add_chunk の高コスト操作によるロード遅延を模擬するために使用されます。値が `<= 0`（デフォルト `-1`）の場合、注入は無効になります。フォールトハンドリング、タイムアウト、レプリカ同期挙動のテストを目的としており、書き込み完了を遅延させ上流のタイムアウトやレプリカの中止を引き起こす可能性があるため、通常の本番ワークロードでは有効にしないでください。
- 導入バージョン: v3.5.0

### streaming_load_thread_pool_idle_time_ms

- デフォルト: 2000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: No
- 説明: streaming-load 関連のスレッドプールに対するスレッドのアイドルタイムアウト（ミリ秒）を設定します。この値は `stream_load_io` プールに対して ThreadPoolBuilder に渡されるアイドルタイムアウトとして使用され、`load_rowset_pool` と `load_segment_pool` にも適用されます。これらのプール内のスレッドはこの期間アイドル状態が続くと回収されます；値を小さくするとアイドル時のリソース使用は減りますがスレッド生成のオーバーヘッドが増え、値を大きくするとスレッドが長く生存します。`stream_load_io` プールは `enable_streaming_load_thread_pool` が有効な場合に使用されます。
- 導入バージョン: v3.2.0

### streaming_load_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: ExecEnv 初期化時に作成される streaming load IO スレッドプール ("stream_load_io") の最小スレッド数。プールは `set_max_threads(INT32_MAX)` と `set_max_queue_size(INT32_MAX)` で構築され、事実上デッドロック回避のために無制限にされます。値が 0 の場合、プールはスレッドを持たずに需要に応じて拡張します；正の値を設定すると起動時にその数のスレッドを確保します。このプールは `enable_streaming_load_thread_pool` が true のときに使用され、アイドルタイムアウトは `streaming_load_thread_pool_idle_time_ms` で制御されます。全体の並行度は依然として `fragment_pool_thread_num_max` と `webserver_num_workers` によって制約されるため、この値を変更する必要は稀であり、高すぎるとリソース使用量が増える可能性があります。
- 導入バージョン: v3.2.0
