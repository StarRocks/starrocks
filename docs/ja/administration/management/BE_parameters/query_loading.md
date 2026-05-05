---
displayed_sidebar: docs
sidebar_label: "Query and Loading"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE構成 - クエリとロード

<BEConfigMethod />

<CNConfigMethod />

## BE構成項目を表示

以下のコマンドを使用してBE構成項目を表示できます。

```SQL
SELECT * FROM information_schema.be_configs [WHERE NAME LIKE "%<name_pattern>%"]
```

## BEパラメーターを設定

<PostBEConfig />

<StaticBEConfigNote />

***

このトピックでは、以下の種類のFE構成について説明します。

- [クエリ](#query)
- [ロードとアンロード](#loading-and-unloading)

## クエリ

### clear_udf_cache_when_start

- デフォルト: false
- タイプ: ブール
- 単位: -
- 変更可能: いいえ
- 説明: 有効にすると、BEのUserFunctionCacheは起動時にローカルにキャッシュされたすべてのユーザー関数ライブラリをクリアします。UserFunctionCache::init中に、コードは_reset_cache_dir()を呼び出し、これにより設定されたUDFライブラリディレクトリ（kLibShardNumサブディレクトリに整理されています）からUDFファイルが削除され、Java/Python UDFサフィックス（.jar/.py）を持つファイルが削除されます。無効の場合（デフォルト）、BEは既存のキャッシュされたUDFファイルを削除する代わりにロードします。これを有効にすると、再起動後の初回使用時にUDFバイナリが再ダウンロードされることになります（ネットワークトラフィックと初回使用時のレイテンシが増加します）。
- 導入バージョン: v4.0.0

### dictionary_speculate_min_chunk_size

- デフォルト: 10000
- タイプ: 整数
- 単位: 行
- 変更可能: いいえ
- 説明: StringColumnWriterとDictColumnWriterが辞書エンコーディングの推測をトリガーするために使用する最小行数（チャンクサイズ）。入力列（または蓄積されたバッファと入力行）のサイズが`dictionary_speculate_min_chunk_size`以上の場合、ライターはすぐに推測を実行し、より多くの行をバッファリングするのではなく、エンコーディング（DICT、PLAIN、またはBIT_SHUFFLE）を設定します。推測は、文字列列には`dictionary_encoding_ratio`を、数値/非文字列列には`dictionary_encoding_ratio_for_non_string_column`を使用して、辞書エンコーディングが有益かどうかを判断します。また、大きな列のbyte_size（UINT32_MAX以上）は、`BinaryColumn<uint32_t>`オーバーフローを避けるために即座の推測を強制します。
- 導入バージョン: v3.2.0

### disable_storage_page_cache

- デフォルト: false
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: PageCacheを無効にするかどうかを制御するブール値。
  - PageCacheが有効な場合、StarRocksは最近スキャンされたデータをキャッシュします。
  - PageCacheは、類似のクエリが頻繁に繰り返される場合に、クエリパフォーマンスを大幅に向上させることができます。
  - `true`はPageCacheを無効にすることを示します。
  - この項目のデフォルト値は、StarRocks v2.4以降、`true`から`false`に変更されました。
- 導入バージョン: -

### enable_bitmap_index_memory_page_cache

- デフォルト: true
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: ビットマップインデックスのメモリキャッシュを有効にするかどうか。ポイントクエリを高速化するためにビットマップインデックスを使用したい場合は、メモリキャッシュを推奨します。
- 導入バージョン: v3.1

### enable_compaction_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位:
- 変更可能: はい
- 説明: Flat JSON データの圧縮を有効にするかどうか。
- 導入バージョン: v3.3.3

### enable_json_flat

- デフォルト: false
- タイプ: Boolean
- 単位:
- 変更可能: はい
- 説明: Flat JSON 機能を有効にするかどうか。この機能が有効になると、新しくロードされた JSON データは自動的にフラット化され、JSON クエリのパフォーマンスが向上します。
- 導入バージョン: v3.3.0

### enable_lazy_dynamic_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位:
- 変更可能: はい
- 説明: クエリが読み取りプロセスで Flat JSON スキーマを見逃した場合に、Lazy Dynamic Flat JSON を有効にするかどうか。この項目が `true` に設定されている場合、StarRocks は Flat JSON 操作を読み取りプロセスではなく計算プロセスに延期します。
- 導入バージョン: v3.3.3

### enable_ordinal_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 順序インデックスのメモリキャッシュを有効にするかどうか。順序インデックスは行 ID からデータページ位置へのマッピングであり、スキャンを高速化するために使用できます。
- 導入バージョン: -

### enable_string_prefix_zonemap

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: プレフィックスベースの最小/最大値を使用して、文字列 (CHAR/VARCHAR) 列の ZoneMap を有効にするかどうか。非キー文字列列の場合、最小/最大値は `string_prefix_zonemap_prefix_len` で設定された固定プレフィックス長に切り詰められます。
- 導入バージョン: -

### enable_zonemap_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ゾーンマップインデックスのメモリキャッシュを有効にするかどうか。スキャンを高速化するためにゾーンマップインデックスを使用したい場合は、メモリキャッシュを推奨します。
- 導入バージョン: -

### exchg_node_buffer_size_bytes

- デフォルト: 10485760
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 各クエリの交換ノードの受信側における最大バッファサイズ。この設定項目はソフトリミットです。データが過剰な速度で受信側に送信されると、バックプレッシャーがトリガーされます。
- 導入バージョン: -

### exec_state_report_max_threads

- デフォルト: 2
- タイプ: Int
- 単位: スレッド
- 変更可能: はい
- 説明: exec-state-report スレッドプールの最大スレッド数。このプールは、`ExecStateReporter` によって、非優先度の実行ステータスレポート（フラグメント完了やエラー状態など）を BE から FE へ RPC 経由で非同期に送信するために使用されます。起動時の実際のプールサイズは `max(1, exec_state_report_max_threads)` です。実行時にこの設定を変更すると、すべてのエグゼキュータセット（共有および排他）のプールで `update_max_threads` がトリガーされます。このプールには固定のタスクキューサイズ 1000 があり、すべてのスレッドがビジーでキューが満杯の場合、レポートの送信はサイレントに破棄されます。高優先度プール用の `priority_exec_state_report_max_threads` と対になっています。高いクエリ同時実行下で遅延または破棄された実行状態レポートが観測された場合は、この値を増やしてください。
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
- 説明: フレームグラフツールのディレクトリ。プロファイルデータからフレームグラフを生成するための pprof、stackcollapse-go.pl、および flamegraph.pl スクリプトが含まれている必要があります。
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
- 説明: クエリに使用されるスレッドの最大数。
- 導入バージョン: -

### fragment_pool_thread_num_min

- デフォルト: 64
- 型: Int
- 単位: 分 -
- 変更可能: いいえ
- 説明: クエリに使用されるスレッドの最小数。
- 導入バージョン: -

### hdfs_client_enable_hedged_read

- デフォルト: false
- 型: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: ヘッジドリード機能を有効にするかどうかを指定します。
- 導入バージョン: v3.0

### hdfs_client_hedged_read_threadpool_size

- デフォルト: 128
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: HDFSクライアント上のヘッジドリードスレッドプールのサイズを指定します。スレッドプールサイズは、HDFSクライアントでヘッジドリードの実行に割り当てるスレッドの数を制限します。これは、`dfs.client.hedged.read.threadpool.size` パラメータに相当します。**hdfs-site.xml** HDFSクラスターのファイル。
- 導入バージョン: v3.0

### hdfs_client_hedged_read_threshold_millis

- デフォルト: 2500
- 型: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: ヘッジドリードを開始する前に待機するミリ秒数を指定します。例えば、このパラメータを `30` に設定したとします。この状況で、ブロックからの読み取りが30ミリ秒以内に返されない場合、HDFSクライアントは直ちに別のブロックレプリカに対して新しい読み取りを開始します。これは、`dfs.client.hedged.read.threshold.millis` パラメータに相当します。**hdfs-site.xml** HDFSクラスターのファイル。
- 導入バージョン: v3.0

### io_coalesce_adaptive_lazy_active

- デフォルト: true
- 型: Boolean
- 単位: -
- 変更可能: はい
- 説明: 述語の選択性に基づいて、述語列と非述語列のI/Oを結合するかどうかを適応的に決定します。
- 導入バージョン: v3.2

### jit_lru_cache_size

- デフォルト: 0
- 型: Int
- 単位: バイト
- 変更可能: はい
- 説明: JITコンパイル用のLRUキャッシュサイズです。0より大きい値に設定されている場合、キャッシュの実際のサイズを表します。0以下の値に設定されている場合、システムは`jit_lru_cache_size = min(mem_limit*0.01, 1GB)`の式を使用してキャッシュを適応的に設定します（ただし、ノードの`mem_limit`は16 GB以上である必要があります）。
- 導入バージョン: -

### json_flat_column_max

- デフォルト: 100
- 型: Int
- 単位:
- 変更可能: はい
- 説明: Flat JSONで抽出できるサブフィールドの最大数です。このパラメータは、`enable_json_flat`が`true`に設定されている場合にのみ有効になります。
- 導入バージョン: v3.3.0

### json_flat_create_zonemap

- デフォルト: true
- 型: Boolean
- 単位:
- 変更可能: はい
- 説明: 書き込み時にフラット化されたJSONサブ列のZoneMapを作成するかどうか。このパラメータは、`enable_json_flat`が`true`に設定されている場合にのみ有効になります。
- 導入バージョン: -

### json_flat_null_factor

- デフォルト: 0.3
- 型: Double
- 単位:
- 変更可能: はい
- 説明: Flat JSONで抽出する列のNULL値の割合。NULL値の割合がこのしきい値より高い場合、列は抽出されません。このパラメータは、`enable_json_flat`が`true`に設定されている場合にのみ有効になります。
- 導入バージョン: v3.3.0

### json_flat_sparsity_factor

- デフォルト: 0.3
- 型: Double
- 単位:
- 変更可能: はい
- 説明: Flat JSONの同名列の割合。同名列の割合がこの値より低い場合、抽出は実行されません。このパラメータは、`enable_json_flat`が`true`に設定されている場合にのみ有効になります。
- 導入バージョン: v3.3.0

### lake_tablet_ignore_invalid_delete_predicate

- デフォルト: false
- 型: ブール
- 単位: -
- 変更可能: はい
- 説明: カラム名変更後に重複キーテーブルへの論理削除によって導入される可能性のある、タブレットのrowsetメタデータ内の無効な削除述語を無視するかどうかを制御するブール値。
- 導入バージョン: v4.0

### late_materialization_ratio

- デフォルト: 10
- 型: 整数
- 単位: -
- 変更可能: いいえ
- 説明: SegmentIterator (ベクトルクエリエンジン) での遅延具現化の使用を制御する、[0-1000] の範囲の整数比率です。`0` (または ≤ 0) の値は遅延具現化を無効にします。`1000` (または ≥ 1000) はすべての読み取りで遅延具現化を強制します。0 より大きく 1000 未満の値は、遅延具現化と早期具現化の両方のコンテキストが準備され、イテレータが述語フィルター比率に基づいて動作を選択する条件付き戦略を有効にします (値が高いほど遅延具現化が優先されます)。セグメントに複雑なメトリック型が含まれている場合、StarRocks は代わりに `metric_late_materialization_ratio` を使用します。`lake_io_opts.cache_file_only` が設定されている場合、遅延具現化は無効になります。
- 導入バージョン: v3.2.0

### max_hdfs_file_handle

- デフォルト: 1000
- 型: 整数
- 単位: -
- 変更可能: はい
- 説明: 開くことができるHDFSファイルディスクリプタの最大数。
- 導入バージョン: -

### max_memory_sink_batch_count

- デフォルト: 20
- 型: 整数
- 単位: -
- 変更可能: はい
- 説明: スキャンキャッシュバッチの最大数。
- 導入バージョン: -

### max_pushdown_conditions_per_column

- デフォルト: 1024
- 型: 整数
- 単位: -
- 変更可能: はい
- 説明: 各カラムでプッシュダウンを許可する条件の最大数。条件の数がこの制限を超えると、述語はストレージ層にプッシュダウンされません。
- 導入バージョン: -

### max_scan_key_num

- デフォルト: 1024
- 型: 整数
- 単位: -
- 変更可能: はい
- 説明: 各クエリによってセグメント化されるスキャンキーの最大数。
- 導入バージョン: -

### metric_late_materialization_ratio

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 複雑なメトリック列を含む読み取りで、遅延具体化行アクセス戦略がいつ使用されるかを制御します。有効範囲: [0-1000]。`0` は遅延具体化を無効にします。`1000` は、適用可能なすべての読み取りに対して遅延具体化を強制します。1～999 の値は、述語/選択性に基づいて、遅延具体化と早期具体化の両方のコンテキストが準備され、実行時に選択される条件付き戦略を有効にします。複雑なメトリックタイプが存在する場合、`metric_late_materialization_ratio` は一般的な `late_materialization_ratio` をオーバーライドします。注: `cache_file_only` I/O モードでは、この設定に関わらず遅延具体化が無効になります。
- 導入バージョン: v3.2.0

### min_file_descriptor_number

- デフォルト: 60000
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE プロセス内のファイルディスクリプタの最小数。
- 導入バージョン: -

### object_storage_connect_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: オブジェクトストレージとのソケット接続を確立するためのタイムアウト期間。`-1` は、SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

### object_storage_request_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: オブジェクトストレージとのHTTP接続を確立するためのタイムアウト期間。`-1` は、SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

### parquet_late_materialization_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Parquet リーダーの遅延具体化を有効にしてパフォーマンスを向上させるかどうかを制御するブール値。`true` は遅延具体化を有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

### parquet_page_index_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: パフォーマンスを向上させるために、Parquetファイルのページインデックスを有効にするかどうかを制御するブール値です。`true` はページインデックスを有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: v3.3

### parquet_reader_bloom_filter_enable

- デフォルト: true
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: パフォーマンスを向上させるために、Parquetファイルのブルームフィルターを有効にするかどうかを制御するブール値です。`true` はブルームフィルターを有効にすることを示し、`false` は無効にすることを示します。この動作は、システム変数 `enable_parquet_reader_bloom_filter` を使用してセッションレベルで制御することもできます。Parquetのブルームフィルターは維持されます。**各行グループ内の列レベルで**。Parquetファイルが特定の列のブルームフィルターを含む場合、クエリはそれらの列の述語を使用して行グループを効率的にスキップできます。
- 導入バージョン: v3.5

### path_gc_check_step

- デフォルト: 1000
- タイプ: 整数
- 単位: -
- 変更可能: はい
- 説明: 毎回連続してスキャンできるファイルの最大数。
- 導入バージョン: -

### path_gc_check_step_interval_ms

- デフォルト: 10
- タイプ: 整数
- 単位: ミリ秒
- 変更可能: はい
- 説明: ファイルスキャン間の時間間隔。
- 導入バージョン: -

### path_scan_interval_second

- デフォルト: 86400
- タイプ: 整数
- 単位: 秒
- 変更可能: はい
- 説明: GCが期限切れのデータをクリーンアップする時間間隔。
- 導入バージョン: -

### pipeline_connector_scan_thread_num_per_cpu

- デフォルト: 8
- タイプ: 倍精度浮動小数点数
- 単位: -
- 変更可能: はい
- 説明: BEノードのCPUコアごとにPipeline Connectorに割り当てられるスレッド数。この設定はv3.1.7以降、動的に変更されました。
- 導入バージョン: -

### pipeline_poller_timeout_guard_ms

- デフォルト: -1
- 型: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: この項目が`0`より大きい値に設定されている場合、ポーラーでの単一ディスパッチにドライバーが`pipeline_poller_timeout_guard_ms`より長くかかると、ドライバーとオペレーターの情報が出力されます。
- 導入バージョン: -

### pipeline_prepare_thread_pool_queue_size

- デフォルト: 102400
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: パイプライン実行エンジンのPREPAREフラグメントスレッドプールの最大キュー長。
- 導入バージョン: -

### pipeline_prepare_thread_pool_thread_num

- デフォルト: 0
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: パイプライン実行エンジンのPREPAREフラグメントスレッドプール内のスレッド数。`0`は、値がシステムVCPUコア数と等しいことを示します。
- 導入バージョン: -

### pipeline_prepare_timeout_guard_ms

- デフォルト: -1
- 型: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: この項目が`0`より大きい値に設定されている場合、PREPAREプロセス中にプランフラグメントが`pipeline_prepare_timeout_guard_ms`を超えると、そのプランフラグメントのスタックトレースが出力されます。
- 導入バージョン: -

### pipeline_scan_thread_pool_queue_size

- デフォルト: 102400
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: パイプライン実行エンジンのSCANスレッドプールの最大タスクキュー長。
- 導入バージョン: -

### enable_lock_free_scan_task_queue

- デフォルト: true
- 型: Boolean
- 単位: -
- 変更可能: はい
- 説明: `ScanExecutor`がOLAPスキャンおよびコネクタスキャンタスクのスケジューリングに`LockFreeWorkGroupScanTaskQueue`を使用するかどうかを制御します。有効にすると、スキャンタスクはロックフリーのワークグループごとのキューとワークグループレベルのコーディネーターを介してスケジュールされ、従来のミューテックスベースの`WorkGroupScanTaskQueue`と比較して競合が減少します。トラブルシューティングまたはロールバック中に従来のスキャンタスクキュー実装に戻すには、この項目を`false`に設定します。
- 導入バージョン: v4.1.0

### pk_index_parallel_get_threadpool_size

- デフォルト: 1048576
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データ (クラウドネイティブ/レイク) モードでの PK インデックス並列取得操作で使用される「cloud_native_pk_index_get」スレッドプールの最大キューサイズ (保留中のタスク数) を設定します。そのプールの実際のスレッド数は `pk_index_parallel_get_threadpool_max_threads` によって制御されます。この設定は、実行を待機しているタスクがキューに入れられる数を制限するだけです。非常に大きなデフォルト値 (2^20) は、キューを実質的に無制限にします。これを下げると、キューに入れられたタスクによる過剰なメモリ増加を防ぎますが、キューが満杯のときにタスクの送信がブロックされたり失敗したりする可能性があります。ワークロードの同時実行性とメモリ制約に基づいて、`pk_index_parallel_get_threadpool_max_threads` とともに調整してください。
- 導入バージョン: -

### priority_exec_state_report_max_threads

- デフォルト: 2
- 型: Int
- 単位: スレッド
- 変更可能: はい
- 説明: 高優先度 exec-state-report スレッドプールの最大スレッド数。このプールは、`ExecStateReporter` によって、高優先度の実行ステータスレポート (緊急のフラグメント障害など) を BE から FE へ RPC 経由で非同期に送信するために使用されます。通常の exec-state-report プールとは異なり、このプールには無制限のタスクキューがあります。起動時の実際のプールサイズは `max(1, priority_exec_state_report_max_threads)` です。実行時にこの設定を変更すると、すべてのエグゼキュータセット (共有および排他) の優先度プールで `update_max_threads` がトリガーされます。通常のプールには `exec_state_report_max_threads` と組み合わせて使用されます。高優先度レポートが大量の同時クエリ負荷の下で遅延する場合、この値を増やしてください。
- 導入バージョン: v4.1.0, v4.0.8, v3.5.15

### priority_queue_remaining_tasks_increased_frequency

- デフォルト: 512
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: BlockingPriorityQueue が、すべての残りのタスクの優先度を飢餓状態を避けるためにどれくらいの頻度で増加 (「エージング」) させるかを制御します。get/pop が成功するたびに内部の `_upgrade_counter` がインクリメントされ、`_upgrade_counter` が `priority_queue_remaining_tasks_increased_frequency` を超えると、キューはすべての要素の優先度をインクリメントし、ヒープを再構築し、カウンターをリセットします。値が低いほど優先度エージングが頻繁に行われ (飢餓状態を軽減しますが、反復処理とヒープ再構築による CPU コストが増加します)、値が高いほどオーバーヘッドは減少しますが、優先度調整が遅れます。この値は単純な操作回数のしきい値であり、時間期間ではありません。
- 導入バージョン: v3.2.0

### query_cache_capacity

- デフォルト: 536870912
- 型: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: BE のクエリキャッシュのサイズ。デフォルトサイズは 512 MB です。サイズは 4 MB 未満にすることはできません。BE のメモリ容量が、期待されるクエリキャッシュサイズをプロビジョニングするのに不十分な場合、BE のメモリ容量を増やすことができます。
- 導入バージョン: -

### query_pool_spill_mem_limit_threshold

- デフォルト: 1.0
- 型: Double
- 単位: -
- 変更可能: いいえ
- 説明: 自動スピルが有効な場合、すべてのクエリのメモリ使用量が `query_pool memory limit * query_pool_spill_mem_limit_threshold` を超えると、中間結果のスピルがトリガーされます。
- 導入バージョン: v3.2.7

### query_scratch_dirs

- デフォルト: `${STARROCKS_HOME}`
- 型: string
- 単位: -
- 変更可能: いいえ
- 説明: クエリ実行が中間データ（例えば、外部ソート、ハッシュ結合、その他の演算子）をスピルするために使用する、書き込み可能なスクラッチディレクトリのコンマ区切りリスト。`;`で区切られた1つ以上のパスを指定します（例: `/mnt/ssd1/tmp;/mnt/ssd2/tmp`）。ディレクトリはBEプロセスからアクセス可能で書き込み可能であり、十分な空き容量が必要です。StarRocksはそれらの中から選択してスピルI/Oを分散します。変更を有効にするには再起動が必要です。ディレクトリが見つからない、書き込み可能でない、または満杯の場合、スピルが失敗したり、クエリパフォーマンスが低下したりする可能性があります。
- 導入バージョン: v3.2.0

### result_buffer_cancelled_interval_time

- デフォルト: 300
- 型: Int
- 単位: 秒
- 変更可能: はい
- 説明: BufferControlBlockがデータを解放するまでの待機時間。
- 導入バージョン: -

### scan_context_gc_interval_min

- デフォルト: 5
- 型: Int
- 単位: 分
- 変更可能: はい
- 説明: スキャンコンテキストをクリーンアップする時間間隔。
- 導入バージョン: -

### scanner_row_num

- デフォルト: 16384
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: スキャンにおいて、各スキャンが返す最大行数。
- 導入バージョン: -

### scanner_thread_pool_queue_size

- デフォルト: 102400
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: ストレージエンジンがサポートするスキャンタスクの数。
- 導入バージョン: -

### scanner_thread_pool_thread_num

- デフォルト: 48
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: ストレージエンジンが並行ストレージボリュームスキャンに使用するスレッド数。すべてのスレッドはスレッドプールで管理されます。
- 導入バージョン: -

### string_prefix_zonemap_prefix_len

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: `enable_string_prefix_zonemap` が有効な場合、文字列の ZoneMap min/max に使用されるプレフィックス長。
- 導入バージョン: -

### udf_thread_pool_size

- デフォルト: 1
- タイプ: Int
- 単位: スレッド
- 変更可能: いいえ
- 説明: ExecEnv で作成される UDF 呼び出し PriorityThreadPool のサイズを設定します (ユーザー定義関数 / UDF 関連タスクの実行に使用されます)。この値は、スレッドプール (PriorityThreadPool("udf", thread_num, queue_size)) を構築する際のプールスレッド数およびプールキュー容量として使用されます。同時 UDF 実行を増やすには値を大きくし、過剰な CPU およびメモリ競合を避けるには値を小さく保ちます。
- 導入バージョン: v3.2.0

### update_memory_limit_percent

- デフォルト: 60
- タイプ: Int
- 単位: パーセント
- 変更可能: いいえ
- 説明: BE プロセスメモリのうち、更新関連のメモリとキャッシュに予約される割合。起動時、`GlobalEnv` は更新用の `MemTracker` を process_mem_limit * clamp(update_memory_limit_percent, 0, 100) / 100 として計算します。`UpdateManager` もこの割合を使用して、プライマリインデックス/インデックスキャッシュ容量 (インデックスキャッシュ容量 = GlobalEnv::process_mem_limit * update_memory_limit_percent / 100) を設定します。HTTP 設定更新ロジックは、更新マネージャーで `update_primary_index_memory_limit` を呼び出すコールバックを登録するため、設定が変更された場合、変更は更新サブシステムに適用されます。この値を増やすと、更新/プライマリインデックスパスにより多くのメモリが割り当てられ (他のプールで利用可能なメモリが減少します)、減らすと更新メモリとキャッシュ容量が減少します。値は 0～100 の範囲にクランプされます。
- 導入バージョン: v3.2.0

### vector_chunk_size

- デフォルト: 4096
- タイプ: Int
- 単位: 行
- 変更可能: いいえ
- 説明: 実行およびストレージのコードパス全体で使用される、ベクトル化されたチャンク (バッチ) あたりの行数。この値は、Chunk および RuntimeState の batch_size 作成を制御し、オペレーターのスループット、オペレーターあたりのメモリフットプリント、スピルおよびソートバッファのサイズ設定、I/O ヒューリスティック (例: ORC ライターの自然な書き込みサイズ) に影響を与えます。値を増やすと、広範囲/CPUバウンドのワークロードで CPU および I/O 効率が向上する可能性がありますが、ピークメモリ使用量が増加し、結果の小さいクエリのレイテンシが増加する可能性があります。バッチサイズがボトルネックであることがプロファイリングで示された場合にのみ調整し、それ以外の場合はメモリとパフォーマンスのバランスのためにデフォルトを維持してください。
- 導入バージョン: v3.2.0

## ロードとアンロード

### clear_transaction_task_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: トランザクションのクリアに使用されるスレッド数。
- 導入バージョン: -

### column_mode_partial_update_insert_batch_size

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 挿入された行を処理する際のカラムモード部分更新のバッチサイズ。この項目が `0` または負の値に設定されている場合、無限ループを避けるために `1` にクランプされます。この項目は、各バッチで処理される新しく挿入された行の数を制御します。値が大きいほど書き込みパフォーマンスが向上しますが、より多くのメモリを消費します。
- 導入バージョン: v3.5.10, v4.0.2

### enable_load_spill_parallel_merge

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 単一のタブレット内で並列スピルマージを有効にするかどうかを指定します。これを有効にすると、データロード中のスピルマージのパフォーマンスが向上する可能性があります。
- 導入バージョン: -

### enable_parallel_memtable_finalize

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: レイクテーブル（共有データモード）にデータをロードする際に、並列memtableファイナライズを有効にするかどうかを指定します。有効にすると、memtableファイナライズ操作（ソート/集計）が書き込みスレッドからフラッシュスレッドに移動され、前のmemtableが並行してファイナライズおよびフラッシュされている間に、書き込みスレッドが新しいmemtableへのデータ挿入を続行できるようになります。これにより、CPU負荷の高いファイナライズ操作とI/Oバウンドのフラッシュ操作をオーバーラップさせることで、ロードスループットを大幅に向上させることができます。この最適化は、自動インクリメント列を埋める必要がある場合には自動的に無効になります。これは、自動インクリメントIDの割り当てがmemtableがフラッシュのために提出される前に発生する必要があるためです。
- 導入バージョン: -

### allow_list_object_for_random_bucketing_on_cache_miss

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ランダムバケットサイズチェック中にレイクメタデータキャッシュがミスした場合に、オブジェクトストレージのLISTフォールバックを許可するかどうかを制御します。`true` は、ベースサイズを計算するためにLISTメタデータファイルにフォールバックすることを意味します（履歴的な動作で、より正確なサイズ推定が可能です）。`false` はLISTをスキップし、`base_size = 0` を使用することを意味します。これにより、LISTオブジェクトのリクエストは減少しますが、サイズ推定の精度が低いため、不変マーキングが遅れる可能性があります。
- 導入バージョン: 4.1.0, 4.0.7, 3.5.15

### enable_stream_load_verbose_log

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Stream LoadジョブのHTTPリクエストとレスポンスをログに記録するかどうかを指定します。
- 導入バージョン: v2.5.17, v3.0.9, v3.1.6, v3.2.1

### flush_thread_num_per_store

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各ストアでMemTableをフラッシュするために使用されるスレッドの数。
- 導入バージョン: -

### lake_flush_thread_num_per_store

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスター内の各ストアでMemTableをフラッシュするために使用されるスレッドの数。
この値が`0`に設定されている場合、システムはCPUコア数の2倍を値として使用します。
この値が`0`未満に設定されている場合、システムはその絶対値とCPUコア数の積を値として使用します。
- 導入バージョン: v3.1.12, 3.2.7

### load_data_reserve_hours

- デフォルト: 4
- タイプ: Int
- 単位: 時間
- 変更可能: いいえ
- 説明: 小規模なロードによって生成されたファイルの予約時間。
- 導入バージョン: -

### load_error_log_reserve_hours

- デフォルト: 48
- タイプ: Int
- 単位: 時間
- 変更可能: はい
- 説明: データロードログが予約される時間。
- 導入バージョン: -

### load_process_max_memory_limit_bytes

- デフォルト: 107374182400
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: BEノード上のすべてのロードプロセスが占有できるメモリリソースの最大サイズ制限。
- 導入バージョン: -

### load_spill_memory_usage_per_merge

- デフォルト: 1073741824
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: スピルマージ中のマージ操作あたりの最大メモリ使用量。デフォルトは1 GB (1073741824バイト) です。このパラメータは、データロードスピルマージ中の個々のマージタスクのメモリ消費を制御し、過剰なメモリ使用を防ぎます。
- 導入バージョン: -

### max_consumer_num_per_group

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ルーチンロードのコンシューマグループにおけるコンシューマの最大数。
- 導入バージョン: -

### max_runnings_transactions_per_txn_map

- デフォルト: 100
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: 各パーティションで同時に実行できるトランザクションの最大数。
- 導入バージョン: -

### number_tablet_writer_threads

- デフォルト: 0
- 型: Int
- 単位: -
- 変更可能: はい
- 説明: Stream Load、Broker Load、Insert などの取り込みで使用されるタブレットライタースレッドの数。パラメータが0以下に設定されている場合、システムはCPUコア数の半分を使用し、最小値は16です。パラメータが0より大きい値に設定されている場合、システムはその値を使用します。この設定はv3.1.7以降、動的に変更されました。
- 導入バージョン: -

### push_worker_count_high_priority

- デフォルト: 3
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: HIGH優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

### push_worker_count_normal_priority

- デフォルト: 3
- 型: Int
- 単位: -
- 変更可能: いいえ
- 説明: NORMAL優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

### streaming_load_max_batch_size_mb

- デフォルト: 100
- 型: Int
- 単位: MB
- 変更可能: はい
- 説明: StarRocksにストリーミングできるJSONファイルの最大サイズ。
- 導入バージョン: -

### streaming_load_max_mb

- デフォルト: 102400
- 型: Int
- 単位: MB
- 変更可能: はい
- 説明: StarRocksにストリーミングできるファイルの最大サイズ。v3.0以降、デフォルト値は`10240`から`102400`に変更されました。
- 導入バージョン: -

### streaming_load_rpc_max_alive_time_sec

- デフォルト: 1200
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: Stream Load の RPC タイムアウトです。
- 導入バージョン: -

### transaction_publish_version_thread_pool_idle_time_ms

- デフォルト: 60000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: Publish Version スレッドプールによってスレッドが再利用されるまでのアイドル時間です。
- 導入バージョン: -

### transaction_publish_version_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: バージョンを公開するために使用されるスレッドの最大数です。この値が `0` 以下に設定されている場合、インポートの並行性が高いにもかかわらず固定数のスレッドしか使用されない場合にスレッドリソースが不足するのを避けるため、システムは CPU コア数を値として使用します。v2.5 以降、デフォルト値は `8` から `0` に変更されました。
- 導入バージョン: -

### write_buffer_size

- デフォルト: 104857600
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: メモリ内の MemTable のバッファサイズです。この設定項目はフラッシュをトリガーするしきい値です。
- 導入バージョン: -

### broker_write_timeout_seconds

- デフォルト: 30
- タイプ: int
- 単位: 秒
- 変更可能: いいえ
- 説明: バックエンドブローカー操作が書き込み/IO RPC に使用するタイムアウト（秒単位）です。この値は 1000 倍されてミリ秒単位のタイムアウトとなり、BrokerFileSystem および BrokerServiceConnection インスタンス（ファイルのエクスポートやスナップショットのアップロード/ダウンロードなど）にデフォルトの timeout_ms として渡されます。ブローカーやネットワークが遅い場合、または大きなファイルを転送する際に、時期尚早なタイムアウトを避けるためにこの値を増やしてください。値を減らすと、ブローカー RPC がより早く失敗する可能性があります。この値は common/config で定義されており、プロセス開始時に適用されます（動的にリロード可能ではありません）。
- 導入バージョン: v3.2.0

### enable_load_channel_rpc_async

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、ロードチャネルオープンRPC（例：`PTabletWriterOpen`）の処理がBRPCワーカーから専用のスレッドプールにオフロードされます。リクエストハンドラは`ChannelOpenTask`を作成し、`LoadChannelMgr::_open`をインラインで実行する代わりに、内部の`_async_rpc_pool`に送信します。これにより、BRPCスレッド内の作業とブロッキングが減少し、`load_channel_rpc_thread_pool_num`と`load_channel_rpc_thread_pool_queue_size`を介して並行処理を調整できます。スレッドプールの送信が失敗した場合（プールが満杯またはシャットダウンされている場合）、リクエストはキャンセルされ、エラー状態が返されます。プールは`LoadChannelMgr::close()`でシャットダウンされるため、この機能を有効にする場合は、リクエストの拒否や処理の遅延を避けるために、容量とライフサイクルを考慮してください。
- 導入バージョン: v3.5.0

### enable_load_diagnose

- デフォルト: true
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、StarRocksは「[E1008]Reached timeout」に一致するbrpcタイムアウトの後、BE OlapTableSink/NodeChannelから自動ロード診断を試行します。このコードは`PLoadDiagnoseRequest`を作成し、リモートのLoadChannelにRPCを送信して、プロファイルやスタックトレース（`load_diagnose_rpc_timeout_profile_threshold_ms`と`load_diagnose_rpc_timeout_stack_trace_threshold_ms`で制御）を収集します。診断RPCは`load_diagnose_send_rpc_timeout_ms`をタイムアウトとして使用します。診断リクエストがすでに進行中の場合、診断はスキップされます。これを有効にすると、ターゲットノードで追加のRPCとプロファイリング作業が発生します。余分なオーバーヘッドを避けるため、機密性の高い本番ワークロードでは無効にしてください。
- 導入バージョン: v3.5.0

### enable_load_segment_parallel

- デフォルト: false
- タイプ: ブール
- 単位: -
- 変更可能: いいえ
- 説明: 有効にすると、StarRocksのバックグラウンドスレッドプール（ExecEnv::load_segment_thread_poolおよびExecEnv::load_rowset_thread_pool）を使用して、rowsetセグメントのロードとrowsetレベルの読み取りが並行して実行されます。Rowset::load_segmentsとTabletReader::get_segment_iteratorsは、セグメントごとまたはrowsetごとのタスクをこれらのプールに送信し、送信が失敗した場合はシリアルロードにフォールバックして警告をログに記録します。これを有効にすると、CPU/IOの並行処理とメモリ負荷が増加する代わりに、大規模なrowsetの読み取り/ロードレイテンシを削減できます。注：並行ロードはセグメントのロード完了順序を変更する可能性があり、部分的なコンパクションを妨げます（コードは`_parallel_load`をチェックし、有効な場合は部分的なコンパクションを無効にします）。セグメントの順序に依存する操作への影響を考慮してください。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

### enable_streaming_load_thread_pool

- デフォルト: true
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: ストリーミングロードスキャナーが専用のストリーミングロードスレッドプールに送信されるかどうかを制御します。有効で、クエリが`TLoadJobType::STREAM_LOAD`を持つLOADである場合、ConnectorScanNodeはスキャナータスクを`streaming_load_thread_pool`（INT32_MAXスレッドとキューサイズで構成されており、実質的に無制限）に送信します。無効の場合、スキャナーは一般的な`thread_pool`とその`PriorityThreadPool`送信ロジック（優先度計算、try_offer/offer動作）を使用します。有効にすると、ストリーミングロード作業が通常のクエリ実行から分離され、干渉が減少します。ただし、専用プールは実質的に無制限であるため、有効にすると、大量のストリーミングロードトラフィック下で同時スレッドとリソース使用量が増加する可能性があります。このオプションはデフォルトでオンになっており、通常は変更する必要はありません。
- 導入バージョン: v3.2.0

### es_http_timeout_ms

- デフォルト: 5000
- タイプ: 整数
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: ESScanReaderのESネットワーククライアントがElasticsearchスクロールリクエストに使用するHTTP接続タイムアウト（ミリ秒単位）。この値は、後続のスクロールPOSTを送信する前にnetwork_client.set_timeout_ms()を介して適用され、スクロール中にクライアントがES応答を待つ時間を制御します。ネットワークが遅い場合や大規模なクエリの場合、早すぎるタイムアウトを避けるためにこの値を増やしてください。応答しないESノードでより早く失敗させるには減らしてください。この設定は、スクロールコンテキストのキープアライブ期間を制御する`es_scroll_keepalive`を補完します。
- 導入バージョン: v3.2.0

### es_index_max_result_window

- デフォルト: 10000
- タイプ: 整数
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがElasticsearchから単一バッチでリクエストするドキュメントの最大数を制限します。StarRocksは、ESリーダー用の`KEY_BATCH_SIZE`を構築する際に、ESリクエストバッチサイズをmin(`es_index_max_result_window`, `chunk_size`)に設定します。ESリクエストがElasticsearchインデックス設定`index.max_result_window`を超えると、ElasticsearchはHTTP 400（Bad Request）を返します。大規模なインデックスをスキャンする場合、またはより大きな単一リクエストを許可するためにElasticsearch側でES `index.max_result_window`を増やす場合に、この値を調整してください。
- 導入バージョン: v3.2.0

### ignore_load_tablet_failure

- デフォルト: false
- タイプ: ブール
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`false`に設定されている場合、システムはタブレットヘッダーのロード失敗（NotFoundおよびAlreadyExist以外のエラー）を致命的とみなし、コードはエラーをログに記録し、LOG(FATAL)を呼び出してBEプロセスを停止します。`true`に設定されている場合、BEはタブレットごとのロードエラーにもかかわらず起動を続行します。失敗したタブレットIDは記録されスキップされますが、成功したタブレットは引き続きロードされます。このパラメータは、RocksDBメタスキャン自体からの致命的なエラーを抑制するものではないことに注意してください。RocksDBメタスキャンからのエラーは常にプロセスを終了させます。
- 導入バージョン: v3.2.0

### load_channel_abort_clean_up_delay_seconds

- デフォルト: 600
- 型: Int
- 単位: 秒
- 変更可能: はい
- 説明: システムが中止されたロードチャネルのロードIDを`_aborted_load_channels`から削除するまでの期間（秒単位）を制御します。ロードジョブがキャンセルまたは失敗した場合、ロードIDは記録されたままになり、遅れて到着するロードRPCをすぐに拒否できます。遅延期間が経過すると、エントリは定期的なバックグラウンドスイープ中にクリーンアップされます（最小スイープ間隔は60秒）。遅延を短くしすぎると、中止後に迷い込んだRPCを受け入れてしまうリスクがあり、長くしすぎると、必要以上に状態を保持し、リソースを消費する可能性があります。遅延リクエストの拒否の正確性と、中止されたロードのリソース保持のバランスを取るように調整してください。
- 導入バージョン: v3.5.11, v4.0.4

### load_channel_rpc_thread_pool_num

- デフォルト: -1
- 型: Int
- 単位: スレッド
- 変更可能: はい
- 説明: ロードチャネル非同期RPCスレッドプールの最大スレッド数。0以下（デフォルト`-1`）に設定すると、プールサイズはCPUコア数（`CpuInfo::num_cores()`）に自動設定されます。設定された値はThreadPoolBuilderの最大スレッドとして使用され、プールの最小スレッドはmin(5, max_threads)に設定されます。プールキューサイズは`load_channel_rpc_thread_pool_queue_size`によって個別に制御されます。この設定は、ロードRPC処理を同期から非同期に切り替えた後も動作の互換性を保つために、非同期RPCプールサイズをbrpcワーカーのデフォルト（`brpc_num_threads`）に合わせるために導入されました。この設定をランタイムで変更すると`ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(...)`がトリガーされます。
- 導入バージョン: v3.5.0

### load_channel_rpc_thread_pool_queue_size

- デフォルト: 1024000
- 型: int
- 単位: カウント
- 変更可能: いいえ
- 説明: LoadChannelMgrによって作成されるロードチャネルRPCスレッドプールの最大保留タスクキューサイズを設定します。このスレッドプールは、`enable_load_channel_rpc_async`が有効な場合に非同期`open`リクエストを実行します。プールサイズは`load_channel_rpc_thread_pool_num`と対になっています。大きなデフォルト値（1024000）は、同期処理から非同期処理への切り替え後も動作を維持するために、brpcワーカーのデフォルトと一致しています。キューが満杯の場合、ThreadPool::submit()は失敗し、受信したオープンRPCはエラーでキャンセルされ、呼び出し元は拒否を受け取ります。この値を増やすと、同時`open`リクエストのより大きなバーストをバッファリングできます。減らすとバックプレッシャーが厳しくなりますが、負荷がかかったときに拒否が増える可能性があります。
- 導入バージョン: v3.5.0

### load_diagnose_rpc_timeout_profile_threshold_ms

- デフォルト: 60000
- 型: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: ロードRPCがタイムアウトし（エラーに「[E1008]Reached timeout」が含まれる）、`enable_load_diagnose`がtrueの場合、このしきい値は完全なプロファイリング診断が要求されるかどうかを制御します。リクエストレベルのRPCタイムアウト`_rpc_timeout_ms`が`load_diagnose_rpc_timeout_profile_threshold_ms`より大きい場合、その診断に対してプロファイリングが有効になります。`_rpc_timeout_ms`の値が小さい場合、リアルタイム/短時間タイムアウトのロードに対する頻繁な重い診断を避けるため、プロファイリングは20回のタイムアウトごとに1回サンプリングされます。この値は、送信される`PLoadDiagnoseRequest`内の`profile`フラグに影響します。スタックトレースの動作は`load_diagnose_rpc_timeout_stack_trace_threshold_ms`によって、送信タイムアウトは`load_diagnose_send_rpc_timeout_ms`によって個別に制御されます。
- 導入バージョン: v3.5.0

### load_diagnose_rpc_timeout_stack_trace_threshold_ms

- デフォルト: 600000
- 型: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 長時間実行されるロードRPCのリモートスタックトレースをいつ要求するかを決定するために使用されるしきい値（ミリ秒単位）。ロードRPCがタイムアウトエラーでタイムアウトし、実効RPCタイムアウト（_rpc_timeout_ms）がこの値を超えると、`OlapTableSink`/`NodeChannel`はターゲットBEへの`load_diagnose` RPCに`stack_trace=true`を含め、BEがデバッグ用のスタックトレースを返せるようにします。セカンダリレプリカの待機がこの間隔を超えた場合、`LocalTabletsChannel::SecondaryReplicasWaiter`もプライマリからベストエフォートのスタックトレース診断をトリガーします。この動作には`enable_load_diagnose`が必要であり、診断RPCタイムアウトには`load_diagnose_send_rpc_timeout_ms`を使用します。プロファイリングは`load_diagnose_rpc_timeout_profile_threshold_ms`によって個別に制御されます。この値を下げると、スタックトレースが要求される頻度が高まります。
- 導入バージョン: v3.5.0

### load_diagnose_send_rpc_timeout_ms

- デフォルト: 2000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: BEロードパスによって開始される診断関連のbrpc呼び出しに適用されるタイムアウト（ミリ秒単位）。これは、`load_diagnose` RPC（LoadChannel brpc呼び出しがタイムアウトしたときにNodeChannel/OlapTableSinkによって送信される）およびレプリカステータスクエリ（プライマリレプリカの状態を確認するときにSecondaryReplicasWaiter / LocalTabletsChannelによって使用される）のコントローラータイムアウトを設定するために使用されます。リモート側がプロファイルまたはスタックトレースデータで応答できる十分な高い値を選択しますが、障害処理が遅延するほど高くしないでください。このパラメータは、診断情報がいつ、何が要求されるかを制御する`enable_load_diagnose`、`load_diagnose_rpc_timeout_profile_threshold_ms`、および`load_diagnose_rpc_timeout_stack_trace_threshold_ms`と連携して機能します。
- 導入バージョン: v3.5.0

### load_fp_brpc_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: `node_channel_set_brpc_timeout`障害点がトリガーされたときにOlapTableSinkが使用するチャネルごとのbrpc RPCタイムアウトを上書きします。正の値に設定すると、NodeChannelはその内部`_rpc_timeout_ms`をこの値（ミリ秒単位）に設定し、open/add-chunk/cancel RPCがより短いタイムアウトを使用するようにし、「[E1008]Reached timeout」エラーを生成するbrpcタイムアウトのシミュレーションを可能にします。デフォルト（`-1`）では上書きが無効になります。この値の変更はテストおよび障害注入を目的としています。小さい値は誤ったタイムアウトを生成し、ロード診断をトリガーする可能性があります（`enable_load_diagnose`、`load_diagnose_rpc_timeout_profile_threshold_ms`、`load_diagnose_rpc_timeout_stack_trace_threshold_ms`、および`load_diagnose_send_rpc_timeout_ms`を参照）。
- 導入バージョン: v3.5.0

### load_fp_tablets_channel_add_chunk_block_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 有効にすると（正のミリ秒値に設定すると）、この障害点設定により、ロード処理中にTabletsChannel::add_chunkが指定された時間スリープします。これは、BRPCタイムアウトエラー（例：「[E1008]Reached timeout」）をシミュレートし、ロードレイテンシを増加させる高価なadd_chunk操作をエミュレートするために使用されます。0以下の値（デフォルト`-1`）は注入を無効にします。障害処理、タイムアウト、およびレプリカ同期動作のテストを目的としています。書き込み完了を遅延させ、アップストリームのタイムアウトやレプリカの中止をトリガーする可能性があるため、通常のプロダクションワークロードでは有効にしないでください。
- 導入バージョン: v3.5.0

### load_segment_thread_pool_num_max

- デフォルト: 128
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BEのロード関連スレッドプールの最大ワーカー数を設定します。この値はThreadPoolBuilderによってexec_env.cppの`load_rowset_pool`と`load_segment_pool`の両方のスレッドを制限するために使用され、ストリーミングおよびバッチロード中のロードされたrowsetとセグメントの処理（例：デコード、インデックス作成、書き込み）の並行性を制御します。この値を増やすと並列性が向上し、ロードスループットが改善される可能性がありますが、CPU、メモリ使用量、および潜在的な競合も増加します。減らすと同時ロード処理が制限され、スループットが低下する可能性があります。`load_segment_thread_pool_queue_size`および`streaming_load_thread_pool_idle_time_ms`と合わせて調整してください。変更にはBEの再起動が必要です。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

### load_segment_thread_pool_queue_size

- デフォルト: 10240
- タイプ: Int
- 単位: タスク
- 変更可能: いいえ
- 説明: 「load_rowset_pool」および「load_segment_pool」として作成されたロード関連スレッドプールの最大キュー長（保留中のタスク数）を設定します。これらのプールは最大スレッド数に`load_segment_thread_pool_num_max`を使用し、この設定はThreadPoolのオーバーフローポリシーが適用される前にバッファリングできるロードセグメント/rowsetタスクの数を制御します（ThreadPoolの実装によっては、それ以上の送信が拒否またはブロックされる場合があります）。保留中のロード作業を増やすには値を増やします（メモリ使用量が増え、レイテンシが上昇する可能性があります）。バッファリングされたロードの同時実行を制限し、メモリ使用量を減らすには値を減らします。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

### max_pulsar_consumer_num_per_group

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE上のルーチンロードのために単一のデータコンシューマグループで作成できるPulsarコンシューマの最大数を制御します。マルチトピックサブスクリプションでは累積確認応答がサポートされていないため、各コンシューマは正確に1つのトピック/パーティションをサブスクライブします。`pulsar_info->partitions`内のパーティション数がこの値を超えると、グループ作成は失敗し、BE上で`max_pulsar_consumer_num_per_group`を増やすか、BEを追加するよう促すエラーが表示されます。この制限はPulsarDataConsumerGroupを構築する際に適用され、BEが1つのルーチンロードグループに対してこれ以上のコンシューマをホストすることを防ぎます。Kafkaルーチンロードの場合、代わりに`max_consumer_num_per_group`が使用されます。
- 導入バージョン: v3.2.0

### pull_load_task_dir

- デフォルト: `${STARROCKS_HOME}/var/pull_load`
- タイプ: string
- 単位: -
- 変更可能: いいえ
- 説明: BEが「プルロード」タスク（ダウンロードされたソースファイル、タスクの状態、一時出力など）のデータと作業ファイルを保存するファイルシステムパスです。このディレクトリはBEプロセスによって書き込み可能であり、受信するロードのために十分なディスク容量が必要です。デフォルトはSTARROCKS_HOMEからの相対パスです。テストはこのディレクトリが作成され、存在することを期待します（テスト設定を参照）。
- 導入バージョン: v3.2.0

### routine_load_kafka_timeout_second

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: Kafka関連のルーチンロード操作に使用されるタイムアウト（秒単位）です。クライアントリクエストがタイムアウトを指定しない場合、`routine_load_kafka_timeout_second`が`get_info`のデフォルトRPCタイムアウト（ミリ秒に変換）として使用されます。また、librdkafkaコンシューマの呼び出しごとのコンシュームポーリングタイムアウト（ミリ秒に変換され、残りの実行時間で上限が設定される）としても使用されます。注: 内部の`get_info`パスは、FE側のタイムアウトラッシュを避けるため、librdkafkaに渡す前にこの値を80%に減らします。タイムリーな障害報告とネットワーク/ブローカー応答に十分な時間のバランスを取る値に設定してください。この設定は変更不可であるため、変更には再起動が必要です。
- 導入バージョン: v3.2.0

### routine_load_pulsar_timeout_second

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: リクエストが明示的なタイムアウトを提供しない場合に、BEがPulsar関連のルーチンロード操作に使用するデフォルトのタイムアウト（秒単位）です。具体的には、`PInternalServiceImplBase::get_pulsar_info`はこの値を1000倍して、Pulsarパーティションのメタデータとバックログを取得するルーチンロードタスクエグゼキュータメソッドに渡されるミリ秒単位のタイムアウトを形成します。Pulsarの応答が遅いことを許容するには値を増やしますが、その分障害検出が長くなります。遅いブローカーでより早く失敗させるには値を減らしてください。Kafkaで使用される`routine_load_kafka_timeout_second`に類似しています。
- 導入バージョン: v3.2.0

### streaming_load_thread_pool_idle_time_ms

- デフォルト: 2000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: ストリーミングロード関連のスレッドプールにおけるスレッドのアイドルタイムアウト（ミリ秒単位）を設定します。この値は、`stream_load_io`プール、`load_rowset_pool`、および`load_segment_pool`のThreadPoolBuilderに渡されるアイドルタイムアウトとして使用されます。これらのプール内のスレッドは、この期間アイドル状態になると解放されます。値が低いとアイドルリソースの使用量が減りますが、スレッド作成のオーバーヘッドが増加し、値が高いとスレッドがより長く存続します。`enable_streaming_load_thread_pool`が有効な場合、`stream_load_io`プールが使用されます。
- 導入バージョン: v3.2.0

### streaming_load_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ExecEnv初期化中に作成されるストリーミングロードIOスレッドプール（「stream_load_io」）の最小スレッド数です。このプールは`set_max_threads(INT32_MAX)`と`set_max_queue_size(INT32_MAX)`で構築されているため、同時ストリーミングロードでのデッドロックを避けるために実質的に無制限です。値が0の場合、プールはスレッドなしで開始し、オンデマンドで増加します。正の値を設定すると、起動時にその数のスレッドが予約されます。このプールは`enable_streaming_load_thread_pool`がtrueの場合に使用され、そのアイドルタイムアウトは`streaming_load_thread_pool_idle_time_ms`によって制御されます。全体的な並行性は依然として`fragment_pool_thread_num_max`と`webserver_num_workers`によって制約されます。この値を変更する必要があることはめったになく、高すぎるとリソース使用量が増加する可能性があります。
- 導入バージョン: v3.2.0
