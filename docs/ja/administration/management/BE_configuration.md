---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 設定

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

## BE パラメータを理解する

### ロギング

##### diagnose_stack_trace_interval_ms

- デフォルト: 1800000 (30 minutes)
- タイプ: long
- 単位: Milliseconds
- 変更可能: Yes
- 説明: DiagnoseDaemon が `STACK_TRACE` リクエストに対して行う連続したスタックトレース診断の最小時間間隔を制御します。診断リクエストが到着したとき、最後の収集が `diagnose_stack_trace_interval_ms` ミリ秒未満であれば、デーモンはスタックトレースの収集およびログ出力をスキップします。頻繁なスタックダンプによる CPU 負荷やログ量を減らすためにこの値を大きくし、短期間の問題をデバッグするためにより頻繁なトレースを取得したい場合（例えば TabletsChannel::add_chunk が長時間ブロックするロードのフェイルポイントシミュレーションなど）には値を小さくしてください。
- 導入バージョン: v3.5.0

##### log_buffer_level

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: ログをフラッシュするための戦略。デフォルト値は、ログがメモリにバッファリングされることを示します。有効な値は `-1` と `0` です。`-1` は、ログがメモリにバッファリングされないことを示します。
- 導入バージョン: -

##### sys_log_dir

- デフォルト: `${STARROCKS_HOME}/log`
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: システムログ (INFO、WARNING、ERROR、FATAL を含む) を保存するディレクトリ。
- 導入バージョン: -

##### sys_log_level

- デフォルト: INFO
- タイプ: String
- 単位: -
- 可変: はい (v3.3.0、v3.2.7、v3.1.12 から)
- 説明: システムログエントリが分類される重大度レベル。 有効な値: INFO、WARN、ERROR、FATAL。この項目は v3.3.0、v3.2.7、v3.1.12 以降、動的設定に変更されました。
- 導入バージョン: -

##### sys_log_roll_mode

- デフォルト: SIZE-MB-1024
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: システムログがログロールに分割されるモード。有効な値には `TIME-DAY`、`TIME-HOUR`、および `SIZE-MB-` サイズが含まれます。デフォルト値は、各ロールが 1 GB であるログロールに分割されることを示します。
- 導入バージョン: -

##### sys_log_roll_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 保持するログロールの数。
- 導入バージョン: -

##### sys_log_timezone

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: ログプレフィックスにタイムゾーン情報を表示するかどうか。`true` はタイムゾーン情報を表示することを示し、`false` は表示しないことを示します。
- 導入バージョン: -

##### sys_log_verbose_level

- デフォルト: 10
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 印刷するログのレベル。この設定項目は、コード内で VLOG で開始されたログの出力を制御するために使用されます。
- 導入バージョン: -

##### sys_log_verbose_modules

- デフォルト: 
- タイプ: Strings
- 単位: -
- 可変: いいえ
- 説明: 印刷するログのモジュール。たとえば、この設定項目を OLAP に設定すると、StarRocks は OLAP モジュールのログのみを印刷します。有効な値は BE の名前空間であり、`starrocks`、`starrocks::debug`、`starrocks::fs`、`starrocks::io`、`starrocks::lake`、`starrocks::pipeline`、`starrocks::query_cache`、`starrocks::stream`、`starrocks::workgroup` などがあります。
- 導入バージョン: -

### サーバー

##### abort_on_large_memory_allocation

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: Yes
- 説明: 単一の割り当て要求が設定された large-allocation 閾値を超えた場合（g_large_memory_alloc_failure_threshold > 0 かつ 要求サイズ > 閾値）、プロセスがどのように応答するかを制御します。true の場合、こうした大きな割り当てが検出されると直ちに std::abort() を呼び出して（ハードクラッシュ）終了します。false の場合は割り当てがブロックされ、アロケータは失敗（nullptr または ENOMEM）を返すため、呼び出し元がエラーを処理できます。このチェックは TRY_CATCH_BAD_ALLOC パスでラップされていない割り当てに対してのみ有効です（mem hook は bad-alloc を捕捉している場合に別のフローを使用します）。予期しない巨大な割り当ての fail-fast デバッグ目的で有効にしてください。運用環境では、過大な割り当て試行で即時プロセス中断を望む場合を除き無効のままにしてください。
- 導入バージョン: 3.4.3, 3.5.0, 4.0.0

##### arrow_flight_port

- デフォルト: -1
- タイプ: Int
- 単位: Port
- 変更可能: いいえ
- 説明: BE の Arrow Flight SQL サーバー用の TCP ポート。Arrow Flight サービスを無効化するには `-1` に設定します。macOS 以外のビルドでは、BE は起動時に Arrow Flight SQL Server を呼び出します。ポートが利用できない場合、サーバーの起動は失敗し BE プロセスは終了します。設定されたポートは HeartBeat Payload で FE に報告されます。BE を起動する前に `be.conf` でこの値を設定してください。
- 導入バージョン: v3.4.0, v3.5.0

##### be_exit_after_disk_write_hang_second

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 可変: いいえ
- 説明: ディスクがハングした後、BE が終了するまでの待機時間。
- 導入バージョン: -

##### be_http_num_workers

- デフォルト: 48
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: HTTP サーバーが使用するスレッドの数。
- 導入バージョン: -

##### be_http_port

- デフォルト: 8040
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE HTTP サーバーポート。
- 導入バージョン: -

##### be_port

- デフォルト: 9060
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE の thrift サーバーポートで、FEs からのリクエストを受け取るために使用されます。
- 導入バージョン: -

##### brpc_max_body_size

- デフォルト: 2147483648
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: bRPC の最大ボディサイズ。
- 導入バージョン: -

##### brpc_max_connections_per_server

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クライアントが各リモートサーバーエンドポイントごとに保持する永続的な bRPC 接続の最大数。各エンドポイントについて `BrpcStubCache` は `_stubs` ベクタをこのサイズに予約した `StubPool` を作成します。最初のアクセス時には制限に達するまで新しい stub が作成され、その後は既存の stub がラウンドロビン方式で返されます。この値を増やすとエンドポイントごとの並列性が高まり（単一チャネルでの競合が減る）、その代わりにファイルディスクリプタ、メモリ、チャネルが増えます。
- 導入バージョン: v3.2.0

##### brpc_num_threads

- デフォルト: -1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: bRPC の bthreads の数。値 `-1` は CPU スレッドと同じ数を示します。
- 導入バージョン: -

##### brpc_port

- デフォルト: 8060
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE bRPC ポートで、bRPC のネットワーク統計を表示するために使用されます。
- 導入バージョン: -

##### brpc_stub_expire_s

- デフォルト: 3600
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: BRPC stub キャッシュの有効期限。デフォルトは60分です。
- 導入バージョン: -

##### compress_rowbatches

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: BE 間の RPC で行バッチを圧縮するかどうかを制御するブール値です。`true` は行バッチを圧縮することを示し、`false` は圧縮しないことを示します。
- 導入バージョン: -

##### delete_worker_count_normal_priority

- デフォルト: 2
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: BE エージェント上で delete (REALTIME_PUSH with DELETE) タスクを処理するために割り当てられる通常優先度のワーカースレッド数。起動時にこの値は delete_worker_count_high_priority に加算されて DeleteTaskWorkerPool のサイズ決定に使われます（agent_server.cpp を参照）。プールは最初の delete_worker_count_high_priority スレッドを HIGH 優先度として割り当て、残りを NORMAL とします。通常優先度スレッドは標準の delete タスクを処理し、全体の削除スループットに寄与します。並行削除容量を上げるには増やしてください（CPU/IO 使用量が増加します）。リソース競合を減らすには減らしてください。
- 導入バージョン: v3.2.0

##### enable_https

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: この項目が `true` に設定されると、BE の bRPC サーバは TLS を使用するように構成されます: `ServerOptions.ssl_options` は BE 起動時に `ssl_certificate_path` と `ssl_private_key_path` で指定された証明書と秘密鍵で設定されます。これにより受信 bRPC 接続に対して HTTPS/TLS が有効になり、クライアントは TLS を用いて接続する必要があります。証明書および鍵ファイルが存在し、BE プロセスからアクセス可能であり、bRPC/SSL の要件に合致していることを確認してください。
- 導入バージョン: v4.0.0

##### enable_jemalloc_memory_tracker

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: この項目が `true` に設定されていると、BE はバックグラウンドスレッド（jemalloc_tracker_daemon）を起動し、jemalloc の統計を1秒ごとにポーリングして、jemalloc の "stats.metadata" 値で GlobalEnv の jemalloc メタデータ MemTracker を更新します。これにより jemalloc のメタデータ消費が StarRocks プロセスのメモリ集計に含まれ、jemalloc 内部により使用されるメモリの過小報告を防ぎます。トラッカーは macOS 以外のビルド（#ifndef __APPLE__）でのみコンパイル/起動され、"jemalloc_tracker_daemon" という名前のデーモンスレッドとして動作します。この設定は起動時の振る舞いや MemTracker の状態を維持するスレッドに影響するため、変更には再起動が必要です。jemalloc を使用していない場合、または jemalloc のトラッキングを別途意図的に管理している場合のみ無効にし、それ以外は正確なメモリ集計と割り当て保護を維持するために有効のままにしてください。
- 導入バージョン: v3.2.12

##### enable_jvm_metrics

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: StarRocks が起動時に JVM 固有のメトリクスを初期化して登録するかどうかを制御します。値は Daemon::init 内の init_starrocks_metrics で読み取られ、StarRocksMetrics::initialize の init_jvm_metrics パラメータとして渡されます。有効にするとメトリクスサブシステムは JVM 関連のコレクタ（例: heap、GC、thread メトリクス）を作成してエクスポートし、無効の場合はそれらのコレクタは初期化されません。このフラグは起動時にのみ評価され、ランタイム中に変更することはできません。前方互換性のための設定であり、将来のリリースで削除される可能性があります。システムレベルのメトリクス収集は `enable_system_metrics` を使用して制御してください。
- 導入バージョン: v4.0.0

##### get_pindex_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: UpdateManager の "get_pindex" スレッドプールのワーカースレッド数を設定します。このプールは永続インデックスデータをロード／取得するために使用され（主キー表の rowset 適用時に使用）、実行時の設定更新はプールの最大スレッド数を調整します：`>0` の場合はその値が適用され、0 の場合はランタイムコールバックが CPU コア数（`CpuInfo::num_cores()`）を使用します。初期化時にはプールの最大スレッド数は max(get_pindex_worker_count, max_apply_thread_cnt * 2) として計算され、ここで max_apply_thread_cnt は apply-thread プールの最大値です。pindex ロードの並列度を上げるには増やし、同時実行性とメモリ／CPU 使用量を減らすには減らしてください。
- 導入バージョン: v3.2.0

##### heartbeat_service_port

- デフォルト: 9050
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE ハートビートサービスポートで、FEs からのハートビートを受け取るために使用されます。
- 導入バージョン: -

##### heartbeat_service_thread_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE ハートビートサービスのスレッド数。
- 導入バージョン: -

##### local_library_dir

- デフォルト: `${UDF_RUNTIME_DIR}`
- タイプ: string
- 単位: -
- 変更可能: No
- 説明: BE 上のローカルディレクトリで、UDF（ユーザー定義関数）ライブラリが配置され、Python UDF ワーカープロセスが動作する場所です。StarRocks は HDFS からこのパスへ UDF ライブラリをコピーし、各ワーカー用の Unix ドメインソケットを `<local_library_dir>/pyworker_<pid>` に作成し、Python ワーカープロセスを exec する前にこのディレクトリへ chdir します。ディレクトリは存在し、BE プロセスが書き込み可能であり、Unix ドメインソケットをサポートするファイルシステム（つまりローカルファイルシステム）上にある必要があります。この設定はランタイムで変更不可能なため、起動前に設定し、各 BE 上で十分な権限とディスク容量を確保してください。
- 導入バージョン: v3.2.0

##### mem_limit

- デフォルト: 90%
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: BE プロセスのメモリ上限。パーセンテージ ("80%") または物理的な制限 ("100G") として設定できます。デフォルトのハードリミットはサーバーのメモリサイズの 90% で、ソフトリミットは 80% です。同じサーバーで他のメモリ集約型サービスと一緒に StarRocks をデプロイしたい場合、このパラメータを設定する必要があります。
- 導入バージョン: -

##### memory_urgent_level

- デフォルト: 85
- タイプ: long
- 単位: Percentage (0-100)
- 変更可能: はい
- 説明: プロセスのメモリ上限に対するパーセンテージで表現される緊急メモリ水位。プロセスのメモリ使用量が `(limit * memory_urgent_level / 100)` を超えると、BE は即時のメモリ回収をトリガーします。これによりデータキャッシュの縮小、update キャッシュの追い出しが行われ、persistent/lake の MemTable は「満杯」と見なされて早期にフラッシュ／コンパクションされます。コードではこの設定が `memory_high_level` より大きく、`memory_high_level` は `1` 以上かつ `100` 以下であることを検証します。値を低くするとより積極的で早期の回収（頻繁なキャッシュ追い出しとフラッシュ）を招きます。値を高くすると回収が遅れ、100 に近すぎると OOM のリスクが高まります。`memory_high_level` および Data Cache 関連の自動調整設定と合わせてチューニングしてください。
- 導入バージョン: v3.2.0

##### net_use_ipv6_when_priority_networks_empty

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: `priority_networks` が指定されていない場合に IPv6 アドレスを優先的に使用するかどうかを制御するブール値です。`true` は、ノードをホストするサーバーが IPv4 と IPv6 の両方のアドレスを持ち、`priority_networks` が指定されていない場合に、システムが IPv6 アドレスを優先的に使用することを許可することを示します。
- 導入バージョン: v3.3.0

##### num_cores

- デフォルト: 0
- タイプ: Int
- 単位: Cores
- 変更可能: No
- 説明: StarRocks が CPU に依存する判断（スレッドプールのサイズ設定やランタイムスケジューリングなど）で使用する CPU コア数を制御します。値が 0 の場合は自動検出が有効になり、StarRocks は /proc/cpuinfo を読み取り、利用可能な全コアを使用します。正の整数 (> 0) に設定すると、その値が CpuInfo::init 内で検出されたコア数を上書きして有効なコア数になります。コンテナ内で実行している場合、cgroup の cpuset や CPU クォータ設定によって使用可能なコアがさらに制限されることがあり、CpuInfo はそれらの cgroup 制限も尊重します。この設定は起動時にのみ適用され、変更するにはサーバーの再起動が必要です。
- 導入バージョン: v3.2.0

##### priority_networks

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: 複数の IP アドレスを持つサーバーの選択戦略を宣言します。注意すべき点は、このパラメータで指定されたリストと一致する IP アドレスは最大で 1 つでなければなりません。このパラメータの値は、CIDR 表記でセミコロン (;) で区切られたエントリからなるリストです。例: `10.10.10.0/24`。このリストのエントリと一致する IP アドレスがない場合、サーバーの利用可能な IP アドレスがランダムに選択されます。v3.3.0 から、StarRocks は IPv6 に基づくデプロイをサポートしています。サーバーが IPv4 と IPv6 の両方のアドレスを持っている場合、このパラメータが指定されていない場合、システムはデフォルトで IPv4 アドレスを使用します。この動作を変更するには、`net_use_ipv6_when_priority_networks_empty` を `true` に設定します。
- 導入バージョン: -

##### ssl_private_key_path

- デフォルト: An empty string
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: BE の brpc サーバがデフォルト証明書のプライベートキーとして使用する TLS/SSL プライベートキー（PEM）のファイルシステムパス。`enable_https` が `true` に設定されていると、プロセス起動時に `brpc::ServerOptions::ssl_options().default_cert.private_key` がこのパスに設定されます。ファイルは BE プロセスからアクセス可能であり、`ssl_certificate_path` で指定した証明書と一致している必要があります。この値が設定されていないか、ファイルが存在しないまたはアクセスできない場合、HTTPS は構成されず bRPC サーバが起動に失敗する可能性があります。このファイルは制限付きのファイルシステム権限（例: 600）で保護してください。
- 導入バージョン: v4.0.0

##### thrift_client_retry_interval_ms

- デフォルト: 100
- タイプ: Int
- 単位: ミリ秒
- 可変: はい
- 説明: thrift クライアントがリトライする時間間隔。
- 導入バージョン: -

##### thrift_connect_timeout_seconds

- デフォルト: 3
- タイプ: Int
- 単位: Seconds
- 変更可能: No
- 説明: Thrift クライアントを作成する際に使用される接続タイムアウト（秒）。ClientCacheHelper::_create_client はこの値に 1000 を掛けて ThriftClientImpl::set_conn_timeout() に渡すため、BE クライアントキャッシュによってオープンされる新しい Thrift 接続の TCP/接続ハンドシェイクのタイムアウトを制御します。この設定は接続確立にのみ影響し、送受信タイムアウトは別途設定されます。非常に小さい値は高レイテンシのネットワークで誤検知による接続失敗を引き起こす可能性があり、大きすぎる値は到達不能なピアの検出を遅らせます。
- 導入バージョン: v3.2.0

##### thrift_port

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: 内部の Thrift ベースの BackendService を公開するために使用するポート。プロセスが Compute Node として動作しており、この項目が非ゼロに設定されている場合、`be_port` をオーバーライドして Thrift サーバはこの値にバインドします。そうでない場合は `be_port` が使用されます。この設定は非推奨です — 非ゼロの `thrift_port` を設定すると、代わりに `be_port` を使用するよう警告がログに記録されます。
- 導入バージョン: v3.2.0

##### thrift_rpc_connection_max_valid_time_ms

- デフォルト: 5000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: いいえ
- 説明: Thrift RPC 接続の最大有効時間。コネクションプールにこの値以上存在すると、コネクションは閉じられます。この値は FE 設定 `thrift_client_timeout_ms` と一致するように設定する必要があります。

##### thrift_rpc_max_body_size

- デフォルト: 0
- タイプ: Int
- 単位:
- 変更可能: いいえ
- 説明: RPC の文字列ボディの最大サイズ。`0` は無制限であることを示す。
- 導入バージョン: -

##### thrift_rpc_strict_mode

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Thrift の Strict 実行モードが有効かどうか。Thrift の Strict モードについては、[Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md) を参照してください。
- 導入バージョン: -

##### thrift_rpc_timeout_ms

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 可変: はい
- 説明: thrift RPC のタイムアウト。
- 導入バージョン: -

##### transaction_apply_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: BE の UpdateManager にある "update_apply" スレッドプール（主キーテーブルの rowset を適用するプール）の最小スレッド数を設定します。値が 0 の場合は固定の最小値が無効（下限なし）となります。`transaction_apply_worker_count` も 0 のときはプールの最大スレッド数はデフォルトで CPU コア数になり、実効的なワーカー数は CPU コア数と等しくなります。トランザクション適用時のベースラインの並行度を保証するために増やすことができますが、あまり高く設定すると CPU 競合が増える可能性があります。変更は update_config HTTP ハンドラを通じてランタイムで適用されます（apply スレッドプールの update_min_threads を呼び出します）。
- 導入バージョン: v3.2.11

### メタデータとクラスタ管理

##### cluster_id

- デフォルト: -1
- タイプ: Int
- 単位: N/A
- 変更可能: No
- 説明: この StarRocks backend のグローバルクラスタ識別子。起動時に StorageEngine は config::cluster_id を実効クラスタ ID として読み取り、すべての data root パスが同じクラスタ ID を含んでいることを検証します（StorageEngine::_check_all_root_path_cluster_id を参照）。値が -1 の場合は「未設定」を意味し、エンジンは既存のデータディレクトリまたはマスターのハートビートから実効 ID を導出することがあります。非負の ID が設定されている場合、設定された ID とデータディレクトリに格納されている ID の不一致は起動時の検証に失敗を引き起こします（Status::Corruption）。一部の root に ID が欠けており、エンジンが ID の書き込みを許可されている場合（options.need_write_cluster_id）、それらの root に実効 ID を永続化します。この設定は不変であるため、変更するには異なる設定でプロセスを再起動する必要があります。
- 導入バージョン: 3.2.0

##### retry_apply_interval_second

- デフォルト: 30
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: 失敗した tablet apply 操作の再試行をスケジュールする際に使用される基本間隔（秒）。サブミッション失敗後の再試行を直接スケジュールするために使用されるほか、バックオフの基礎乗数としても使用されます：次の再試行遅延は min(600, `retry_apply_interval_second` * failed_attempts) として計算されます。コードはまた累積再試行時間（等差数列の和）を計算するために `retry_apply_interval_second` を使用し、その値を `retry_apply_timeout_second` と比較して再試行を継続するか判断します。`enable_retry_apply` が true の場合にのみ有効です。この値を増やすと個々の再試行遅延および累積の再試行時間が長くなり、減らすと再試行がより頻繁になり `retry_apply_timeout_second` に達する前に試行回数が増える可能性があります。
- 導入バージョン: v3.2.9

##### update_schema_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: BE の "update_schema" 動的 ThreadPool で TTaskType::UPDATE_SCHEMA タスクを処理するワーカースレッドの最大数を設定します。ThreadPool は起動時に agent_server 内で作成され、最小 0 スレッド（アイドル時にゼロまでスケールダウン可能）、最大はこの設定値と等しくなります。プールはデフォルトのアイドルタイムアウトと事実上無制限のキューを使用します。より多くの同時スキーマ更新タスクを許可するにはこの値を増やします（CPU とメモリ使用量が増加します）。並列スキーマ操作を制限したい場合は値を下げます。このオプションはランタイムで変更できないため、変更には BE の再起動が必要です。
- 導入バージョン: 3.2.3

### ユーザー、ロール、および権限

##### ssl_certificate_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: `enable_https` が true のときに BE の brpc サーバが使用する TLS/SSL 証明書ファイル（PEM）への絶対パス。BE 起動時にこの値は `brpc::ServerOptions::ssl_options().default_cert.certificate` にコピーされます。対応する秘密鍵は必ず `ssl_private_key_path` に設定してください。必要に応じてサーバ証明書および中間証明書を PEM 形式（証明書チェーン）で提供してください。ファイルは StarRocks BE プロセスから読み取り可能でなければならず、起動時にのみ適用されます。`enable_https` が有効でこの値が未設定または無効な場合、brpc の TLS 設定が失敗しサーバが正しく起動できない可能性があります。
- 導入バージョン: v4.0.0

### クエリエンジン

##### dictionary_speculate_min_chunk_size

- デフォルト: 10000
- タイプ: Int
- 単位: Rows
- 変更可能: No
- 説明: StringColumnWriter および DictColumnWriter が辞書エンコーディングの推測を開始するために使用する最小行数（チャンクサイズ）。受信カラム（または蓄積されたバッファに加えた受信行）のサイズが `dictionary_speculate_min_chunk_size` 以上であれば、ライターは即座に推測を実行してエンコーディング（DICT、PLAIN、または BIT_SHUFFLE のいずれか）を設定し、さらに行をバッファリングしません。推測では文字列カラムに対して `dictionary_encoding_ratio`、数値/非文字列カラムに対して `dictionary_encoding_ratio_for_non_string_column` を使用して辞書エンコーディングが有利かどうかを判断します。また、カラムのバイトサイズが大きく（UINT32_MAX 以上）なる場合は、`BinaryColumn<uint32_t>` のオーバーフローを避けるために即時に推測が行われます。
- 導入バージョン: v3.2.0

##### disable_storage_page_cache

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: PageCache を無効にするかどうかを制御するブール値。
  - PageCache が有効な場合、StarRocks は最近スキャンされたデータをキャッシュします。
  - PageCache は、類似のクエリが頻繁に繰り返される場合にクエリパフォーマンスを大幅に向上させることができます。
  - `true` は PageCache を無効にすることを示します。
  - この項目のデフォルト値は StarRocks v2.4 以降、`true` から `false` に変更されました。
- 導入バージョン: -

##### enable_bitmap_index_memory_page_cache

- デフォルト: true 
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明:Bitmap インデックスのメモリキャッシュを有効にするかどうか。Bitmap インデックスを使用してポイントクエリを高速化したい場合は、メモリキャッシュを使用することを推奨します。
- 導入バージョン: v3.1

##### enable_compaction_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位: 
- 可変: はい
- 説明: Flat JSON データのコンパクションを有効にするかどうか。
- 導入バージョン: v3.3.3

##### enable_json_flat

- デフォルト: false
- タイプ: Boolean
- 単位: 
- 可変: はい
- 説明: Flat JSON 機能を有効にするかどうか。 この機能を有効にすると、新しくロードされた JSON データが自動的にフラット化され、JSON クエリパフォーマンスが向上します。
- 導入バージョン: v3.3.0

##### enable_lazy_dynamic_flat_json

- デフォルト: True
- タイプ: Boolean
- 単位: 
- 可変: はい
- 説明: クエリが読み取りプロセスで Flat JSON スキーマを見逃した場合に Lazy Dynamic Flat JSON を有効にするかどうか。この項目が `true` に設定されている場合、StarRocks は Flat JSON 操作を読み取りプロセスではなく計算プロセスに延期します。
- 導入バージョン: v3.3.3

##### enable_ordinal_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: オーディナルインデックスのメモリキャッシュを有効にするかどうか。オーディナルインデックスは行IDからデータページの位置へのマッピングであり、スキャンを高速化するために使用できる。
- 導入バージョン:  -

##### enable_string_prefix_zonemap

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 文字列（CHAR/VARCHAR）列に対して、前置長に基づくゾーンマップを有効にするかどうか。非キー列では、最小値/最大値は `string_prefix_zonemap_prefix_len` で指定した長さに切り詰められます。
- 導入バージョン: -

##### enable_zonemap_index_memory_page_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: ゾーンマップインデックスのメモリーキャッシュを有効にするかどうか。ゾーンマップインデックスを使用してスキャンを高速化したい場合は、メモリキャッシュを使用することを推奨します。
- 導入バージョン: -

##### exchg_node_buffer_size_bytes

- デフォルト: 10485760
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: 各クエリの交換ノードの受信側の最大バッファサイズ。この設定項目はソフトリミットです。データが過剰な速度で受信側に送信されると、バックプレッシャーがトリガーされます。
- 導入バージョン: -

##### file_descriptor_cache_capacity

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: キャッシュできるファイルディスクリプタの数。
- 導入バージョン: -

##### flamegraph_tool_dir

- デフォルト: `${STARROCKS_HOME}/bin/flamegraph`
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: フレームグラフツールのディレクトリ。このディレクトリには、プロファイルデータからフレームグラフを生成するための pprof、stackcollapse-go.pl、flamegraph.pl スクリプトが含まれている必要があります。
- 導入バージョン: -

##### fragment_pool_queue_size

- デフォルト: 2048
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 各 BE ノードで処理できるクエリ数の上限。
- 導入バージョン: -

##### fragment_pool_thread_num_max

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: クエリに使用される最大スレッド数。
- 導入バージョン: -

##### fragment_pool_thread_num_min

- デフォルト: 64
- タイプ: Int
- 単位: 分
- 可変: いいえ
- 説明: クエリに使用される最小スレッド数。
- 導入バージョン: -

##### hdfs_client_enable_hedged_read

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: ヘッジドリード機能を有効にするかどうかを指定します。
- 導入バージョン: v3.0

##### hdfs_client_hedged_read_threadpool_size

- デフォルト: 128
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: HDFS クライアントのヘッジドリードスレッドプールのサイズを指定します。スレッドプールのサイズは、HDFS クライアントでヘッジドリードを実行するために専用されるスレッドの数を制限します。これは、HDFS クラスターの **hdfs-site.xml** ファイルの `dfs.client.hedged.read.threadpool.size` パラメータに相当します。
- 導入バージョン: v3.0

##### hdfs_client_hedged_read_threshold_millis

- デフォルト: 2500
- タイプ: Int
- 単位: ミリ秒
- 可変: いいえ
- 説明: ヘッジドリードを開始する前に待機するミリ秒数を指定します。たとえば、このパラメータを `30` に設定した場合、ブロックからの読み取りが 30 ミリ秒以内に返されない場合、HDFS クライアントはすぐに別のブロックレプリカに対して新しい読み取りを開始します。これは、HDFS クラスターの **hdfs-site.xml** ファイルの `dfs.client.hedged.read.threshold.millis` パラメータに相当します。
- 導入バージョン: v3.0

##### io_coalesce_adaptive_lazy_active

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 述語の選択性に基づいて、述語列と非述語列の I/O を結合するかどうかを適応的に決定します。
- 導入バージョン: v3.2

##### jit_lru_cache_size

- デフォルト: 0
- タイプ: Int
- 単位: Bytes
- 可変: はい
- 説明: JIT コンパイルのための LRU キャッシュサイズ。0 より大きい場合、キャッシュの実際のサイズを表します。0 以下に設定されている場合、システムは `jit_lru_cache_size = min(mem_limit*0.01, 1GB)` の式を使用してキャッシュを適応的に設定します (ノードの `mem_limit` は 16 GB 以上でなければなりません)。
- 導入バージョン: -

##### json_flat_column_max

- デフォルト: 100
- タイプ: Int
- 単位: 
- 可変: はい
- 説明: Flat JSON によって抽出できるサブフィールドの最大数。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

##### json_flat_create_zonemap

- デフォルト: true
- タイプ: Boolean
- 単位: 
- 可変: はい
- 説明: フラット化された JSON のサブカラムに対してゾーンマップを作成するかどうか。`enable_json_flat` が `true` の場合にのみ有効です。
- 導入バージョン: -

##### json_flat_null_factor

- デフォルト: 0.3
- タイプ: Double
- 単位: 
- 可変: はい
- 説明: Flat JSON のために抽出する NULL 値の割合。NULL 値の割合がこのしきい値を超える列は抽出されません。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

##### json_flat_sparsity_factor

- デフォルト: 0.3
- タイプ: Double
- 単位: 
- 可変: はい
- 説明: Flat JSON の同じ名前を持つ列の割合。この値より低い場合、抽出は行われません。このパラメータは `enable_json_flat` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.0

##### lake_tablet_ignore_invalid_delete_predicate

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: カラム名が変更された後に論理削除によって重複キーのテーブルの tablet rowset メタデータに導入される可能性のある無効な delete predicate を無視するかどうかを制御するブール値。
- 導入バージョン: v4.0

##### max_hdfs_file_handle

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 開くことができる HDFS ファイルディスクリプタの最大数。
- 導入バージョン: -

##### max_memory_sink_batch_count

- デフォルト: 20
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: スキャンキャッシュバッチの最大数。
- 導入バージョン: -

##### max_pushdown_conditions_per_column

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各列でプッシュダウンを許可する条件の最大数。この制限を超えると、述語はストレージレイヤーにプッシュダウンされません。
- 導入バージョン: -

##### max_scan_key_num

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各クエリによってセグメント化される最大スキャンキー数。
- 導入バージョン: -

##### min_file_descriptor_number

- デフォルト: 60000
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE プロセスの最小ファイルディスクリプタ数。
- 導入バージョン: -

##### object_storage_connect_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 可変: いいえ
- 説明: オブジェクトストレージとのソケット接続を確立するためのタイムアウト期間。`-1` は SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

##### object_storage_request_timeout_ms

- デフォルト: -1
- タイプ: Int
- 単位: ミリ秒
- 可変: いいえ
- 説明: オブジェクトストレージとの HTTP 接続を確立するためのタイムアウト期間。`-1` は SDK 設定のデフォルトのタイムアウト期間を使用することを示します。
- 導入バージョン: v3.0.9

##### parquet_late_materialization_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: パフォーマンスを向上させるために Parquet リーダーの後期実体化を有効にするかどうかを制御するブール値。`true` は後期実体化を有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

##### parquet_page_index_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: パフォーマンスを向上させるために Parquet ファイルのページインデックスを有効にするかどうかを制御するブール値。`true` はページインデックスを有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: v3.3

##### parquet_reader_bloom_filter_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: パフォーマンスを向上させるために Parquet ファイルのブルームフィルターを有効にするかどうかを制御するブール値。`true` はブルームフィルタを有効にすることを示し、`false` は無効にすることを示す。システム変数 `enable_parquet_reader_bloom_filter` を使用して、セッションレベルでこの動作を制御することもできます。Parquet におけるブルームフィルタは、**各行グループ内のカラムレベルで管理されます**。Parquet ファイルに特定の列に対するブルームフィルタが含まれている場合、クエリはそれらの列に対する述語を使用して行グループを効率的にスキップすることができます。
- 導入バージョン: v3.5

##### path_gc_check_step

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 連続してスキャンできる最大ファイル数。
- 導入バージョン: -

##### path_gc_check_step_interval_ms

- デフォルト: 10
- タイプ: Int
- 単位: ミリ秒
- 可変: はい
- 説明: ファイルスキャン間の時間間隔。
- 導入バージョン: -

##### path_scan_interval_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: GC が期限切れデータをクリーンアップする時間間隔。
- 導入バージョン: -

##### pipeline_connector_scan_thread_num_per_cpu

- デフォルト: 8
- タイプ: Double
- 単位: -
- 可変: はい
- 説明: BE ノードの CPU コアごとに Pipeline Connector に割り当てられるスキャンスレッドの数。この設定は v3.1.7 以降、動的に変更されました。
- 導入バージョン: -

##### pipeline_poller_timeout_guard_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 可変: はい
- 説明: この項目が `0` より大きい値に設定されている場合、ドライバがポーラの 1 回のディスパッチに `pipeline_poller_timeout_guard_ms` 以上の時間がかかると、ドライバとオペレータの情報が出力される。
- 導入バージョン: -

##### pipeline_prepare_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: Pipeline 実行エンジンの PREPARE Fragment スレッドプールの最大キュー長。
- 導入バージョン: -

##### pipeline_prepare_thread_pool_thread_num

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: Pipeline 実行エンジン PREPARE Fragment スレッドプールのスレッド数。`0` はシステムの VCPU コア数と同じであることを示す。
- 導入バージョン: -

##### pipeline_prepare_timeout_guard_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 可変: はい
- 説明: この項目が `0` より大きい値に設定されている場合、PREPARE 処理中にプランの Fragment が `pipeline_prepare_timeout_guard_ms` を超えると、プランの Fragment のスタックトレースが出力される。

##### pipeline_scan_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: Pipeline 実行エンジンの SCAN スレッドプールの最大タスクキュー長。
- 導入バージョン: -

##### query_cache_capacity

- デフォルト: 536870912
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: BE のクエリキャッシュのサイズ。デフォルトサイズは 512 MB です。サイズは 4 MB 未満にすることはできません。BE のメモリ容量が期待するクエリキャッシュサイズを提供するのに不十分な場合、BE のメモリ容量を増やすことができます。
- 導入バージョン: -

##### query_pool_spill_mem_limit_threshold

- デフォルト: 1.0
- タイプ: Double
- 単位: -
- 可変: いいえ
- 説明: 自動スピリングが有効な場合、すべてのクエリのメモリ使用量が `query_pool memory limit * query_pool_spill_mem_limit_threshold` を超えると、中間結果のスピリングがトリガーされます。
- 導入バージョン: v3.2.7

##### result_buffer_cancelled_interval_time

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: BufferControlBlock がデータを解放するまでの待機時間。
- 導入バージョン: -

##### scan_context_gc_interval_min

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 可変: はい
- 説明: スキャンコンテキストをクリーンアップする時間間隔。
- 導入バージョン: -

##### scanner_row_num

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各スキャンで各スキャンスレッドが返す最大行数。
- 導入バージョン: -

##### scanner_thread_pool_queue_size

- デフォルト: 102400
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: ストレージエンジンがサポートするスキャンタスクの数。
- 導入バージョン: -

##### scanner_thread_pool_thread_num

- デフォルト: 48
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: ストレージエンジンが同時ストレージボリュームスキャンに使用するスレッドの数。すべてのスレッドはスレッドプールで管理されます。
- 導入バージョン: -

##### string_prefix_zonemap_prefix_len

- デフォルト: 16
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: `enable_string_prefix_zonemap` が有効な場合に、文字列ゾーンマップの最小値/最大値に使用する前置長。
- 導入バージョン: -

### ロード

##### clear_transaction_task_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: トランザクションをクリアするために使用されるスレッドの数。
- 導入バージョン: -

##### column_mode_partial_update_insert_batch_size

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 挿入行を処理する際の列モード部分更新におけるバッチサイズ。この項目が `0` または負の数値に設定されている場合、無限ループを回避するため `1` に制限されます。この項目は各バッチで処理される新規挿入行の数を制御します。大きな値は書き込みパフォーマンスを向上させますが、より多くのメモリを消費します。
- 導入バージョン: v3.5.10, v4.0.2

##### enable_stream_load_verbose_log

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: Stream Load ジョブの HTTP リクエストとレスポンスをログに記録するかどうかを指定します。
- 導入バージョン: v2.5.17, v3.0.9, v3.1.6, v3.2.1

##### flush_thread_num_per_store

- デフォルト: 2
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各ストアで MemTable をフラッシュするために使用されるスレッドの数。
- 導入バージョン: -

##### lake_flush_thread_num_per_store

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードで各ストアで MemTable をフラッシュするために使用されるスレッドの数。
  この値が `0` に設定されている場合、システムは CPU コア数の 2 倍を値として使用します。
  この値が `0` 未満に設定されている場合、システムはその絶対値と CPU コア数の積を値として使用します。
- 導入バージョン: 3.1.12, 3.2.7

##### load_data_reserve_hours

- デフォルト: 4
- タイプ: Int
- 単位: 時間
- 可変: いいえ
- 説明: 小規模ロードによって生成されたファイルの予約時間。
- 導入バージョン: -

##### load_error_log_reserve_hours

- デフォルト: 48
- タイプ: Int
- 単位: 時間
- 可変: はい
- 説明: データロードログが保持される時間。
- 導入バージョン: -

##### load_process_max_memory_limit_bytes

- デフォルト: 107374182400
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースの最大サイズ制限。
- 導入バージョン: -

##### max_consumer_num_per_group

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: Routine Load のコンシューマーグループ内の最大コンシューマー数。
- 導入バージョン: -

##### max_runnings_transactions_per_txn_map

- デフォルト: 100
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各パーティションで同時に実行できるトランザクションの最大数。
- 導入バージョン: -

##### number_tablet_writer_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: インポート用の tablet writer のスレッド数，Stream Load、Broker Load、Insert などに使用されます。パラメータが 0 以下に設定されている場合、システムは CPU コア数の半分（最小で 16）を使用します。パラメータが 0 より大きい値に設定されている場合、システムはその値を使用します。この設定は v3.1.7 以降、動的に変更されました。
- 導入バージョン: -

##### push_worker_count_high_priority

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: HIGH 優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

##### push_worker_count_normal_priority

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: NORMAL 優先度のロードタスクを処理するために使用されるスレッドの数。
- 導入バージョン: -

##### streaming_load_max_batch_size_mb

- デフォルト: 100
- タイプ: Int
- 単位: MB
- 可変: はい
- 説明: StarRocks にストリーミングできる JSON ファイルの最大サイズ。
- 導入バージョン: -

##### streaming_load_max_mb

- デフォルト: 102400
- タイプ: Int
- 単位: MB
- 可変: はい
- 説明: StarRocks にストリーミングできるファイルの最大サイズ。v3.0 以降、デフォルト値は `10240` から `102400` に変更されました。
- 導入バージョン: -

##### streaming_load_rpc_max_alive_time_sec

- デフォルト: 1200
- タイプ: Int
- 単位: 秒
- 可変: いいえ
- 説明: Stream Load の RPC タイムアウト。
- 導入バージョン: -

##### transaction_publish_version_thread_pool_idle_time_ms

- デフォルト: 60000
- タイプ: Int
- 単位: ミリ秒
- 可変: いいえ
- 説明: スレッドが Publish Version スレッドプールによって再利用されるまでの Idle 時間。
- 導入バージョン: -

##### transaction_publish_version_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: バージョンを公開するために使用される最大スレッド数。この値が `0` 以下に設定されている場合、システムは CPU コア数を値として使用し、インポートの同時実行が高いが固定スレッド数しか使用されない場合にスレッドリソースが不足するのを回避します。v2.5 以降、デフォルト値は `8` から `0` に変更されました。
- 導入バージョン: -

##### write_buffer_size

- デフォルト: 104857600
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: メモリ内の MemTable のバッファサイズ。この設定項目はフラッシュをトリガーするしきい値です。
- 導入バージョン: -

### ロードとアンロード

##### broker_write_timeout_seconds

- デフォルト: 30
- タイプ: int
- 単位: Seconds
- 変更可能: No
- 説明: バックエンドの broker 操作で書き込み/IO RPC に使用されるタイムアウト（秒）。この値は 1000 を掛けられてミリ秒タイムアウトとなり、BrokerFileSystem および BrokerServiceConnection インスタンス（例：ファイルエクスポートやスナップショットのアップロード/ダウンロード）にデフォルトの timeout_ms として渡されます。broker やネットワークが遅い場合、または大きなファイルを転送する場合は早期タイムアウトを避けるために増やしてください；減らすと broker RPC が早期に失敗する可能性があります。この値は common/config に定義され、プロセス起動時に適用されます（動的リロード不可）。
- 導入バージョン: v3.2.0

##### enable_load_channel_rpc_async

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、load-channel の open RPC（例: PTabletWriterOpen）の処理が BRPC ワーカーから専用のスレッドプールへオフロードされます。リクエストハンドラは ChannelOpenTask を生成して内部 `_async_rpc_pool` に投入し、`LoadChannelMgr::_open` をインラインで実行しません。これにより BRPC スレッド内の作業量とブロッキングが減少し、`load_channel_rpc_thread_pool_num` と `load_channel_rpc_thread_pool_queue_size` で同時実行性を調整できるようになります。スレッドプールへの投入が失敗する（プールが満杯またはシャットダウン済み）と、リクエストはキャンセルされエラー状態が返されます。プールは `LoadChannelMgr::close()` でシャットダウンされるため、有効化する際は容量とライフサイクルを考慮し、リクエストの拒否や処理遅延を避けるようにしてください。
- 導入バージョン: v3.5.0

##### enable_streaming_load_thread_pool

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: ストリーミングロード用のスキャナを専用の streaming load スレッドプールに送るかどうかを制御します。有効で、かつクエリが `TLoadJobType::STREAM_LOAD` の LOAD の場合、ConnectorScanNode はスキャナタスクを `streaming_load_thread_pool`（INT32_MAX スレッドおよびキューサイズで構成され、事実上無制限）に送信します。無効にすると、スキャナは一般的な `thread_pool` とその `PriorityThreadPool` 提出ロジック（優先度計算、try_offer/offer の振る舞い）を使用します。有効にすると通常のクエリ実行からストリーミングロードの作業を分離して干渉を減らせますが、専用プールは事実上無制限であるため、トラフィックが多いと同時スレッド数やリソース使用量が増える可能性があります。このオプションはデフォルトでオンになっており、通常は変更を必要としません。
- 導入バージョン: v3.2.0

##### es_http_timeout_ms

- デフォルト: 5000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: No
- 説明: Elasticsearch の scroll リクエストに対して ESScanReader 内の ES ネットワーククライアントが使用する HTTP 接続タイムアウト（ミリ秒）。この値は次の scroll POST を送信する前に `network_client.set_timeout_ms()` を介して適用され、スクロール処理中にクライアントが ES の応答を待つ時間を制御します。ネットワークが遅い場合や大きなクエリで早期タイムアウトを回避するためにこの値を増やし、応答しない ES ノードに対しては早めに失敗させたい場合は値を小さくしてください。この設定はスクロールコンテキストのキープアライブ期間を制御する `es_scroll_keepalive` を補完します。
- 導入バージョン: v3.2.0

##### es_index_max_result_window

- デフォルト: 10000
- タイプ: Int
- 単位: Documents
- 変更可能: No
- 説明: StarRocks が単一バッチで Elasticsearch に要求する最大ドキュメント数を制限します。ES リーダーの `KEY_BATCH_SIZE` を構築する際、StarRocks は ES リクエストバッチサイズを min(`es_index_max_result_window`, `chunk_size`) に設定します。ES リクエストが Elasticsearch のインデックス設定 `index.max_result_window` を超えると、Elasticsearch は HTTP 400 (Bad Request) を返します。大きなインデックスをスキャンする場合はこの値を調整するか、Elasticsearch 側で `index.max_result_window` を増やしてより大きな単一リクエストを許可してください。
- 導入バージョン: v3.2.0

##### load_channel_rpc_thread_pool_num

- デフォルト: -1
- タイプ: Int
- 単位: Threads
- 変更可能: はい
- 説明: load-channel 非同期 RPC スレッドプールの最大スレッド数。`<= 0`（デフォルト `-1`）に設定するとプールサイズは自動的に CPU コア数（CpuInfo::num_cores()）に設定されます。設定された値は ThreadPoolBuilder の max threads として使われ、プールの最小スレッド数は min(5, max_threads) に設定されます。プールのキューサイズは `load_channel_rpc_thread_pool_queue_size` によって別途制御されます。この設定は、load RPC の処理を同期から非同期に切り替えた後も動作が互換となるように、async RPC プールサイズを brpc ワーカーのデフォルト（`brpc_num_threads`）に合わせるために導入されました。ランタイムでこの設定を変更すると ExecEnv::GetInstance()->load_channel_mgr()->async_rpc_pool()->update_max_threads(...) がトリガーされます。
- 導入バージョン: v3.5.0

##### load_channel_rpc_thread_pool_queue_size

- デフォルト: 1024000
- タイプ: int
- 単位: Count
- 変更可能: いいえ
- 説明: LoadChannelMgr によって作成される Load チャネル RPC スレッドプールの保留タスク最大キューサイズを設定します。このスレッドプールは `enable_load_channel_rpc_async` が有効なときに非同期の `open` リクエストを実行し、プールサイズは `load_channel_rpc_thread_pool_num` と組になっています。大きなデフォルト値（1024000）は、同期処理から非同期処理への切り替え後も動作が保たれるように brpc ワーカーのデフォルトに合わせたものです。キューが満杯になると ThreadPool::submit() は失敗し、到着した open RPC はエラーでキャンセルされ、呼び出し元は拒否を受け取ります。大量の同時 `open` リクエストをバッファしたい場合はこの値を増やしてください；逆に小さくするとバックプレッシャーが強まり、負荷時に拒否が増える可能性があります。
- 導入バージョン: v3.5.0

##### load_fp_tablets_channel_add_chunk_block_ms

- デフォルト: -1
- タイプ: Int
- 単位: Milliseconds
- 変更可能: Yes
- 説明: 有効にすると（正のミリ秒値に設定すると）このフェイルポイント設定は load 処理中に TabletsChannel::add_chunk を指定した時間だけスリープさせます。BRPC のタイムアウトエラー（例: "[E1008]Reached timeout"）をシミュレートしたり、add_chunk の高コスト操作によるロード遅延を模擬するために使用されます。値が `<= 0`（デフォルト `-1`）の場合、注入は無効になります。フォールトハンドリング、タイムアウト、レプリカ同期挙動のテストを目的としており、書き込み完了を遅延させ上流のタイムアウトやレプリカの中止を引き起こす可能性があるため、通常の本番ワークロードでは有効にしないでください。
- 導入バージョン: v3.5.0

##### streaming_load_thread_pool_idle_time_ms

- デフォルト: 2000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: No
- 説明: streaming-load 関連のスレッドプールに対するスレッドのアイドルタイムアウト（ミリ秒）を設定します。この値は `stream_load_io` プールに対して ThreadPoolBuilder に渡されるアイドルタイムアウトとして使用され、`load_rowset_pool` と `load_segment_pool` にも適用されます。これらのプール内のスレッドはこの期間アイドル状態が続くと回収されます；値を小さくするとアイドル時のリソース使用は減りますがスレッド生成のオーバーヘッドが増え、値を大きくするとスレッドが長く生存します。`stream_load_io` プールは `enable_streaming_load_thread_pool` が有効な場合に使用されます。
- 導入バージョン: v3.2.0

##### streaming_load_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: ExecEnv 初期化時に作成される streaming load IO スレッドプール ("stream_load_io") の最小スレッド数。プールは `set_max_threads(INT32_MAX)` と `set_max_queue_size(INT32_MAX)` で構築され、事実上デッドロック回避のために無制限にされます。値が 0 の場合、プールはスレッドを持たずに需要に応じて拡張します；正の値を設定すると起動時にその数のスレッドを確保します。このプールは `enable_streaming_load_thread_pool` が true のときに使用され、アイドルタイムアウトは `streaming_load_thread_pool_idle_time_ms` で制御されます。全体の並行度は依然として `fragment_pool_thread_num_max` と `webserver_num_workers` によって制約されるため、この値を変更する必要は稀であり、高すぎるとリソース使用量が増える可能性があります。
- 導入バージョン: v3.2.0

### 統計レポート

##### report_disk_state_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ストレージボリュームの状態を報告する時間間隔。これには、ボリューム内のデータサイズが含まれます。
- 導入バージョン: -

##### report_tablet_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: すべてのタブレットの最新バージョンを報告する時間間隔。
- 導入バージョン: -

##### report_task_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: タスクの状態を報告する時間間隔。タスクは、テーブルの作成、テーブルの削除、データのロード、またはテーブルスキーマの変更を行うことができます。
- 導入バージョン: -

##### report_workgroup_interval_seconds

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: すべてのワークグループの最新バージョンを報告する時間間隔。
- 導入バージョン: -

### ストレージ

##### alter_tablet_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: スキーマ変更のために使用されるスレッドの数。
- 導入バージョン: -

##### avro_ignore_union_type_tag

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: Avro の Union データタイプからシリアライズされた JSON 文字列からタイプタグを取り除くかどうか。
- 導入バージョン: v3.3.7, v3.4

##### base_compaction_check_interval_seconds

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ベースコンパクションのスレッドポーリングの時間間隔。
- 導入バージョン: -

##### base_compaction_interval_seconds_since_last_operation

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 最後のベースコンパクションからの時間間隔。この設定項目はベースコンパクションをトリガーする条件の一つです。
- 導入バージョン: -

##### base_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 各ストレージボリュームでのベースコンパクションに使用されるスレッド数。
- 導入バージョン: -

##### base_cumulative_delta_ratio

- デフォルト: 0.3
- タイプ: Double
- 単位: -
- 可変: はい
- 説明: 累積ファイルサイズとベースファイルサイズの比率。この比率がこの値に達することがベースコンパクションをトリガーする条件の一つです。
- 導入バージョン: -

##### check_consistency_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: タブレットの一貫性をチェックするために使用されるスレッドの数。
- 導入バージョン: -

##### clear_expired_replication_snapshots_interval_seconds

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 異常なレプリケーションによって残された期限切れのスナップショットをシステムがクリアする時間間隔。
- 導入バージョン: v3.3.5

##### compact_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 同時コンパクションタスクに使用される最大スレッド数。この設定は v3.1.7 および v3.2.2 以降、動的に変更されました。
- 導入バージョン: v3.0.0

##### compaction_memory_limit_per_worker

- デフォルト: 2147483648
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: 各コンパクションスレッドに許可される最大メモリサイズ。
- 導入バージョン: -

##### compaction_trace_threshold

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 各コンパクションの時間しきい値。コンパクションがこの時間しきい値を超えて時間がかかる場合、StarRocks は対応するトレースを出力します。
- 導入バージョン: -

##### create_tablet_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: FE により送信される TTaskType::CREATE（create-tablet）タスクを処理する AgentServer のスレッドプール内の最大ワーカースレッド数を設定します。BE 起動時にこの値はスレッドプールの max として使用されます（プールは min threads = 1、max queue size = unlimited で作成されます）。ランタイムで変更すると ExecEnv::agent_server()->get_thread_pool(TTaskType::CREATE)->update_max_threads(...) が呼ばれます。バルクロードやパーティション作成時など、同時の tablet 作成スループットを上げたい場合に増やしてください。減らすと同時作成操作が制限されます。値を上げると CPU、メモリ、I/O の並列性が増し競合が発生する可能性があります。スレッドプールは少なくとも 1 スレッドを保証するため、1 未満の値は実質的な効果がありません。
- 導入バージョン: 3.2.0

##### cumulative_compaction_check_interval_seconds

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 累積コンパクションのスレッドポーリングの時間間隔。
- 導入バージョン: -

##### cumulative_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: ディスクごとの累積コンパクションスレッド数。
- 導入バージョン: -

##### data_page_size

- デフォルト: 65536
- タイプ: Int
- 単位: Bytes
- 変更可能: No
- 説明: 列データおよびインデックスページを構築する際に使用されるターゲットの非圧縮ページサイズ（バイト）。この値は ColumnWriterOptions.data_page_size と IndexedColumnWriterOptions.index_page_size にコピーされ、ページビルダ（例: BinaryPlainPageBuilder::is_page_full およびバッファ予約ロジック）によってページを完了するタイミングや確保するメモリ量の判断に参照されます。値が 0 の場合、ビルダ内のページサイズ制限は無効化されます。この値を変更するとページ数、メタデータのオーバーヘッド、メモリ予約、および I/O/圧縮のトレードオフに影響します（ページを小さくするとページ数とメタデータが増え、ページを大きくするとページ数は減り圧縮効率が向上する可能性があるがメモリのスパイクが大きくなる）。変更はランタイムで反映されないため、完全に有効にするにはプロセスの再起動と再作成された rowset が必要です。
- 導入バージョン: 3.2.4

##### default_num_rows_per_column_file_block

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各行ブロックに格納できる最大行数。
- 導入バージョン: -

##### delete_worker_count_high_priority

- デフォルト: 1
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: DeleteTaskWorkerPool 内で HIGH-priority の削除スレッドとして割り当てられるワーカースレッドの数。起動時に AgentServer は total threads = delete_worker_count_normal_priority + delete_worker_count_high_priority で削除プールを作成し、最初の delete_worker_count_high_priority スレッドは専ら TPriority::HIGH タスクをポップしようとするようにマークされます（高優先度削除タスクがない場合はポーリングしてスリープ/ループします）。この値を増やすと高優先度削除リクエストの並列性が高まり、減らすと専用容量が減って高優先度削除のレイテンシが増加する可能性があります。
- 導入バージョン: v3.2.0

##### disk_stat_monitor_interval

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ディスクの健康状態を監視する時間間隔。
- 導入バージョン: -

##### download_low_speed_limit_kbps

- デフォルト: 50
- タイプ: Int
- 単位: KB/秒
- 可変: はい
- 説明: 各 HTTP リクエストのダウンロード速度の下限。この値より低い速度で一定時間動作すると、HTTP リクエストは中止されます。この時間は、設定項目 `download_low_speed_time` で指定されます。
- 導入バージョン: -

##### download_low_speed_time

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ダウンロード速度が下限より低い状態で動作できる最大時間。この時間内に `download_low_speed_limit_kbps` の値より低い速度で動作し続けると、HTTP リクエストは中止されます。
- 導入バージョン: -

##### download_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: BE ノードでのリストアジョブのダウンロードタスクの最大スレッド数。`0` は、BE が存在するマシンの CPU コア数に値を設定することを示します。
- 導入バージョン: -

##### drop_tablet_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: タブレットを削除するために使用されるスレッドの数。
- 導入バージョン: -

##### enable_check_string_lengths

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: 文字列の長さをチェックして、範囲外の VARCHAR データによるコンパクションの失敗を解決するかどうか。
- 導入バージョン: -

##### enable_event_based_compaction_framework

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: イベントベースのコンパクションフレームワークを有効にするかどうか。`true` はイベントベースのコンパクションフレームワークが有効であることを示し、`false` は無効であることを示します。イベントベースのコンパクションフレームワークを有効にすると、多くのタブレットがある場合や単一のタブレットに大量のデータがある場合のコンパクションのオーバーヘッドを大幅に削減できます。
- 導入バージョン: -

##### enable_lazy_delta_column_compaction

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、部分的なカラム更新によって生成される delta columns に対する compaction に「lazy」戦略を優先します。StarRocks は compaction の I/O を節約するために、delta-column ファイルをメインのセグメントファイルへ積極的にマージすることを避けます。実際には compaction 選択コードが部分カラム更新の rowset と複数の候補をチェックし、それらが見つかりこのフラグが true の場合、エンジンは compaction にさらに入力を追加するのを止めるか、空の rowset（レベル -1）のみをマージして delta columns を別に保持します。これにより compaction 時の即時 I/O と CPU が削減されますが、統合の遅延（セグメント数増加や一時的なストレージオーバーヘッドの可能性）というコストが発生します。正確性やクエリのセマンティクスに変更はありません。
- 導入バージョン: v3.2.3

##### enable_new_load_on_memory_limit_exceeded

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: ハードメモリリソース制限に達したときに新しいロードプロセスを許可するかどうか。`true` は新しいロードプロセスが許可されることを示し、`false` は拒否されることを示します。
- 導入バージョン: v3.3.2

##### enable_pk_index_parallel_compaction

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データモードでプライマリキーインデックスの並列コンパクションを有効にするかどうか。
- 導入バージョン: -

##### enable_pk_index_parallel_get

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データモードでプライマリキーインデックスの並列取得を有効にするかどうか。
- 導入バージョン: -

##### enable_pk_parallel_execution

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: Primary Key テーブルの並列実行戦略を有効にするかどうかを決定します。有効化されると、インポートおよびコンパクションの段階で PK インデックスファイルが生成されます。
- 導入バージョン: -

##### enable_pk_size_tiered_compaction_strategy

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: 主キーテーブルのサイズ階層型コンパクションポリシーを有効にするかどうか。`true` はサイズ階層型コンパクション戦略が有効であることを示し、`false` は無効であることを示します。この項目は、共有データクラスタでは v3.2.4 および v3.1.10 以降、共有なしクラスタでは v3.2.5 および v3.1.10 以降で有効になります。
- 導入バージョン: -

##### enable_rowset_verify

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 生成された rowset の正確性を検証するかどうか。 有効にすると、コンパクションとスキーマ変更後に生成された rowset の正確性がチェックされます。
- 導入バージョン: -

##### enable_size_tiered_compaction_strategy

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: サイズ階層型コンパクションポリシー (主キーテーブルを除く) を有効にするかどうか。`true` はサイズ階層型コンパクション戦略が有効であることを示し、`false` は無効であることを示します。
- 導入バージョン: -

##### enable_strict_delvec_crc_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: enable_strict_delvec_crc_check を true に設定すると、delete vector の CRC32 を厳密にチェックし、一致しない場合はエラーを返します。
- 導入バージョン: -

##### enable_transparent_data_encryption

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: No
- 説明: 有効にすると、StarRocks は新規に書き込まれるストレージオブジェクト（segment files、delete/update files、rowset segments、lake SSTs、persistent index files など）に対してオンディスクの暗号化アーティファクトを作成します。Writers（RowsetWriter/SegmentWriter、lake UpdateManager/LakePersistentIndex および関連するコードパス）は KeyCache から暗号化情報を要求し、書き込み可能なファイルに encryption_info を付与し、rowset / segment / sstable メタデータ（segment_encryption_metas、delete/update encryption metadata）に encryption_meta を永続化します。Frontend と Backend/CN の暗号化フラグは一致している必要があり、不一致の場合は BE がハートビート時に中止します（LOG(FATAL)）。このフラグはランタイムで変更できないため、デプロイ前に有効化し、鍵管理（KEK）および KeyCache がクラスタ全体で適切に構成・同期されていることを確認してください。
- 導入バージョン: 3.3.1, 3.4.0, 3.5.0, 4.0.0

##### file_descriptor_cache_clean_interval

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 一定期間使用されていないファイルディスクリプタをクリーンアップする時間間隔。
- 導入バージョン: -

##### inc_rowset_expired_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 受信データの有効期限。この設定項目はインクリメンタルクローンで使用されます。
- 導入バージョン: -

##### load_process_max_memory_hard_limit_ratio

- デフォルト: 2
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースのハードリミット (比率)。`enable_new_load_on_memory_limit_exceeded` が `false` に設定されており、すべてのロードプロセスのメモリ消費が `load_process_max_memory_limit_percent * load_process_max_memory_hard_limit_ratio` を超える場合、新しいロードプロセスは拒否されます。
- 導入バージョン: v3.3.2

##### load_process_max_memory_limit_percent

- デフォルト: 30
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE ノード上のすべてのロードプロセスが占有できるメモリリソースのソフトリミット (パーセンテージ)。
- 導入バージョン: -

##### lz4_acceleration

- デフォルト: 1
- タイプ: Int
- 単位: N/A
- 変更可能: はい
- 説明: 組み込みの LZ4 圧縮器で使用される LZ4 の "acceleration" パラメータを制御します（LZ4_compress_fast_continue に渡されます）。値を大きくすると圧縮率を犠牲にして圧縮速度を優先し、値を小さく（1）すると圧縮は良くなりますが遅くなります。有効範囲: MIN=1, MAX=65537。この設定は BlockCompression にあるすべての LZ4 ベースのコーデック（例: LZ4 および Hadoop-LZ4）に影響し、圧縮の実行方法のみを変更します — LZ4 フォーマットや復号の互換性は変わりません。出力サイズが許容される CPU バウンドや低レイテンシのワークロードでは上げてチューニングしてください（例: 4、8、...）；ストレージや I/O に敏感なワークロードでは 1 のままにしてください。スループットとサイズのトレードオフはデータ依存性が高いため、変更前に代表的なデータでテストしてください。
- 導入バージョン: 3.4.1, 3.5.0, 4.0.0

##### lz4_expected_compression_ratio

- デフォルト: 2.1
- タイプ: double
- 単位: Dimensionless (compression ratio)
- 変更可能: はい
- 説明: シリアライゼーション圧縮戦略が観測された LZ4 圧縮を「良い」と判断するために使用する閾値です。compress_strategy.cpp では、この値が観測された compress_ratio を割る形で lz4_expected_compression_speed_mbps と合わせて報酬メトリクスを計算します；結合した報酬が > 1.0 であれば戦略は正のフィードバックを記録します。この値を上げると期待される圧縮率が高くなり（条件を満たしにくく）、下げると観測された圧縮が満足と見なされやすくなります。典型的なデータの圧縮しやすさに合わせて調整してください。 有効範囲: MIN=1, MAX=65537。
- 導入バージョン: 3.4.1, 3.5.0, 4.0.0

##### lz4_expected_compression_speed_mbps

- デフォルト: 600
- タイプ: double
- 単位: MB/s
- 変更可能: Yes
- 説明: adaptive compression policy (CompressStrategy) で使用される、期待される LZ4 圧縮スループット（メガバイト毎秒）。フィードバックルーチンは reward_ratio = (observed_compression_ratio / lz4_expected_compression_ratio) * (observed_speed / lz4_expected_compression_speed_mbps) を計算します。reward_ratio が `>` 1.0 の場合は正のカウンタ（alpha）を増やし、それ以外は負のカウンタ（beta）を増やします；これにより将来のデータが圧縮されるかどうかが影響を受けます。ハードウェア上の典型的な LZ4 スループットを反映するようこの値を調整してください — 値を上げると「良好」と判定するのが厳しく（より高い観測速度が必要）、下げると判定が容易になります。正の有限数でなければなりません。
- 導入バージョン: v3.4.1, 3.5.0, 4.0.0

##### make_snapshot_worker_count

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: BE ノードでのスナップショット作成タスクの最大スレッド数。
- 導入バージョン: -

##### manual_compaction_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 手動コンパクションのスレッド数。
- 導入バージョン: -

##### max_base_compaction_num_singleton_deltas

- デフォルト: 100
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 各ベースコンパクションでコンパクト化できる最大セグメント数。
- 導入バージョン: -

##### max_compaction_candidate_num

- デフォルト: 40960
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: コンパクションの候補タブレットの最大数。値が大きすぎると、高いメモリ使用量と高い CPU 負荷を引き起こします。
- 導入バージョン: -

##### max_compaction_concurrency

- デフォルト: -1
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: コンパクションの最大同時実行数 (ベースコンパクションと累積コンパクションの両方を含む)。値 `-1` は同時実行数に制限がないことを示します。`0` はコンパクションを無効にすることを示します。このパラメータは、イベントベースのコンパクションフレームワークが有効な場合に可変です。
- 導入バージョン: -

##### max_cumulative_compaction_num_singleton_deltas

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 単一の累積コンパクションでマージできる最大セグメント数。コンパクション中に OOM が発生した場合、この値を減少させることができます。
- 導入バージョン: -

##### max_download_speed_kbps

- デフォルト: 50000
- タイプ: Int
- 単位: KB/秒
- 可変: はい
- 説明: 各 HTTP リクエストの最大ダウンロード速度。この値は、BE ノード間のデータレプリカ同期のパフォーマンスに影響を与えます。
- 導入バージョン: -

##### max_garbage_sweep_interval

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ストレージボリュームのガーベジコレクションの最大時間間隔。この設定は v3.0 以降、動的に変更されました。
- 導入バージョン: -

##### max_percentage_of_error_disk

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 対応する BE ノードが終了する前にストレージボリュームで許容されるエラーの最大パーセンテージ。
- 導入バージョン: -

##### max_row_source_mask_memory_bytes

- デフォルト: 209715200
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: 行ソースマスクバッファの最大メモリサイズ。この値を超えると、データはディスク上の一時ファイルに保存されます。この値は `compaction_memory_limit_per_worker` の値よりも低く設定する必要があります。
- 導入バージョン: -

##### max_update_compaction_num_singleton_deltas

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 主キーテーブルの単一コンパクションでマージできる最大 rowset 数。
- 導入バージョン: -

##### memory_limitation_per_thread_for_schema_change

- デフォルト: 2
- タイプ: Int
- 単位: GB
- 可変: はい
- 説明: 各スキーマ変更タスクに許可される最大メモリサイズ。
- 導入バージョン: -

##### min_base_compaction_num_singleton_deltas

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: ベースコンパクションをトリガーする最小セグメント数。
- 導入バージョン: -

##### min_compaction_failure_interval_sec

- デフォルト: 120
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 前回のコンパクション失敗からタブレットコンパクションをスケジュールできる最小時間間隔。
- 導入バージョン: -

##### min_cumulative_compaction_failure_interval_sec

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 累積コンパクションが失敗時にリトライする最小時間間隔。
- 導入バージョン: -

##### min_cumulative_compaction_num_singleton_deltas

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 累積コンパクションをトリガーする最小セグメント数。
- 導入バージョン: -

##### min_garbage_sweep_interval

- デフォルト: 180
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ストレージボリュームのガーベジコレクションの最小時間間隔。この設定は v3.0 以降、動的に変更されました。
- 導入バージョン: -

##### parallel_clone_task_per_path

- デフォルト: 8
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: BE 上の各ストレージパスに割り当てられる並列 clone ワーカースレッドの数。BE 起動時にクローンスレッドプールの max threads は max(number_of_store_paths * parallel_clone_task_per_path, MIN_CLONE_TASK_THREADS_IN_POOL) として計算されます。例えばストレージパスが4つでデフォルト=8 の場合、クローンプールの max = 32 になります。この設定は BE が処理する CLONE タスク（tablet レプリカのコピー）の並列度を直接制御します：値を増やすと並列クローンスループットが向上しますが CPU、ディスク、ネットワークの競合も増えます；値を減らすと同時実行クローンタスクが制限され、FE がスケジュールしたクローン操作をスロットルする可能性があります。値は動的 clone スレッドプールに適用され、update-config パス経由でランタイムに変更可能です（agent_server がクローンプールの max threads を更新します）。
- 導入バージョン: v3.2.0

##### pending_data_expire_time_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ストレージエンジン内の保留中データの有効期限。
- 導入バージョン: -

##### pindex_major_compaction_limit_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: ディスク上のコンパクションの最大同時実行数。これは、コンパクションによるディスク間の不均一な I/O の問題に対処します。この問題は、特定のディスクに対して過度に高い I/O を引き起こす可能性があります。
- 導入バージョン: v3.0.9

##### pk_index_compaction_score_ratio

- デフォルト: 1.5
- タイプ: Double
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックスのコンパクションスコア比率。たとえば、N 個のファイルセットがある場合、コンパクションスコアは N * pk_index_compaction_score_ratio になります。
- 導入バージョン: -

##### pk_index_ingest_sst_compaction_threshold

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックス Ingest SST コンパクションの閾値。
- 導入バージョン: -

##### pk_index_memtable_flush_threadpool_max_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックス Memtable フラッシュ用のスレッドプールの最大スレッド数。
- 導入バージョン: -

##### pk_index_memtable_max_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックスの最大 Memtable 数。
- 導入バージョン: -

##### pk_index_parallel_compaction_task_split_threshold_bytes

- デフォルト: 104857600
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: プライマリキーインデックスコンパクションタスクの分割閾値。タスクに関連するファイルの合計サイズがこの閾値より小さい場合、タスクは分割されません。デフォルトは 100MB です。
- 導入バージョン: -

##### pk_index_parallel_compaction_threadpool_max_threads

- デフォルト: 4
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのクラウドネイティブプライマリキーインデックス並列コンパクション用のスレッドプールの最大スレッド数。
- 導入バージョン: -

##### pk_index_parallel_get_min_rows

- デフォルト: 16384
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでプライマリキーインデックスの並列取得を有効にするための最小行数閾値。
- 導入バージョン: -

##### pk_index_parallel_get_threadpool_max_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックス並列取得用のスレッドプールの最大スレッド数。0 は自動設定を意味します。
- 導入バージョン: -

##### pk_index_size_tiered_level_multiplier

- デフォルト: 10
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略のレベル倍数パラメータ。
- 導入バージョン: -

##### pk_index_size_tiered_max_level

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略のレベル数パラメータ。
- 導入バージョン: -

##### pk_index_size_tiered_min_level_size

- デフォルト: 131072
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: プライマリキーインデックス Size-Tiered コンパクション戦略の最小レベルサイズパラメータ。
- 導入バージョン: -

##### pk_index_target_file_size

- デフォルト: 67108864
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データモードでのプライマリキーインデックスのターゲットファイルサイズ。デフォルトは 64MB です。
- 導入バージョン: -

##### pk_parallel_execution_threshold_bytes

- デフォルト: 104857600
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: enable_pk_parallel_execution が true に設定されている場合、インポートまたはコンパクションで生成されるデータがこの閾値を超えると、Primary Key テーブルの並列実行戦略が有効になります。デフォルトは 100MB です。
- 導入バージョン: -

##### primary_key_limit_size

- デフォルト: 128
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: 主キーテーブルのキー列の最大サイズ。
- 導入バージョン: v2.5

##### release_snapshot_worker_count

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: BE ノードでのスナップショットリリースタスクの最大スレッド数。
- 導入バージョン: -

##### repair_compaction_interval_seconds

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 修復コンパクションスレッドをポーリングする時間間隔。
- 導入バージョン: -

##### replication_max_speed_limit_kbps

- デフォルト: 50000
- タイプ: Int
- 単位: KB/s
- 可変: はい
- 説明: 各レプリケーションスレッドの最大速度。
- 導入バージョン: v3.3.5

##### replication_min_speed_limit_kbps

- デフォルト: 50
- タイプ: Int
- 単位: KB/s
- 可変: はい
- 説明: 各レプリケーションスレッドの最小速度。
- 導入バージョン: v3.3.5

##### replication_min_speed_time_seconds

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: レプリケーションスレッドが最小速度を下回ることが許可される時間。実際の速度が `replication_min_speed_limit_kbps` を下回る時間がこの値を超えると、レプリケーションは失敗します。
- 導入バージョン: v3.3.5

##### replication_threads

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: レプリケーションに使用される最大スレッド数。`0` は、スレッド数を BE CPU コア数の 4 倍に設定することを示します。
- 導入バージョン: v3.3.5

##### size_tiered_level_multiple

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: サイズ階層型コンパクションポリシーにおける、2 つの連続するレベル間のデータサイズの倍率。
- 導入バージョン: -

##### size_tiered_level_multiple_dupkey

- デフォルト: 10
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: サイズ階層型コンパクションポリシーにおいて、重複キーテーブルの 2 つの隣接するレベル間のデータ量の差の倍率。
- 導入バージョン: -

##### size_tiered_level_num

- デフォルト: 7
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: サイズ階層型コンパクションポリシーのレベル数。各レベルには最大で 1 つの rowset が保持されます。したがって、安定した状態では、この設定項目で指定されたレベル数と同じ数の rowset が最大で存在します。
- 導入バージョン: -

##### size_tiered_min_level_size

- デフォルト: 131072
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: サイズ階層型コンパクションポリシーの最小レベルのデータサイズ。この値より小さい rowset はすぐにデータコンパクションをトリガーします。
- 導入バージョン: -

##### snapshot_expire_time_sec

- デフォルト: 172800
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: スナップショットファイルの有効期限。
- 導入バージョン: -

##### storage_flood_stage_left_capacity_bytes

- デフォルト: 107374182400
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: すべての BE ディレクトリにおける残りのストレージスペースのハードリミット。BE ストレージディレクトリの残りのストレージスペースがこの値より少なく、ストレージ使用率 (パーセンテージ) が `storage_flood_stage_usage_percent` を超える場合、ロードおよびリストアジョブは拒否されます。この項目を FE 設定項目 `storage_usage_hard_limit_reserve_bytes` と一緒に設定する必要があります。
- 導入バージョン: -

##### storage_flood_stage_usage_percent

- デフォルト: 95
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: すべての BE ディレクトリにおけるストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_flood_stage_left_capacity_bytes` より少ない場合、ロードおよびリストアジョブは拒否されます。この項目を FE 設定項目 `storage_usage_hard_limit_percent` と一緒に設定する必要があります。
- 導入バージョン: -

##### storage_medium_migrate_count

- デフォルト: 3
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 記憶媒体の移行 (SATA から SSD への移行) に使用されるスレッドの数。
- 導入バージョン: -

##### storage_root_path

- デフォルト: `${STARROCKS_HOME}/storage`
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: ストレージボリュームのディレクトリと媒体。例: `/data1,medium:hdd;/data2,medium:ssd`。
  - 複数のボリュームはセミコロン (`;`) で区切られます。
  - ストレージ媒体が SSD の場合、ディレクトリの末尾に `,medium:ssd` を追加します。
  - ストレージ媒体が HDD の場合、ディレクトリの末尾に `,medium:hdd` を追加します。
- 導入バージョン: -

##### sync_tablet_meta

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: タブレットメタデータの同期を有効にするかどうかを制御するブール値。`true` は同期を有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

##### tablet_map_shard_size

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: タブレットマップシャードサイズ。値は 2 の累乗でなければなりません。
- 導入バージョン: -

##### tablet_max_pending_versions

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 主キー タブレットで許容される最大保留バージョン数。保留バージョンは、コミットされているがまだ適用されていないバージョンを指します。
- 導入バージョン: -

##### tablet_max_versions

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: タブレットで許可される最大バージョン数。この値を超えると、新しい書き込みリクエストは失敗します。
- 導入バージョン: -

##### tablet_meta_checkpoint_min_interval_secs

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: TabletMeta チェックポイントのスレッドポーリングの時間間隔。
- 導入バージョン: -

##### tablet_meta_checkpoint_min_new_rowsets_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 最後の TabletMeta チェックポイント以降に作成される最小 rowset 数。
- 導入バージョン: -

##### tablet_rowset_stale_sweep_time_sec

- デフォルト: 1800
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: タブレット内の古い rowset をスイープする時間間隔。
- 導入バージョン: -

##### tablet_stat_cache_update_interval_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: タブレット統計キャッシュが更新される時間間隔。
- 導入バージョン: -

##### trash_file_expire_time_sec

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: ゴミファイルをクリーンアップする時間間隔。デフォルト値は v2.5.17、v3.0.9、v3.1.6 以降、259,200 から 86,400 に変更されました。
- 導入バージョン: -

##### unused_rowset_monitor_interval

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 期限切れの rowset をクリーンアップする時間間隔。
- 導入バージョン: -

##### update_cache_expire_sec

- デフォルト: 360
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: Update Cache の有効期限。
- 導入バージョン: -

##### update_compaction_check_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 主キーテーブルのコンパクションをチェックする時間間隔。
- 導入バージョン: -

##### update_compaction_delvec_file_io_amp_ratio

- デフォルト: 2
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 主キーテーブルの Delvec ファイルを含む rowset のコンパクションの優先順位を制御するために使用されます。値が大きいほど優先順位が高くなります。
- 導入バージョン: -

##### update_compaction_num_threads_per_disk

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 主キーテーブルのディスクごとのコンパクションスレッド数。
- 導入バージョン: -

##### update_compaction_per_tablet_min_interval_seconds

- デフォルト: 120
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: 主キーテーブル内の各タブレットに対してコンパクションがトリガーされる最小時間間隔。
- 導入バージョン: -

##### update_compaction_ratio_threshold

- デフォルト: 0.5
- タイプ: Double
- 単位: -
- 可変: はい
- 説明: 共有データクラスタ内の主キーテーブルに対してコンパクションがマージできるデータの最大割合。単一のタブレットが過度に大きくなる場合、この値を縮小することをお勧めします。
- 導入バージョン: v3.1.5

##### update_compaction_result_bytes

- デフォルト: 1073741824
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: 主キーテーブルの単一コンパクションの最大結果サイズ。
- 導入バージョン: -

##### update_compaction_size_threshold

- デフォルト: 268435456
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 主キーテーブルのコンパクションスコアはファイルサイズに基づいて計算され、他のテーブルタイプとは異なります。このパラメータは、主キーテーブルのコンパクションスコアを他のテーブルタイプのコンパクションスコアに似せるために使用でき、ユーザーが理解しやすくなります。
- 導入バージョン: -

##### upload_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: BE ノードでのバックアップジョブのアップロードタスクの最大スレッド数。`0` は、BE が存在するマシンの CPU コア数に値を設定することを示します。
- 導入バージョン: -

##### vertical_compaction_max_columns_per_group

- デフォルト: 5
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 垂直コンパクションのグループごとの最大列数。
- 導入バージョン: -

### 共有データ

##### download_buffer_size

- デフォルト: 4194304
- タイプ: Int
- 単位: Bytes
- 変更可能: Yes
- 説明: スナップショットファイルをダウンロードする際に使用されるメモリ内コピー用バッファのサイズ（バイト）。SnapshotLoader::download はこの値を fs::copy に対してリモートの sequential file からローカルの writable file へ読み込む際の 1 回あたりのチャンクサイズとして渡します。帯域幅の大きいリンクでは、より大きな値にすることで syscall/IO オーバーヘッドが減りスループットが向上する可能性があります。小さい値はアクティブな転送ごとのピークメモリ使用量を削減します。注意: このパラメータはストリームごとのバッファサイズを制御するものであり、ダウンロードスレッド数を制御するものではありません — 総メモリ消費量 = download_buffer_size * number_of_concurrent_downloads です。
- 導入バージョン: v3.2.13

##### graceful_exit_wait_for_frontend_heartbeat

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: グレースフルシャットダウンを完了する前に、少なくとも1件のフロントエンドからの heartbeat 応答で SHUTDOWN 状態が返されるのを待つかどうかを決定します。有効にすると、heartbeat RPC を介して SHUTDOWN の確認が返されるまでグレースフルシャットダウン処理は継続され、フロントエンドが通常の2回のハートビート間隔内で終了状態を検出するための十分な時間を確保します。
- 導入バージョン: v3.4.5

##### lake_compaction_stream_buffer_size_bytes

- デフォルト: 1048576
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: 共有データクラスタでのクラウドネイティブテーブルコンパクションのためのリーダーのリモート I/O バッファサイズ。デフォルト値は 1MB です。この値を増やすことでコンパクションプロセスを加速できます。
- 導入バージョン: v3.2.3

##### lake_pk_compaction_max_input_rowsets

- デフォルト: 500
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: 共有データクラスタでの主キーテーブルコンパクションタスクで許可される最大入力 rowset 数。このパラメータのデフォルト値は v3.2.4 および v3.1.10 以降 `5` から `1000` に、v3.3.1 および v3.2.9 以降 `500` に変更されました。主キーテーブルのためのサイズ階層型コンパクションポリシーが有効になった後 (`enable_pk_size_tiered_compaction_strategy` を `true` に設定することで)、StarRocks は各コンパクションの rowset 数を制限して書き込み増幅を減らす必要がなくなります。したがって、このパラメータのデフォルト値は増加しました。
- 導入バージョン: v3.1.8, v3.2.3

##### loop_count_wait_fragments_finish

- デフォルト: 2
- 型: Int
- 単位: -
- 変更可能: Yes
- 説明: BE/CN プロセスが終了する際に待機するループ回数。各ループは固定間隔の 10 秒です。ループ待機を無効にするには `0` に設定できます。v3.4 以降、この項目は変更可能になり、デフォルト値は `0` から `2` に変更されました。
- 導入: v2.5

##### starlet_filesystem_instance_cache_capacity

- デフォルト: 10000
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: starlet filesystem インスタンスのキャッシュ容量。
- 導入バージョン: v3.2.16, v3.3.11, v3.4.1

##### starlet_filesystem_instance_cache_ttl_sec

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: starlet filesystem インスタンス キャッシュの有効期限。
- 導入バージョン: v3.3.15, 3.4.5

##### starlet_port

- デフォルト: 9070
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: BE および CN のための追加のエージェントサービスポート。
- 導入バージョン: -

##### starlet_star_cache_disk_size_percent

- デフォルト: 80
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 共有データクラスタで Data Cache が使用できるディスク容量の割合。
- 導入バージョン: v3.1

##### starlet_use_star_cache

- デフォルト: v3.1 では false、v3.2.3 以降は true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データクラスタで Data Cache を有効にするかどうか。`true` はこの機能を有効にすることを示し、`false` は無効にすることを示します。デフォルト値は v3.2.3 以降、`false` から `true` に設定されました。
- 導入バージョン: v3.1

##### starlet_write_file_with_tag

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データクラスターにおいて、オブジェクトストレージに書き込まれたファイルにオブジェクトストレージタグを付与し、便利なカスタムファイル管理を行うかどうか。
- 導入バージョン: v3.5.3

##### table_schema_service_max_retries

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: Table Schema Service リクエストの最大リトライ回数。
- 導入バージョン: v4.1

### データレイク

##### datacache_block_buffer_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: Data Cache の効率を最適化するために Block Buffer を有効にするかどうか。Block Buffer が有効な場合、システムは Data Cache から Block データを読み取り、一時バッファにキャッシュし、頻繁なキャッシュ読み取りによる余分なオーバーヘッドを削減します。
- 導入バージョン: v3.2.0

##### datacache_disk_adjust_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: Data Cache の自動容量スケーリングの間隔。定期的に、システムはキャッシュディスクの使用状況をチェックし、必要に応じて自動スケーリングをトリガーします。
- 導入バージョン: v3.3.0

##### datacache_disk_idle_seconds_for_expansion

- デフォルト: 7200
- タイプ: Int
- 単位: 秒
- 可変: はい
- 説明: Data Cache の自動拡張のための最小待機時間。ディスク使用率がこの期間を超えて `datacache_disk_low_level` を下回る場合にのみ、自動スケーリングがトリガーされます。
- 導入バージョン: v3.3.0

##### datacache_disk_size

- デフォルト: 0
- タイプ: String
- 単位: -
- 可変: はい
- 説明: 単一ディスクにキャッシュできるデータの最大量。パーセンテージ (例: `80%`) または物理的な制限 (例: `2T`、`500G`) として設定できます。たとえば、2 つのディスクを使用し、`datacache_disk_size` パラメータの値を `21474836480` (20 GB) に設定した場合、これらの 2 つのディスクに最大 40 GB のデータをキャッシュできます。デフォルト値は `0` で、これはメモリのみがデータをキャッシュするために使用されることを示します。
- 導入バージョン: -

##### datacache_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: いいえ
- 説明: Data Cache を有効にするかどうか。`true` は Data Cache が有効であることを示し、`false` は無効であることを示します。デフォルト値は v3.3 から `true` に変更されました。
- 導入バージョン: -

##### datacache_eviction_policy

- デフォルト: slru
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: Data Cache のエビクションポリシー。有効な値: `lru` (最も最近使用されていない) および `slru` (セグメント化された LRU)。
- 導入バージョン: v3.4.0

##### datacache_inline_item_count_limit

- デフォルト: 130172
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: Data Cache のインラインキャッシュアイテムの最大数。特に小さいキャッシュブロックの場合、Data Cache はそれらを `inline` モードで保存し、ブロックデータとメタデータをメモリに一緒にキャッシュします。
- 導入バージョン: v3.4.0

##### datacache_mem_size

- デフォルト: 0
- タイプ: String
- 単位: -
- 可変: はい
- 説明: メモリにキャッシュできるデータの最大量。パーセンテージ (例: `10%`) または物理的な制限 (例: `10G`、`21474836480`) として設定できます。
- 導入バージョン: -

##### datacache_min_disk_quota_for_adjustment

- デフォルト: 10737418240
- タイプ: Int
- 単位: バイト
- 可変: はい
- 説明: Data Cache 自動スケーリングのための最小有効容量。システムがキャッシュ容量をこの値未満に調整しようとする場合、キャッシュ容量は直接 `0` に設定され、キャッシュ容量の不足による頻繁なキャッシュの充填と削除によるパフォーマンスの低下を防ぎます。
- 導入バージョン: v3.3.0

##### disk_high_level

- デフォルト: 90
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: キャッシュ容量の自動スケーリングをトリガーするディスク使用率 (パーセンテージ) の上限。この値を超えると、システムは Data Cache からキャッシュデータを自動的に削除します。v3.4.0 以降、デフォルト値は `80` から `90` に変更されました。この項目はバージョン4.0以降、`datacache_disk_high_level` から `disk_high_level` に名称変更されました。
- 導入バージョン: v3.3.0

##### disk_low_level

- デフォルト: 60
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: キャッシュ容量の自動スケーリングをトリガーするディスク使用率 (パーセンテージ) の下限。ディスク使用率が `datacache_disk_idle_seconds_for_expansion` で指定された期間を超えてこの値を下回り、Data Cache に割り当てられたスペースが完全に利用される場合、システムは上限を増やしてキャッシュ容量を自動的に拡張します。この項目はバージョン4.0以降、`datacache_disk_low_level` から `disk_low_level` に名称変更されました。
- 導入バージョン: v3.3.0

##### disk_safe_level

- デフォルト: 80
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: Data Cache のディスク使用率 (パーセンテージ) の安全レベル。Data Cache が自動スケーリングを実行する際、システムはディスク使用率をこの値にできるだけ近づけることを目標にキャッシュ容量を調整します。v3.4.0 以降、デフォルト値は `70` から `80` に変更されました。この項目はバージョン4.0以降、`datacache_disk_safe_level` から `disk_safe_level` に名称変更されました。
- 導入バージョン: v3.3.0

##### enable_connector_sink_spill

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 外部テーブルへの書き込み時にスピリングを有効化するかどうか。この機能を有効にすると、メモリ不足時に外部テーブルへの書き込みによって大量の小さなファイルが生成されるのを防ぎます。現在、この機能は Iceberg テーブルへの書き込みのみをサポートしています。
- 導入バージョン: v4.0.0

##### enable_datacache_disk_auto_adjust

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: Data Cache ディスク容量の自動スケーリングを有効にするかどうか。これを有効にすると、システムは現在のディスク使用率に基づいてキャッシュ容量を動的に調整します。この項目はバージョン4.0以降、`datacache_auto_adjust_enable` から `enable_datacache_disk_auto_adjust` に名称変更されました。
- 導入バージョン: v3.3.0

##### jdbc_connection_idle_timeout_ms

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 可変: いいえ
- 説明: JDBC 接続プール内のアイドル接続が期限切れになるまでの時間。JDBC 接続プール内の接続アイドル時間がこの値を超えると、接続プールは設定項目 `jdbc_minimum_idle_connections` で指定された数を超えるアイドル接続を閉じます。
- 導入バージョン: -

##### jdbc_connection_pool_size

- デフォルト: 8
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: JDBC 接続プールのサイズ。各 BE ノードで、同じ `jdbc_url` を持つ外部テーブルにアクセスするクエリは同じ接続プールを共有します。
- 導入バージョン: -

##### jdbc_minimum_idle_connections

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: JDBC 接続プール内の最小アイドル接続数。
- 導入バージョン: -

##### lake_clear_corrupted_cache_data

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データクラスタにおいて、システムが破損したデータキャッシュをクリアすることを許可するかどうか。
- 導入バージョン: v3.4

##### lake_clear_corrupted_cache_meta

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データクラスタにおいて、システムが破損したメタデータキャッシュをクリアすることを許可するかどうか。
- 導入バージョン: v3.3

##### lake_enable_vertical_compaction_fill_data_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: 共有データクラスタでコンパクションタスクがローカルディスクにデータをキャッシュすることを許可するかどうか。
- 導入バージョン: v3.1.7, v3.2.3

##### lake_service_max_concurrency

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: 共有データクラスタにおける RPC リクエストの最大同時実行数。このしきい値に達すると、受信リクエストは拒否されます。この項目が `0` に設定されている場合、同時実行数に制限はありません。
- 導入バージョン: -

##### query_max_memory_limit_percent

- デフォルト: 90
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: クエリプールが使用できる最大メモリ。プロセスメモリ制限のパーセンテージとして表されます。
- 導入バージョン: v3.1.0

##### rocksdb_max_write_buffer_memory_bytes

- デフォルト: 1073741824
- タイプ: Int64
- 単位: -
- 変更可能: No
- 説明: RocksDB の meta 用 write buffer の最大サイズです。デフォルトは 1GB です。
- 導入バージョン: v3.5.0

##### rocksdb_write_buffer_memory_percent

- デフォルト: 5
- タイプ: Int64
- 単位: -
- 変更可能: No
- 説明: RocksDB の meta 用 write buffer に割り当てるメモリの割合です。デフォルトはシステムメモリの 5% です。ただし、これに加えて、最終的に算出される write buffer メモリのサイズは 64MB 未満にならず、1G を超えません（rocksdb_max_write_buffer_memory_bytes）。
- 導入バージョン: v3.5.0

### その他

##### default_mv_resource_group_concurrency_limit

- デフォルト: 0
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクの最大同時実行数 (BE ノードごと)。デフォルト値 `0` は制限がないことを示します。
- 導入バージョン: v3.1

##### default_mv_resource_group_cpu_limit

- デフォルト: 1
- タイプ: Int
- 単位: -
- 可変: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで使用できる最大 CPU コア数 (BE ノードごと)。
- 導入バージョン: v3.1

##### default_mv_resource_group_memory_limit

- デフォルト: 0.8
- タイプ: Double
- 単位: 
- 可変: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで使用できる最大メモリ比率 (BE ノードごと)。デフォルト値はメモリの 80% を示します。
- 導入バージョン: v3.1

##### default_mv_resource_group_spill_mem_limit_threshold

- デフォルト: 0.8
- タイプ: Double
- 単位: -
- 可変: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで中間結果のスピリングをトリガーする前のメモリ使用量のしきい値。デフォルト値はメモリの 80% を示します。
- 導入バージョン: v3.1

##### enable_resolve_hostname_to_ip_in_load_error_url

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: `error_urls` デバッグのために、オペレーターがFEハートビートからの元のホスト名を使用するか、環境要件に基づいてIPアドレスへの解決を強制するかを選択できるようにするかどうか。
  - `true`: ホスト名をIPアドレスに変換します。
  - `false` (デフォルト): エラーURLに元のホスト名を保持します。
- 導入バージョン: v4.0.1

##### enable_retry_apply

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効な場合、再試行可能と分類される Tablet の apply 失敗（例えば一時的なメモリ制限エラー）は直ちにタブレットをエラーにマークするのではなく、再試行のために再スケジュールされます。TabletUpdates の再試行経路は次の試行を現在の失敗回数に `retry_apply_interval_second` を乗じてスケジュールし、最大 600s にクランプするため、連続する失敗に伴ってバックオフが大きくなります。明示的に再試行不可なエラー（例えば corruption）は再試行をバイパスして apply プロセスを直ちにエラー状態にします。再試行は全体のタイムアウト／終了条件に達するまで続き、その後 apply はエラー状態になります。これをオフにすると、失敗した apply タスクの自動再スケジュールが無効になり、失敗した apply は再試行なしでエラー状態に移行します。
- 導入バージョン: v3.2.9

##### enable_token_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 可変: はい
- 説明: トークンチェックを有効にするかどうかを制御するブール値。`true` はトークンチェックを有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

##### load_replica_status_check_interval_ms_on_failure

- デフォルト: 2000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 最後のチェック RPC が失敗した場合に、セカンダリレプリカがプライマリレプリカに対して状態を確認する間隔。
- 導入バージョン: 3.5.1

##### load_replica_status_check_interval_ms_on_success

- デフォルト: 15000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 最後のチェック RPC が成功した場合に、セカンダリレプリカがプライマリレプリカに対して状態を確認する間隔。
- 導入バージョン: 3.5.1

##### max_length_for_bitmap_function

- デフォルト: 1000000
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: ビットマップ関数の入力値の最大長。
- 導入バージョン: -

##### max_length_for_to_base64

- デフォルト: 200000
- タイプ: Int
- 単位: バイト
- 可変: いいえ
- 説明: to_base64() 関数の入力値の最大長。
- 導入バージョン: -

##### memory_high_level

- デフォルト: 75
- タイプ: Long
- 単位: Percent
- 変更可能: Yes
- 説明: プロセスのメモリ上限に対する割合で表されるハイウォーターメモリ閾値。総メモリ使用量がこの割合を超えると、BE はメモリ圧力を緩和するために徐々にメモリを解放し始めます（現在はデータキャッシュと更新キャッシュの追い出しで実施）。モニタはこの値を用いて memory_high = mem_limit * memory_high_level / 100 を計算し、消費量が `>` memory_high の場合は GC アドバイザに導かれた制御されたエビクションを行います；消費量が別の設定である memory_urgent_level を超えると、より攻撃的な即時削減が行われます。この値は閾値超過時に一部のメモリ集約的な操作（例えば primary-key preload）を無効化するかどうかの判断にも使われます。memory_urgent_level と合わせた検証を満たす必要があります（memory_urgent_level `>` memory_high_level、memory_high_level `>=` 1、memory_urgent_level `<=` 100）。
- 導入バージョン: v3.2.0

##### report_exec_rpc_request_retry_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: FE に exec RPC リクエストを報告する際の RPC リクエストの再試行回数です。デフォルト値は 10 で、fragment instance finish RPC の場合に限り失敗した際に最大10回再試行されます。Report exec RPC request は load job にとって重要で、もしある fragment instance の finish 報告が失敗すると、load job はタイムアウトするまでハングする可能性があります。
- 導入バージョン: -

##### small_file_dir

- デフォルト: `${STARROCKS_HOME}/lib/small_file/`
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: ファイルマネージャーによってダウンロードされたファイルを保存するために使用されるディレクトリ。
- 導入バージョン: -

##### upload_buffer_size

- デフォルト: 4194304
- タイプ: Int
- 単位: Bytes
- 変更可能: Yes
- 説明: スナップショットファイルをリモートストレージ（broker や直接 FileSystem）へアップロードする際のファイルコピー操作で使用するバッファサイズ（バイト単位）。アップロード経路（snapshot_loader.cpp）では、この値が各アップロードストリームの読み書きチャンクサイズとして fs::copy に渡されます。デフォルトは 4 MiB です。高レイテンシや高帯域のリンクではこの値を増やすことでスループットが向上することがありますが、同時アップロードごとのメモリ使用量が増加します。値を小さくするとストリームごとのメモリは減少しますが転送効率が落ちる可能性があります。upload_worker_count や利用可能な全体メモリと合わせて調整してください。
- 導入バージョン: 3.2.13

##### user_function_dir

- デフォルト: `${STARROCKS_HOME}/lib/udf`
- タイプ: String
- 単位: -
- 可変: いいえ
- 説明: ユーザー定義関数 (UDF) を保存するために使用されるディレクトリ。
- 導入バージョン: -

