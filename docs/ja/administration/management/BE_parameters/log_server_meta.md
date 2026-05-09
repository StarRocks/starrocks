---
displayed_sidebar: docs
sidebar_label: "ログ、サーバー、およびメタデータ"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 設定 - ログ、サーバー、およびメタデータ

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```SQL
SELECT * FROM information_schema.be_configs [WHERE NAME LIKE "%<name_pattern>%"]
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

---

このトピックでは、以下の種類のFE構成について紹介します：
- [ログ](#ログ)
- [サーバー](#サーバー)
- [メタデータとクラスター管理](#メタデータとクラスター管理)

## ログ

### diagnose_stack_trace_interval_ms

- デフォルト: 1800000 (30 minutes)
- タイプ: long
- 単位: Milliseconds
- 変更可能: Yes
- 説明: DiagnoseDaemon が `STACK_TRACE` リクエストに対して行う連続したスタックトレース診断の最小時間間隔を制御します。診断リクエストが到着したとき、最後の収集が `diagnose_stack_trace_interval_ms` ミリ秒未満であれば、デーモンはスタックトレースの収集およびログ出力をスキップします。頻繁なスタックダンプによる CPU 負荷やログ量を減らすためにこの値を大きくし、短期間の問題をデバッグするためにより頻繁なトレースを取得したい場合（例えば TabletsChannel::add_chunk が長時間ブロックするロードのフェイルポイントシミュレーションなど）には値を小さくしてください。
- 導入バージョン: v3.5.0

### log_buffer_level

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ログをフラッシュするための戦略。デフォルト値は、ログがメモリにバッファリングされることを示します。有効な値は `-1` と `0` です。`-1` は、ログがメモリにバッファリングされないことを示します。
- 導入バージョン: -

### sys_log_dir

- デフォルト: `${STARROCKS_HOME}/log`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログ (INFO、WARNING、ERROR、FATAL を含む) を保存するディレクトリ。
- 導入バージョン: -

### sys_log_level

- デフォルト: INFO
- タイプ: String
- 単位: -
- 変更可能: はい (v3.3.0、v3.2.7、v3.1.12 から)
- 説明: システムログエントリが分類される重大度レベル。 有効な値: INFO、WARNING、ERROR、FATAL。この項目は v3.3.0、v3.2.7、v3.1.12 以降、動的設定に変更されました。
- 導入バージョン: -

### sys_log_roll_mode

- デフォルト: SIZE-MB-1024
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログがログロールに分割されるモード。有効な値には `TIME-DAY`、`TIME-HOUR`、および `SIZE-MB-` サイズが含まれます。デフォルト値は、各ロールが 1 GB であるログロールに分割されることを示します。
- 導入バージョン: -

### sys_log_roll_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 保持するログロールの数。
- 導入バージョン: -

### sys_log_timezone

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: ログプレフィックスにタイムゾーン情報を表示するかどうか。`true` はタイムゾーン情報を表示することを示し、`false` は表示しないことを示します。
- 導入バージョン: -

### sys_log_verbose_level

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 印刷するログのレベル。この設定項目は、コード内で VLOG で開始されたログの出力を制御するために使用されます。
- 導入バージョン: -

### sys_log_verbose_modules

- デフォルト: 
- タイプ: Strings
- 単位: -
- 変更可能: いいえ
- 説明: VLOGログを出力するファイル名（拡張子を除く）またはファイル名のワイルドカードを指定します。複数のファイル名はカンマで区切ることができます。たとえば、この設定項目を `storage_engine,tablet_manager` に設定すると、StarRocks は storage_engine.cpp および tablet_manager.cpp ファイルの VLOG ログを出力します。ワイルドカードも使用可能で、`*` に設定するとすべてのファイルの VLOG ログを出力します。VLOG ログの出力レベルは `sys_log_verbose_level` パラメータで制御されます。
- 導入バージョン: -

## サーバー

### abort_on_large_memory_allocation

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: Yes
- 説明: 単一の割り当て要求が設定された large-allocation 閾値を超えた場合（g_large_memory_alloc_failure_threshold > 0 かつ 要求サイズ > 閾値）、プロセスがどのように応答するかを制御します。true の場合、こうした大きな割り当てが検出されると直ちに std::abort() を呼び出して（ハードクラッシュ）終了します。false の場合は割り当てがブロックされ、アロケータは失敗（nullptr または ENOMEM）を返すため、呼び出し元がエラーを処理できます。このチェックは TRY_CATCH_BAD_ALLOC パスでラップされていない割り当てに対してのみ有効です（mem hook は bad-alloc を捕捉している場合に別のフローを使用します）。予期しない巨大な割り当ての fail-fast デバッグ目的で有効にしてください。運用環境では、過大な割り当て試行で即時プロセス中断を望む場合を除き無効のままにしてください。
- 導入バージョン: 3.4.3, 3.5.0, 4.0.0

### arrow_flight_port

- デフォルト: -1
- タイプ: Int
- 単位: Port
- 変更可能: いいえ
- 説明: BE の Arrow Flight SQL サーバー用の TCP ポート。Arrow Flight サービスを無効化するには `-1` に設定します。macOS 以外のビルドでは、BE は起動時に Arrow Flight SQL Server を呼び出します。ポートが利用できない場合、サーバーの起動は失敗し BE プロセスは終了します。設定されたポートは HeartBeat Payload で FE に報告されます。BE を起動する前に `be.conf` でこの値を設定してください。
- 導入バージョン: v3.4.0, v3.5.0

### be_exit_after_disk_write_hang_second

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ディスクがハングした後、BE が終了するまでの待機時間。
- 導入バージョン: -

### be_http_num_workers

- デフォルト: 48
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: HTTP サーバーが使用するスレッドの数。
- 導入バージョン: -

### be_http_port

- デフォルト: 8040
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE HTTP サーバーポート。
- 導入バージョン: -

### be_port

- デフォルト: 9060
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE の thrift サーバーポートで、FEs からのリクエストを受け取るために使用されます。
- 導入バージョン: -

### brpc_max_body_size

- デフォルト: 2147483648
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: bRPC の最大ボディサイズ。
- 導入バージョン: -

### brpc_max_connections_per_server

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クライアントが各リモートサーバーエンドポイントごとに保持する永続的な bRPC 接続の最大数。各エンドポイントについて `BrpcStubCache` は `_stubs` ベクタをこのサイズに予約した `StubPool` を作成します。最初のアクセス時には制限に達するまで新しい stub が作成され、その後は既存の stub がラウンドロビン方式で返されます。この値を増やすとエンドポイントごとの並列性が高まり（単一チャネルでの競合が減る）、その代わりにファイルディスクリプタ、メモリ、チャネルが増えます。
- 導入バージョン: v3.2.0

### brpc_num_threads

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: bRPC の bthreads の数。値 `-1` は CPU スレッドと同じ数を示します。
- 導入バージョン: -

### brpc_port

- デフォルト: 8060
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE bRPC ポートで、bRPC のネットワーク統計を表示するために使用されます。
- 導入バージョン: -

### brpc_stub_expire_s

- デフォルト: 3600
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: BRPC stub キャッシュの有効期限。デフォルトは60分です。
- 導入バージョン: -

### compress_rowbatches

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: BE 間の RPC で行バッチを圧縮するかどうかを制御するブール値です。`true` は行バッチを圧縮することを示し、`false` は圧縮しないことを示します。
- 導入バージョン: -

### delete_worker_count_normal_priority

- デフォルト: 2
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: BE エージェント上で delete (REALTIME_PUSH with DELETE) タスクを処理するために割り当てられる通常優先度のワーカースレッド数。起動時にこの値は delete_worker_count_high_priority に加算されて DeleteTaskWorkerPool のサイズ決定に使われます（agent_server.cpp を参照）。プールは最初の delete_worker_count_high_priority スレッドを HIGH 優先度として割り当て、残りを NORMAL とします。通常優先度スレッドは標準の delete タスクを処理し、全体の削除スループットに寄与します。並行削除容量を上げるには増やしてください（CPU/IO 使用量が増加します）。リソース競合を減らすには減らしてください。
- 導入バージョン: v3.2.0

### enable_https

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: この項目が `true` に設定されると、BE の bRPC サーバは TLS を使用するように構成されます: `ServerOptions.ssl_options` は BE 起動時に `ssl_certificate_path` と `ssl_private_key_path` で指定された証明書と秘密鍵で設定されます。これにより受信 bRPC 接続に対して HTTPS/TLS が有効になり、クライアントは TLS を用いて接続する必要があります。証明書および鍵ファイルが存在し、BE プロセスからアクセス可能であり、bRPC/SSL の要件に合致していることを確認してください。
- 導入バージョン: v4.0.0

### enable_jemalloc_memory_tracker

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: この項目が `true` に設定されていると、BE はバックグラウンドスレッド（jemalloc_tracker_daemon）を起動し、jemalloc の統計を1秒ごとにポーリングして、jemalloc の "stats.metadata" 値で GlobalEnv の jemalloc メタデータ MemTracker を更新します。これにより jemalloc のメタデータ消費が StarRocks プロセスのメモリ集計に含まれ、jemalloc 内部により使用されるメモリの過小報告を防ぎます。トラッカーは macOS 以外のビルド（#ifndef __APPLE__）でのみコンパイル/起動され、"jemalloc_tracker_daemon" という名前のデーモンスレッドとして動作します。この設定は起動時の振る舞いや MemTracker の状態を維持するスレッドに影響するため、変更には再起動が必要です。jemalloc を使用していない場合、または jemalloc のトラッキングを別途意図的に管理している場合のみ無効にし、それ以外は正確なメモリ集計と割り当て保護を維持するために有効のままにしてください。
- 導入バージョン: v3.2.12

### enable_jvm_metrics

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: StarRocks が起動時に JVM 固有のメトリクスを初期化して登録するかどうかを制御します。値は Daemon::init 内の init_starrocks_metrics で読み取られ、StarRocksMetrics::initialize の init_jvm_metrics パラメータとして渡されます。有効にするとメトリクスサブシステムは JVM 関連のコレクタ（例: heap、GC、thread メトリクス）を作成してエクスポートし、無効の場合はそれらのコレクタは初期化されません。このフラグは起動時にのみ評価され、ランタイム中に変更することはできません。前方互換性のための設定であり、将来のリリースで削除される可能性があります。システムレベルのメトリクス収集は `enable_system_metrics` を使用して制御してください。
- 導入バージョン: v4.0.0

### get_pindex_worker_count

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: UpdateManager の "get_pindex" スレッドプールのワーカースレッド数を設定します。このプールは永続インデックスデータをロード／取得するために使用され（主キー表の rowset 適用時に使用）、実行時の設定更新はプールの最大スレッド数を調整します：`>0` の場合はその値が適用され、0 の場合はランタイムコールバックが CPU コア数（`CpuInfo::num_cores()`）を使用します。初期化時にはプールの最大スレッド数は max(get_pindex_worker_count, max_apply_thread_cnt * 2) として計算され、ここで max_apply_thread_cnt は apply-thread プールの最大値です。pindex ロードの並列度を上げるには増やし、同時実行性とメモリ／CPU 使用量を減らすには減らしてください。
- 導入バージョン: v3.2.0

### heartbeat_service_port

- デフォルト: 9050
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE ハートビートサービスポートで、FEs からのハートビートを受け取るために使用されます。
- 導入バージョン: -

### heartbeat_service_thread_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE ハートビートサービスのスレッド数。
- 導入バージョン: -

### local_library_dir

- デフォルト: `${UDF_RUNTIME_DIR}`
- タイプ: string
- 単位: -
- 変更可能: No
- 説明: BE 上のローカルディレクトリで、UDF（ユーザー定義関数）ライブラリが配置され、Python UDF ワーカープロセスが動作する場所です。StarRocks は HDFS からこのパスへ UDF ライブラリをコピーし、各ワーカー用の Unix ドメインソケットを `<local_library_dir>/pyworker_<pid>` に作成し、Python ワーカープロセスを exec する前にこのディレクトリへ chdir します。ディレクトリは存在し、BE プロセスが書き込み可能であり、Unix ドメインソケットをサポートするファイルシステム（つまりローカルファイルシステム）上にある必要があります。この設定はランタイムで変更不可能なため、起動前に設定し、各 BE 上で十分な権限とディスク容量を確保してください。
- 導入バージョン: v3.2.0

### mem_limit

- デフォルト: 90%
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: BE プロセスのメモリ上限。パーセンテージ ("80%") または物理的な制限 ("100G") として設定できます。デフォルトのハードリミットはサーバーのメモリサイズの 90% で、ソフトリミットは 80% です。同じサーバーで他のメモリ集約型サービスと一緒に StarRocks をデプロイしたい場合、このパラメータを設定する必要があります。
- 導入バージョン: -

### memory_urgent_level

- デフォルト: 85
- タイプ: long
- 単位: Percentage (0-100)
- 変更可能: はい
- 説明: プロセスのメモリ上限に対するパーセンテージで表現される緊急メモリ水位。プロセスのメモリ使用量が `(limit * memory_urgent_level / 100)` を超えると、BE は即時のメモリ回収をトリガーします。これによりデータキャッシュの縮小、update キャッシュの追い出しが行われ、persistent/lake の MemTable は「満杯」と見なされて早期にフラッシュ／コンパクションされます。コードではこの設定が `memory_high_level` より大きく、`memory_high_level` は `1` 以上かつ `100` 以下であることを検証します。値を低くするとより積極的で早期の回収（頻繁なキャッシュ追い出しとフラッシュ）を招きます。値を高くすると回収が遅れ、100 に近すぎると OOM のリスクが高まります。`memory_high_level` および Data Cache 関連の自動調整設定と合わせてチューニングしてください。
- 導入バージョン: v3.2.0

### net_use_ipv6_when_priority_networks_empty

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: `priority_networks` が指定されていない場合に IPv6 アドレスを優先的に使用するかどうかを制御するブール値です。`true` は、ノードをホストするサーバーが IPv4 と IPv6 の両方のアドレスを持ち、`priority_networks` が指定されていない場合に、システムが IPv6 アドレスを優先的に使用することを許可することを示します。
- 導入バージョン: v3.3.0

### num_cores

- デフォルト: 0
- タイプ: Int
- 単位: Cores
- 変更可能: No
- 説明: StarRocks が CPU に依存する判断（スレッドプールのサイズ設定やランタイムスケジューリングなど）で使用する CPU コア数を制御します。値が 0 の場合は自動検出が有効になり、StarRocks は /proc/cpuinfo を読み取り、利用可能な全コアを使用します。正の整数 (> 0) に設定すると、その値が CpuInfo::init 内で検出されたコア数を上書きして有効なコア数になります。コンテナ内で実行している場合、cgroup の cpuset や CPU クォータ設定によって使用可能なコアがさらに制限されることがあり、CpuInfo はそれらの cgroup 制限も尊重します。この設定は起動時にのみ適用され、変更するにはサーバーの再起動が必要です。
- 導入バージョン: v3.2.0

### plugin_path

- デフォルト: `${STARROCKS_HOME}/plugin`
- タイプ: string
- 単位: Path
- 変更可能: No
- 説明: StarRocks が外部プラグイン（動的ライブラリ、コネクタアーティファクト、UDF バイナリなど）をロードするファイルシステム上のディレクトリ。`plugin_path` は BE プロセスからアクセス可能なディレクトリ（読み取りおよび実行権限）を指し、プラグインがロードされる前に存在している必要があります。テストコード（be/src/testutil/init_config.h）は起動時にこのディレクトリを作成します。`plugin_path` を変更した場合は、新しいパスを反映させるためにプロセスを再起動する必要があります。所有権が正しいこと、プラグインファイルがプラットフォームのネイティブなバイナリ拡張子（例：Linux の .so）を使用していることを確認してください。
- 導入バージョン: v3.2.0

### priority_networks

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 複数の IP アドレスを持つサーバーの選択戦略を宣言します。注意すべき点は、このパラメータで指定されたリストと一致する IP アドレスは最大で 1 つでなければなりません。このパラメータの値は、CIDR 表記でセミコロン (;) で区切られたエントリからなるリストです。例: `10.10.10.0/24`。このリストのエントリと一致する IP アドレスがない場合、サーバーの利用可能な IP アドレスがランダムに選択されます。v3.3.0 から、StarRocks は IPv6 に基づくデプロイをサポートしています。サーバーが IPv4 と IPv6 の両方のアドレスを持っている場合、このパラメータが指定されていない場合、システムはデフォルトで IPv4 アドレスを使用します。この動作を変更するには、`net_use_ipv6_when_priority_networks_empty` を `true` に設定します。
- 導入バージョン: -

### ssl_private_key_path

- デフォルト: An empty string
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: BE の brpc サーバがデフォルト証明書のプライベートキーとして使用する TLS/SSL プライベートキー（PEM）のファイルシステムパス。`enable_https` が `true` に設定されていると、プロセス起動時に `brpc::ServerOptions::ssl_options().default_cert.private_key` がこのパスに設定されます。ファイルは BE プロセスからアクセス可能であり、`ssl_certificate_path` で指定した証明書と一致している必要があります。この値が設定されていないか、ファイルが存在しないまたはアクセスできない場合、HTTPS は構成されず bRPC サーバが起動に失敗する可能性があります。このファイルは制限付きのファイルシステム権限（例: 600）で保護してください。
- 導入バージョン: v4.0.0

### thrift_client_retry_interval_ms

- デフォルト: 100
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: thrift クライアントがリトライする時間間隔。
- 導入バージョン: -

### thrift_connect_timeout_seconds

- デフォルト: 3
- タイプ: Int
- 単位: Seconds
- 変更可能: No
- 説明: Thrift クライアントを作成する際に使用される接続タイムアウト（秒）。ClientCacheHelper::_create_client はこの値に 1000 を掛けて ThriftClientImpl::set_conn_timeout() に渡すため、BE クライアントキャッシュによってオープンされる新しい Thrift 接続の TCP/接続ハンドシェイクのタイムアウトを制御します。この設定は接続確立にのみ影響し、送受信タイムアウトは別途設定されます。非常に小さい値は高レイテンシのネットワークで誤検知による接続失敗を引き起こす可能性があり、大きすぎる値は到達不能なピアの検出を遅らせます。
- 導入バージョン: v3.2.0

### thrift_port

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: 内部の Thrift ベースの BackendService を公開するために使用するポート。プロセスが Compute Node として動作しており、この項目が非ゼロに設定されている場合、`be_port` をオーバーライドして Thrift サーバはこの値にバインドします。そうでない場合は `be_port` が使用されます。この設定は非推奨です — 非ゼロの `thrift_port` を設定すると、代わりに `be_port` を使用するよう警告がログに記録されます。
- 導入バージョン: v3.2.0

### thrift_rpc_connection_max_valid_time_ms

- デフォルト: 5000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: いいえ
- 説明: Thrift RPC 接続の最大有効時間。コネクションプールにこの値以上存在すると、コネクションは閉じられます。この値は FE 設定 `thrift_client_timeout_ms` と一致するように設定する必要があります。
- 導入バージョン: -

### thrift_rpc_max_body_size

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: RPC の文字列ボディの最大サイズ。`0` は無制限であることを示す。
- 導入バージョン: -

### thrift_rpc_strict_mode

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Thrift の Strict 実行モードが有効かどうか。Thrift の Strict モードについては、[Thrift Binary protocol encoding](https://github.com/apache/thrift/blob/master/doc/specs/thrift-binary-protocol.md) を参照してください。
- 導入バージョン: -

### thrift_rpc_timeout_ms

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: thrift RPC のタイムアウト。
- 導入バージョン: -

### transaction_apply_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: BE の UpdateManager にある "update_apply" スレッドプール（主キーテーブルの rowset を適用するプール）の最小スレッド数を設定します。値が 0 の場合は固定の最小値が無効（下限なし）となります。`transaction_apply_worker_count` も 0 のときはプールの最大スレッド数はデフォルトで CPU コア数になり、実効的なワーカー数は CPU コア数と等しくなります。トランザクション適用時のベースラインの並行度を保証するために増やすことができますが、あまり高く設定すると CPU 競合が増える可能性があります。変更は update_config HTTP ハンドラを通じてランタイムで適用されます（apply スレッドプールの update_min_threads を呼び出します）。
- 導入バージョン: v3.2.11

### transaction_publish_version_thread_pool_num_min

- デフォルト: 0
- タイプ: Int
- 単位: Threads
- 変更可能: Yes
- 説明: AgentServer の "publish_version" 動的スレッドプール（トランザクションバージョンの公開 / TTaskType::PUBLISH_VERSION タスクの処理に使用）で確保される最小スレッド数を設定します。起動時、プールは min = max(config value, MIN_TRANSACTION_PUBLISH_WORKER_COUNT) (MIN_TRANSACTION_PUBLISH_WORKER_COUNT = 1) で作成されるため、デフォルトの 0 は最小で 1 スレッドになります。ランタイムでこの値を変更すると更新コールバックが呼び出され ThreadPool::update_min_threads を実行し、プールの保証最小数を増減します（ただし強制される最小値 1 を下回りません）。transaction_publish_version_worker_count（最大スレッド）および transaction_publish_version_thread_pool_idle_time_ms（アイドルタイムアウト）と調整してください。
- 導入バージョン: v3.2.11

### ssl_certificate_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: `enable_https` が true のときに BE の brpc サーバが使用する TLS/SSL 証明書ファイル（PEM）への絶対パス。BE 起動時にこの値は `brpc::ServerOptions::ssl_options().default_cert.certificate` にコピーされます。対応する秘密鍵は必ず `ssl_private_key_path` に設定してください。必要に応じてサーバ証明書および中間証明書を PEM 形式（証明書チェーン）で提供してください。ファイルは StarRocks BE プロセスから読み取り可能でなければならず、起動時にのみ適用されます。`enable_https` が有効でこの値が未設定または無効な場合、brpc の TLS 設定が失敗しサーバが正しく起動できない可能性があります。
- 導入バージョン: v4.0.0

## メタデータとクラスター管理

### cluster_id

- デフォルト: -1
- タイプ: Int
- 単位: N/A
- 変更可能: No
- 説明: この StarRocks backend のグローバルクラスタ識別子。起動時に StorageEngine は config::cluster_id を実効クラスタ ID として読み取り、すべての data root パスが同じクラスタ ID を含んでいることを検証します（StorageEngine::_check_all_root_path_cluster_id を参照）。値が -1 の場合は「未設定」を意味し、エンジンは既存のデータディレクトリまたはマスターのハートビートから実効 ID を導出することがあります。非負の ID が設定されている場合、設定された ID とデータディレクトリに格納されている ID の不一致は起動時の検証に失敗を引き起こします（Status::Corruption）。一部の root に ID が欠けており、エンジンが ID の書き込みを許可されている場合（options.need_write_cluster_id）、それらの root に実効 ID を永続化します。この設定は不変であるため、変更するには異なる設定でプロセスを再起動する必要があります。
- 導入バージョン: 3.2.0

### consistency_max_memory_limit

- デフォルト: 10G
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: CONSISTENCY メモリトラッカー用のメモリサイズ指定。
- 導入バージョン: v3.2.0

### retry_apply_interval_second

- デフォルト: 30
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: 失敗した tablet apply 操作の再試行をスケジュールする際に使用される基本間隔（秒）。サブミッション失敗後の再試行を直接スケジュールするために使用されるほか、バックオフの基礎乗数としても使用されます：次の再試行遅延は min(600, `retry_apply_interval_second` * failed_attempts) として計算されます。コードはまた累積再試行時間（等差数列の和）を計算するために `retry_apply_interval_second` を使用し、その値を `retry_apply_timeout_second` と比較して再試行を継続するか判断します。`enable_retry_apply` が true の場合にのみ有効です。この値を増やすと個々の再試行遅延および累積の再試行時間が長くなり、減らすと再試行がより頻繁になり `retry_apply_timeout_second` に達する前に試行回数が増える可能性があります。
- 導入バージョン: v3.2.9

### update_schema_worker_count

- デフォルト: 3
- タイプ: Int
- 単位: Threads
- 変更可能: No
- 説明: BE の "update_schema" 動的 ThreadPool で TTaskType::UPDATE_SCHEMA タスクを処理するワーカースレッドの最大数を設定します。ThreadPool は起動時に agent_server 内で作成され、最小 0 スレッド（アイドル時にゼロまでスケールダウン可能）、最大はこの設定値と等しくなります。プールはデフォルトのアイドルタイムアウトと事実上無制限のキューを使用します。より多くの同時スキーマ更新タスクを許可するにはこの値を増やします（CPU とメモリ使用量が増加します）。並列スキーマ操作を制限したい場合は値を下げます。このオプションはランタイムで変更できないため、変更には BE の再起動が必要です。
- 導入バージョン: 3.2.3

### update_tablet_meta_info_worker_count

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: BE が tablet のメタデータ更新タスクを処理する動的スレッドプールの最大ワーカースレッド数を設定します。スレッドプールは起動時に作成され、最小 0 スレッド（アイドル時にゼロまでスケールダウン可能）、最大はこの設定値（最小 1 にクランプ）となります。ランタイムでこの値を変更するとスレッドプールの最大スレッド数が更新されます。並列度を上げたい場合は値を増やし、制限したい場合は下げてください。
- 導入バージョン: v4.1.0, v4.0.6, v3.5.13
