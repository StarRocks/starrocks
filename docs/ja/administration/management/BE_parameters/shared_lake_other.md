---
displayed_sidebar: docs
sidebar_label: "共有データ、データレイク、その他"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 設定 - 共有データ、データレイク、その他

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

このトピックでは、以下の種類のBE構成について紹介します：
- [共有データ](#共有データ)
- [データレイク](#データレイク)
- [その他](#その他)

## 共有データ

### cloud_native_pk_index_rebuild_files_threshold

- デフォルト: 50
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: クラウドネイティブ主キーインデックスのリビルド時に許容される最大 Segment ファイル数。リビルドが必要なファイル数がこの閾値を超えた場合、StarRocks はメモリ内の MemTable を即座にフラッシュし、リプレイが必要な Segment 数を削減します。`0` に設定するとこの早期フラッシュ戦略は無効になります。
- 導入バージョン: -

### cloud_native_pk_index_rebuild_rows_threshold

- デフォルト: 10000000
- タイプ: Long
- 単位: 行
- 変更可能: Yes
- 説明: クラウドネイティブ主キーインデックスのリビルド時に許容される最大行数。リビルドが必要な行数がこの閾値を超えた場合、StarRocks はメモリ内の MemTable を即座にフラッシュし、インデックス再構築のコストを削減します。`0` に設定するとこの早期フラッシュ戦略は無効になります。`cloud_native_pk_index_rebuild_files_threshold` と連携して動作し、いずれかの閾値を超えるとフラッシュがトリガーされます。
- 導入バージョン: -

### download_buffer_size

- デフォルト: 4194304
- タイプ: Int
- 単位: Bytes
- 変更可能: Yes
- 説明: スナップショットファイルをダウンロードする際に使用されるメモリ内コピー用バッファのサイズ（バイト）。SnapshotLoader::download はこの値を fs::copy に対してリモートの sequential file からローカルの writable file へ読み込む際の 1 回あたりのチャンクサイズとして渡します。帯域幅の大きいリンクでは、より大きな値にすることで syscall/IO オーバーヘッドが減りスループットが向上する可能性があります。小さい値はアクティブな転送ごとのピークメモリ使用量を削減します。注意: このパラメータはストリームごとのバッファサイズを制御するものであり、ダウンロードスレッド数を制御するものではありません — 総メモリ消費量 = download_buffer_size * number_of_concurrent_downloads です。
- 導入バージョン: v3.2.13

### graceful_exit_wait_for_frontend_heartbeat

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: グレースフルシャットダウンを完了する前に、少なくとも1件のフロントエンドからの heartbeat 応答で SHUTDOWN 状態が返されるのを待つかどうかを決定します。有効にすると、heartbeat RPC を介して SHUTDOWN の確認が返されるまでグレースフルシャットダウン処理は継続され、フロントエンドが通常の2回のハートビート間隔内で終了状態を検出するための十分な時間を確保します。
- 導入バージョン: v3.4.5

### lake_compaction_stream_buffer_size_bytes

- デフォルト: 1048576
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 共有データクラスタでのクラウドネイティブテーブルコンパクションのためのリーダーのリモート I/O バッファサイズ。デフォルト値は 1MB です。この値を増やすことでコンパクションプロセスを加速できます。
- 導入バージョン: v3.2.3

### lake_pk_compaction_max_input_rowsets

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでの主キーテーブルコンパクションタスクで許可される最大入力 rowset 数。このパラメータのデフォルト値は v3.2.4 および v3.1.10 以降 `5` から `1000` に、v3.3.1 および v3.2.9 以降 `500` に変更されました。主キーテーブルのためのサイズ階層型コンパクションポリシーが有効になった後 (`enable_pk_size_tiered_compaction_strategy` を `true` に設定することで)、StarRocks は各コンパクションの rowset 数を制限して書き込み増幅を減らす必要がなくなります。したがって、このパラメータのデフォルト値は増加しました。
- 導入バージョン: v3.1.8, v3.2.3

### loop_count_wait_fragments_finish

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: BE/CN プロセスが終了する際に待機するループ回数。各ループは固定間隔の 10 秒です。ループ待機を無効にするには `0` に設定できます。v3.4 以降、この項目は変更可能になり、デフォルト値は `0` から `2` に変更されました。
- 導入バージョン: v2.5

### starlet_filesystem_instance_cache_capacity

- デフォルト: 10000
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: starlet filesystem インスタンスのキャッシュ容量。
- 導入バージョン: v3.2.16, v3.3.11, v3.4.1

### starlet_filesystem_instance_cache_ttl_sec

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: starlet filesystem インスタンス キャッシュの有効期限。
- 導入バージョン: v3.3.15, 3.4.5

### starlet_port

- デフォルト: 9070
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: BE および CN のための追加のエージェントサービスポート。
- 導入バージョン: -

### starlet_star_cache_disk_size_percent

- デフォルト: 80
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 共有データクラスタで Data Cache が使用できるディスク容量の割合。`datacache_unified_instance_enable` が `false` の場合のみ有効です。
- 導入バージョン: v3.1

### starlet_use_star_cache

- デフォルト: v3.1 では false、v3.2.3 以降は true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタで Data Cache を有効にするかどうか。`true` はこの機能を有効にすることを示し、`false` は無効にすることを示します。デフォルト値は v3.2.3 以降、`false` から `true` に設定されました。
- 導入バージョン: v3.1

### starlet_write_file_with_tag

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターにおいて、オブジェクトストレージに書き込まれたファイルにオブジェクトストレージタグを付与し、便利なカスタムファイル管理を行うかどうか。
- 導入バージョン: v3.5.3

### table_schema_service_max_retries

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: Table Schema Service リクエストの最大リトライ回数。
- 導入バージョン: v4.1

## データレイク

### datacache_block_buffer_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Data Cache の効率を最適化するために Block Buffer を有効にするかどうか。Block Buffer が有効な場合、システムは Data Cache から Block データを読み取り、一時バッファにキャッシュし、頻繁なキャッシュ読み取りによる余分なオーバーヘッドを削減します。
- 導入バージョン: v3.2.0

### datacache_disk_adjust_interval_seconds

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Data Cache の自動容量スケーリングの間隔。定期的に、システムはキャッシュディスクの使用状況をチェックし、必要に応じて自動スケーリングをトリガーします。
- 導入バージョン: v3.3.0

### datacache_disk_idle_seconds_for_expansion

- デフォルト: 7200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Data Cache の自動拡張のための最小待機時間。ディスク使用率がこの期間を超えて `datacache_disk_low_level` を下回る場合にのみ、自動スケーリングがトリガーされます。
- 導入バージョン: v3.3.0

### datacache_disk_size

- デフォルト: 0
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 単一ディスクにキャッシュできるデータの最大量。パーセンテージ (例: `80%`) または物理的な制限 (例: `2T`、`500G`) として設定できます。たとえば、2 つのディスクを使用し、`datacache_disk_size` パラメータの値を `21474836480` (20 GB) に設定した場合、これらの 2 つのディスクに最大 40 GB のデータをキャッシュできます。デフォルト値は `0` で、これはメモリのみがデータをキャッシュするために使用されることを示します。
- 導入バージョン: -

### datacache_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Data Cache を有効にするかどうか。`true` は Data Cache が有効であることを示し、`false` は無効であることを示します。デフォルト値は v3.3 から `true` に変更されました。
- 導入バージョン: -

### datacache_eviction_policy

- デフォルト: slru
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Data Cache のエビクションポリシー。有効な値: `lru` (最も最近使用されていない) および `slru` (セグメント化された LRU)。
- 導入バージョン: v3.4.0

### datacache_inline_item_count_limit

- デフォルト: 130172
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Data Cache のインラインキャッシュアイテムの最大数。特に小さいキャッシュブロックの場合、Data Cache はそれらを `inline` モードで保存し、ブロックデータとメタデータをメモリに一緒にキャッシュします。
- 導入バージョン: v3.4.0

### datacache_mem_size

- デフォルト: 0
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: メモリにキャッシュできるデータの最大量。パーセンテージ (例: `10%`) または物理的な制限 (例: `10G`、`21474836480`) として設定できます。
- 導入バージョン: -

### datacache_min_disk_quota_for_adjustment

- デフォルト: 10737418240
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: Data Cache 自動スケーリングのための最小有効容量。システムがキャッシュ容量をこの値未満に調整しようとする場合、キャッシュ容量は直接 `0` に設定され、キャッシュ容量の不足による頻繁なキャッシュの充填と削除によるパフォーマンスの低下を防ぎます。
- 導入バージョン: v3.3.0

### disk_high_level

- デフォルト: 90
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: キャッシュ容量の自動スケーリングをトリガーするディスク使用率 (パーセンテージ) の上限。この値を超えると、システムは Data Cache からキャッシュデータを自動的に削除します。v3.4.0 以降、デフォルト値は `80` から `90` に変更されました。この項目はバージョン4.0以降、`datacache_disk_high_level` から `disk_high_level` に名称変更されました。
- 導入バージョン: v3.3.0

### disk_low_level

- デフォルト: 60
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: キャッシュ容量の自動スケーリングをトリガーするディスク使用率 (パーセンテージ) の下限。ディスク使用率が `datacache_disk_idle_seconds_for_expansion` で指定された期間を超えてこの値を下回り、Data Cache に割り当てられたスペースが完全に利用される場合、システムは上限を増やしてキャッシュ容量を自動的に拡張します。この項目はバージョン4.0以降、`datacache_disk_low_level` から `disk_low_level` に名称変更されました。
- 導入バージョン: v3.3.0

### disk_safe_level

- デフォルト: 80
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: Data Cache のディスク使用率 (パーセンテージ) の安全レベル。Data Cache が自動スケーリングを実行する際、システムはディスク使用率をこの値にできるだけ近づけることを目標にキャッシュ容量を調整します。v3.4.0 以降、デフォルト値は `70` から `80` に変更されました。この項目はバージョン4.0以降、`datacache_disk_safe_level` から `disk_safe_level` に名称変更されました。
- 導入バージョン: v3.3.0

### enable_connector_sink_spill

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 外部テーブルへの書き込み時にスピリングを有効化するかどうか。この機能を有効にすると、メモリ不足時に外部テーブルへの書き込みによって大量の小さなファイルが生成されるのを防ぎます。現在、この機能は Iceberg テーブルへの書き込みのみをサポートしています。
- 導入バージョン: v4.0.0

### enable_datacache_disk_auto_adjust

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Data Cache ディスク容量の自動スケーリングを有効にするかどうか。これを有効にすると、システムは現在のディスク使用率に基づいてキャッシュ容量を動的に調整します。この項目はバージョン4.0以降、`datacache_auto_adjust_enable` から `enable_datacache_disk_auto_adjust` に名称変更されました。
- 導入バージョン: v3.3.0

### datacache_unified_instance_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: 共有データクラスタで、internal catalog と external catalog のデータキャッシュを統一された Data Cache インスタンスで管理するかどうか。
- 導入バージョン: v3.4.0

### jdbc_connection_idle_timeout_ms

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBC 接続プール内のアイドル接続が期限切れになるまでの時間。JDBC 接続プール内の接続アイドル時間がこの値を超えると、接続プールは設定項目 `jdbc_minimum_idle_connections` で指定された数を超えるアイドル接続を閉じます。
- 導入バージョン: -

### jdbc_connection_pool_size

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBC 接続プールのサイズ。各 BE ノードで、同じ `jdbc_url` を持つ外部テーブルにアクセスするクエリは同じ接続プールを共有します。
- 導入バージョン: -

### jdbc_minimum_idle_connections

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBC 接続プール内の最小アイドル接続数。
- 導入バージョン: -

### jdbc_connection_max_lifetime_ms

- デフォルト: 300000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBC接続プール内の接続の最大有効期間。古い接続を防ぐため、このタイムアウトの前に接続はリサイクルされます。許可される最小値は30000（30秒）です。
- 導入バージョン: -

### jdbc_connection_keepalive_time_ms

- デフォルト: 30000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: アイドル状態のJDBC接続のキープアライブ間隔。アイドル状態の接続は、古い接続をプロアクティブに検出するために、この間隔でテストされます。0に設定するとキープアライブプロービングを無効にします。有効な場合、>= 30000かつ`jdbc_connection_max_lifetime_ms`より小さい必要があります。無効な有効値はサイレントに無効化されます（0にリセット）。
- 導入バージョン: -

### lake_clear_corrupted_cache_data

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタにおいて、システムが破損したデータキャッシュをクリアすることを許可するかどうか。
- 導入バージョン: v3.4

### lake_clear_corrupted_cache_meta

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタにおいて、システムが破損したメタデータキャッシュをクリアすることを許可するかどうか。
- 導入バージョン: v3.3

### lake_enable_vertical_compaction_fill_data_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでコンパクションタスクがローカルディスクにデータをキャッシュすることを許可するかどうか。
- 導入バージョン: v3.1.7, v3.2.3

### lake_service_max_concurrency

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 共有データクラスタにおける RPC リクエストの最大同時実行数。このしきい値に達すると、受信リクエストは拒否されます。この項目が `0` に設定されている場合、同時実行数に制限はありません。
- 導入バージョン: -

### query_max_memory_limit_percent

- デフォルト: 90
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クエリプールが使用できる最大メモリ。プロセスメモリ制限のパーセンテージとして表されます。
- 導入バージョン: v3.1.0

### rocksdb_max_write_buffer_memory_bytes

- デフォルト: 1073741824
- タイプ: Int64
- 単位: -
- 変更可能: No
- 説明: RocksDB の meta 用 write buffer の最大サイズです。デフォルトは 1GB です。
- 導入バージョン: v3.5.0

### rocksdb_write_buffer_memory_percent

- デフォルト: 5
- タイプ: Int64
- 単位: -
- 変更可能: No
- 説明: RocksDB の meta 用 write buffer に割り当てるメモリの割合です。デフォルトはシステムメモリの 5% です。ただし、これに加えて、最終的に算出される write buffer メモリのサイズは 64MB 未満にならず、1G を超えません（rocksdb_max_write_buffer_memory_bytes）。
- 導入バージョン: v3.5.0

##### lake_balance_tablets_threshold

- デフォルト: 0.15
- タイプ: Double
- 単位: -
- 変更可能: Yes
- 説明: 共有データクラスタでのワーカー間の tablet バランスを判断するためにシステムが使用するしきい値。アンバランスファクターは次のように計算されます: `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`。ファクターが `lake_balance_tablets_threshold` を超える場合、tablet バランスがトリガーされます。この項目は `lake_enable_balance_tablets_between_workers` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.3.4

## その他

### default_mv_resource_group_concurrency_limit

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクの最大同時実行数 (BE ノードごと)。デフォルト値 `0` は制限がないことを示します。
- 導入バージョン: v3.1

### default_mv_resource_group_cpu_limit

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで使用できる最大 CPU コア数 (BE ノードごと)。
- 導入バージョン: v3.1

### default_mv_resource_group_memory_limit

- デフォルト: 0.8
- タイプ: Double
- 単位: 
- 変更可能: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで使用できる最大メモリ比率 (BE ノードごと)。デフォルト値はメモリの 80% を示します。
- 導入バージョン: v3.1

### default_mv_resource_group_spill_mem_limit_threshold

- デフォルト: 0.8
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: リソースグループ `default_mv_wg` のマテリアライズドビューリフレッシュタスクで中間結果のスピリングをトリガーする前のメモリ使用量のしきい値。デフォルト値はメモリの 80% を示します。
- 導入バージョン: v3.1

### enable_resolve_hostname_to_ip_in_load_error_url

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: `error_urls` デバッグのために、オペレーターがFEハートビートからの元のホスト名を使用するか、環境要件に基づいてIPアドレスへの解決を強制するかを選択できるようにするかどうか。
  - `true`: ホスト名をIPアドレスに変換します。
  - `false` (デフォルト): エラーURLに元のホスト名を保持します。
- 導入バージョン: v4.0.1

### enable_retry_apply

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効な場合、再試行可能と分類される Tablet の apply 失敗（例えば一時的なメモリ制限エラー）は直ちにタブレットをエラーにマークするのではなく、再試行のために再スケジュールされます。TabletUpdates の再試行経路は次の試行を現在の失敗回数に `retry_apply_interval_second` を乗じてスケジュールし、最大 600s にクランプするため、連続する失敗に伴ってバックオフが大きくなります。明示的に再試行不可なエラー（例えば corruption）は再試行をバイパスして apply プロセスを直ちにエラー状態にします。再試行は全体のタイムアウト／終了条件に達するまで続き、その後 apply はエラー状態になります。これをオフにすると、失敗した apply タスクの自動再スケジュールが無効になり、失敗した apply は再試行なしでエラー状態に移行します。
- 導入バージョン: v3.2.9

### enable_token_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: トークンチェックを有効にするかどうかを制御するブール値。`true` はトークンチェックを有効にすることを示し、`false` は無効にすることを示します。
- 導入バージョン: -

### load_replica_status_check_interval_ms_on_failure

- デフォルト: 2000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 最後のチェック RPC が失敗した場合に、セカンダリレプリカがプライマリレプリカに対して状態を確認する間隔。
- 導入バージョン: 3.5.1

### load_replica_status_check_interval_ms_on_success

- デフォルト: 15000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 最後のチェック RPC が成功した場合に、セカンダリレプリカがプライマリレプリカに対して状態を確認する間隔。
- 導入バージョン: 3.5.1

### max_length_for_bitmap_function

- デフォルト: 1000000
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: ビットマップ関数の入力値の最大長。
- 導入バージョン: -

### max_length_for_to_base64

- デフォルト: 200000
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: to_base64() 関数の入力値の最大長。
- 導入バージョン: -

### memory_high_level

- デフォルト: 75
- タイプ: Long
- 単位: Percent
- 変更可能: Yes
- 説明: プロセスのメモリ上限に対する割合で表されるハイウォーターメモリ閾値。総メモリ使用量がこの割合を超えると、BE はメモリ圧力を緩和するために徐々にメモリを解放し始めます（現在はデータキャッシュと更新キャッシュの追い出しで実施）。モニタはこの値を用いて memory_high = mem_limit * memory_high_level / 100 を計算し、消費量が `>` memory_high の場合は GC アドバイザに導かれた制御されたエビクションを行います；消費量が別の設定である memory_urgent_level を超えると、より攻撃的な即時削減が行われます。この値は閾値超過時に一部のメモリ集約的な操作（例えば primary-key preload）を無効化するかどうかの判断にも使われます。memory_urgent_level と合わせた検証を満たす必要があります（memory_urgent_level `>` memory_high_level、memory_high_level `>=` 1、memory_urgent_level `<=` 100）。
- 導入バージョン: v3.2.0

### report_exec_rpc_request_retry_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: FE に exec RPC リクエストを報告する際の RPC リクエストの再試行回数です。デフォルト値は 10 で、fragment instance finish RPC の場合に限り失敗した際に最大10回再試行されます。Report exec RPC request は load job にとって重要で、もしある fragment instance の finish 報告が失敗すると、load job はタイムアウトするまでハングする可能性があります。
- 導入バージョン: -

### sleep_one_second

- デフォルト: 1
- タイプ: Int
- 単位: Seconds
- 変更可能: No
- 説明: マスターアドレス/ハートビートがまだ利用できない場合や短いリトライ/バックオフが必要な場合に、BE エージェントのワーカースレッドが 1 秒間の一時停止として使用する小さなグローバルスリープ間隔（秒）。コードベースではいくつかのレポートワーカープール（例: ReportDiskStateTaskWorkerPool、ReportOlapTableTaskWorkerPool、ReportWorkgroupTaskWorkerPool）から参照され、ビジーウェイトを避けてリトライ中の CPU 消費を低減します。この値を増やすとリトライ頻度とマスターへの応答性が遅くなり、減らすとポーリング率と CPU 使用率が上がります。応答性とリソース使用のトレードオフを意識してのみ調整してください。
- 導入バージョン: v3.2.0

### small_file_dir

- デフォルト: `${STARROCKS_HOME}/lib/small_file/`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ファイルマネージャーによってダウンロードされたファイルを保存するために使用されるディレクトリ。
- 導入バージョン: -

### upload_buffer_size

- デフォルト: 4194304
- タイプ: Int
- 単位: Bytes
- 変更可能: Yes
- 説明: スナップショットファイルをリモートストレージ（broker や直接 FileSystem）へアップロードする際のファイルコピー操作で使用するバッファサイズ（バイト単位）。アップロード経路（snapshot_loader.cpp）では、この値が各アップロードストリームの読み書きチャンクサイズとして fs::copy に渡されます。デフォルトは 4 MiB です。高レイテンシや高帯域のリンクではこの値を増やすことでスループットが向上することがありますが、同時アップロードごとのメモリ使用量が増加します。値を小さくするとストリームごとのメモリは減少しますが転送効率が落ちる可能性があります。upload_worker_count や利用可能な全体メモリと合わせて調整してください。
- 導入バージョン: 3.2.13

### user_function_dir

- デフォルト: `${STARROCKS_HOME}/lib/udf`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ユーザー定義関数 (UDF) を保存するために使用されるディレクトリ。
- 導入バージョン: -
