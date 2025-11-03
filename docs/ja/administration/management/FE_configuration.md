---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 設定

<FEConfigMethod />

## FE の設定項目を表示する

FE が起動した後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行してパラメータ設定を確認できます。特定のパラメータの設定を確認したい場合は、次のコマンドを実行してください。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスタ管理関連のコマンドを実行するには、管理者権限が必要です。
:::

## FE パラメータを設定する

### FE 動的パラメータを設定する

[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して FE 動的パラメータの設定を変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメータを設定する

<StaticFEConfigNote />

## FE パラメータを理解する

### ロギング

### サーバー

### メタデータとクラスタ管理

### ユーザー、役割、特権

### クエリエンジン

### ロードとアンロード

<<<<<<< HEAD
=======
##### load_straggler_wait_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BE レプリカによって許容される最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするためにクローンが実行されます。
- 導入バージョン: -

##### load_checker_interval_second

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがローリングベースで処理される時間間隔。
- 導入バージョン: -

##### broker_load_default_timeout_second

- デフォルト: 14400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Broker Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### min_bytes_per_broker_scanner

- デフォルト: 67108864
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: Broker Load インスタンスによって処理されることができる最小許容データ量。
- 導入バージョン: -

##### insert_load_default_timeout_second

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データをロードするために使用される INSERT INTO ステートメントのタイムアウト期間。
- 導入バージョン: -

##### stream_load_default_timeout_second

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各 Stream Load ジョブのデフォルトタイムアウト期間。
- 導入バージョン: -

##### max_stream_load_timeout_second

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Stream Load ジョブの最大許容タイムアウト期間。
- 導入バージョン: -

##### max_load_timeout_second

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべてのタイプのロードジョブに適用されます。
- 導入バージョン: -

##### min_load_timeout_second

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最小タイムアウト期間。この制限はすべてのタイプのロードジョブに適用されます。
- 導入バージョン: -

##### prepared_transaction_default_timeout_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 準備済みトランザクションのデフォルトのタイムアウト期間。
- 導入バージョン: -

##### spark_dpp_version

- デフォルト: 1.0.0
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用される Spark Dynamic Partition Pruning (DPP) のバージョン。
- 導入バージョン: -

##### spark_load_default_timeout_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各 Spark Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### spark_home_default_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark クライアントのルートディレクトリ。
- 導入バージョン: -

##### spark_resource_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark 依存パッケージのルートディレクトリ。
- 導入バージョン: -

##### spark_launcher_log_dir

- デフォルト: sys_log_dir + "/spark_launcher_log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark ログファイルを保存するディレクトリ。
- 導入バージョン: -

##### yarn_client_path

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Yarn クライアントパッケージのルートディレクトリ。
- 導入バージョン: -

##### yarn_config_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Yarn 設定ファイルを保存するディレクトリ。
- 導入バージョン: -

##### desired_max_waiting_jobs

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE における保留中ジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FE の保留中ジョブの数がこの値に達すると、FE は新しいロードリクエストを拒否します。このパラメータは非同期ロードにのみ有効です。v2.5 以降、デフォルト値は 100 から 1024 に変更されました。
- 導入バージョン: -

##### max_running_txn_num_per_db

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスタ内の各データベースで実行中のロードトランザクションの最大数。デフォルト値は `1000` です。v3.1 以降、デフォルト値は `100` から `1000` に変更されました。データベースで実行中のロードトランザクションの実際の数がこのパラメータの値を超える場合、新しいロードリクエストは処理されません。同期ロードジョブの新しいリクエストは拒否され、非同期ロードジョブの新しいリクエストはキューに入れられます。このパラメータの値を増やすことはお勧めしません。システム負荷が増加する可能性があります。
- 導入バージョン: -

##### max_broker_load_job_concurrency

- デフォルト: 5
- 別名: async_load_task_pool_size
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスタ内で許可される最大同時 Broker Load ジョブ数。このパラメータは Broker Load にのみ有効です。このパラメータの値は `max_running_txn_num_per_db` の値より小さくなければなりません。v2.5 以降、デフォルト値は `10` から `5` に変更されました。
- 導入バージョン: -

##### load_parallel_instance_num (廃止予定)

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE 上の各ロードジョブに対する同時ロードインスタンスの最大数。この項目は v3.1 以降廃止されます。
- 導入バージョン: -

##### disable_load_job

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラスタがエラーに遭遇したときにロードを無効にするかどうか。これにより、クラスタエラーによる損失を防ぎます。デフォルト値は `FALSE` で、ロードが無効になっていないことを示します。`TRUE` はロードが無効になり、クラスタが読み取り専用状態であることを示します。
- 導入バージョン: -

##### history_job_keep_max_second

- デフォルト: 7 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スキーマ変更ジョブなどの履歴ジョブを保持できる最大期間。
- 導入バージョン: -

##### label_keep_max_second

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 完了したロードジョブのラベルを保持する最大期間（FINISHED または CANCELLED 状態）。デフォルト値は 3 日です。この期間が経過すると、ラベルは削除されます。このパラメータはすべてのタイプのロードジョブに適用されます。値が大きすぎると、多くのメモリを消費します。
- 導入バージョン: -

##### label_keep_max_num

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報が削除されます。
- 導入バージョン: -

##### max_routine_load_task_concurrent_num

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各 Routine Load ジョブの最大同時タスク数。
- 導入バージョン: -

##### max_routine_load_task_num_per_be

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各 BE 上の最大同時 Routine Load タスク数。v3.1.0 以降、このパラメータのデフォルト値は 5 から 16 に増加し、BE 静的パラメータ `routine_load_thread_pool_size` (廃止予定) の値以下である必要がなくなりました。
- 導入バージョン: -

##### max_routine_load_batch_size

- デフォルト: 4294967296
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: Routine Load タスクによってロードされる最大データ量。
- 導入バージョン: -

##### routine_load_task_consume_second

- デフォルト: 15
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: クラスタ内の各 Routine Load タスクがデータを消費する最大時間。v3.1.0 以降、Routine Load ジョブは [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメータ `task_consume_second` をサポートしています。このパラメータは Routine Load ジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### routine_load_task_timeout_second

- デフォルト: 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: クラスタ内の各 Routine Load タスクのタイムアウト期間。v3.1.0 以降、Routine Load ジョブは [job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメータ `task_timeout_second` をサポートしています。このパラメータは Routine Load ジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### routine_load_unstable_threshold_second

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: Routine Load ジョブ内のタスクが遅延すると、Routine Load ジョブは UNSTABLE 状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在の時間の差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。
- 導入バージョン: -

##### enable_routine_load_lag_metrics

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Routine Load パーティションのオフセットラグをメトリクスで収集するかどうか。この項目を `true` に設定すると、Kafka API を呼び出してパーティションの最新のオフセットを取得することに注意。
- 導入バージョン: -

##### min_routine_load_lag_for_metrics

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 監視メトリクスに表示される Routine Load ジョブの最小オフセットラグ。オフセットラグがこの値より大きい Routine Load ジョブは、メトリクスに表示されます。
- 導入バージョン: -

##### max_tolerable_backend_down_num

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許容される故障 BE ノードの最大数。この数を超えると、Routine Load ジョブは自動的に回復できません。
- 導入バージョン: -

##### period_of_auto_resume_min

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 変更可能: はい
- 説明: Routine Load ジョブが自動的に回復される間隔。
- 導入バージョン: -

##### export_task_default_timeout_second

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データエクスポートタスクのタイムアウト期間。
- 導入バージョン: -

##### export_max_bytes_per_be_per_task

- デフォルト: 268435456
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: 単一の BE から単一のデータアンロードタスクによってエクスポートされる最大データ量。
- 導入バージョン: -

##### export_task_pool_size

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: アンロードタスクスレッドプールのサイズ。
- 導入バージョン: -

##### export_checker_interval_second

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがスケジュールされる時間間隔。
- 導入バージョン: -

##### export_running_job_num_limit

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 並行して実行できるデータエクスポートタスクの最大数。
- 導入バージョン: -

##### empty_load_as_error

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: データがロードされていない場合に「すべてのパーティションにロードデータがありません」というエラーメッセージを返すかどうか。有効な値:
  - `true`: データがロードされていない場合、システムは失敗メッセージを表示し、「すべてのパーティションにロードデータがありません」というエラーを返します。
  - `false`: データがロードされていない場合、システムは成功メッセージを表示し、エラーの代わりに OK を返します。
- 導入バージョン: -

##### external_table_commit_timeout_ms

- デフォルト: 10000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: StarRocks 外部テーブルへの書き込みトランザクションをコミット（公開）するためのタイムアウト期間。デフォルト値 `10000` は 10 秒のタイムアウト期間を示します。
- 導入バージョン: -

##### enable_sync_publish

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ロードトランザクションの公開フェーズで適用タスクを同期的に実行するかどうか。このパラメータは主キーテーブルにのみ適用されます。有効な値:
  - `TRUE` (デフォルト): ロードトランザクションの公開フェーズで適用タスクが同期的に実行されます。これは、適用タスクが完了した後にロードトランザクションが成功として報告され、ロードされたデータが実際にクエリ可能であることを意味します。タスクが一度に大量のデータをロードするか、頻繁にデータをロードする場合、このパラメータを `true` に設定すると、クエリパフォーマンスと安定性が向上しますが、ロードの遅延が増加する可能性があります。
  - `FALSE`: ロードトランザクションの公開フェーズで適用タスクが非同期的に実行されます。これは、適用タスクが送信された後にロードトランザクションが成功として報告されますが、ロードされたデータはすぐにはクエリできないことを意味します。この場合、同時クエリは適用タスクが完了するかタイムアウトするまで待機する必要があります。タスクが一度に大量のデータをロードするか、頻繁にデータをロードする場合、このパラメータを `false` に設定すると、クエリパフォーマンスと安定性に影響を与える可能性があります。
- 導入バージョン: v3.2.0

##### label_clean_interval_second

- デフォルト: 4 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### transaction_clean_interval_second

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: 完了したトランザクションがクリーンアップされる時間間隔。単位: 秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### transaction_stream_load_coordinator_cache_capacity

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：是
- 説明：ストレージトランザクションタグからcoordinatorノードへのマッピングのキャッシュ容量を設定します。

- 導入バージョン：-

##### transaction_stream_load_coordinator_cache_expire_seconds

- デフォルト：900
- タイプ：Int
- 単位：-
- 変更可能：是
- 説明：トランザクションタグとcoordinatorノードのマッピング関係がキャッシュ内に保持される生存時間（TTL）。
- 導入バージョン：-

##### enable_file_bundling

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラウドネイティブテーブルに対してファイルバンドリング最適化を有効にするかどうか。この機能を有効に設定（`true` に設定）すると、システムはロード、コンパクション、またはパブリッシュ操作によって生成されたデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減します。この動作は、CREATE TABLE プロパティ `file_bundling` を使用してテーブルレベルで制御することもできます。詳細な手順については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。
- 導入バージョン: v4.0

##### txn_latency_metric_report_groups

- デフォルト: ""
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 報告するトランザクションレイテンシメトリックグループのカンマ区切りリスト。ロードタイプはモニタリングのために論理グループに分類されます。グループが有効になると、その名前がトランザクションメトリックに「group」ラベルとして追加されます。一般的なグループには、「stream_load」、「routine_load」、「broker_load」、「insert」、「compaction」（共有データクラスタ用）などがあります。例：「stream_load,routine_load」。
- 導入バージョン: -

>>>>>>> 1db13c58715 (add txn metrics)
### ストレージ

### 共有データ

### その他



<EditionSpecificFEItem />
