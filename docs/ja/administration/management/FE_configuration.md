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

##### audit_log_delete_age

- デフォルト: 30d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 監査ログファイルの保持期間。デフォルト値 `30d` は、各監査ログファイルが 30 日間保持されることを指定します。StarRocks は各監査ログファイルをチェックし、30 日前に生成されたものを削除します。
- 導入バージョン: -

##### audit_log_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 監査ログファイルを保存するディレクトリ。
- 導入バージョン: -

##### audit_log_modules

- デフォルト: slow_query, query
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks が監査ログエントリを生成するモジュール。デフォルトでは、StarRocks は `slow_query` モジュールと `query` モジュールの監査ログを生成します。`connection` モジュールは v3.0 からサポートされています。モジュール名はカンマ (,) とスペースで区切ります。
- 導入バージョン: -

##### audit_log_roll_interval

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks が監査ログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、監査ログファイルの名前に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、監査ログファイルの名前に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入バージョン: -

##### audit_log_roll_num

- デフォルト: 90
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `audit_log_roll_interval` パラメータで指定された保持期間内に保持できる監査ログファイルの最大数。
- 導入バージョン: -

##### big_query_log_dir

- デフォルト: Config.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: FE が big query ダンプログを書き込むディレクトリ（ファイル名: fe.big_query.log）。Log4j の設定はこのパスを使用して `fe.big_query.log` とローテートされたファイル用の RollingFile appender を作成します。ローテーションと保持は `big_query_log_roll_interval`（時間ベースのサフィックス）、`log_roll_size_mb`（サイズトリガー）、`big_query_log_roll_num`（最大ファイル数）、および `big_query_log_delete_age`（年齢ベースの削除）によって制御されます。big-query レコードは `big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold`、`big_query_log_scan_bytes_threshold` のようなユーザー定義の閾値を超えたクエリについて出力されます。どのモジュールがこのファイルにログを出力するかは `big_query_log_modules` で制御してください。
- 導入バージョン: v3.2.0

##### big_query_log_modules

- デフォルト: `{"query"}`
- タイプ: String[]
- 単位: -
- 変更可能: No
- 説明: モジュール単位の big query ログを有効にするモジュール名サフィックスの一覧です。典型的な値は論理コンポーネント名です。たとえばデフォルトの `query` は `big_query.query` を生成します。
- 導入バージョン: v3.2.0

##### big_query_log_roll_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: `big_query_log_roll_interval` ごとに保持する FE big query ログのローテート済みファイルの最大数です。この値は RollingFile appender の DefaultRolloverStrategy の `max` 属性（`fe.big_query.log` 用）にバインドされます。ログが時間または `log_roll_size_mb` によってロールすると、StarRocks は最大で `big_query_log_roll_num` 個のインデックス付きファイルを保持します（filePattern は時間サフィックスとインデックスを使用します）。この数より古いファイルはロールオーバーにより削除される可能性があり、`big_query_log_delete_age` によって最終更新日時に基づく削除が追加で行われることがあります。この値を変更するには、FE の再起動が必要です。
- 導入バージョン: v3.2.0

##### dump_log_delete_age

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ダンプログファイルの保持期間。デフォルト値 `7d` は、各ダンプログファイルが 7 日間保持されることを指定します。StarRocks は各ダンプログファイルをチェックし、7 日前に生成されたものを削除します。
- 導入バージョン: -

##### dump_log_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ダンプログファイルを保存するディレクトリ。
- 導入バージョン: -

##### dump_log_modules

- デフォルト: query
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks がダンプログエントリを生成するモジュール。デフォルトでは、StarRocks は query モジュールのダンプログを生成します。モジュール名はカンマ (,) とスペースで区切ります。
- 導入バージョン: -

##### dump_log_roll_interval

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks がダンプログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、ダンプログファイルの名前に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、ダンプログファイルの名前に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入バージョン: -

##### dump_log_roll_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `dump_log_roll_interval` パラメータで指定された保持期間内に保持できるダンプログファイルの最大数。
- 導入バージョン: -

##### enable_audit_sql

- デフォルト: true
- タイプ: Boolean
- 単位: N/A
- 変更可能: No
- 説明: true の場合、Frontend の監査サブシステムは ConnectProcessor によって処理される FE の監査ログ (fe.audit.log) にステートメントの SQL テキストを記録します。格納されるステートメントは他の制御を尊重します：暗号化されたステートメントは (AuditEncryptionChecker により) マスキングされ、認証情報は enable_sql_desensitize_in_log が設定されていると赤字化または脱感作される可能性があり、ダイジェストの記録は enable_sql_digest で制御されます。false の場合、ConnectProcessor は監査イベント内のステートメントテキストを "?" に置き換えます — 他の監査フィールド（user、host、duration、status、qe_slow_log_ms によるスロークエリ検出、メトリクス）は引き続き記録されます。SQL 監査を有効にするとフォレンジックやトラブルシューティングの可視性は向上しますが、機密性の高い SQL 内容が露出したりログの量および I/O が増加したりする可能性があります。無効にすると監査ログでの完全なステートメントの可視性を失う代わりにプライバシーが向上します。このオプションはランタイムで変更できません。
- 導入バージョン: -

##### internal_log_delete_age

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: FE の内部ログファイル（`internal_log_dir` に書き込まれる）の保持期間を指定します。値は期間文字列で、サフィックスとして `d`（日）、`h`（時間）、`m`（分）、`s`（秒）をサポートします。例: `7d`（7日）、`10h`（10時間）、`60m`（60分）、`120s`（120秒）。この項目は log4j 設定内の `<IfLastModified age="..."/>` 述語（RollingFile Delete ポリシーで使用）に代入されます。最終更新時刻がこの期間より前のファイルはロールオーバー時に削除されます。内部のマテリアライズドビューや統計ログを長く保持したい場合は値を小さくするのではなく、逆に保持期間を延長してください。ディスク容量を早く確保したい場合はこの値を短くしてください。
- 導入バージョン: v3.2.4

##### internal_log_modules

- デフォルト: `{"base", "statistic"}`
- タイプ: String[]
- 単位: -
- 変更可能: No
- 説明: 専用の内部ログを受け取るモジュール識別子のリスト。各エントリ X に対して、Log4j はレベル INFO かつ additivity="false" のロガー `internal.<X>` を作成します。これらのロガーは internal appender（`fe.internal.log` に書き込まれる）または `sys_log_to_console` が有効な場合はコンソールにルーティングされます。必要に応じて短い名前やパッケージの断片を使用してください — 正確なロガー名は `internal.` + 設定した文字列になります。内部ログファイルのローテーションと保持は `internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval`、`log_roll_size_mb` に従います。モジュールを追加すると、そのランタイムメッセージが内部ロガーストリームに分離され、デバッグや監査が容易になります。
- 導入バージョン: v3.2.4

##### log_roll_size_mb

- デフォルト: 1024
- タイプ: Int
- 単位: MB
- 変更可能: いいえ
- 説明: システムログファイルまたは監査ログファイルの最大サイズ。
- 導入バージョン: -

##### profile_log_roll_size_mb

- デフォルト: 1024
- タイプ: Int
- 単位: MB
- 変更可能: No
- 説明: FE のプロファイルログファイルをサイズベースでローテーションするトリガーとなる閾値（メガバイト単位）を設定します。この値は `ProfileFile` appender の Log4j RollingFile SizeBasedTriggeringPolicy によって使用され、プロファイルログが `profile_log_roll_size_mb` を超えるとローテーションされます。ローテーションは `profile_log_roll_interval` に達したときの時間ベースでも発生するため、いずれかの条件でロールオーバーが行われます。`profile_log_roll_num` および `profile_log_delete_age` と組み合わせて、過去のプロファイルファイルの保持数や古いファイルの削除タイミングを制御します。ローテートされたファイルの圧縮は `enable_profile_log_compress` によって制御されます。この値を変更した場合、適用するにはプロセスの再起動が必要です。
- 導入バージョン: v3.2.5

##### qe_slow_log_ms

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: クエリがスロークエリであるかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超える場合、**fe.audit.log** にスロークエリとして記録されます。
- 導入バージョン: -

##### slow_lock_log_every_ms

- デフォルト: 3000L
- タイプ: Long
- 単位: Milliseconds
- 変更可能: Yes
- 説明: 同じ SlowLockLogStats インスタンスに対して別の「slow lock」警告を出力するまでに待機する最小間隔（ms）。ロック待機が slow_lock_threshold_ms を超えた後、LockUtils はこの値をチェックし、最後にログに記録されたスローロックイベントから slow_lock_log_every_ms ミリ秒が経過するまでは追加の警告を抑制します。長時間の競合でログ量を減らしたい場合は大きな値を、より頻繁に診断情報を得たい場合は小さな値を使用してください。変更はランタイムで次回以降のチェックに反映されます。
- 導入バージョン: v3.2.0

##### slow_lock_threshold_ms

- デフォルト: 3000L
- タイプ: long
- 単位: Milliseconds
- 変更可能: はい
- 説明: ロック操作または保持中のロックを「遅い(slow)」と分類するための閾値（ms単位）。ロックの経過待機時間または保持時間がこの値を超えると、StarRocks は（コンテキストに応じて）診断ログを出力したり、スタックトレースや待ち手/所有者情報を含めたり、—LockManagerではこの遅延後にデッドロック検出を開始します。LockUtils（slow-lock ロギング）、QueryableReentrantReadWriteLock（遅いリーダーのフィルタリング）、LockManager（デッドロック検出の遅延および遅いロックのトレース）、LockChecker（定期的な遅いロック検出）、およびその他の呼び出し元（例: DiskAndTabletLoadReBalancer のログ出力）で使用されます。値を下げると感度およびログ/診断のオーバーヘッドが増加します。値を 0 または負に設定すると、初期の待機ベースのデッドロック検出遅延の動作が無効になります。slow_lock_log_every_ms、slow_lock_print_stack、slow_lock_stack_trace_reserve_levels と合わせて調整してください。
- 導入バージョン: 3.2.0

##### sys_log_delete_age

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログファイルの保持期間。デフォルト値 `7d` は、各システムログファイルが 7 日間保持されることを指定します。StarRocks は各システムログファイルをチェックし、7 日前に生成されたものを削除します。
- 導入バージョン: -

##### sys_log_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログファイルを保存するディレクトリ。
- 導入バージョン: -

##### sys_log_enable_compress

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が `true` に設定されていると、システムはローテートされたシステムログファイル名に ".gz" の後置を付け、Log4j によって gzip 圧縮されたローテート済み FE システムログ（例: fe.log.*）が出力されるようになります。この値は Log4j 設定生成時（Log4jConfig.initLogging / generateActiveLog4jXmlConfig）に読み取られ、RollingFile の filePattern で使用される `sys_file_postfix` プロパティを制御します。この機能を有効にすると保持ログのディスク使用量は減少しますが、ロールオーバー時の CPU と I/O が増加し、ログファイル名が変更されるためログを読むツールやスクリプトが .gz ファイルを扱える必要があります。なお、監査ログは圧縮に別の設定（`audit_log_enable_compress`）を使用します。
- 導入バージョン: v3.2.12

##### sys_log_format

- デフォルト: "plaintext"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: FE ログに使用する Log4j レイアウトを選択します。有効な値: `"plaintext"`（デフォルト）および `"json"`。大文字小文字は区別されません。`"plaintext"` は PatternLayout を設定し、人間が読みやすいタイムスタンプ、レベル、スレッド、class.method:line および WARN/ERROR のスタックトレースを出力します。`"json"` は JsonTemplateLayout を設定し、ログ集約ツール（ELK、Splunk 等）向けの構造化された JSON イベント（UTC タイムスタンプ、level、thread id/name、ソースファイル/メソッド/行、message、例外の stackTrace）を出力します。JSON 出力は最大文字列長に関して `sys_log_json_max_string_length` および `sys_log_json_profile_max_string_length` を順守します。
- 導入バージョン: v3.2.10

##### sys_log_json_max_string_length

- デフォルト: 1048576
- タイプ: Int
- 単位: Bytes
- 変更可能: No
- 説明: JSON形式のシステムログに使用される JsonTemplateLayout の "maxStringLength" 値を設定します。`sys_log_format` が `"json"` に設定されている場合、文字列値のフィールド（例えば "message" や文字列化された例外スタックトレース）は、その長さがこの上限を超えると切り詰められます。この値は `Log4jConfig.generateActiveLog4jXmlConfig()` 内で生成される Log4j XML に注入され、default、warning、audit、dump および bigquery のレイアウトに適用されます。profile レイアウトは別の設定（`sys_log_json_profile_max_string_length`）を使用します。この値を下げるとログサイズは小さくなりますが、有用な情報が切り落とされる可能性があります。
- 導入バージョン: 3.2.11

##### sys_log_json_profile_max_string_length

- デフォルト: 104857600 (100 MB)
- タイプ: Int
- 単位: Bytes
- 変更可能: いいえ
- 説明: `sys_log_format` が "json" のとき、profile（および関連機能）のログ appender に対して JsonTemplateLayout の maxStringLength を設定します。JSON 形式の profile ログ内の文字列フィールドの値はこのバイト長で切り詰められ、非文字列フィールドには影響しません。この設定は Log4jConfig の `JsonTemplateLayout maxStringLength` に適用され、`plaintext` ログが使用されている場合は無視されます。必要な全メッセージが収まるように十分大きな値にしてください。ただし、大きな値はログサイズと I/O を増加させる点に注意してください。
- 導入バージョン: v3.2.11

##### sys_log_level

- デフォルト: INFO
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログエントリが分類される重大度レベル。 有効な値: `INFO`, `WARN`, `ERROR`, `FATAL`。
- 導入バージョン: -

##### sys_log_roll_interval

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks がシステムログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、システムログファイルの名前に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、システムログファイルの名前に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入バージョン: -

##### sys_log_roll_num

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `sys_log_roll_interval` パラメータで指定された保持期間内に保持できるシステムログファイルの最大数。
- 導入バージョン: -

##### sys_log_to_console

- デフォルト: false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: この項目を `true` に設定すると、システムは Log4j を構成してファイルベースの appender の代わりにすべてのログをコンソール（ConsoleErr appender）に送るようにします。この値は、アクティブな Log4j XML 設定を生成する際に読み込まれ（root logger とモジュールごとの logger の appender 選択に影響します）、プロセス起動時に環境変数 `SYS_LOG_TO_CONSOLE` から取得されます。ランタイムで変更しても効果はありません。本設定は、ログをファイルに書き込む代わりに stdout/stderr の収集が好まれるコンテナ化環境や CI 環境で一般的に使用されます。
- 導入バージョン: v3.2.0

##### sys_log_verbose_modules

- デフォルト: 空の文字列
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks がシステムログを生成するモジュール。このパラメータが `org.apache.starrocks.catalog` に設定されている場合、StarRocks は catalog モジュールのシステムログのみを生成します。モジュール名はカンマ (,) とスペースで区切ります。
- 導入バージョン: -

##### log_cleaner_disk_util_based_enable

- デフォルト: false 
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ディスク使用率に基づく自動ログクリーンアップを有効にします。有効にすると、ログディレクトリのディスク使用率がしきい値を超えた場合、ログファイルが自動的にクリーンアップされます。ログクリーンアップは FE ノードでバックグラウンドデーモンとして実行され、ログファイルの蓄積によるディスク容量の枯渇を防ぐのに役立ちます。
- 導入バージョン: -

##### log_cleaner_disk_usage_threshold

- デフォルト: 80
- タイプ: Int
- 単位: パーセンテージ
- 変更可能: はい
- 説明: ログクリーンアップをトリガーするディスク使用率のしきい値（パーセンテージ）。ログディレクトリのディスク使用率がこのしきい値を超えると、ログクリーンアップが開始されます。クリーンアップは、設定された各ログディレクトリを個別にチェックし、このしきい値を超えるディレクトリを処理します。
- 導入バージョン: -

##### log_cleaner_disk_usage_target

- デフォルト: 60
- タイプ: Int
- 単位: パーセンテージ
- 変更可能: はい
- 説明: ログクリーンアップ後の目標ディスク使用率（パーセンテージ）。ログクリーンアップは、ディスク使用率がこのしきい値を下回るまで継続されます。クリーンアップは、目標値に達するまで最も古いログファイルを1つずつ削除します。
- 導入バージョン: -

##### log_cleaner_audit_log_min_retention_days

- デフォルト: 3
- タイプ: Int
- 単位: 日
- 変更可能: はい
- 説明: 監査ログファイルの最小保持日数。ディスク使用率が高くても、この値より新しい監査ログファイルは削除されません。これにより、コンプライアンスとトラブルシューティングの目的で監査ログが保持されます。
- 導入バージョン: -

##### log_cleaner_check_interval_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ディスク使用率をチェックしてログをクリーンアップする間隔（秒）。クリーンアップは、各ログディレクトリのディスク使用率を定期的にチェックし、必要に応じてクリーンアップをトリガーします。デフォルト値は 300 秒（5 分）です。
- 導入バージョン: -

##### sys_log_warn_modules

- デフォルト: {}
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: システム起動時に WARN レベルのロガーとして設定し、警告アペンダ（SysWF）— `fe.warn.log` ファイル— にルーティングするロガー名またはパッケージプレフィックスのリストです。エントリは生成された Log4j 設定に挿入され（org.apache.kafka、org.apache.hudi、org.apache.hadoop.io.compress といった組み込みの warn モジュールとともに）、`<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` のような logger 要素を生成します。冗長な INFO/DEBUG 出力を通常のログに抑制し、警告を別途捕捉するために完全修飾のパッケージやクラスのプレフィックス（例: "com.example.lib"）を使用することが推奨されます。
- 導入バージョン: v3.2.13

### サーバー

##### brpc_idle_wait_max_time

- デフォルト: 10000
- タイプ: Int
- 単位: ms
- 変更可能: いいえ
- 説明: bRPC クライアントがアイドル状態で待機する最大時間。
- 導入バージョン: -

##### cluster_name

- デフォルト: StarRocks Cluster
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE が属する StarRocks クラスタの名前。クラスタ名は、Web ページの `Title` に表示されます。
- 導入バージョン: -

##### enable_http_async_handler

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: システムが HTTP リクエストを非同期に処理できるようにするかどうかを設定します。この機能を有効にすると、Netty ワーカースレッドが受信した HTTP リクエストは、HTTP サーバのブロックを回避するために、サービスロジックを処理する別のスレッドプールに送信されます。無効にすると、Netty ワーカーがサービスロジックを処理します。
- 導入バージョン: 4.0.0

##### enable_https

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: No
- 説明: FEノードにおいて、HTTP サーバーと並行して HTTPS サーバーを有効化するかどうか。
- 導入バージョン: v4.0

##### frontend_address

- デフォルト: 0.0.0.0
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE ノードの IP アドレス。
- 導入バージョン: -

##### http_async_threads_num

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 非同期 HTTP リクエスト処理用のスレッドプールのサイズ。別名は `max_http_sql_service_task_threads_num` である。
- 導入バージョン: 4.0.0

##### http_backlog_num

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の HTTP サーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### http_max_initial_line_length

- デフォルト: 4096
- タイプ: Int
- 単位: Bytes
- 変更可能: No
- 説明: HttpServer で使用される Netty の `HttpServerCodec` が受け付ける HTTP 初期リクエスト行（メソッド + request-target + HTTP バージョン）の最大許容長（バイト単位）を設定します。この値は Netty のデコーダに渡され、初期行がこの長さを超えるリクエストは拒否されます（TooLongFrameException）。非常に長いリクエスト URI をサポートする必要がある場合にのみ増やしてください。値を大きくするとメモリ使用量が増え、誤った形式やリクエスト悪用に対する露出が増える可能性があります。`http_max_header_size`、`http_max_chunk_size` と合わせて調整してください。
- 導入バージョン: v3.2.0

##### http_port

- デフォルト: 8030
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の HTTP サーバーがリッスンするポート。
- 導入バージョン: -

##### http_worker_threads_num

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: HTTP リクエストを処理するための HTTP サーバーのワーカースレッドの数。負の値または 0 の場合、スレッド数は CPU コア数の 2 倍になります。
- 導入バージョン: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### https_port

- デフォルト: 8443
- タイプ: Int
- 単位:  -
- 変更可能: No
- 説明: FE ノード内の HTTPS サーバーがリスニングするポート番号。
- 導入バージョン: v4.0

##### max_mysql_service_task_threads_num

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の MySQL サーバーがタスクを処理するために実行できる最大スレッド数。
- 導入バージョン: -

##### mysql_nio_backlog_num

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の MySQL サーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### mysql_server_version

- デフォルト: 8.0.33
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: クライアントに返される MySQL サーバーバージョン。このパラメータを変更すると、次の状況でバージョン情報に影響を与えます:
  1. `select version();`
  2. ハンドシェイクパケットバージョン
  3. グローバル変数 `version` の値 (`show variables like 'version';`)
- 導入バージョン: -

##### mysql_service_io_threads_num

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の MySQL サーバーが I/O イベントを処理するために実行できる最大スレッド数。
- 導入バージョン: -

##### mysql_service_nio_enable_keep_alive

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: MySQL 接続の TCP Keep-Alive を有効にします。ロードバランサーの背後で長時間アイドル状態の接続に役立ちます。
- 導入バージョン: -

##### net_use_ipv6_when_priority_networks_empty

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: `priority_networks` が指定されていない場合に IPv6 アドレスを優先的に使用するかどうかを制御するブール値。`true` は、ノードをホストするサーバーが IPv4 と IPv6 の両方のアドレスを持っており、`priority_networks` が指定されていない場合に、システムが IPv6 アドレスを優先的に使用することを示します。
- 導入バージョン: v3.3.0

##### priority_networks

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 複数の IP アドレスを持つサーバーの選択戦略を宣言します。このパラメータで指定されたリストと一致する IP アドレスは最大で 1 つでなければなりません。このパラメータの値は、CIDR 表記でセミコロン (;) で区切られたエントリからなるリストです。たとえば、10.10.10.0/24 です。このリストのエントリと一致する IP アドレスがない場合、サーバーの利用可能な IP アドレスがランダムに選択されます。v3.3.0 から、StarRocks は IPv6 に基づくデプロイメントをサポートしています。サーバーが IPv4 と IPv6 の両方のアドレスを持っている場合、このパラメータが指定されていない場合、システムはデフォルトで IPv4 アドレスを使用します。この動作を変更するには、`net_use_ipv6_when_priority_networks_empty` を `true` に設定します。
- 導入バージョン: -

##### qe_max_connection

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: すべてのユーザーが FE ノードに確立できる最大接続数。v3.1.12 および v3.2.7 以降、デフォルト値は `1024` から `4096` に変更されました。
- 導入バージョン: -

##### query_port

- デフォルト: 9030
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の MySQL サーバーがリッスンするポート。
- 導入バージョン: -

##### rpc_port

- デフォルト: 9020
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の Thrift サーバーがリッスンするポート。
- 導入バージョン: -

##### ssl_cipher_blacklist

- デフォルト: Empty string
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: カンマ区切りのリストで、IANA 名称による SSL 暗号スイートを正規表現でブラックリスト登録します。ホワイトリストとブラックリストの両方が設定されている場合はブラックリストが優先されます。
- 導入バージョン: v4.0

##### ssl_cipher_whitelist

- デフォルト: Empty string
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: カンマ区切りのリストで、IANA 名称による SSL 暗号スイートを正規表現でホワイトリスト登録します。ホワイトリストとブラックリストの両方が設定されている場合はブラックリストが優先されます。
- 導入バージョン: v4.0

##### thrift_backlog_num

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE ノード内の Thrift サーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### thrift_client_timeout_ms

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: アイドル状態のクライアント接続がタイムアウトするまでの時間。
- 導入バージョン: -

##### thrift_rpc_max_body_size

- デフォルト: -1
- タイプ: Int
- 単位: Bytes
- 変更可能: No
- 説明: サーバの Thrift プロトコルを構築する際（`ThriftServer` の TBinaryProtocol.Factory に渡される）に使用される、許可される Thrift RPC メッセージ本文の最大サイズ（バイト単位）を制御します。値 `-1` は制限を無効にします（無制限）。正の値を設定すると上限が強制され、それより大きいメッセージは Thrift 層で拒否されます。これによりメモリ使用量を制限し、過大なリクエストや DoS のリスクを緩和するのに役立ちます。正当なリクエスト（大きな struct やバッチ化されたデータなど）が拒否されないように、期待されるペイロードに十分な大きさに設定してください。
- 導入バージョン: v3.2.0

##### thrift_server_max_worker_threads

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE ノード内の Thrift サーバーがサポートする最大ワーカースレッド数。
- 導入バージョン: -

##### thrift_server_queue_size

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: リクエストが保留中のキューの長さ。Thrift サーバーで処理中のスレッド数が `thrift_server_max_worker_threads` で指定された値を超える場合、新しいリクエストは保留キューに追加されます。
- 導入バージョン: -

### メタデータとクラスタ管理

##### alter_max_worker_queue_size

- デフォルト: 4096
- タイプ: Int
- 単位: Tasks
- 変更可能: No
- 説明: alter サブシステムで使用される内部ワーカースレッドプールのキューの容量を制御します。`AlterHandler` 内で `alter_max_worker_threads` とともに `ThreadPoolManager.newDaemonCacheThreadPool` に渡されます。保留中の alter タスク数が `alter_max_worker_queue_size` を超えると、新しい送信は拒否され、`RejectedExecutionException` がスローされる可能性があります（`AlterHandler.handleFinishAlterTask` を参照）。この値を調整して、メモリ使用量と同時実行される alter タスクのバックログ許容量のバランスを取ってください。
- 導入バージョン: v3.2.0

##### automated_cluster_snapshot_interval_seconds

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 自動クラスタスナップショットのタスクがトリガされる間隔。
- 導入バージョン: v3.4.2

##### background_refresh_metadata_interval_millis

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 2 回の連続した Hive メタデータキャッシュ更新の間隔。
- 導入バージョン: v2.5.5

##### background_refresh_metadata_time_secs_since_last_access_secs

- デフォルト: 3600 * 24
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: Hive メタデータキャッシュ更新タスクの有効期限。アクセスされた Hive catalog に対して、指定された時間を超えてアクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Hive catalog に対して、StarRocks はそのキャッシュされたメタデータを更新しません。
- 導入バージョン: v2.5.5

##### bdbje_heartbeat_timeout_second

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: StarRocks クラスタ内のリーダー、フォロワー、およびオブザーバー FEs 間のハートビートがタイムアウトするまでの時間。
- 導入バージョン: -

##### bdbje_lock_timeout_second

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: BDB JE ベースの FE 内のロックがタイムアウトするまでの時間。
- 導入バージョン: -

##### bdbje_replica_ack_timeout_second

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: メタデータがリーダー FE からフォロワー FEs に書き込まれるときに、リーダー FE が指定された数のフォロワー FEs からの ACK メッセージを待つことができる最大時間。単位: 秒。大量のメタデータが書き込まれている場合、フォロワー FEs はリーダー FE に ACK メッセージを返すまでに長い時間がかかり、ACK タイムアウトが発生します。この状況を防ぐために、このパラメータの値を増やすことをお勧めします。
- 導入バージョン: -

##### bdbje_reserved_disk_size

- デフォルト: 512L * 1024 * 1024 (536870912)
- タイプ: Long
- 単位: Bytes
- 変更可能: いいえ
- 説明: Berkeley DB JE が「保護されていない」（削除可能な）ログ/データファイルとして予約するバイト数の上限を制限します。StarRocks はこの値を BDBEnvironment の `EnvironmentConfig.RESERVED_DISK` を介して JE に渡します。JE の組み込みデフォルトは 0（無制限）です。StarRocks のデフォルト（512 MiB）は、JE が保護されていないファイルに過剰なディスク領域を予約するのを防ぎつつ、不要ファイルの安全なクリーンアップを可能にします。ディスク制約のあるシステムではこの値を調整してください：値を小さくすると JE はより早くファイルを解放でき、値を大きくすると JE はより多くの予約領域を保持します。変更はプロセスの再起動が必要です。
- 導入バージョン: v3.2.0

##### bdbje_reset_election_group

- デフォルト: false
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: BDBJE レプリケーショングループをリセットするかどうか。このパラメータが `TRUE` に設定されている場合、FE は BDBJE レプリケーショングループをリセットし（つまり、すべての選出可能な FE ノードの情報を削除し）、リーダー FE として開始します。リセット後、この FE はクラスタ内の唯一のメンバーとなり、他の FEs は `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` を使用してこのクラスタに再参加できます。フォロワー FEs のデータがほとんど破損しているためにリーダー FE を選出できない場合にのみこの設定を使用してください。`reset_election_group` は `metadata_failure_recovery` を置き換えるために使用されます。
- 導入バージョン: -

##### black_host_connect_failures_within_time

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ブラックリストに登録された BE ノードに許可される接続失敗のしきい値。BE ノードが自動的に BE ブラックリストに追加されると、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` に設定されたしきい値よりも少ない接続失敗を持っている場合にのみ、BE ブラックリストから削除できます。
- 導入バージョン: v3.3.0

##### black_host_history_sec

- デフォルト: 2 * 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BE ブラックリスト内の BE ノードの過去の接続失敗を保持する時間。BE ノードが自動的に BE ブラックリストに追加されると、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` に設定されたしきい値よりも少ない接続失敗を持っている場合にのみ、BE ブラックリストから削除できます。
- 導入バージョン: v3.3.0

##### brpc_connection_pool_size

- デフォルト: 16
- タイプ: Int
- 単位: Connections
- 変更可能: No
- 説明: FE の BrpcProxy がエンドポイントごとにプールする BRPC 接続の最大数です。この値は `RpcClientOptions` に対して `setMaxTotoal` と `setMaxIdleSize` を通じて適用されるため、各リクエストがプールから接続を借用する必要があるため、同時発生する送信 BRPC リクエスト数を直接制限します。高同時実行のシナリオではリクエストの待ち行列を避けるためにこの値を増やしてください。ただし増やすとソケットやメモリ使用量が増え、リモートサーバの負荷も高まる可能性があります。チューニング時は `brpc_idle_wait_max_time`, `brpc_short_connection`, `brpc_inner_reuse_pool`, `brpc_reuse_addr`, `brpc_min_evictable_idle_time_ms` などの関連設定も考慮してください。この値の変更はホットリロード不可で、プロセスの再起動が必要です。
- 導入バージョン: v3.2.0

##### catalog_try_lock_timeout_ms

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: グローバルロックを取得するためのタイムアウト期間。
- 導入バージョン: -

##### checkpoint_timeout_seconds

- デフォルト: 24 * 3600
- タイプ: Long
- 単位: Seconds
- 変更可能: はい
- 説明: リーダーの CheckpointController がチェックポイントワーカーのチェックポイント完了を待つ最大時間（秒）です。コントローラはこの値をナノ秒に変換してワーカーの結果キューをポーリングします；このタイムアウト内に成功完了が受信されない場合、チェックポイントは失敗とみなされ、createImage は失敗を返します。値を増やすと長時間実行されるチェックポイントに対応できますが、失敗検出とその後のイメージ伝播が遅れます。値を減らすとフェイルオーバー/再試行は速くなりますが、遅いワーカーに対して誤ったタイムアウトが発生する可能性があります。この設定はチェックポイント作成中の `CheckpointController` における待機期間のみを制御し、ワーカー内部のチェックポイント動作を変更するものではありません。
- 導入バージョン: v3.4.0, v3.5.0

##### db_used_data_quota_update_interval_secs

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データベースの使用データクォータを更新する間隔。StarRocksは定期的にすべてのデータベースの使用データクォータを更新して、ストレージ消費を追跡します。この値はクォータ制御とメトリクス収集に使用されます。システム負荷を過度に高めないため、許可される最小間隔は30秒です。30未満の値は拒否されます。
- 導入バージョン: -

##### drop_backend_after_decommission

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: BE を廃止した後に削除するかどうか。`TRUE` は、BE が廃止された直後に削除されることを示します。`FALSE` は、BE が廃止された後に削除されないことを示します。
- 導入バージョン: -

##### edit_log_port

- デフォルト: 9010
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クラスタ内の Leader、Follower、および Observer FEs 間の通信に使用されるポート。
- 導入バージョン: -

##### edit_log_roll_num

- デフォルト: 50000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ログファイルがこれらのログエントリのために作成される前に書き込むことができるメタデータログエントリの最大数。このパラメータはログファイルのサイズを制御するために使用されます。新しいログファイルは BDBJE データベースに書き込まれます。
- 導入バージョン: -

##### edit_log_type

- デフォルト: BDB
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 生成できる編集ログのタイプ。値を `BDB` に設定します。
- 導入バージョン: -

##### enable_background_refresh_connector_metadata

- デフォルト: v3.0 以降では true、v2.5 では false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 定期的な Hive メタデータキャッシュの更新を有効にするかどうか。有効にすると、StarRocks は Hive クラスタのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Hive catalogs のキャッシュされたメタデータを更新してデータの変更を認識します。`true` は Hive メタデータキャッシュの更新を有効にし、`false` は無効にします。
- 導入バージョン: v2.5.5

##### enable_collect_query_detail_info

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリのプロファイルを収集するかどうか。このパラメータが `TRUE` に設定されている場合、システムはクエリのプロファイルを収集します。このパラメータが `FALSE` に設定されている場合、システムはクエリのプロファイルを収集しません。
- 導入バージョン: -

##### enable_internal_sql

- デフォルト: true
- タイプ: Boolean
- 単位: N/A
- 変更可能: No
- 説明: 有効にすると、内部コンポーネント（たとえば SimpleExecutor）によって実行される内部 SQL ステートメントが保持され、内部の監査/ログ出力に書き込まれます（enable_sql_desensitize_in_log が設定されている場合はさらに脱感作されることがあります）。無効にすると内部 SQL テキストは抑止されます：フォーマットコード（SimpleExecutor.formatSQL）は "?" を返し、実際のステートメントは内部の監査/ログメッセージに出力されません。このフラグは内部ステートメントの実行意味論を変更するものではなく、プライバシー/セキュリティのために内部 SQL のログ出力と可視性のみを制御します。フラグは変更可能ではないため、変更するには再起動が必要です。
- 導入バージョン: -

##### enable_legacy_compatibility_for_replication

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: レプリケーションのレガシー互換性を有効にするかどうか。StarRocks は古いバージョンと新しいバージョンの間で異なる動作をする可能性があり、クロスクラスタデータ移行中に問題が発生する可能性があります。したがって、データ移行の前にターゲットクラスタでレガシー互換性を有効にし、データ移行が完了した後に無効にする必要があります。`true` はこのモードを有効にすることを示します。
- 導入バージョン: v3.1.10, v3.2.6

##### enable_statistics_collect_profile

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 統計クエリのプロファイルを生成するかどうか。この項目を `true` に設定すると、StarRocks はシステム統計に関するクエリのプロファイルを生成します。
- 導入バージョン: v3.1.5

##### enable_table_name_case_insensitive

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: カタログ名、データベース名、テーブル名、ビュー名、および非同期マテリアライズドビュー名に対して、大文字小文字を区別しない処理を有効にするかどうか。現在、テーブル名はデフォルトで大文字小文字を区別します。
  - この機能を有効にすると、関連するすべての名前が小文字で保存され、これらの名前を含むすべての SQL コマンドは自動的に小文字に変換されます。  
  - この機能はクラスターを作成する際にのみ有効にできます。**クラスターが起動された後、この設定項目の値はどのような手段でも変更できません**。この設定を変更しようとするとエラーが発生します。FE は、この設定項目の値がクラスターが最初に起動された際の値と一致しない場合、起動に失敗します。  
  - 現在は、この機能は JDBC カタログ名とテーブル名をサポートしていません。JDBC または ODBC データソースで大文字小文字を区別しない処理を実行したい場合は、この機能を有効にしないでください。
- 導入バージョン: v4.0

##### enable_task_history_archive

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、終了した task-run レコードが永続的な task-run history テーブルにアーカイブされ、edit log に記録されるため、ルックアップ（例: `lookupHistory`, `lookupHistoryByTaskNames`, `lookupLastJobOfTasks`）にアーカイブされた結果が含まれます。アーカイブ処理は FE リーダーによって実行され、ユニットテスト中（`FeConstants.runningUnitTest`）はスキップされます。有効時はインメモリの有効期限処理および強制 GC の経路がバイパスされ（`removeExpiredRuns` および `forceGC` から早期に戻る）、保持/追放は `task_runs_ttl_second` や `task_runs_max_history_number` の代わりに永続的なアーカイブで管理されます。無効にすると、履歴はメモリ内に留まり、これらの設定によって剪定されます。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### enable_task_run_fe_evaluation

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、FE はシステムテーブル `task_runs` に対して `TaskRunsSystemTable.supportFeEvaluation` 内でローカル評価を行います。FE 側評価は列と定数を比較する結合された等価述語（conjunctive equality predicates）のみ許可され、対象列は `QUERY_ID` と `TASK_NAME` に制限されます。これを有効にすると、広範なスキャンや追加のリモート処理を回避してターゲットを絞ったルックアップのパフォーマンスが向上します。無効にするとプランナーは `task_runs` の FE 評価をスキップするため、述語の刈り込みが減少し、これらのフィルターに対するクエリレイテンシに影響を与える可能性があります。
- 導入バージョン: v3.3.13, v3.4.3, v3.5.0

##### heartbeat_mgr_blocking_queue_size

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ハートビートマネージャーが実行するハートビートタスクを保存するブロッキングキューのサイズ。
- 導入バージョン: -

##### heartbeat_mgr_threads_num

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ハートビートタスクを実行するためにハートビートマネージャーが実行できるスレッドの数。
- 導入バージョン: -

##### ignore_materialized_view_error

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FE がマテリアライズドビューエラーによって引き起こされたメタデータ例外を無視するかどうか。FE がマテリアライズドビューエラーによって引き起こされたメタデータ例外のために起動に失敗した場合、このパラメータを `true` に設定して FE が例外を無視することを許可できます。
- 導入バージョン: v2.5.10

##### ignore_meta_check

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非リーダー FEs がリーダー FE からのメタデータギャップを無視するかどうか。値が TRUE の場合、非リーダー FEs はリーダー FE からのメタデータギャップを無視し、データ読み取りサービスの提供を続けます。このパラメータは、リーダー FE を長時間停止している場合でも、継続的なデータ読み取りサービスを保証します。値が FALSE の場合、非リーダー FEs はリーダー FE からのメタデータギャップを無視せず、データ読み取りサービスの提供を停止します。
- 導入バージョン: -

##### master_sync_policy

- デフォルト: SYNC
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: リーダー FE がログをディスクにフラッシュするポリシー。このパラメータは、現在の FE がリーダー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときにログエントリの生成とフラッシュは同時に行われません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにフラッシュされません。

  フォロワー FE を 1 つだけデプロイした場合、このパラメータを `SYNC` に設定することをお勧めします。フォロワー FE を 3 つ以上デプロイした場合、このパラメータと `replica_sync_policy` の両方を `WRITE_NO_SYNC` に設定することをお勧めします。

- 導入バージョン: -

##### max_bdbje_clock_delta_ms

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: StarRocks クラスタ内のリーダー FE とフォロワーまたはオブザーバー FEs 間で許可される最大クロックオフセット。
- 導入バージョン: -

##### meta_delay_toleration_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: フォロワーおよびオブザーバー FEs のメタデータがリーダー FE のメタデータに遅れることができる最大期間。単位: 秒。この期間を超えると、非リーダー FEs はサービスの提供を停止します。
- 導入バージョン: -

##### meta_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: メタデータを保存するディレクトリ。
- 導入バージョン: -

##### metadata_ignore_unknown_operation_type

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 不明なログ ID を無視するかどうか。FE がロールバックされると、以前のバージョンの FEs は一部のログ ID を認識できない場合があります。値が `TRUE` の場合、FE は不明なログ ID を無視します。値が `FALSE` の場合、FE は終了します。
- 導入バージョン: -

##### profile_info_format

- デフォルト: default
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: システムが出力する Profile のフォーマット。有効な値：`default` と `json`. `default` に設定すると、Profile はデフォルトのフォーマットで出力される。`json` に設定すると、システムは JSON フォーマットで Profile を出力する。
- 導入バージョン: -

##### replica_ack_policy

- デフォルト: SIMPLE_MAJORITY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ログエントリが有効と見なされるポリシー。デフォルト値 `SIMPLE_MAJORITY` は、フォロワー FEs の過半数が ACK メッセージを返すとログエントリが有効と見なされることを指定します。
- 導入バージョン: -

##### replica_sync_policy

- デフォルト: SYNC
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: フォロワー FE がログをディスクにフラッシュするポリシー。このパラメータは、現在の FE がフォロワー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときにログエントリの生成とフラッシュは同時に行われません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにフラッシュされません。
- 導入バージョン: -

##### task_ttl_second

- デフォルト: 24 * 3600
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: タスクの存続時間（TTL）を秒単位で指定します。スケジュールが設定されていない手動タスクの場合、TaskBuilder はこの値を用いてタスクの expireTime を算出します（expireTime = now + task_ttl_second * 1000L）。TaskRun もランの実行タイムアウトを計算する際の上限としてこの値を参照します — 実効的な実行タイムアウトは min(`task_runs_timeout_second`, `task_runs_ttl_second`, `task_ttl_second`) です。この値を調整すると、手動で作成されたタスクが有効でいられる期間が変わり、間接的にタスクランの最大実行時間の上限を制限することになります。
- 導入バージョン: v3.2.0

##### txn_latency_metric_report_groups

- デフォルト: 空の文字列
- タイプ: String
- Unit: -
- 変更可能: はい
- 説明: カンマ区切りで指定する、レポート対象のトランザクション遅延メトリックグループのリスト。 ロードタイプは監視用に論理グループに分類されます。 グループを有効化すると、その名前がトランザクションメトリックの 'type' ラベルとして追加されます。 有効な値: `stream_load`, `routine_load`, `broker_load`, `insert`, `compaction` (共有データクラスタでのみ利用可能)。 例: `"stream_load,routine_load"`。
- 導入バージョン: v4.0

##### txn_rollback_limit

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ロールバックできるトランザクションの最大数。
- 導入バージョン: -

### ユーザー、ロール、および権限

##### privilege_max_role_depth

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ロールの最大ロール深度（継承レベル）。
- 導入バージョン: v3.0.0

##### privilege_max_total_roles_per_user

- デフォルト: 64
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ユーザーが持つことができる最大ロール数。
- 導入バージョン: v3.0.0

### クエリエンジン

##### connector_table_query_trigger_analyze_large_table_interval

- デフォルト: 12 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 大きなテーブルのクエリトリガー分析タスクの間隔。
- 導入バージョン: v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE 上で保留状態のクエリトリガー分析タスクの最大数。
- 導入バージョン: v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE 上で実行中のクエリトリガー分析タスクの最大数。
- 導入バージョン: v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 小さなテーブルのクエリトリガー分析タスクの間隔。
- 導入バージョン: v3.4.0

##### connector_table_query_trigger_analyze_small_table_rows

- デフォルト: 10000000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: クエリトリガー分析タスクのためにテーブルが小さなテーブルであるかどうかを判断するためのしきい値。
- 導入バージョン: v3.4.0

##### connector_table_query_trigger_task_schedule_interval

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Scheduler スレッドがクエリトリガーのバックグラウンドタスクをスケジュールする間隔。この項目は v3.4.0 で導入された `connector_table_query_trigger_analyze_schedule_interval` を置き換えるものである。ここで、バックグラウンドタスクとは、v3.4 では `ANALYZE` タスク、v3.4 より後のバージョンでは低カーディナリティ列の辞書のコレクションタスクを指す。
- 導入バージョン: v3.4.2

##### create_table_max_serial_replicas

- デフォルト: 128
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 直列に作成されるレプリカの最大数。実際のレプリカ数がこの値を超える場合、レプリカは並行して作成されます。テーブル作成に時間がかかる場合は、この値を減らすことを検討してください。
- 導入バージョン: -

##### default_mv_partition_refresh_number

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新に複数のパーティションが含まれる場合、このパラメータは、デフォルトで 1 回のバッチでいくつのパーティションを更新するかを制御します。バージョン 3.3.0 以降では、メモリ不足 (OOM) の問題を回避するため、一度に 1 つのパーティションを更新するようにデフォルト設定されています。以前のバージョンでは、デフォルトですべてのパーティションが一度にリフレッシュされたため、メモリが枯渇してタスクが失敗する可能性がありました。ただし、マテリアライズドビューのリフレッシュが多数のパーティションを含む場合、一度に 1 つのパーティションだけをリフレッシュすると、スケジューリングのオーバーヘッドが過剰になり、全体のリフレッシュ時間が長くなり、リフレッシュ・レコードの数が多くなる可能性があることに注意してください。このような場合は、このパラメータを適切に調整し、リフレッシュ効率を向上させ、スケジューリング・コストを削減することをお勧めします。
- 導入バージョン: v3.3.0

##### default_mv_refresh_immediate

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非同期マテリアライズドビューを作成直後に即座に更新するかどうか。この項目が `true` に設定されている場合、新しく作成されたマテリアライズドビューは即座に更新されます。
- 導入バージョン: v3.2.3

##### dynamic_partition_check_interval_seconds

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 新しいデータがチェックされる間隔。新しいデータが検出されると、StarRocks は自動的にデータのためにパーティションを作成します。
- 導入バージョン: -

##### dynamic_partition_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 動的パーティション化機能を有効にするかどうか。この機能が有効になっている場合、StarRocks は新しいデータのために動的にパーティションを作成し、データの新鮮さを確保するために期限切れのパーティションを自動的に削除します。
- 導入バージョン: -

##### enable_active_materialized_view_schema_strict_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非アクティブなマテリアライズドビューをアクティブ化する際に、データ型の長さの一貫性を厳密にチェックするかどうか。この項目が `false` に設定されている場合、ベーステーブルでデータ型の長さが変更されても、マテリアライズドビューのアクティブ化には影響しません。
- 導入バージョン: v3.3.4

##### enable_auto_collect_array_ndv

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ARRAY タイプの NDV 情報の自動収集を有効にするかどうか。
- 導入バージョン: v4.0

##### enable_backup_materialized_view

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 特定のデータベースをバックアップまたは復元する際に、非同期マテリアライズドビューのバックアップと復元を有効にするかどうか。この項目が `false` に設定されている場合、StarRocks は非同期マテリアライズドビューのバックアップをスキップします。
- 導入バージョン: v3.2.0

##### enable_collect_full_statistic

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 自動フル統計収集を有効にするかどうか。この機能はデフォルトで有効です。
- 導入バージョン: -

##### enable_colocate_mv_index

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 同期マテリアライズドビューを作成する際に、ベーステーブルと同期マテリアライズドビューインデックスをコロケートすることをサポートするかどうか。この項目が `true` に設定されている場合、tablet sink は同期マテリアライズドビューの書き込みパフォーマンスを向上させます。
- 導入バージョン: v3.2.0

##### enable_decimal_v3

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: DECIMAL V3 データ型をサポートするかどうか。
- 導入バージョン: -

##### enable_experimental_mv

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非同期マテリアライズドビュー機能を有効にするかどうか。TRUE はこの機能が有効であることを示します。v2.5.2 以降、この機能はデフォルトで有効になっています。v2.5.2 より前のバージョンでは、この機能はデフォルトで無効です。
- 導入バージョン: v2.4

##### enable_local_replica_selection

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリのためにローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク伝送コストを削減します。このパラメータが TRUE に設定されている場合、CBO は現在の FE と同じ IP アドレスを持つ BEs 上の tablet レプリカを優先的に選択します。このパラメータが `FALSE` に設定されている場合、ローカルレプリカと非ローカルレプリカの両方が選択される可能性があります。
- 導入バージョン: -

##### enable_manual_collect_array_ndv

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ARRAY タイプの NDV 情報の手動収集を有効にするかどうか。
- 導入バージョン: v4.0

##### enable_materialized_view

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの作成を有効にするかどうか。
- 導入バージョン: -

##### enable_materialized_view_metrics_collect

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非同期マテリアライズドビューの監視メトリクスをデフォルトで収集するかどうか。
- 導入バージョン: v3.1.11, v3.2.5

##### enable_materialized_view_spill

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新タスクに対する中間結果のスピリングを有効にするかどうか。
- 導入バージョン: v3.1.1

##### enable_materialized_view_text_based_rewrite

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: テキストベースのクエリ書き換えをデフォルトで有効にするかどうか。この項目が `true` に設定されている場合、システムは非同期マテリアライズドビューを作成する際に抽象構文ツリーを構築します。
- 導入バージョン: v3.2.5

##### enable_mv_automatic_active_check

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: スキーマ変更が行われたか、削除され再作成されたベーステーブル（ビュー）のために非アクティブに設定された非同期マテリアライズドビューをシステムが自動的にチェックし、再アクティブ化するかどうか。この機能は、ユーザーによって手動で非アクティブに設定されたマテリアライズドビューを再アクティブ化することはありません。
- 導入バージョン: v3.1.6

##### enable_predicate_columns_collection

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 述語カラムの収集を有効にするかどうか。無効にすると、クエリ最適化中に述語カラムは記録されません。
- 導入バージョン: -

##### enable_sql_blacklist

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: SQL クエリのブラックリストチェックを有効にするかどうか。この機能が有効になっている場合、ブラックリストにあるクエリは実行できません。
- 導入バージョン: -

##### enable_statistic_collect

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: CBO のために統計を収集するかどうか。この機能はデフォルトで有効です。
- 導入バージョン: -

##### enable_statistic_collect_on_first_load

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: データロード操作によってトリガーされる自動統計情報の収集とメンテナンスを制御します。これには以下が含まれます：
  - データがパーティションに初めてロードされる際の統計情報収集（パーティションバージョンが 2 の場合）。
  - マルチパーティションテーブルの空のパーティションにデータがロードされる際の統計情報収集。
  - INSERT OVERWRITE 操作における統計情報のコピーと更新。

  **統計収集タイプの決定方針：**
  
  - INSERT OVERWRITE の場合: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計情報の収集は行われません。既存の統計情報のみがコピーされます。
    - それ以外の場合、`targetRows > statistic_sample_collect_rows` (デフォルト: 200000) であれば、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  - 初回ロードの場合: `deltaRatio = loadRows / (totalRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計情報の収集は行われません。
    - それ以外の場合、`loadRows > statistic_sample_collect_rows` (デフォルト: 200000) であれば、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  **同期の動作:**
  
  - DML ステートメント（INSERT INTO/INSERT OVERWRITE）の場合：テーブルロックを伴う同期モード。ロード操作は統計情報の収集が完了するまで待機します（最大 `semi_sync_collect_statistic_await_seconds` 秒間）。
  - Stream Load および Broker Load の場合：ロックなしの非同期モード。統計情報の収集はバックグラウンドで実行され、ロード操作をブロックしません。
  
  :::note
  この設定を無効にすると、INSERT OVERWRITE の統計情報メンテナンスを含む、ロードによってトリガーされるすべての統計情報操作が防止されます。これにより、テーブルに統計情報が不足する可能性があります。新しいテーブルが頻繁に作成され、データが頻繁にロードされる場合、この機能を有効にするとメモリと CPU のオーバーヘッドが増加します。
  :::

- 導入バージョン: v3.1

##### enable_statistic_collect_on_update

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: UPDATE ステートメントが自動統計収集をトリガーできるかどうかを制御します。有効にすると、テーブルデータを変更する UPDATE 操作は、`enable_statistic_collect_on_first_load` によって制御されるインジェスションベースの統計フレームワークを通じて統計収集をスケジュールする場合があります。この設定を無効にすると、ロードによってトリガーされる統計収集の動作はそのままに、UPDATE ステートメントの統計収集がスキップされます。
- 導入バージョン: v3.5.11, v4.0.4

##### enable_udf

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: UDF を有効にするかどうか。
- 導入バージョン: -

##### expr_children_limit

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 式内で許可される子式の最大数。
- 導入バージョン: -

##### histogram_buckets_size

- デフォルト: 64
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのデフォルトバケット数。
- 導入バージョン: -

##### histogram_max_sample_row_count

- デフォルト: 10000000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのために収集する最大行数。
- 導入バージョン: -

##### histogram_mcv_size

- デフォルト: 100
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムの最も一般的な値 (MCV) の数。
- 導入バージョン: -

##### histogram_sample_ratio

- デフォルト: 0.1
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのサンプリング比率。
- 導入バージョン: -

##### http_slow_request_threshold_ms

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: HTTP リクエストの応答時間がこのパラメータで指定された値を超える場合、このリクエストを追跡するためのログが生成されます。
- 導入バージョン: v2.5.15, v3.1.5

##### low_cardinality_threshold

- デフォルト: 255
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 低基数辞書のしきい値。
- 導入バージョン: v3.5.0

##### max_allowed_in_element_num_of_delete

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: DELETE ステートメントの IN 述語に許可される要素の最大数。
- 導入バージョン: -

##### max_create_table_timeout_second

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: テーブル作成の最大タイムアウト期間。
- 導入バージョン: -

##### max_distribution_pruner_recursion_depth

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: パーティションプルーニングで許可される最大再帰深度。再帰深度を増やすことで、より多くの要素をプルーニングできますが、CPU 消費も増加します。
- 導入バージョン: -

##### max_partitions_in_one_batch

- デフォルト: 4096
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: パーティションを一括作成する際に作成できる最大パーティション数。
- 導入バージョン: -

##### max_planner_scalar_rewrite_num

- デフォルト: 100000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: オプティマイザがスカラーオペレーターを書き換える最大回数。
- 導入バージョン: -

##### max_query_retry_time

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE でのクエリの最大再試行回数。
- 導入バージョン: -

##### max_running_rollup_job_num_per_table

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルに対して並行して実行できるロールアップジョブの最大数。
- 導入バージョン: -

##### max_scalar_operator_flat_children

- デフォルト: 256
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ScalarOperator のフラットチルドレンの最大数。この上限を設定することで、オプティマイザがメモリを使いすぎるのを防ぐことができます。
- 導入バージョン: -

##### max_scalar_operator_optimize_depth

- デフォルト: 256
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ScalarOperator 最適化を適用できる最大深度。
- 導入バージョン: -

##### mv_active_checker_interval_seconds

- デフォルト: 60
- タイプ: Long
- 単位: Seconds
- 変更可能: はい
- 説明: バックグラウンドのactive_checkerスレッドが有効な場合、スキーマの変更やベーステーブル（またはビュー）の再構築により非アクティブになったマテリアライズドビューが定期的に検出され、自動的に再アクティブ化されます。このパラメータはチェッカスレッドのスケジューリング間隔を秒単位で制御します。デフォルト値はシステム定義です。
- 導入バージョン: v3.1.6

##### publish_version_interval_ms

- デフォルト: 10
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: リリース検証タスクが発行される時間間隔。
- 導入バージョン: -

##### query_queue_v2_cpu_costs_per_slot

- デフォルト: 1000000000
- タイプ: Long
- 単位: planner CPU cost units
- 変更可能: Yes
- 説明: プランナーの CPU コストからクエリが必要とするスロット数を推定するために使用される、スロットあたりの CPU コスト閾値です。スケジューラはスロット数を integer(plan_cpu_costs / `query_queue_v2_cpu_costs_per_slot`) として計算し、その結果を [1, totalSlots] の範囲にクランプします（totalSlots はクエリキュー V2 の `V2` パラメータから導出されます）。V2 のコードは非正の設定を 1 に正規化します（Math.max(1, value)）ので、非正の値は実質的に `1` になります。この値を増やすとクエリあたりに割り当てられるスロット数が減り（少数の大きなスロットを持つクエリが有利になります）、逆に減らすとクエリあたりのスロット数が増えます。並列度とリソース粒度を制御するために、`query_queue_v2_num_rows_per_slot` や同時実行設定と併せてチューニングしてください。
- 導入バージョン: v3.3.4, v3.4.0, v3.5.0

##### semi_sync_collect_statistic_await_seconds

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: DML 操作（INSERT INTO および INSERT OVERWRITE ステートメント）中の半同期統計収集の最大待機時間。Stream Load および Broker Load は非同期モードを使用するため、この設定の影響を受けません。統計収集時間がこの値を超える場合、ロード操作は収集完了を待たずに続行されます。この設定は `enable_statistic_collect_on_first_load` と連動して機能します。
- 導入バージョン: v3.1

##### slow_query_analyze_threshold

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: クエリフィードバックの分析をトリガーするクエリの実行時間しきい値。
- 導入バージョン: v3.4.0

##### statistic_analyze_status_keep_second

- デフォルト: 3 * 24 * 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 収集タスクの履歴を保持する期間。デフォルト値は 3 日です。
- 導入バージョン: -

##### statistic_auto_analyze_end_time

- デフォルト: 23:59:59
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 自動収集の終了時間。値の範囲: `00:00:00` - `23:59:59`。
- 導入バージョン: -

##### statistic_auto_analyze_start_time

- デフォルト: 00:00:00
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 自動収集の開始時間。値の範囲: `00:00:00` - `23:59:59`。
- 導入バージョン: -

##### statistic_auto_collect_ratio

- デフォルト: 0.8
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 自動収集の統計が健全かどうかを判断するためのしきい値。統計の健全性がこのしきい値を下回る場合、自動収集がトリガーされます。
- 導入バージョン: -

##### statistic_auto_collect_small_table_rows

- デフォルト: 10000000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 自動収集中に外部データソース (Hive, Iceberg, Hudi) のテーブルが小さなテーブルであるかどうかを判断するためのしきい値。テーブルの行数がこの値未満の場合、テーブルは小さなテーブルと見なされます。
- 導入バージョン: v3.2

##### statistic_cache_columns

- デフォルト: 100000
- タイプ: Long
- 単位: -
- 変更可能: いいえ
- 説明: 統計テーブルにキャッシュできる行数。
- 導入バージョン: -

##### statistic_cache_thread_pool_size

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 統計キャッシュを更新するために使用されるスレッドプールのサイズ。
- 導入バージョン: -

##### statistic_collect_interval_sec

- デフォルト: 5 * 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 自動収集中にデータの更新をチェックする間隔。
- 導入バージョン: -

##### statistic_max_full_collect_data_size

- デフォルト: 100 * 1024 * 1024 * 1024
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: 統計の自動収集のデータサイズしきい値。合計サイズがこの値を超える場合、フル収集の代わりにサンプリング収集が実行されます。
- 導入バージョン: -

##### statistic_sample_collect_rows

- デフォルト: 200000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: サンプリング収集のために収集する最小行数。パラメータ値がテーブルの実際の行数を超える場合、フル収集が実行されます。
- 導入バージョン: -

##### statistic_update_interval_sec

- デフォルト: 24 * 60 * 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 統計情報のキャッシュが更新される間隔。
- 導入バージョン: -

### ロードとアンロード

##### broker_load_default_timeout_second

- デフォルト: 14400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Broker Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### desired_max_waiting_jobs

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE における保留中ジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FE の保留中ジョブの数がこの値に達すると、FE は新しいロードリクエストを拒否します。このパラメータは非同期ロードにのみ有効です。v2.5 以降、デフォルト値は 100 から 1024 に変更されました。
- 導入バージョン: -

##### disable_load_job

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラスタがエラーに遭遇したときにロードを無効にするかどうか。これにより、クラスタエラーによる損失を防ぎます。デフォルト値は `FALSE` で、ロードが無効になっていないことを示します。`TRUE` はロードが無効になり、クラスタが読み取り専用状態であることを示します。
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

##### enable_file_bundling

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラウドネイティブテーブルに対してファイルバンドリング最適化を有効にするかどうか。この機能を有効に設定（`true` に設定）すると、システムはロード、コンパクション、またはパブリッシュ操作によって生成されたデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによる API コストを削減します。この動作は、CREATE TABLE プロパティ `file_bundling` を使用してテーブルレベルで制御することもできます。詳細な手順については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。
- 導入バージョン: v4.0

##### enable_routine_load_lag_metrics

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Routine Load パーティションのオフセットラグをメトリクスで収集するかどうか。この項目を `true` に設定すると、Kafka API を呼び出してパーティションの最新のオフセットを取得することに注意。
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

##### export_checker_interval_second

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがスケジュールされる時間間隔。
- 導入バージョン: -

##### export_max_bytes_per_be_per_task

- デフォルト: 268435456
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: 単一の BE から単一のデータアンロードタスクによってエクスポートされる最大データ量。
- 導入バージョン: -

##### export_running_job_num_limit

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 並行して実行できるデータエクスポートタスクの最大数。
- 導入バージョン: -

##### export_task_default_timeout_second

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データエクスポートタスクのタイムアウト期間。
- 導入バージョン: -

##### export_task_pool_size

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: アンロードタスクスレッドプールのサイズ。
- 導入バージョン: -

##### external_table_commit_timeout_ms

- デフォルト: 10000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: StarRocks 外部テーブルへの書き込みトランザクションをコミット（公開）するためのタイムアウト期間。デフォルト値 `10000` は 10 秒のタイムアウト期間を示します。
- 導入バージョン: -

##### finish_transaction_default_lock_timeout_ms

- デフォルト: 1000
- タイプ: Int
- 単位: MilliSeconds
- 変更可能: Yes
- 説明: トランザクションを完了する際に db および table ロックを取得するためのデフォルトのタイムアウト。
- 導入バージョン: v4.0.0, v3.5.8

##### history_job_keep_max_second

- デフォルト: 7 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スキーマ変更ジョブなどの履歴ジョブを保持できる最大期間。
- 導入バージョン: -

##### insert_load_default_timeout_second

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データをロードするために使用される INSERT INTO ステートメントのタイムアウト期間。
- 導入バージョン: -

##### label_clean_interval_second

- デフォルト: 4 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### label_keep_max_num

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報が削除されます。
- 導入バージョン: -

##### label_keep_max_second

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 完了したロードジョブのラベルを保持する最大期間（FINISHED または CANCELLED 状態）。デフォルト値は 3 日です。この期間が経過すると、ラベルは削除されます。このパラメータはすべてのタイプのロードジョブに適用されます。値が大きすぎると、多くのメモリを消費します。
- 導入バージョン: -

##### load_checker_interval_second

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがローリングベースで処理される時間間隔。
- 導入バージョン: -

##### load_straggler_wait_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BE レプリカによって許容される最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするためにクローンが実行されます。
- 導入バージョン: -

##### loads_history_retained_days

- デフォルト: 30
- タイプ: Int
- 単位: Days
- 変更可能: Yes
- 説明: 内部テーブル `_statistics_.loads_history` におけるロード履歴の保持日数。この値はテーブル作成時にテーブルプロパティ `partition_live_number` を設定するために使用され、`TableKeeper` に渡され（日数は最小1にクランプされます）、保持する日別パーティション数を決定します。この値を増減すると完了したロードジョブが日別パーティションにどの程度残るかが調整されます；新規テーブル作成とTableKeeperの削除挙動に影響しますが、過去のパーティションを自動的に再作成することはありません。`LoadsHistorySyncer` はロード履歴のライフサイクル管理にこの保持設定を利用します；同期の頻度は `loads_history_sync_interval_second` によって制御されます。
- 導入バージョン: v3.3.6, v3.4.0, v3.5.0

##### max_broker_load_job_concurrency

- デフォルト: 5
- 別名: async_load_task_pool_size
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスタ内で許可される最大同時 Broker Load ジョブ数。このパラメータは Broker Load にのみ有効です。このパラメータの値は `max_running_txn_num_per_db` の値より小さくなければなりません。v2.5 以降、デフォルト値は `10` から `5` に変更されました。
- 導入バージョン: -

##### max_load_timeout_second

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべてのタイプのロードジョブに適用されます。
- 導入バージョン: -

##### max_routine_load_batch_size

- デフォルト: 4294967296
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: Routine Load タスクによってロードされる最大データ量。
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

##### max_running_txn_num_per_db

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスタ内の各データベースで実行中のロードトランザクションの最大数。デフォルト値は `1000` です。v3.1 以降、デフォルト値は `100` から `1000` に変更されました。データベースで実行中のロードトランザクションの実際の数がこのパラメータの値を超える場合、新しいロードリクエストは処理されません。同期ロードジョブの新しいリクエストは拒否され、非同期ロードジョブの新しいリクエストはキューに入れられます。このパラメータの値を増やすことはお勧めしません。システム負荷が増加する可能性があります。
- 導入バージョン: -

##### max_stream_load_timeout_second

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Stream Load ジョブの最大許容タイムアウト期間。
- 導入バージョン: -

##### max_tolerable_backend_down_num

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許容される故障 BE ノードの最大数。この数を超えると、Routine Load ジョブは自動的に回復できません。
- 導入バージョン: -

##### min_bytes_per_broker_scanner

- デフォルト: 67108864
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: Broker Load インスタンスによって処理されることができる最小許容データ量。
- 導入バージョン: -

##### min_load_timeout_second

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最小タイムアウト期間。この制限はすべてのタイプのロードジョブに適用されます。
- 導入バージョン: -

##### min_routine_load_lag_for_metrics

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 監視メトリクスに表示される Routine Load ジョブの最小オフセットラグ。オフセットラグがこの値より大きい Routine Load ジョブは、メトリクスに表示されます。
- 導入バージョン: -

##### period_of_auto_resume_min

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 変更可能: はい
- 説明: Routine Load ジョブが自動的に回復される間隔。
- 導入バージョン: -

##### prepared_transaction_default_timeout_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 準備済みトランザクションのデフォルトのタイムアウト期間。
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

##### spark_dpp_version

- デフォルト: 1.0.0
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用される Spark Dynamic Partition Pruning (DPP) のバージョン。
- 導入バージョン: -

##### spark_home_default_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark クライアントのルートディレクトリ。
- 導入バージョン: -

##### spark_launcher_log_dir

- デフォルト: sys_log_dir + "/spark_launcher_log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark ログファイルを保存するディレクトリ。
- 導入バージョン: -

##### spark_load_default_timeout_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各 Spark Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### spark_resource_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark 依存パッケージのルートディレクトリ。
- 導入バージョン: -

##### stream_load_default_timeout_second

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各 Stream Load ジョブのデフォルトタイムアウト期間。
- 導入バージョン: -

##### stream_load_task_keep_max_num

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: StreamLoadMgrがメモリ内で保持するStream Loadタスクの最大数（全データベースに跨るグローバルな設定）。追跡中のタスク数（`idToStreamLoadTask`）がこの閾値を超えると、StreamLoadMgrはまず `cleanSyncStreamLoadTasks()` を呼び出して完了した同期ストリームロードタスクを削除します；それでもサイズがこの閾値の半分より大きいままの場合、`cleanOldStreamLoadTasks(true)` を呼び出して古いまたは終了したタスクを強制的に削除します。メモリ内により多くのタスク履歴を保持したい場合はこの値を増やし、メモリ使用量を減らしてクリーンアップをより積極的に行いたい場合はこの値を減らしてください。この値はメモリ内での保持にのみ影響し、永続化／リプレイされたタスクには影響しません。
- 導入バージョン: v3.2.0

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

### 統計レポート

### ストレージ

##### alter_table_timeout_second

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スキーマ変更操作 (ALTER TABLE) のタイムアウト期間。
- 導入バージョン: -

##### catalog_trash_expire_second

- デフォルト: 86400
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: データベース、テーブル、またはパーティションが削除された後にメタデータが保持される最長期間。この期間が経過すると、データは削除され、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) コマンドを使用して回復することはできません。
- 導入バージョン: -

##### check_consistency_default_timeout_second

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカの整合性チェックのタイムアウト期間。tablet のサイズに基づいてこのパラメータを設定できます。
- 導入バージョン: -

##### default_replication_num

- デフォルト: 3
- タイプ: Short
- 単位: -
- 変更可能: はい
- 説明: StarRocks でテーブルを作成する際に各データパーティションのデフォルトのレプリカ数を設定します。この設定は、CREATE TABLE DDL で `replication_num=x` を指定することでオーバーライドできます。
- 導入バージョン: -

##### enable_auto_tablet_distribution

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: バケット数を自動的に設定するかどうか。
  - このパラメータが `TRUE` に設定されている場合、テーブルを作成する際やパーティションを追加する際にバケット数を指定する必要はありません。StarRocks は自動的にバケット数を決定します。
  - このパラメータが `FALSE` に設定されている場合、テーブルを作成する際やパーティションを追加する際にバケット数を手動で指定する必要があります。新しいパーティションをテーブルに追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションのバケット数を手動で指定することもできます。
- 導入バージョン: v2.5.7

##### enable_experimental_rowstore

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: [行と列のハイブリッドストレージ](../../table_design/hybrid_table.md) 機能を有効にするかどうか。
- 導入バージョン: v3.2.3

##### enable_fast_schema_evolution

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスタ内のすべてのテーブルに対して高速スキーマ進化を有効にするかどうか。有効な値は `TRUE` と `FALSE` (デフォルト) です。高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加や削除時のリソース使用量が減少します。
- 導入バージョン: v3.2.0

> **NOTE**
>
> - StarRocks 共有データクラスタは v3.3.0 からこのパラメータをサポートしています。
> - 特定のテーブルに対して高速スキーマ進化を設定する必要がある場合、たとえば特定のテーブルに対して高速スキーマ進化を無効にする場合、テーブル作成時にテーブルプロパティ [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) を設定できます。

##### enable_online_optimize_table

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: StarRocks が optimize ジョブを作成する際に、書き込みをブロックしないオンライン最適化パスを使用するかどうかを制御します。`enable_online_optimize_table` が true で、対象テーブルが互換性チェックを満たす場合（パーティション/キー/ソート指定がなく、distribution が `RandomDistributionDesc` でない、ストレージタイプが `COLUMN_WITH_ROW` でない、レプリケートされたストレージが有効で、テーブルがクラウドネイティブテーブルやマテリアライズドビューでない）、プランナーは書き込みをブロックせずに最適化を行うために `OnlineOptimizeJobV2` を作成します。false の場合、またはいずれかの互換性条件を満たさない場合、最適化中に書き込みをブロックする可能性のある `OptimizeJobV2` にフォールバックします。
- 導入バージョン: v3.3.3, v3.4.0, v3.5.0

##### enable_strict_storage_medium_check

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ユーザーがテーブルを作成する際に、FE が BEs の記憶媒体を厳密にチェックするかどうか。このパラメータが `TRUE` に設定されている場合、ユーザーがテーブルを作成する際に FE は BEs の記憶媒体をチェックし、CREATE TABLE ステートメントで指定された `storage_medium` パラメータと異なる場合はエラーを返します。たとえば、CREATE TABLE ステートメントで指定された記憶媒体が SSD であるが、BEs の実際の記憶媒体が HDD である場合、テーブル作成は失敗します。このパラメータが `FALSE` の場合、ユーザーがテーブルを作成する際に FE は BEs の記憶媒体をチェックしません。
- 導入バージョン: -

##### max_bucket_number_per_partition

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: パーティションに作成できる最大バケット数。
- 導入バージョン: v3.3.2

##### max_column_number_per_table

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルに作成できる最大列数。
- 導入バージョン: v3.3.2

##### max_partition_number_per_table

- デフォルト: 100000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルに作成できる最大パーティション数。
- 導入バージョン: v3.3.2

##### partition_recycle_retention_period_secs

- デフォルト: 1800
- イプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: INSERT OVERWRITE またはマテリアライズドビューのリフレッシュ操作によって削除されるパーティションのメタデータ保持期間。このようなメタデータは、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) を実行しても復元できないことに注意してください。
- 導入バージョン: v3.5.9

##### recover_with_empty_tablet

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 失われたまたは破損した tablet レプリカを空のものに置き換えるかどうか。tablet レプリカが失われたり破損したりすると、この tablet または他の正常な tablets に対するデータクエリが失敗する可能性があります。失われたまたは破損した tablet レプリカを空の tablet に置き換えることで、クエリを実行し続けることができます。ただし、データが失われているため、結果が正しくない可能性があります。デフォルト値は `FALSE` で、失われたまたは破損した tablet レプリカは空のものに置き換えられず、クエリは失敗します。
- 導入バージョン: -

##### storage_usage_hard_limit_percent

- デフォルト: 95
- 別名: storage_flood_stage_usage_percent
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ディレクトリ内のストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、残りのストレージスペースが `storage_usage_hard_limit_reserve_bytes` より少ない場合、Load および Restore ジョブは拒否されます。この項目を BE 設定項目 `storage_flood_stage_usage_percent` と一緒に設定する必要があります。
- 導入バージョン: -

##### storage_usage_hard_limit_reserve_bytes

- デフォルト: 100 * 1024 * 1024 * 1024
- 別名: storage_flood_stage_left_capacity_bytes
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BE ディレクトリ内の残りストレージスペースのハードリミット。BE ストレージディレクトリの残りストレージスペースがこの値より少なく、ストレージ使用率（パーセンテージ）が `storage_usage_hard_limit_percent` を超える場合、Load および Restore ジョブは拒否されます。この項目を BE 設定項目 `storage_flood_stage_left_capacity_bytes` と一緒に設定する必要があります。
- 導入バージョン: -

##### storage_usage_soft_limit_percent

- デフォルト: 90
- 別名: storage_high_watermark_usage_percent
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ディレクトリ内のストレージ使用率のソフトリミット。BE ストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、残りのストレージスペースが `storage_usage_soft_limit_reserve_bytes` より少ない場合、tablets はこのディレクトリにクローンできません。
- 導入バージョン: -

##### storage_usage_soft_limit_reserve_bytes

- デフォルト: 200 * 1024 * 1024 * 1024
- 別名: storage_min_left_capacity_bytes
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BE ディレクトリ内の残りストレージスペースのソフトリミット。BE ストレージディレクトリの残りストレージスペースがこの値より少なく、ストレージ使用率（パーセンテージ）が `storage_usage_soft_limit_percent` を超える場合、tablets はこのディレクトリにクローンできません。
- 導入バージョン: -

##### tablet_checker_lock_time_per_cycle_ms

- デフォルト: 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: Tablet Checker がテーブルロックを解放して再取得する前の、サイクルごとの最大ロック保持時間（ミリ秒）。100 未満の値は 100 として扱われます。
- 導入バージョン: v3.5.9, v4.0.2

##### tablet_create_timeout_second

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: tablet 作成のタイムアウト期間。デフォルト値は v3.1 以降、1 から 10 に変更されました。
- 導入バージョン: -

##### tablet_delete_timeout_second

- デフォルト: 2
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: tablet 削除のタイムアウト期間。
- 導入バージョン: -

##### tablet_sched_balance_load_disk_safe_threshold

- デフォルト: 0.5
- 別名: balance_load_disk_safe_threshold
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BEs のディスク使用率がバランスされているかどうかを判断するためのパーセンテージしきい値。すべての BEs のディスク使用率がこの値より低い場合、バランスされていると見なされます。ディスク使用率がこの値を超え、最高と最低の BE ディスク使用率の差が 10% を超える場合、ディスク使用率はバランスされていないと見なされ、tablet の再バランスがトリガーされます。
- 導入バージョン: -

##### tablet_sched_balance_load_score_threshold

- デフォルト: 0.1
- 別名: balance_load_score_threshold
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BE の負荷がバランスされているかどうかを判断するためのパーセンテージしきい値。BE の負荷がすべての BEs の平均負荷より低く、その差がこの値を超える場合、この BE は低負荷状態にあります。逆に、BE の負荷が平均負荷より高く、その差がこの値を超える場合、この BE は高負荷状態にあります。
- 導入バージョン: -

##### tablet_sched_be_down_tolerate_time_s

- デフォルト: 900
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: スケジューラが BE ノードの非アクティブ状態を許容する最大期間。この時間のしきい値に達すると、その BE ノード上の tablets は他のアクティブな BE ノードに移行されます。
- 導入バージョン: v2.5.7

##### tablet_sched_disable_balance

- デフォルト: false
- 別名: disable_balance
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: tablet のバランスを無効にするかどうか。`TRUE` は tablet のバランスが無効であることを示します。`FALSE` は tablet のバランスが有効であることを示します。
- 導入バージョン: -

##### tablet_sched_disable_colocate_balance

- デフォルト: false
- 別名: disable_colocate_balance
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Colocate Table のレプリカバランスを無効にするかどうか。`TRUE` はレプリカバランスが無効であることを示します。`FALSE` はレプリカバランスが有効であることを示します。
- 導入バージョン: -

##### tablet_sched_max_balancing_tablets

- デフォルト: 500
- 別名: max_balancing_tablets
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にバランスを取ることができる tablet の最大数。この値を超えると、tablet の再バランスがスキップされます。
- 導入バージョン: -

##### tablet_sched_max_clone_task_timeout_sec

- デフォルト: 2 * 60 * 60
- 別名: max_clone_task_timeout_sec
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: tablet をクローンするための最大タイムアウト期間。
- 導入バージョン: -

##### tablet_sched_max_not_being_scheduled_interval_ms

- デフォルト: 15 * 60 * 1000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: tablet クローンタスクがスケジュールされている場合、このパラメータで指定された時間内に tablet がスケジュールされていない場合、StarRocks はできるだけ早くスケジュールする優先順位を与えます。
- 導入バージョン: -

##### tablet_sched_max_scheduling_tablets

- デフォルト: 10000
- 別名: max_scheduling_tablets
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にスケジュールできる tablet の最大数。この値を超えると、tablet のバランスと修復チェックがスキップされます。
- 導入バージョン: -

##### tablet_sched_min_clone_task_timeout_sec

- デフォルト: 3 * 60
- 別名: min_clone_task_timeout_sec
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: tablet をクローンするための最小タイムアウト期間。
- 導入バージョン: -

##### tablet_sched_num_based_balance_threshold_ratio

- デフォルト: 0.5
- 別名: -
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 数に基づくバランスを行うと、ディスクサイズのバランスが崩れる可能性がありますが、ディスク間の最大ギャップは tablet_sched_num_based_balance_threshold_ratio * tablet_sched_balance_load_score_threshold を超えることはできません。クラスタ内の tablets が A から B に、B から A に絶えずバランスを取っている場合、この値を減らしてください。tablet の分布をよりバランスさせたい場合、この値を増やしてください。
- 導入バージョン: - 3.1

##### tablet_sched_repair_delay_factor_second

- デフォルト: 60
- 別名: tablet_repair_delay_factor_second
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカが修復される間隔（秒単位）。
- 導入バージョン: -

##### tablet_sched_slot_num_per_path

- デフォルト: 8
- 別名: schedule_slot_num_per_path
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ストレージディレクトリ内で同時に実行できる tablet 関連タスクの最大数。v2.5 以降、このパラメータのデフォルト値は `4` から `8` に変更されました。
- 導入バージョン: -

##### tablet_sched_storage_cooldown_second

- デフォルト: -1
- 別名: storage_cooldown_second
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: テーブル作成時から自動冷却が開始されるまでの遅延。デフォルト値 `-1` は自動冷却が無効であることを指定します。自動冷却を有効にする場合、このパラメータを `-1` より大きい値に設定します。
- 導入バージョン: -

##### tablet_stat_update_interval_second

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: FE が各 BE から tablet 統計を取得する時間間隔。
- 導入バージョン: -

### 共有データ

##### aws_s3_access_key

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3 バケットにアクセスするために使用されるアクセスキー ID。
- 導入バージョン: v3.0

##### aws_s3_endpoint

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3 バケットにアクセスするために使用されるエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。
- 導入バージョン: v3.0

##### aws_s3_external_id

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID。
- 導入バージョン: v3.0

##### aws_s3_iam_role_arn

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データファイルが保存されている S3 バケットに対する権限を持つ IAM ロールの ARN。
- 導入バージョン: v3.0

##### aws_s3_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用される S3 パス。S3 バケットの名前とその下のサブパス（存在する場合）で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.0

##### aws_s3_region

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3 バケットが存在するリージョン。例: `us-west-2`。
- 導入バージョン: v3.0

##### aws_s3_secret_key

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3 バケットにアクセスするために使用されるシークレットアクセスキー。
- 導入バージョン: v3.0

##### aws_s3_use_aws_sdk_default_behavior

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: AWS SDK のデフォルト認証資格情報を使用するかどうか。有効な値: true および false (デフォルト)。
- 導入バージョン: v3.0

##### aws_s3_use_instance_profile

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: S3 にアクセスするための資格情報メソッドとしてインスタンスプロファイルとアサインドロールを使用するかどうか。有効な値: true および false (デフォルト)。
  - IAM ユーザーに基づく資格情報（アクセスキーとシークレットキー）を使用して S3 にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。
  - インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に指定する必要があります。
  - アサインドロールを使用して S3 にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。
  - 外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。
- 導入バージョン: v3.0

##### azure_adls2_endpoint

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 アカウントのエンドポイント、例えば `https://test.dfs.core.windows.net`。
- 導入バージョン: v3.4.1

##### azure_adls2_oauth2_client_id

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 へのリクエストを認証するために使用される Managed Identity の Client ID。
- 導入バージョン: v3.4.4

##### azure_adls2_oauth2_tenant_id

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 へのリクエストを認証するために使用される Managed Identity の Tenant ID。
- 導入バージョン: v3.4.4

##### azure_adls2_oauth2_use_managed_identity

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 へのリクエストを認証するために Managed Identity を使用するかどうか。
- 導入バージョン: v3.4.4

##### azure_adls2_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 のデータ保存に使用するパス。ファイルシステム名とディレクトリ名で構成され、例えば `testfilesystem/starrocks` のようになる。
- 導入バージョン: v3.4.1

##### azure_adls2_sas_token

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 へのリクエストを承認するために使用される共有アクセス署名（SAS）。
- 導入バージョン: v3.4.1

##### azure_adls2_shared_key

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2 へのリクエストを承認するために使用される Shared Key。
- 導入バージョン: v3.4.1

##### azure_blob_endpoint

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storage アカウントのエンドポイント。例: `https://test.blob.core.windows.net`。
- 導入バージョン: v3.1

##### azure_blob_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用される Azure Blob Storage パス。ストレージアカウント内のコンテナの名前と、その下のサブパス（存在する場合）で構成されます。例: `testcontainer/subpath`。
- 導入バージョン: v3.1

##### azure_blob_sas_token

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storage のリクエストを承認するために使用される共有アクセス署名 (SAS)。
- 導入バージョン: v3.1

##### azure_blob_shared_key

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storage のリクエストを承認するために使用される共有キー。
- 導入バージョン: v3.1

##### azure_use_native_sdk

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Azure Blob Storage へのアクセスにネイティブ SDK を使用し、Managed Identity と Service Principal による認証を許可するかどうか。この項目を `false` に設定すると、Shared Key と SAS Token による認証のみが許可される。
- 導入バージョン: v3.4.4

##### cloud_native_hdfs_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能か：いいえ
- 説明: HDFS ストレージの URL。例: `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 導入バージョン: -

##### cloud_native_meta_port

- デフォルト: 6090
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FE クラウドネイティブメタデータサーバー RPC リッスンポート。
- 導入バージョン: -

##### cloud_native_storage_type

- デフォルト: S3
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocksは、HDFS、Azure Blob（v3.1.1以降でサポート）、Azure Data Lake Storage Gen2（v3.4.1以降でサポート）、Google Storage（ネイティブ SDK は v3.5.1 以降でサポート）、およびS3プロトコルと互換性のあるオブジェクトストレージシステム（AWS S3、MinIOなど）へのデータ保存をサポートしています。有効な値: `S3` (デフォルト) 、`HDFS`、`AZBLOB`、`ADLS2`、`GS`。このパラメータを `S3` に指定する場合、`aws_s3` で始まるパラメータを追加する必要があります。このパラメータを `AZBLOB` に指定する場合、`azure_blob` で始まるパラメータを追加する必要があります。このパラメータを `ADLS2` に指定する場合、`azure_adls2` で始まるパラメータを追加する必要があります。このパラメータを `GS` に指定する場合、`gcp_gcs` で始まるパラメータを追加する必要があります。このパラメータを `HDFS` に指定する場合、`cloud_native_hdfs_url` を追加する必要があります。
- 導入バージョン: -

##### enable_load_volume_from_conf

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks が FE 設定ファイルに指定されたオブジェクトストレージ関連のプロパティを使用して組み込みストレージボリュームを作成できるかどうか。デフォルト値は v3.4.1 以降、`true` から `false` に変更されました。
- 導入バージョン: v3.1.0

##### gcp_gcs_impersonation_service_account

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: なりすましベースの認証を使用する場合、なりすます Service Account です。
- 導入バージョン: v3.5.1

##### gcp_gcs_path

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用される Google Cloud パス。Google Cloud バケットの名前とその下のサブパス（存在する場合）で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.5.1

##### gcp_gcs_service_account_email

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account 作成時に生成された JSON ファイル内のメールアドレスです。例：`user@hello.iam.gserviceaccount.com`。
- 導入バージョン: v3.5.1

##### gcp_gcs_service_account_private_key

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account 作成時に生成された JSON ファイル内の秘密鍵です。例：`-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 導入バージョン: v3.5.1

##### gcp_gcs_service_account_private_key_id

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account 作成時に生成された JSON ファイル内の秘密鍵 ID です。
- 導入バージョン: v3.5.1

##### gcp_gcs_use_compute_engine_service_account

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Compute Engine にバインドされている Service Account を使用するかどうか。
- 導入バージョン: v3.5.1

##### lake_autovacuum_grace_period_minutes

- デフォルト: 30
- タイプ: Long
- 単位: 分
- 変更可能: はい
- 説明: 共有データクラスタで履歴データバージョンを保持する時間範囲。この時間範囲内の履歴データバージョンは、Compactions 後に AutoVacuum によって自動的にクリーンアップされません。実行中のクエリによってアクセスされるデータがクエリの終了前に削除されないようにするために、この値を最大クエリ時間より大きく設定する必要があります。デフォルト値は v3.3.0、v3.2.5、および v3.1.10 以降、`5` から `30` に変更されました。
- 導入バージョン: v3.1.0

##### lake_autovacuum_parallel_partitions

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 共有データクラスタで同時に AutoVacuum を受けることができるパーティションの最大数。AutoVacuum は Compactions 後のガーベジコレクションです。
- 導入バージョン: v3.1.0

##### lake_autovacuum_partition_naptime_seconds

- デフォルト: 180
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 共有データクラスタで同じパーティションに対して AutoVacuum 操作が行われる最小間隔。
- 導入バージョン: v3.1.0

##### lake_autovacuum_stale_partition_threshold

- デフォルト: 12
- タイプ: Long
- 単位: 時間
- 変更可能: はい
- 説明: パーティションがこの時間範囲内で更新（ロード、DELETE、または Compactions）がない場合、システムはこのパーティションに対して AutoVacuum を実行しません。
- 導入バージョン: v3.1.0

##### lake_compaction_allow_partial_success

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目を `true` に設定すると、サブタスクの 1 つが成功したときに共有データクラスタでのコンパクション操作が成功したとみなす。
- 導入バージョン: v3.5.2

##### lake_compaction_disable_ids

- デフォルト: ""
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでCompactionが無効になっているテーブルまたはパーティションのリスト。形式は `tableId1;partitionId2` で、セミコロンで区切ります。例: `12345;98765`。
- 導入バージョン: v3.4.4

##### lake_compaction_history_size

- デフォルト: 20
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタの Leader FE ノードのメモリに保持される最近の成功した Compaction タスク記録の数。`SHOW PROC '/compactions'` コマンドを使用して、最近の成功した Compaction タスク記録を表示できます。Compaction の履歴は FE プロセスメモリに保存され、FE プロセスが再起動されると失われます。
- 導入バージョン: v3.1.0

##### lake_compaction_max_tasks

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタで許可される最大同時 Compaction タスク数。この項目を `-1` に設定すると、同時タスク数が適応的に計算されます。この値を `0` に設定すると、Compaction が無効になります。
- 導入バージョン: v3.1.0

##### lake_compaction_score_selector_min_score

- デフォルト: 10.0
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタで Compaction 操作をトリガーする Compaction Score のしきい値。パーティションの Compaction Score がこの値以上の場合、システムはそのパーティションで Compaction を実行します。
- 導入バージョン: v3.1.0

##### lake_compaction_score_upper_bound

- デフォルト: 2000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでのパーティションの Compaction Score の上限。`0` は上限がないことを示します。この項目は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効です。パーティションの Compaction Score がこの上限に達するか超えると、受信するロードタスクは拒否されます。v3.3.6 以降、デフォルト値は `0` から `2000` に変更されました。
- 導入バージョン: v3.2.0

##### lake_enable_balance_tablets_between_workers

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでクラウドネイティブテーブルの tablet 移行中に Compute Nodes 間で tablet の数をバランスさせるかどうか。`true` は Compute Nodes 間で tablet をバランスさせることを示し、`false` はこの機能を無効にすることを示します。
- 導入バージョン: v3.3.4

##### lake_enable_ingest_slowdown

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでデータ取り込みのスローダウンを有効にするかどうか。データ取り込みのスローダウンが有効になっている場合、パーティションの Compaction Score が `lake_ingest_slowdown_threshold` を超えると、そのパーティションのロードタスクがスローダウンされます。この設定は `run_mode` が `shared_data` に設定されている場合にのみ有効です。v3.3.6 以降、デフォルト値は `false` から `true` に変更されました。
- 導入バージョン: v3.2.0

##### lake_ingest_slowdown_threshold

- デフォルト: 100
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでデータ取り込みのスローダウンをトリガーする Compaction Score のしきい値。この設定は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効です。
- 導入バージョン: v3.2.0

##### lake_publish_version_max_threads

- デフォルト: 512
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスタでのバージョン公開タスクの最大スレッド数。
- 導入バージョン: v3.2.0

##### meta_sync_force_delete_shard_meta

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: リモートストレージ内のファイルのクリーニングをバイパスして、共有データクラスタのメタデータを直接削除できるかどうか。この項目を `true` に設定するのは、クリーニングする Shard の数が多すぎて FE JVM のメモリが極端に圧迫される場合のみにすることを推奨します。この機能を有効にすると、Shard や Tablet に属するデータファイルは自動的にクリーニングできなくなることに注意してください。
- 導入バージョン: v3.2.10, v3.3.3

##### run_mode

- デフォルト: shared_nothing
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks クラスタの実行モード。有効な値: `shared_data` と `shared_nothing` (デフォルト)。
  - `shared_data` は StarRocks を共有データモードで実行することを示します。
  - `shared_nothing` は StarRocks を共有なしモードで実行することを示します。

  > **CAUTION**
  >
  > - StarRocks クラスタに対して `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイメントはサポートされていません。
  > - クラスタがデプロイされた後に `run_mode` を変更しないでください。そうしないと、クラスタが再起動に失敗します。共有なしクラスタから共有データクラスタへの変換、またはその逆はサポートされていません。

- 導入バージョン: -

##### shard_group_clean_threshold_sec

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: FE が共有データクラスター内の未使用の Tablet および Shard Group をクリーニングするまでの時間。この期間内に作成された Tablet と Shard Group はクリーニングされません。
- 導入バージョン: -

##### star_mgr_meta_sync_interval_sec

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: 共有データクラスタ内の FE が StarMgr と の定期的なメタデータ同期を実行する間隔。
- 導入バージョン: -

##### starmgr_grpc_server_max_worker_threads

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: FE の starmgr モジュール内で grpc サーバが使用するワーカースレッドの最大数。
- 導入バージョン: v4.0.0, v3.5.8

##### starmgr_grpc_timeout_seconds

- デフォルト: 5
- タイプ: Int
- 単位: Seconds
- 変更可能: Yes
- 説明:
- 導入バージョン: -

### データレイク

##### lake_batch_publish_min_version_num

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: Lake テーブルの publish バッチを形成するために必要な連続したトランザクションバージョンの最小数を設定します。DatabaseTransactionMgr.getReadyToPublishTxnListBatch は、この値を `lake_batch_publish_max_version_num` とともに transactionGraph.getTxnsWithTxnDependencyBatch に渡して依存トランザクションを選択します。値が `1` の場合は単一トランザクションの publish（バッチ化なし）を許可します。`1` より大きい値は、少なくともその数だけ連続するバージョンを持つ、単一テーブルかつレプリケーションでないトランザクションが利用可能であることを要求します。バージョンが連続していない場合、レプリケーショントランザクションが現れた場合、またはスキーマ変更がバージョンを消費した場合はバッチ化が中止されます。この値を大きくするとコミットをグループ化して publish スループットを改善できる一方、十分な連続トランザクションを待つために公開が遅延する可能性があります。
- 導入バージョン: v3.2.0

##### lake_use_combined_txn_log

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、Lake テーブルは関連するトランザクションで combined transaction log パスを使用することを許可します。このフラグはクラスタが shared-data モード（RunMode.isSharedDataMode()）で動作している場合にのみ考慮されます。`lake_use_combined_txn_log = true` のとき、BACKEND_STREAMING、ROUTINE_LOAD_TASK、INSERT_STREAMING、BATCH_LOAD_JOB タイプのロードトランザクションは combined txn log を使用する対象になります（詳細は LakeTableHelper.supportCombinedTxnLog を参照）。compaction を含むコードパスは isTransactionSupportCombinedTxnLog を通じて combined-log のサポートを確認します。無効化されているか shared-data モードでない場合は、combined transaction log の動作は使用されません。
- 導入バージョン: v3.3.7, v3.4.0, v3.5.0

### その他

##### agent_task_resend_wait_time_ms

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: エージェントタスクを再送信する前に FE が待機する必要がある期間。エージェントタスクは、タスク作成時間と現在の時間のギャップがこのパラメータの値を超える場合にのみ再送信できます。このパラメータはエージェントタスクの繰り返し送信を防ぐために使用されます。
- 導入バージョン: -

##### allow_system_reserved_names

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ユーザーが `__op` および `__row` で始まる名前の列を作成できるかどうか。この機能を有効にするには、このパラメータを `TRUE` に設定します。これらの名前形式は StarRocks で特別な目的のために予約されており、そのような列を作成すると未定義の動作が発生する可能性があるため、この機能はデフォルトで無効になっています。
- 導入バージョン: v3.2.0

##### auth_token

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE が属する StarRocks クラスタ内での ID 認証に使用されるトークン。このパラメータが指定されていない場合、StarRocks はクラスタの Leader FE が初めて起動されたときにクラスタのランダムなトークンを生成します。
- 導入バージョン: -

##### authentication_ldap_simple_bind_base_dn

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAP サーバーがユーザーの認証情報を検索する開始点であるベース DN。
- 導入バージョン: -

##### authentication_ldap_simple_bind_root_dn

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: ユーザーの認証情報を検索するために使用される管理者 DN。
- 導入バージョン: -

##### authentication_ldap_simple_bind_root_pwd

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: ユーザーの認証情報を検索するために使用される管理者のパスワード。
- 導入バージョン: -

##### authentication_ldap_simple_server_host

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAP サーバーが実行されているホスト。
- 導入バージョン: -

##### authentication_ldap_simple_server_port

- デフォルト: 389
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: LDAP サーバーのポート。
- 導入バージョン: -

##### authentication_ldap_simple_user_search_attr

- デフォルト: uid
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAP オブジェクト内でユーザーを識別する属性の名前。
- 導入バージョン: -

##### backup_job_default_timeout_ms

- デフォルト: 86400 * 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: バックアップジョブのタイムアウト期間。この値を超えると、バックアップジョブは失敗します。
- 導入バージョン: -

##### enable_collect_tablet_num_in_show_proc_backend_disk_path

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: `SHOW PROC /BACKENDS/{id}` コマンドで各ディスクの tablet 数を収集する機能を有効にするかどうか。
- 導入バージョン: v4.0.1, v3.5.8

##### enable_colocate_restore

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Colocate Tables のバックアップと復元を有効にするかどうか。`true` は Colocate Tables のバックアップと復元を有効にし、`false` は無効にすることを示します。
- 導入バージョン: v3.2.10, v3.3.3

##### enable_materialized_view_concurrent_prepare

- デフォルト: true
- タイプ: Boolean
- 単位:
- 変更可能: はい
- 説明: パフォーマンスを向上させるために、マテリアライズドビューを同時に準備するかどうか。
- 導入バージョン: v3.4.4

##### enable_metric_calculator

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: メトリクスを定期的に収集するための機能を有効にするかどうかを指定します。有効な値: `TRUE` および `FALSE`。`TRUE` はこの機能を有効にすることを指定し、`FALSE` はこの機能を無効にすることを指定します。
- 導入バージョン: -

##### enable_mv_post_image_reload_cache

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: システムがヒストリカルノードをトレースすることを許可するかどうか。この項目を `true` に設定することで、キャッシュ共有機能を有効にし、エラスティックなスケーリング時にシステムが適切なキャッシュノードを選択できるようにすることができる。
- 導入バージョン: v3.5.1

##### enable_mv_query_context_cache

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリ書き換えのパフォーマンスを向上させるために、クエリレベルのマテリアライズドビュー書き換えキャッシュを有効にするかどうか。
- 導入バージョン: v3.3

##### enable_mv_refresh_collect_profile

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: すべてのマテリアライズドビューで、デフォルトでマテリアライズドビューの更新時にプロファイルを有効にするかどうかを指定します。
- 導入バージョン: v3.3.0

##### enable_mv_refresh_extra_prefix_logging

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: より良いデバッグのために、ログでマテリアライズドビュー名の接頭辞を有効にするかどうか。
- 導入バージョン: v3.4.0

##### enable_mv_refresh_query_rewrite

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリのパフォーマンスを向上させるために、ベーステーブルではなく書き換えられたマテリアライズドビューを直接使用できるように、マテリアライズドビューの更新時に書き換えクエリを有効にするかどうか。
- 導入バージョン: v3.3

##### enable_trace_historical_node

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: システムが履歴ノードをトレースできるようにするかどうか。これを`true`に設定すると、Cache Sharing機能を有効にし、弾性スケーリング時にシステムが適切なキャッシュノードを選択できるようになります。
- 導入バージョン: v3.5.1

##### es_state_sync_interval_second

- デフォルト: 10
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: FE が Elasticsearch インデックスを取得し、StarRocks 外部テーブルのメタデータを同期する時間間隔。
- 導入バージョン: -

##### hive_meta_cache_refresh_interval_s

- デフォルト: 3600 * 2
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: Hive 外部テーブルのキャッシュされたメタデータが更新される時間間隔。
- 導入バージョン: -

##### hive_meta_store_timeout_s

- デフォルト: 10
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: Hive メタストアへの接続がタイムアウトするまでの時間。
- 導入バージョン: -

##### jdbc_connection_idle_timeout_ms

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBC catalog にアクセスするための接続がタイムアウトするまでの最大時間。タイムアウトした接続はアイドルと見なされます。
- 導入バージョン: -

##### jdbc_connection_pool_size

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBC catalogs にアクセスするための JDBC 接続プールの最大容量。
- 導入バージョン: -

##### jdbc_meta_default_cache_enable

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: JDBC Catalog メタデータキャッシュが有効かどうかのデフォルト値。`True` に設定すると、新しく作成された JDBC Catalogs はデフォルトでメタデータキャッシュが有効になります。
- 導入バージョン: -

##### jdbc_meta_default_cache_expire_sec

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: JDBC Catalog メタデータキャッシュのデフォルトの有効期限。`jdbc_meta_default_cache_enable` が true に設定されている場合、新しく作成された JDBC Catalogs はデフォルトでメタデータキャッシュの有効期限を設定します。
- 導入バージョン: -

##### jdbc_minimum_idle_connections

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBC catalogs にアクセスするための JDBC 接続プールの最小アイドル接続数。
- 導入バージョン: -

##### jwt_jwks_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JSON Web Key Set (JWKS) サービスの URL または `fe/conf` ディレクトリ内の公開鍵ローカルファイルへのパス。
- 導入バージョン: v3.5.0

##### jwt_principal_field

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内でサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と一致している必要があります。
- 導入バージョン: v3.5.0

##### jwt_required_audience

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内の受信者 (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 受信者と一致する場合にのみ、JWT は有効と見なされます。
- 導入バージョン: v3.5.0

##### jwt_required_issuer

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT 発行者と一致する場合にのみ、JWT は有効と見なされます。
- 導入バージョン: v3.5.0

##### locale

- デフォルト: zh_CN.UTF-8
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE が使用する文字セット。
- 導入バージョン: -

##### max_agent_task_threads_num

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: エージェントタスクスレッドプールで許可される最大スレッド数。
- 導入バージョン: -

##### max_download_task_per_be

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各 RESTORE 操作で、StarRocks が BE ノードに割り当てる最大ダウンロードタスク数。この項目が 0 以下に設定されている場合、タスク数に制限はありません。
- 導入バージョン: v3.1.0

##### max_mv_check_base_table_change_retry_times

- デフォルト: 10
- タイプ: -
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新時にベーステーブルの変更を検出するための最大再試行回数。
- 導入バージョン: v3.3.0

##### max_mv_refresh_failure_retry_times

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新に失敗した場合の最大再試行回数。
- 導入バージョン: v3.3.0

##### max_mv_refresh_try_lock_failure_retry_times

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューのリフレッシュに失敗した場合の、トライロックの最大再試行回数。
- 導入バージョン: v3.3.0

##### max_small_file_number

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE ディレクトリに保存できる小さなファイルの最大数。
- 導入バージョン: -

##### max_small_file_size_bytes

- デフォルト: 1024 * 1024
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 小さなファイルの最大サイズ。
- 導入バージョン: -

##### max_upload_task_per_be

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各 BACKUP 操作で、StarRocks が BE ノードに割り当てる最大アップロードタスク数。この項目が 0 以下に設定されている場合、タスク数に制限はありません。
- 導入バージョン: v3.1.0

##### mv_create_partition_batch_interval_ms

- デフォルト: 1000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: はい
- 説明: マテリアライズドビューの更新時に、複数のパーティションを一括作成する必要がある場合、システムはそれらを 64 個ずつのバッチに分割します。パーティションの頻繁な作成による障害のリスクを軽減するために、各バッチの間にデフォルトの間隔（ミリ秒単位）が設定され、作成頻度が制御されます。
- 導入バージョン: v3.3

##### mv_plan_cache_max_size

- デフォルト: 1000
- タイプ: Long
- 単位:
- 変更可能: はい
- 説明: 実体化されたビューの書き換えに使用されるプランキャッシュの最大サイズ。クエリ書き換えに使用される実体化ビューが多い場合は、この値を大きくすることができます。
- 導入バージョン: v3.2

##### mv_plan_cache_thread_pool_size

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューのプランキャッシュ（マテリアライズドビューの書き換えに使用される）のデフォルトのスレッドプールのサイズ。
- 導入バージョン: v3.2

##### mv_refresh_default_planner_optimize_timeout

- デフォルト: 30000
- タイプ: -
- 単位: -
- 変更可能: はい
- 説明: オプティマイザがマテリアライズドビューを更新する際の計画フェーズのデフォルトのタイムアウト。
- 導入バージョン: v3.3.0

##### mv_refresh_fail_on_filter_data

- デフォルト値：true
- タイプ: ブール値
- 単位：-
- 変更可能: はい
- 説明：リフレッシュ中にフィルターされたデータが存在する場合、マテリアライズドビューのリフレッシュは失敗します（デフォルトは true）。false に設定すると、フィルターされたデータを無視して正常終了として扱います。
- 導入バージョン：-

##### mv_refresh_try_lock_timeout_ms

- デフォルト: 30000
- タイプ: Int
- 単位: Milliseconds
- 変更可能: はい
- 説明: マテリアライズドビューのリフレッシュの既定のロック試行タイムアウトは、ベーステーブル/マテリアライズドビューのDBロックを試行します。
- 導入バージョン: v3.3.0

##### oauth2_auth_server_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 認可 URL。OAuth 2.0 認可プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。
- 導入バージョン: v3.5.0

##### oauth2_client_id

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks クライアントの公開識別子。
- 導入バージョン: v3.5.0

##### oauth2_client_secret

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 認可サーバーで StarRocks クライアントを認証するために使用される秘密。
- 導入バージョン: v3.5.0

##### oauth2_jwks_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JSON Web Key Set (JWKS) サービスの URL または `conf` ディレクトリ内のローカルファイルのパス。
- 導入バージョン: v3.5.0

##### oauth2_principal_field

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内でサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は、StarRocks にログインするためのユーザー名と同一でなければなりません。
- 導入バージョン: v3.5.0

##### oauth2_redirect_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。この URL に認可コードが送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として設定する必要があります。
- 導入バージョン: v3.5.0

##### oauth2_required_audience

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内のオーディエンス (`aud`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT のオーディエンスと一致する場合にのみ、JWT は有効と見なされます。
- 導入バージョン: v3.5.0

##### oauth2_required_issuer

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWT 内の発行者 (`iss`) を識別するために使用される文字列のリスト。リスト内のいずれかの値が JWT の発行者と一致する場合にのみ、JWT は有効と見なされます。
- 導入バージョン: v3.5.0

##### oauth2_token_server_url

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocks がアクセストークンを取得する認可サーバーのエンドポイントの URL。
- 導入バージョン: v3.5.0

##### plugin_dir

- デフォルト: System.getenv("STARROCKS_HOME") + "/plugins"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: プラグインインストールパッケージを保存するディレクトリ。
- 導入バージョン: -

##### plugin_enable

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEs にプラグインをインストールできるかどうか。プラグインは Leader FE のみでインストールまたはアンインストールできます。
- 導入バージョン: -

##### query_detail_explain_level

- デフォルト: COSTS
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: EXPLAIN ステートメントによって返されるクエリプランの詳細レベル。有効な値: COSTS, NORMAL, VERBOSE。
- 導入バージョン: v3.2.12, v3.3.5

##### replication_interval_ms

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: レプリケーションタスクがスケジュールされる最小時間間隔。
- 導入バージョン: v3.3.5

##### replication_max_parallel_data_size_mb

- デフォルト: 1048576
- タイプ: Int
- 単位: MB
- 変更可能: はい
- 説明: 許可される最大同時同期データサイズ。
- 導入バージョン: v3.3.5

##### replication_max_parallel_replica_count

- デフォルト: 10240
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許可される最大同時同期タブレットレプリカ数。
- 導入バージョン: v3.3.5

##### replication_max_parallel_table_count

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許可される最大同時データ同期タスク数。StarRocks は各テーブルに対して 1 つの同期タスクを作成します。
- 導入バージョン: v3.3.5

##### replication_transaction_timeout_sec

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 同期タスクのタイムアウト期間。
- 導入バージョン: v3.3.5

##### small_file_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 小さなファイルのルートディレクトリ。
- 導入バージョン: -

##### tmp_dir

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: バックアップおよび復元手順中に生成されたファイルなどの一時ファイルを保存するディレクトリ。これらの手順が終了すると、生成された一時ファイルは削除されます。
- 導入バージョン: -

##### transform_type_prefer_string_for_varchar

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの作成とCTAS操作において、固定長のVARCHAR列に対してSTRING型を優先するかどうか。
- 導入バージョン: v4.0.0



<EditionSpecificFEItem />
