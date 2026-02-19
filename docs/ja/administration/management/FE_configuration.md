---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FEの設定

<FEConfigMethod />

## FE設定項目を表示する

FEの起動後、MySQLクライアントで`ADMIN SHOW FRONTEND CONFIG`コマンドを実行すると、パラメータ設定を確認できます。特定のパラメータの設定をクエリしたい場合は、以下のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細については、[ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)を参照してください。

:::note
クラスター管理関連コマンドを実行するには、管理者権限が必要です。
:::

## FEパラメータの設定

### FE動的パラメータの設定

FEの動的パラメータの設定または変更は、[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md)を使用して行えます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE静的パラメータの設定

<StaticFEConfigNote />

## FEパラメータの理解

### ロギング

##### `audit_log_delete_age`

- デフォルト: 30d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 監査ログファイルの保持期間。デフォルト値`30d`は、各監査ログファイルが30日間保持されることを指定します。StarRocksは各監査ログファイルをチェックし、30日前に生成されたファイルを削除します。
- 導入バージョン: -

##### `audit_log_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 監査ログファイルを格納するディレクトリ。
- 導入バージョン: -

##### `audit_log_enable_compress`

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: いいえ
- 説明: trueの場合、生成されたLog4j2設定は、ローテーションされた監査ログファイル名 (fe.audit.log.*) に".gz"接尾辞を追加し、Log4j2がロールオーバー時に圧縮された(.gz)アーカイブ監査ログファイルを生成するようにします。この設定は、FE起動時にLog4jConfig.initLoggingで読み取られ、監査ログのRollingFileアペンダーに適用されます。アクティブな監査ログではなく、ローテーション/アーカイブされたファイルにのみ影響します。値は起動時に初期化されるため、変更を反映させるにはFEの再起動が必要です。監査ログローテーション設定 (audit_log_dir, audit_log_roll_interval, audit_roll_maxsize, audit_log_roll_num) と一緒に使用してください。
- 導入バージョン: 3.2.12

##### `audit_log_json_format`

- デフォルト: false
- タイプ: Boolean
- 単位: N/A
- 変更可能: はい
- 説明: trueの場合、FE監査イベントはデフォルトのパイプ区切り"key=value"文字列ではなく、構造化されたJSON (Jackson ObjectMapperがアノテーション付きAuditEventフィールドのMapをシリアライズ) として出力されます。この設定は、AuditLogBuilderが処理するすべての組み込み監査シンクに影響します。接続監査、クエリ監査、ビッグクエリ監査 (イベントが条件を満たす場合、ビッグクエリしきい値フィールドがJSONに追加されます)、および低速監査出力です。ビッグクエリしきい値用にアノテーションされたフィールドと"features"フィールドは特別に扱われます (通常の監査エントリから除外され、適用可能な場合、ビッグクエリまたは機能ログに含まれます)。ログコレクターやSIEMでログを機械的に解析できるようにするためにこれを有効にしますが、ログ形式が変更されるため、従来のパイプ区切り形式を期待する既存のパーサーを更新する必要がある場合があります。
- 導入バージョン: 3.2.7

##### `audit_log_modules`

- デフォルト: slow_query, query
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksが監査ログエントリを生成するモジュール。デフォルトでは、StarRocksは`slow_query`モジュールと`query`モジュールに対して監査ログを生成します。`connection`モジュールはv3.0からサポートされます。モジュール名をカンマ(`,`)とスペースで区切ってください。
- 導入バージョン: -

##### `audit_log_roll_interval`

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksが監査ログエントリをローテーションする時間間隔。有効な値: `DAY`と`HOUR`。
  - このパラメータが`DAY`に設定されている場合、監査ログファイル名に`yyyyMMdd`形式の接尾辞が追加されます。
  - このパラメータが`HOUR`に設定されている場合、監査ログファイル名に`yyyyMMddHH`形式の接尾辞が追加されます。
- 導入バージョン: -

##### `audit_log_roll_num`

- デフォルト: 90
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `audit_log_roll_interval`パラメータで指定された各保持期間内に保持できる監査ログファイルの最大数。
- 導入バージョン: -

##### `bdbje_log_level`

- デフォルト: INFO
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksで使用されるBerkeley DB Java Edition (BDB JE) のロギングレベルを制御します。BDB環境の初期化中、BDBEnvironment.initConfigs() はこの値を`com.sleepycat.je`パッケージのJavaロガーとBDB JE環境ファイルロギングレベル (EnvironmentConfig.FILE_LOGGING_LEVEL) に適用します。SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFFなどの標準的なjava.util.logging.Level名を受け入れます。ALLに設定すると、すべてのログメッセージが有効になります。詳細度を上げるとログ量が増加し、ディスクI/Oとパフォーマンスに影響を与える可能性があります。この値はBDB環境が初期化されるときに読み取られるため、環境の(再)初期化後にのみ有効になります。
- 導入バージョン: v3.2.0

##### `big_query_log_delete_age`

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEビッグクエリログファイル (`fe.big_query.log.*`) が自動削除されるまでの保持期間を制御します。この値はLog4jの削除ポリシーにIfLastModified ageとして渡されます。最後に変更された時刻がこの値よりも古いローテーションされたビッグクエリログは削除されます。サポートされる接尾辞には、`d` (日)、`h` (時間)、`m` (分)、`s` (秒) が含まれます。例: `7d` (7日)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。この項目は、`big_query_log_roll_interval`および`big_query_log_roll_num`と連携して、どのファイルを保持またはパージするかを決定します。
- 導入バージョン: v3.2.0

##### `big_query_log_dir`

- デフォルト: `Config.STARROCKS_HOME_DIR + "/log"`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEがビッグクエリダンプログ (`fe.big_query.log.*`) を書き込むディレクトリ。Log4j設定は、このパスを使用して`fe.big_query.log`とそのローテーションされたファイルのRollingFileアペンダーを作成します。ローテーションと保持は、`big_query_log_roll_interval` (時間ベースの接尾辞)、`log_roll_size_mb` (サイズトリガー)、`big_query_log_roll_num` (最大ファイル数)、および`big_query_log_delete_age` (経過時間ベースの削除) によって管理されます。ビッグクエリレコードは、`big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold`、または`big_query_log_scan_bytes_threshold`などのユーザー定義のしきい値を超えるクエリに対してログに記録されます。このファイルにログを記録するモジュールを制御するには、`big_query_log_modules`を使用します。
- 導入バージョン: v3.2.0

##### `big_query_log_modules`

- デフォルト: `{"query"}`
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: モジュールごとのビッグクエリロギングを有効にするモジュール名接尾辞のリスト。一般的な値は論理コンポーネント名です。例えば、デフォルトの`query`は`big_query.query`を生成します。
- 導入バージョン: v3.2.0

##### `big_query_log_roll_interval`

- デフォルト: `"DAY"`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: `big_query`ログアペンダーのローリングファイル名の日付コンポーネントを構築するために使用される時間間隔を指定します。有効な値 (大文字と小文字を区別しない): `DAY` (デフォルト) と `HOUR`。`DAY`は日次パターン (`"%d{yyyyMMdd}"`) を生成し、`HOUR`は時間ごとのパターン (`"%d{yyyyMMddHH}"`) を生成します。この値はサイズベースのロールオーバー (`big_query_roll_maxsize`) とインデックスベースのロールオーバー (`big_query_log_roll_num`) と組み合わせてRollingFile filePatternを形成します。無効な値はログ構成の生成に失敗し (IOException)、ログの初期化または再構成を妨げる可能性があります。`big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num`、および`big_query_log_delete_age`と組み合わせて使用してください。
- 導入バージョン: v3.2.0

##### `big_query_log_roll_num`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `big_query_log_roll_interval`ごとに保持するFEビッグクエリログファイルの最大ローテーション数。この値は、`fe.big_query.log`のRollingFileアペンダーのDefaultRolloverStrategy `max`属性にバインドされます。ログがロール (時間または`log_roll_size_mb`によって) されると、StarRocksは最大`big_query_log_roll_num`個のインデックス付きファイルを保持し (filePatternは時間接尾辞とインデックスを使用)、この数よりも古いファイルはロールオーバーによって削除される場合があります。また、`big_query_log_delete_age`は最終更新時刻に基づいてファイルを削除できます。
- 導入バージョン: v3.2.0

##### `dump_log_delete_age`

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ダンプログファイルの保持期間。デフォルト値`7d`は、各ダンプログファイルが7日間保持されることを指定します。StarRocksは各ダンプログファイルをチェックし、7日前に生成されたファイルを削除します。
- 導入バージョン: -

##### `dump_log_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ダンプログファイルを格納するディレクトリ。
- 導入バージョン: -

##### `dump_log_modules`

- デフォルト: query
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがダンプログエントリを生成するモジュール。デフォルトでは、StarRocksはクエリモジュールに対してダンプログを生成します。モジュール名をカンマ(`,`)とスペースで区切ってください。
- 導入バージョン: -

##### `dump_log_roll_interval`

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがダンプログエントリをローテーションする時間間隔。有効な値: `DAY`と`HOUR`。
  - このパラメータが`DAY`に設定されている場合、ダンプログファイル名に`yyyyMMdd`形式の接尾辞が追加されます。
  - このパラメータが`HOUR`に設定されている場合、ダンプログファイル名に`yyyyMMddHH`形式の接尾辞が追加されます。
- 導入バージョン: -

##### `dump_log_roll_num`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `dump_log_roll_interval`パラメータで指定された各保持期間内に保持できるダンプログファイルの最大数。
- 導入バージョン: -

##### `edit_log_write_slow_log_threshold_ms`

- デフォルト: 2000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: JournalWriterが低速編集ログバッチ書き込みを検出およびログ記録するために使用するしきい値 (ミリ秒)。バッチコミット後、バッチ期間がこの値を超えると、JournalWriterはバッチサイズ、期間、および現在のジャーナルキューサイズとともにWARNを発行します (~2秒に1回にレート制限されます)。この設定は、FEリーダーでの潜在的なIOまたはレプリケーションレイテンシに対するログ記録/アラートのみを制御します。コミットまたはロールの動作は変更されません (`edit_log_roll_num`およびコミット関連の設定を参照)。メトリックの更新は、このしきい値に関係なく発生します。
- 導入バージョン: v3.2.3

##### `enable_audit_sql`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`true`に設定されている場合、FE監査サブシステムは、ConnectProcessorによって処理されたSQLステートメントのテキストをFE監査ログ (`fe.audit.log`) に記録します。格納されるステートメントは、他の制御を尊重します。暗号化されたステートメントは編集され (`AuditEncryptionChecker`)、`enable_sql_desensitize_in_log`が設定されている場合、機密性の高い資格情報は編集または非感度化される可能性があり、ダイジェスト記録は`enable_sql_digest`によって制御されます。`false`に設定されている場合、ConnectProcessorは監査イベントのステートメントテキストを"?"に置き換えます。他の監査フィールド (ユーザー、ホスト、期間、ステータス、`qe_slow_log_ms`による低速クエリ検出、およびメトリック) は引き続き記録されます。SQL監査を有効にすると、フォレンジックとトラブルシューティングの可視性が向上しますが、機密性の高いSQLコンテンツが公開され、ログ量とI/Oが増加する可能性があります。無効にすると、監査ログでの完全なステートメントの可視性が失われる代わりにプライバシーが向上します。
- 導入バージョン: -

##### `enable_profile_log`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: プロファイルロギングを有効にするかどうか。この機能が有効になっている場合、FEはクエリごとのプロファイルログ (`ProfileManager`によって生成されたシリアライズされた`queryDetail` JSON) をプロファイルログシンクに書き込みます。このロギングは、`enable_collect_query_detail_info`も有効になっている場合にのみ実行されます。`enable_profile_log_compress`が有効になっている場合、JSONはロギング前にgzip圧縮されることがあります。プロファイルログファイルは、`profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval`によって管理され、`profile_log_delete_age` (`7d`、`10h`、`60m`、`120s`などの形式をサポート) に従ってローテーション/削除されます。この機能を無効にすると、プロファイルログの書き込みが停止し (ディスクI/O、圧縮CPU、ストレージ使用量が減少します)。
- 導入バージョン: v3.2.5

##### `enable_qe_slow_log`

- デフォルト: true
- タイプ: Boolean
- 単位: N/A
- 変更可能: はい
- 説明: 有効にすると、FE組み込み監査プラグイン (AuditLogBuilder) は、測定された実行時間 ("Time"フィールド) が`qe_slow_log_ms`で構成されたしきい値を超えるクエリイベントを低速クエリ監査ログ (AuditLog.getSlowAudit) に書き込みます。無効にすると、これらの低速クエリエントリは抑制されます (通常のクエリおよび接続監査ログは影響を受けません)。低速監査エントリはグローバルな`audit_log_json_format`設定 (JSON vs. プレーン文字列) に従います。このフラグを使用して、通常の監査ロギングとは独立して低速クエリ監査量を制御します。これをオフにすると、`qe_slow_log_ms`が低い場合やワークロードが多くの長時間実行クエリを生成する場合に、ログI/Oが減少する可能性があります。
- 導入バージョン: 3.2.11

##### `enable_sql_desensitize_in_log`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`true`に設定されている場合、ログおよびクエリ詳細レコードに書き込まれる前に、機密性の高いSQLコンテンツがシステムによって置き換えまたは非表示にされます。この設定を尊重するコードパスには、ConnectProcessor.formatStmt (監査ログ)、StmtExecutor.addRunningQueryDetail (クエリ詳細)、およびSimpleExecutor.formatSQL (内部エグゼキュータログ) が含まれます。この機能が有効になっている場合、無効なSQLは固定された非機密化メッセージに置き換えられる可能性があり、資格情報 (ユーザー/パスワード) は非表示になり、SQLフォーマッターはサニタイズされた表現を生成する必要があります (ダイジェストスタイルの出力も有効にできます)。これにより、監査/内部ログでの機密リテラルや資格情報の漏洩が減少しますが、ログとクエリ詳細に元の完全なSQLテキストが含まれなくなることも意味します (これはリプレイやデバッグに影響を与える可能性があります)。
- 導入バージョン: -

##### `internal_log_delete_age`

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE内部ログファイル (`internal_log_dir`に書き込まれる) の保持期間を指定します。値は期間文字列です。サポートされる接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7日)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。この項目は、RollingFile Deleteポリシーで使用される`<IfLastModified age="..."/>`述語としてlog4j構成に置き換えられます。最終変更時刻がこの期間よりも前のファイルは、ログロールオーバー中に削除されます。この値を増やすとディスク領域が早く解放され、減らすと内部マテリアライズドビューまたは統計ログが長く保持されます。
- 導入バージョン: v3.2.4

##### `internal_log_dir`

- デフォルト: `Config.STARROCKS_HOME_DIR + "/log"`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEロギングサブシステムが内部ログ (`fe.internal.log`) を格納するために使用するディレクトリ。この設定はLog4j構成に置き換えられ、InternalFileアペンダーが内部/マテリアライズドビュー/統計ログを書き込む場所、および`internal.<module>`下のモジュールごとのロガーがファイルを配置する場所を決定します。ディレクトリが存在し、書き込み可能であり、十分なディスク容量があることを確認してください。このディレクトリ内のファイルのログローテーションと保持は、`log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age`、および`internal_log_roll_interval`によって制御されます。`sys_log_to_console`が有効になっている場合、内部ログはこのディレクトリではなくコンソールに書き込まれることがあります。
- 導入バージョン: v3.2.4

##### `internal_log_json_format`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、内部統計/監査エントリはコンパクトなJSONオブジェクトとして統計監査ロガーに書き込まれます。JSONには、"executeType" (InternalType: QUERY または DML)、"queryId"、"sql"、および"time" (経過ミリ秒) のキーが含まれます。`false`に設定されている場合、同じ情報は単一のフォーマットされたテキスト行 ("statistic execute: ... | QueryId: [...] | SQL: ...") としてログに記録されます。JSONを有効にすると、機械解析とログプロセッサーとの統合が向上しますが、ログに生のSQLテキストが含まれるため、機密情報が公開されたり、ログサイズが増加したりする可能性があります。
- 導入バージョン: -

##### `internal_log_modules`

- デフォルト: `{"base", "statistic"}`
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: 専用の内部ロギングを受け取るモジュール識別子のリスト。各エントリXについて、Log4jはレベルINFOと`additivity="false"`の`internal.<X>`という名前のロガーを作成します。これらのロガーは、内部アペンダー (fe.internal.logに書き込まれます) または`sys_log_to_console`が有効な場合はコンソールにルーティングされます。必要に応じて短い名前またはパッケージフラグメントを使用してください。正確なロガー名は`internal.` + 構成された文字列になります。内部ログファイルのローテーションと保持は、`internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval`、および`log_roll_size_mb`に従います。モジュールを追加すると、実行時メッセージが内部ロガーストリームに分離され、デバッグと監査が容易になります。
- 導入バージョン: v3.2.4

##### `internal_log_roll_interval`

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FE内部ログアペンダーの時間ベースのロール間隔を制御します。受け入れられる値 (大文字と小文字を区別しない): `HOUR`と`DAY`。`HOUR`は時間ごとのファイルパターン (`"%d{yyyyMMddHH}"`) を生成し、`DAY`は日ごとのファイルパターン (`"%d{yyyyMMdd}"`) を生成します。これらはRollingFile TimeBasedTriggeringPolicyによってローテーションされた`fe.internal.log`ファイルの名前を付けるために使用されます。無効な値は初期化に失敗し (アクティブなLog4j構成を構築するときにIOExceptionがスローされます)、ロール動作は`internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num`、および`internal_log_delete_age`などの関連設定にも依存します。
- 導入バージョン: v3.2.4

##### `internal_log_roll_num`

- デフォルト: 90
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 内部アペンダー (`fe.internal.log`) に対して保持するFE内部ログファイルの最大ロール数。この値はLog4j DefaultRolloverStrategy `max`属性として使用されます。ロールオーバーが発生すると、StarRocksは最大`internal_log_roll_num`個のアーカイブファイルを保持し、古いファイルを削除します (これも`internal_log_delete_age`によって管理されます)。値が低いとディスク使用量が減少しますが、ログ履歴が短くなります。値が高いとより多くの履歴内部ログが保持されます。この項目は`internal_log_dir`、`internal_log_roll_interval`、および`internal_roll_maxsize`と連携して機能します。
- 導入バージョン: v3.2.4

##### `log_cleaner_audit_log_min_retention_days`

- デフォルト: 3
- タイプ: Int
- 単位: 日
- 変更可能: はい
- 説明: 監査ログファイルの最小保持日数。これより新しい監査ログファイルは、ディスク使用量が高くても削除されません。これにより、監査ログがコンプライアンスおよびトラブルシューティングの目的で保持されることが保証されます。
- 導入バージョン: -

##### `log_cleaner_check_interval_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ディスク使用量をチェックし、ログをクリーンアップする間隔 (秒単位)。クリーナーは定期的に各ログディレクトリのディスク使用量をチェックし、必要に応じてクリーンアップをトリガーします。デフォルトは300秒 (5分) です。
- 導入バージョン: -

##### `log_cleaner_disk_usage_target`

- デフォルト: 60
- タイプ: Int
- 単位: パーセンテージ
- 変更可能: はい
- 説明: ログクリーニング後の目標ディスク使用量 (パーセンテージ)。ディスク使用量がこのしきい値を下回るまでログクリーニングが継続されます。クリーナーは、目標が達成されるまで最も古いログファイルを1つずつ削除します。
- 導入バージョン: -

##### `log_cleaner_disk_usage_threshold`

- デフォルト: 80
- タイプ: Int
- 単位: パーセンテージ
- 変更可能: はい
- 説明: ログクリーニングをトリガーするディスク使用量のしきい値 (パーセンテージ)。ディスク使用量がこのしきい値を超えると、ログクリーニングが開始されます。クリーナーは、設定された各ログディレクトリを個別にチェックし、このしきい値を超えるディレクトリを処理します。
- 導入バージョン: -

##### `log_cleaner_disk_util_based_enable`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ディスク使用量に基づいた自動ログクリーニングを有効にします。有効にすると、ディスク使用量がしきい値を超えた場合にログがクリーンアップされます。ログクリーナーはFEノードでバックグラウンドデーモンとして実行され、ログファイルの蓄積によるディスク容量の枯渇を防ぐのに役立ちます。
- 導入バージョン: -

##### `log_plan_cancelled_by_crash_be`

- デフォルト: true
- タイプ: boolean
- 単位: -
- 変更可能: はい
- 説明: BEクラッシュまたはRPC例外によりクエリがキャンセルされた場合に、クエリ実行計画のロギングを有効にするかどうか。この機能が有効な場合、BEクラッシュまたは`RpcException`によりクエリがキャンセルされると、StarRocksはクエリ実行計画 (`TExplainLevel.COSTS`) をWARNエントリとしてログに記録します。ログエントリにはQueryId、SQL、およびCOSTS計画が含まれます。ExecuteExceptionHandlerパスでは、例外スタックトレースもログに記録されます。`enable_collect_query_detail_info`が有効な場合 (その場合、計画はクエリ詳細に格納されます) ロギングはスキップされます。コードパスでは、クエリ詳細がnullであるかどうかを検証することでチェックが実行されます。ExecuteExceptionHandlerでは、計画は最初の再試行 (`retryTime == 0`) でのみログに記録されることに注意してください。これを有効にすると、完全なCOSTS計画が大きくなる可能性があるため、ログ量が増加する可能性があります。
- 導入バージョン: v3.2.0

##### `log_register_and_unregister_query_id`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEがQeProcessorImplからクエリ登録および登録解除メッセージ (例: `"register query id = {}"` および `"deregister query id = {}"`) をログに記録することを許可するかどうか。ログは、クエリがnull以外のConnectContextを持ち、かつコマンドが`COM_STMT_EXECUTE`ではないか、セッション変数`isAuditExecuteStmt()`がtrueの場合にのみ出力されます。これらのメッセージはすべてのクエリライフサイクルイベントに対して書き込まれるため、この機能を有効にすると、高コンカレンシー環境でログ量が非常に多くなり、スループットのボトルネックになる可能性があります。デバッグまたは監査のために有効にし、ロギングオーバーヘッドを減らしパフォーマンスを向上させるために無効にしてください。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `log_roll_size_mb`

- デフォルト: 1024
- タイプ: Int
- 単位: MB
- 変更可能: いいえ
- 説明: システムログファイルまたは監査ログファイルの最大サイズ。
- 導入バージョン: -

##### `proc_profile_file_retained_days`

- デフォルト: 1
- タイプ: Int
- 単位: 日
- 変更可能: はい
- 説明: `sys_log_dir/proc_profile`の下に生成されるプロセスプロファイリングファイル (CPUおよびメモリ) を保持する日数。ProcProfileCollectorは、現在の時刻 (yyyyMMdd-HHmmss形式) から`proc_profile_file_retained_days`日を差し引いてカットオフを計算し、タイムスタンプ部分がそのカットオフよりも辞書順で早いプロファイルファイル (`timePart.compareTo(timeToDelete) < 0`であるファイル) を削除します。ファイル削除は`proc_profile_file_retained_size_bytes`によって制御されるサイズベースのカットオフも尊重します。プロファイルファイルは`cpu-profile-`および`mem-profile-`のプレフィックスを使用し、収集後に圧縮されます。
- 導入バージョン: v3.2.12

##### `proc_profile_file_retained_size_bytes`

- デフォルト: 2L * 1024 * 1024 * 1024 (2147483648)
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: プロファイルディレクトリの下に保持される収集されたCPUおよびメモリプロファイルファイル (`cpu-profile-`および`mem-profile-`のプレフィックスを持つファイル) の最大合計バイト数。有効なプロファイルファイルの合計が`proc_profile_file_retained_size_bytes`を超えると、残りの合計サイズが`proc_profile_file_retained_size_bytes`以下になるまで、コレクターは最も古いプロファイルファイルを削除します。`proc_profile_file_retained_days`よりも古いファイルはサイズに関係なく削除されます。この設定はプロファイルアーカイブのディスク使用量を制御し、`proc_profile_file_retained_days`と相互作用して削除順序と保持を決定します。
- 導入バージョン: v3.2.12

##### `profile_log_delete_age`

- デフォルト: 1d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEプロファイルログファイルが削除対象となるまでの保持期間を制御します。この値はLog4jの`<IfLastModified age="..."/>`ポリシー (`Log4jConfig`経由) に挿入され、`profile_log_roll_interval`や`profile_log_roll_num`などのローテーション設定と組み合わせて適用されます。サポートされる接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7日)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。
- 導入バージョン: v3.2.5

##### `profile_log_dir`

- デフォルト: `Config.STARROCKS_HOME_DIR + "/log"`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEプロファイルログが書き込まれるディレクトリ。Log4jConfigはこの値を使用してプロファイル関連のアペンダーを配置します (このディレクトリの下に`fe.profile.log`や`fe.features.log`のようなファイルを作成します)。これらのファイルのローテーションと保持は、`profile_log_roll_size_mb`、`profile_log_roll_num`、および`profile_log_delete_age`によって管理されます。タイムスタンプ接尾辞形式は`profile_log_roll_interval` (DAYまたはHOURをサポート) によって制御されます。デフォルトのディレクトリは`STARROCKS_HOME_DIR`の下にあるため、FEプロセスにこのディレクトリへの書き込みおよびローテーション/削除権限があることを確認してください。
- 導入バージョン: v3.2.5

##### `profile_log_roll_interval`

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: プロファイルログファイル名の日付部分を生成するために使用される時間粒度を制御します。有効な値 (大文字と小文字を区別しない): `HOUR`と`DAY`。`HOUR`は`"%d{yyyyMMddHH}"`のパターン (時間ごとのタイムバケット) を生成し、`DAY`は`"%d{yyyyMMdd}"` (日ごとのタイムバケット) を生成します。この値はLog4j設定で`profile_file_pattern`を計算する際に使用され、ロールオーバーファイル名の時間ベースのコンポーネントのみに影響します。サイズベースのロールオーバーは`profile_log_roll_size_mb`によって、保持は`profile_log_roll_num` / `profile_log_delete_age`によって引き続き制御されます。無効な値はロギング初期化中にIOExceptionを引き起こします (エラーメッセージ: `"profile_log_roll_interval config error: <value>"`)。高ボリュームのプロファイリングの場合は、ファイルごとのサイズを時間ごとに制限するために`HOUR`を、日ごとの集計の場合は`DAY`を選択してください。
- 導入バージョン: v3.2.5

##### `profile_log_roll_num`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: プロファイルロガーのLog4jのDefaultRolloverStrategyによって保持されるローテーションされたプロファイルログファイルの最大数を指定します。この値は、ロギングXMLに`${profile_log_roll_num}`として挿入されます (例: `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`)。ローテーションは`profile_log_roll_size_mb`または`profile_log_roll_interval`によってトリガーされます。ローテーションが発生すると、Log4jは最大でこれらのインデックス付きファイルを保持し、古いインデックスファイルは削除対象となります。ディスク上の実際の保持は、`profile_log_delete_age`および`profile_log_dir`の場所にも影響されます。値が低いとディスク使用量は減少しますが、保持される履歴が制限されます。値が高いとより多くの履歴プロファイルログが保持されます。
- 導入バージョン: v3.2.5

##### `profile_log_roll_size_mb`

- デフォルト: 1024
- タイプ: Int
- 単位: MB
- 変更可能: いいえ
- 説明: FEプロファイルログファイルのサイズベースのロールオーバーをトリガーするサイズしきい値 (メガバイト単位) を設定します。この値は、`ProfileFile`アペンダーのLog4j RollingFile SizeBasedTriggeringPolicyによって使用されます。プロファイルログが`profile_log_roll_size_mb`を超えると、ローテーションされます。ローテーションは、`profile_log_roll_interval`に達したときに時間によっても発生する可能性があります。どちらの条件もロールオーバーをトリガーします。`profile_log_roll_num`および`profile_log_delete_age`と組み合わせて、この項目は履歴プロファイルファイルがどれだけ保持され、古いファイルがいつ削除されるかを制御します。ローテーションされたファイルの圧縮は`enable_profile_log_compress`によって制御されます。
- 導入バージョン: v3.2.5

##### `qe_slow_log_ms`

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: クエリが低速クエリであるかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超えると、**fe.audit.log**に低速クエリとして記録されます。
- 導入バージョン: -

##### `slow_lock_log_every_ms`

- デフォルト: 3000L
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: 同じSlowLockLogStatsインスタンスに対して別の「低速ロック」警告を発行するまでに待機する最小間隔 (ミリ秒)。LockUtilsは、ロックの待機時間がslow_lock_threshold_msを超えた後にこの値をチェックし、最後のログに記録された低速ロックイベントからslow_lock_log_every_msミリ秒が経過するまで追加の警告を抑制します。長期的な競合中にログ量を減らすには大きな値を、より頻繁な診断を得るには小さな値を使用します。変更は、後続のチェックに対してランタイムに有効になります。
- 導入バージョン: v3.2.0

##### `slow_lock_print_stack`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: LockManagerが、`logSlowLockTrace`によって発行される低速ロック警告のJSONペイロードに、所有スレッドの完全なスタックトレースを含めることを許可するかどうか ("stack"配列は`LogUtil.getStackTraceToJsonArray`により`start=0`および`max=Short.MAX_VALUE`で設定されます)。この設定は、ロック取得が`slow_lock_threshold_ms`で構成されたしきい値を超えた場合に表示されるロック所有者に対する追加のスタック情報のみを制御します。この機能を有効にすると、ロックを保持している正確なスレッドスタックを提供することでデバッグに役立ちます。無効にすると、高コンカレンシー環境でスタックトレースをキャプチャおよびシリアライズすることによって発生するログ量とCPU/メモリオーバーヘッドが削減されます。
- 導入バージョン: v3.3.16, v3.4.5, v3.5.1

##### `slow_lock_threshold_ms`

- デフォルト: 3000L
- タイプ: long
- 単位: ミリ秒
- 変更可能: はい
- 説明: ロック操作または保持されたロックを「低速」として分類するために使用されるしきい値 (ミリ秒)。ロックの経過待機時間または保持時間がこの値を超えると、StarRocksは (コンテキストに応じて) 診断ログを出力し、スタックトレースまたはウェイター/所有者情報を含め、LockManagerではこの遅延後にデッドロック検出を開始します。これはLockUtils (低速ロックロギング)、QueryableReentrantReadWriteLock (低速リーダーのフィルタリング)、LockManager (デッドロック検出遅延と低速ロックトレース)、LockChecker (定期的な低速ロック検出)、およびその他の呼び出し元 (例: DiskAndTabletLoadReBalancerロギング) で使用されます。値を下げると感度とロギング/診断オーバーヘッドが増加します。0または負の値を設定すると、最初の待機ベースのデッドロック検出遅延動作が無効になります。`slow_lock_log_every_ms`、`slow_lock_print_stack`、および`slow_lock_stack_trace_reserve_levels`と組み合わせて調整してください。
- 導入バージョン: 3.2.0

##### `sys_log_delete_age`

- デフォルト: 7d
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログファイルの保持期間。デフォルト値`7d`は、各システムログファイルが7日間保持されることを指定します。StarRocksは各システムログファイルをチェックし、7日前に生成されたファイルを削除します。
- 導入バージョン: -

##### `sys_log_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログファイルを格納するディレクトリ。
- 導入バージョン: -

##### `sys_log_enable_compress`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`true`に設定されている場合、システムはローテーションされたシステムログファイル名に".gz"接尾辞を追加し、Log4jがgzip圧縮されたローテーションFEシステムログ (例: fe.log.*) を生成するようにします。この値はLog4j構成生成時 (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) に読み取られ、RollingFile filePatternで使用される`sys_file_postfix`プロパティを制御します。この機能を有効にすると、保持されるログのディスク使用量は減少しますが、ロールオーバー中のCPUとI/Oが増加し、ログファイル名が変更されるため、ログを読み取るツールやスクリプトは.gzファイルを処理できる必要があります。監査ログは圧縮に対して別の構成を使用します (`audit_log_enable_compress`)。
- 導入バージョン: v3.2.12

##### `sys_log_format`

- デフォルト: "plaintext"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEログに使用されるLog4jレイアウトを選択します。有効な値: `"plaintext"` (デフォルト) および `"json"`。値は大文字と小文字を区別しません。`"plaintext"`は、人間が読めるタイムスタンプ、レベル、スレッド、class.method:line、およびWARN/ERRORのスタックトレースを含むPatternLayoutを構成します。`"json"`はJsonTemplateLayoutを構成し、ログアグリゲータ (ELK、Splunk) に適した構造化JSONイベント (UTCタイムスタンプ、レベル、スレッドID/名、ソースファイル/メソッド/行、メッセージ、例外スタックトレース) を出力します。JSON出力は、最大文字列長に対して`sys_log_json_max_string_length`および`sys_log_json_profile_max_string_length`に準拠します。
- 導入バージョン: v3.2.10

##### `sys_log_json_max_string_length`

- デフォルト: 1048576
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: JSON形式のシステムログに使用されるJsonTemplateLayoutの"maxStringLength"値を設定します。`sys_log_format`が`"json"`に設定されている場合、文字列値フィールド (例: "message"および文字列化された例外スタックトレース) は、長さがこの制限を超えると切り捨てられます。この値は、生成されたLog4j XMLに`Log4jConfig.generateActiveLog4jXmlConfig()`で挿入され、デフォルト、警告、監査、ダンプ、ビッグクエリレイアウトに適用されます。プロファイルレイアウトは別の構成 (`sys_log_json_profile_max_string_length`) を使用します。この値を下げるとログサイズは減少しますが、有用な情報が切り捨てられる可能性があります。
- 導入バージョン: 3.2.11

##### `sys_log_json_profile_max_string_length`

- デフォルト: 104857600 (100 MB)
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: `sys_log_format`が"json"の場合、プロファイル (および関連機能) ログアペンダーのJsonTemplateLayoutのmaxStringLengthを設定します。JSON形式のプロファイルログの文字列フィールド値は、このバイト長に切り捨てられます。非文字列フィールドは影響を受けません。この項目はLog4jConfig `JsonTemplateLayout maxStringLength`に適用され、`plaintext`ロギングが使用されている場合は無視されます。必要な完全なメッセージに対して十分な大きさの値を維持しますが、値が大きいほどログサイズとI/Oが増加することに注意してください。
- 導入バージョン: v3.2.11

##### `sys_log_level`

- デフォルト: INFO
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: システムログエントリが分類される重大度レベル。有効な値: `INFO`、`WARN`、`ERROR`、および`FATAL`。
- 導入バージョン: -

##### `sys_log_roll_interval`

- デフォルト: DAY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがシステムログエントリをローテーションする時間間隔。有効な値: `DAY`と`HOUR`。
  - このパラメータが`DAY`に設定されている場合、システムログファイル名に`yyyyMMdd`形式の接尾辞が追加されます。
  - このパラメータが`HOUR`に設定されている場合、システムログファイル名に`yyyyMMddHH`形式の接尾辞が追加されます。
- 導入バージョン: -

##### `sys_log_roll_num`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: `sys_log_roll_interval`パラメータで指定された各保持期間内に保持できるシステムログファイルの最大数。
- 導入バージョン: -

##### `sys_log_to_console`

- デフォルト: false (ただし、環境変数`SYS_LOG_TO_CONSOLE`が"1"に設定されている場合を除く)
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`true`に設定されている場合、システムはLog4jを構成し、すべてのログをファイルベースのアペンダーではなくコンソール (ConsoleErrアペンダー) に送信します。この値は、アクティブなLog4j XML構成を生成するときに読み取られます (これはルートロガーとモジュールごとのロガーアペンダーの選択に影響します)。その値は、プロセス起動時に`SYS_LOG_TO_CONSOLE`環境変数からキャプチャされます。実行時に変更しても効果はありません。この構成は、stdout/stderrログ収集がログファイルの書き込みよりも好まれるコンテナ化されたまたはCI環境で一般的に使用されます。
- 導入バージョン: v3.2.0

##### `sys_log_verbose_modules`

- デフォルト: 空文字列
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがシステムログを生成するモジュール。このパラメータが`org.apache.starrocks.catalog`に設定されている場合、StarRocksはカタログモジュールに対してのみシステムログを生成します。モジュール名をカンマ(`,`)とスペースで区切ってください。
- 導入バージョン: -

##### `sys_log_warn_modules`

- デフォルト: {}
- タイプ: String[]
- 単位: -
- 変更可能: いいえ
- 説明: システムが起動時にWARNレベルロガーとして構成し、警告アペンダー (SysWF) — `fe.warn.log`ファイルにルーティングするロガー名またはパッケージプレフィックスのリスト。エントリは生成されたLog4j構成 (org.apache.kafka、org.apache.hudi、org.apache.hadoop.io.compressなどの組み込み警告モジュールとともに) に挿入され、`<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>`のようなロガー要素を生成します。一般的なログにノイズの多いINFO/DEBUG出力を抑制し、警告を個別にキャプチャできるようにするために、完全修飾パッケージおよびクラスプレフィックス (例: "com.example.lib") を使用することをお勧めします。
- 導入バージョン: v3.2.13

### サーバー

##### `brpc_idle_wait_max_time`

- デフォルト: 10000
- タイプ: Int
- 単位: ms
- 変更可能: いいえ
- 説明: bRPCクライアントがアイドル状態で待機する最大時間。
- 導入バージョン: -

##### `brpc_inner_reuse_pool`

- デフォルト: true
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: 基盤となるBRPCクライアントが接続/チャネルに内部共有再利用プールを使用するかどうかを制御します。StarRocksはBrpcProxyでRpcClientOptionsを構築する際に`brpc_inner_reuse_pool`を読み取ります (`rpcOptions.setInnerResuePool(...)`経由)。有効な場合 (true)、RPCクライアントは内部プールを再利用して、呼び出しごとの接続作成を減らし、FE-to-BE / LakeService RPCの接続チャーン、メモリ、ファイル記述子の使用量を削減します。無効な場合 (false)、クライアントはより分離されたプールを作成する可能性があります (リソース使用量が増加する代わりに、並列処理の分離が増加します)。この値を変更するには、プロセスを再起動して有効にする必要があります。
- 導入バージョン: v3.3.11, v3.4.1, v3.5.0

##### `brpc_min_evictable_idle_time_ms`

- デフォルト: 120000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: アイドル状態のBRPC接続が接続プール内で待機する必要がある最小時間 (ミリ秒)。`BrpcProxy`で使用されるRpcClientOptionsに適用されます (RpcClientOptions.setMinEvictableIdleTime経由)。この値を上げるとアイドル接続を長く保持し (再接続の頻度を減らす)、下げると未使用のソケットをより速く解放し (リソース使用量を減らす) ます。`brpc_connection_pool_size`および`brpc_idle_wait_max_time`と組み合わせて、接続の再利用、プールの拡大、および退去の動作のバランスを取ります。
- 導入バージョン: v3.3.11, v3.4.1, v3.5.0

##### `brpc_reuse_addr`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: trueの場合、StarRocksはbrpc RpcClientによって作成されたクライアントソケットのローカルアドレス再利用を許可するようにソケットオプションを設定します (RpcClientOptions.setReuseAddress経由)。これを有効にすると、バインドの失敗が減少し、ソケットが閉じられた後のローカルポートのより高速な再バインドが可能になります。これは、高レートの接続チャーンや迅速な再起動に役立ちます。falseの場合、アドレス/ポートの再利用が無効になり、意図しないポート共有の可能性は減りますが、一時的なバインドエラーが増加する可能性があります。このオプションは、`brpc_connection_pool_size`および`brpc_short_connection`によって構成される接続動作と相互作用します。これは、クライアントソケットがどれだけ迅速に再バインドおよび再利用できるかに影響するためです。
- 導入バージョン: v3.3.11, v3.4.1, v3.5.0

##### `cluster_name`

- デフォルト: StarRocks Cluster
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEが属するStarRocksクラスターの名前。クラスター名はWebページの`Title`に表示されます。
- 導入バージョン: -

##### `dns_cache_ttl_seconds`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: 成功したDNSルックアップのDNSキャッシュTTL (Time-To-Live) (秒単位)。これは、JVMが成功したDNSルックアップをキャッシュする期間を制御するJavaセキュリティプロパティ`networkaddress.cache.ttl`を設定します。情報を常にキャッシュできるようにするには、この項目を`-1`に設定し、キャッシュを無効にするには`0`に設定します。これは、Kubernetesデプロイメントや動的DNSが使用される場合など、IPアドレスが頻繁に変更される環境で特に役立ちます。
- 導入バージョン: v3.5.11, v4.0.4

##### `enable_http_async_handler`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: システムがHTTPリクエストを非同期で処理することを許可するかどうか。この機能が有効になっている場合、Nettyワーカー スレッドによって受信されたHTTPリクエストは、HTTPサーバーのブロックを回避するために、サービスロジック処理用の別のスレッド プールに送信されます。無効になっている場合、Nettyワーカーがサービスロジックを処理します。
- 導入バージョン: 4.0.0

##### `enable_http_validate_headers`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: NettyのHttpServerCodecが厳密なHTTPヘッダー検証を実行するかどうかを制御します。この値は、HttpServerでHTTPパイプラインが初期化されるときにHttpServerCodecに渡されます (UseLocationsを参照)。新しいnettyバージョンではより厳密なヘッダールールが適用されるため (https://github.com/netty/netty/pull/12760)、下位互換性のためにデフォルトはfalseです。RFC準拠のヘッダーチェックを強制するにはtrueに設定します。これにより、レガシークライアントやプロキシからの不正な形式の要求や非準拠の要求が拒否される可能性があります。変更を反映させるにはHTTPサーバーの再起動が必要です。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `enable_https`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: FEノードでHTTPサーバーとともにHTTPSサーバーを有効にするかどうか。
- 導入バージョン: v4.0

##### `frontend_address`

- デフォルト: 0.0.0.0
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのIPアドレス。
- 導入バージョン: -

##### `http_async_threads_num`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 非同期HTTPリクエスト処理用スレッドプールのサイズ。エイリアスは`max_http_sql_service_task_threads_num`です。
- 導入バージョン: 4.0.0

##### `http_backlog_num`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのHTTPサーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### `http_max_chunk_size`

- デフォルト: 8192
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: FE HTTPサーバーでNettyのHttpServerCodecが処理する単一のHTTPチャンクの最大許容サイズ (バイト単位) を設定します。HttpServerCodecの3番目の引数として渡され、チャンク転送またはストリーミングリクエスト/応答中のチャンクの長さを制限します。受信チャンクがこの値を超えると、Nettyはフレームが大きすぎるエラー (例: TooLongFrameException) を発生させ、リクエストが拒否される可能性があります。正当な大規模チャンクアップロードの場合はこれを増やし、メモリ圧力を減らしDoS攻撃の表面積を減らすために小さく保ちます。この設定は、`http_max_initial_line_length`、`http_max_header_size`、および`enable_http_validate_headers`と組み合わせて使用されます。
- 導入バージョン: v3.2.0

##### `http_max_header_size`

- デフォルト: 32768
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: Nettyの`HttpServerCodec`によって解析されるHTTPリクエストヘッダーブロックの最大許容サイズ (バイト単位)。StarRocksはこの値を`HttpServerCodec`に渡します (`Config.http_max_header_size`として)。受信リクエストのヘッダー (名前と値を合わせたもの) がこの制限を超えると、コーデックはリクエストを拒否し (デコーダー例外)、接続/リクエストは失敗します。クライアントが正当に非常に大きなヘッダー (大きなCookieまたは多数のカスタムヘッダー) を送信する場合にのみ増やしてください。値が大きいほど、接続あたりのメモリ使用量が増加します。`http_max_initial_line_length`および`http_max_chunk_size`と組み合わせて調整してください。変更にはFEの再起動が必要です。
- 導入バージョン: v3.2.0

##### `http_max_initial_line_length`

- デフォルト: 4096
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: HttpServerで使用されるNetty `HttpServerCodec`が受け入れるHTTP初期リクエストライン (メソッド + リクエストターゲット + HTTPバージョン) の最大許容長 (バイト単位) を設定します。この値はNettyのデコーダーに渡され、これよりも長い初期ラインを持つリクエストは拒否されます (TooLongFrameException)。非常に長いリクエストURIをサポートする必要がある場合にのみこれを増やしてください。値が大きいほどメモリ使用量が増加し、不正な形式の/リクエストの乱用に対する露出が増加する可能性があります。`http_max_header_size`および`http_max_chunk_size`と組み合わせて調整してください。
- 導入バージョン: v3.2.0

##### `http_port`

- デフォルト: 8030
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのHTTPサーバーがリッスンするポート。
- 導入バージョン: -

##### `http_web_page_display_hardware`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: trueの場合、HTTPインデックスページ (/index) にoshiライブラリ (CPU、メモリ、プロセス、ディスク、ファイルシステム、ネットワークなど) を介して入力されるハードウェア情報セクションが含まれます。oshiは、システムユーティリティを呼び出したり、システムファイルを間接的に読み取ったりする可能性があります (例: `getent passwd`などのコマンドを実行できます)。これにより、機密性の高いシステムデータが表面化する可能性があります。より厳密なセキュリティが必要な場合、またはホストでこれらの間接コマンドの実行を避けたい場合は、この設定をfalseに設定して、Web UIでのハードウェア詳細の収集と表示を無効にします。
- 導入バージョン: v3.2.0

##### `http_worker_threads_num`

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: HTTPリクエストを処理するためのHTTPサーバーのワーカー スレッド数。負の値または0の場合、スレッド数はCPUコア数の2倍になります。
- 導入バージョン: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### `https_port`

- デフォルト: 8443
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのHTTPSサーバーがリッスンするポート。
- 導入バージョン: v4.0

##### `max_mysql_service_task_threads_num`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのMySQLサーバーがタスクを処理するために実行できるスレッドの最大数。
- 導入バージョン: -

##### `max_task_runs_threads_num`

- デフォルト: 512
- タイプ: Int
- 単位: スレッド
- 変更可能: いいえ
- 説明: タスク実行エグゼキュータースレッドプール内のスレッドの最大数を制御します。この値は、同時タスク実行の上限です。これを増やすと並列処理が増加しますが、CPU、メモリ、ネットワーク使用量も増加し、減らすとタスク実行のバックログが発生し、レイテンシが高くなる可能性があります。この値は、予想される同時スケジューリングジョブと利用可能なシステムリソースに応じて調整してください。
- 導入バージョン: v3.2.0

##### `memory_tracker_enable`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEメモリトラッカーサブシステムを有効にします。`memory_tracker_enable`が`true`に設定されている場合、`MemoryUsageTracker`は定期的に登録されたメタデータモジュールをスキャンし、メモリ内の`MemoryUsageTracker.MEMORY_USAGE`マップを更新し、合計をログに記録し、`MetricRepo`がメモリ使用量とオブジェクト数ゲージをメトリック出力に公開するようにします。サンプリング間隔を制御するには`memory_tracker_interval_seconds`を使用します。この機能を有効にすると、メモリ消費量の監視とデバッグに役立ちますが、CPUとI/Oのオーバーヘッドと追加のメトリックカーディナリティが発生します。
- 導入バージョン: v3.2.4

##### `memory_tracker_interval_seconds`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: FE `MemoryUsageTracker`デーモンがFEプロセスと登録された`MemoryTrackable`モジュールのメモリ使用量をポーリングして記録する間隔 (秒単位)。`memory_tracker_enable`が`true`に設定されている場合、トラッカーはこの頻度で実行され、`MEMORY_USAGE`を更新し、集計されたJVMおよび追跡されたモジュールの使用量をログに記録します。
- 導入バージョン: v3.2.4

##### `mysql_nio_backlog_num`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのMySQLサーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### `mysql_server_version`

- デフォルト: 8.0.33
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: クライアントに返されるMySQLサーバーバージョン。このパラメータを変更すると、以下の状況でバージョン情報に影響します。
  1. `select version();`
  2. ハンドシェイクパケットバージョン
  3. グローバル変数`version`の値 (`show variables like 'version';`)
- 導入バージョン: -

##### `mysql_service_io_threads_num`

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのMySQLサーバーがI/Oイベントを処理するために実行できるスレッドの最大数。
- 導入バージョン: -

##### `mysql_service_kill_after_disconnect`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: MySQL TCP接続が閉じられたことが検出されたときに、サーバーがセッションをどのように処理するかを制御します (読み取り時にEOF)。`true`に設定されている場合、サーバーはその接続で実行中のクエリを直ちに強制終了し、直ちにクリーンアップを実行します。`false`の場合、サーバーは切断時に実行中のクエリを強制終了せず、保留中のリクエストタスクがない場合にのみクリーンアップを実行し、クライアントが切断した後も長時間実行されるクエリが続行できるようにします。注: TCPキープアライブを示唆する簡単なコメントにもかかわらず、このパラメータは切断後の強制終了動作を具体的に管理し、孤立したクエリを終了させるか (信頼できない/負荷分散されたクライアントの背後で推奨) 、完了させるかを希望に応じて設定する必要があります。
- 導入バージョン: -

##### `mysql_service_nio_enable_keep_alive`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: MySQL接続のTCP Keep-Aliveを有効にします。ロードバランサーの背後にある長期間アイドル状態の接続に役立ちます。
- 導入バージョン: -

##### `net_use_ipv6_when_priority_networks_empty`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: `priority_networks`が指定されていない場合に、IPv6アドレスを優先的に使用するかどうかを制御するブール値。`true`は、ノードをホストするサーバーがIPv4アドレスとIPv6アドレスの両方を持っており、`priority_networks`が指定されていない場合に、システムがIPv6アドレスを優先的に使用することを許可することを示します。
- 導入バージョン: v3.3.0

##### `priority_networks`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 複数のIPアドレスを持つサーバーの選択戦略を宣言します。最大で1つのIPアドレスがこのパラメータで指定されたリストと一致する必要があることに注意してください。このパラメータの値は、CIDR表記でセミコロン(;)で区切られたエントリ (例: 10.10.10.0/24) で構成されるリストです。このリストのエントリにIPアドレスが一致しない場合、サーバーの利用可能なIPアドレスがランダムに選択されます。v3.3.0以降、StarRocksはIPv6ベースのデプロイメントをサポートしています。サーバーがIPv4とIPv6の両方のアドレスを持っており、このパラメータが指定されていない場合、システムはデフォルトでIPv4アドレスを使用します。この動作は、`net_use_ipv6_when_priority_networks_empty`を`true`に設定することで変更できます。
- 導入バージョン: -

##### `proc_profile_cpu_enable`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、バックグラウンドの`ProcProfileCollector`は`AsyncProfiler`を使用してCPUプロファイルを収集し、HTMLレポートを`sys_log_dir/proc_profile`の下に書き込みます。各収集実行では、`proc_profile_collect_time_s`で設定された期間のCPUスタックが記録され、Javaスタック深度には`proc_profile_jstack_depth`が使用されます。生成されたプロファイルは圧縮され、古いファイルは`proc_profile_file_retained_days`と`proc_profile_file_retained_size_bytes`に従ってパージされます。`AsyncProfiler`にはネイティブライブラリ (`libasyncProfiler.so`) が必要です。`/tmp`でのnoexecの問題を回避するために、`one.profiler.extractPath`は`STARROCKS_HOME_DIR/bin`に設定されます。
- 導入バージョン: v3.2.12

##### `qe_max_connection`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: すべてのユーザーがFEノードに確立できる接続の最大数。v3.1.12およびv3.2.7以降、デフォルト値が`1024`から`4096`に変更されました。
- 導入バージョン: -

##### `query_port`

- デフォルト: 9030
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのMySQLサーバーがリッスンするポート。
- 導入バージョン: -

##### `rpc_port`

- デフォルト: 9020
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのThriftサーバーがリッスンするポート。
- 導入バージョン: -

##### `slow_lock_stack_trace_reserve_levels`

- デフォルト: 15
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocksが低速または保持されたロックのロックデバッグ情報をダンプするときに、キャプチャされ出力されるスタックトレースフレームの数を制御します。この値は、排他ロック所有者、現在のスレッド、および最も古い/共有リーダーのJSONを生成する際に、`QueryableReentrantReadWriteLock`によって`LogUtil.getStackTraceToJsonArray`に渡されます。この値を増やすと、低速ロックまたはデッドロックの問題を診断するためのより多くのコンテキストが提供されますが、JSONペイロードが大きくなり、スタックキャプチャのためのCPU/メモリがわずかに増加します。減らすとオーバーヘッドが減少します。注: 低速ロックのみをログに記録する場合、リーダーエントリは`slow_lock_threshold_ms`によってフィルタリングできます。
- 導入バージョン: v3.4.0, v3.5.0

##### `ssl_cipher_blacklist`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: IANA名でSSL暗号スイートをブラックリスト化する、正規表現をサポートするカンマ区切りのリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- 導入バージョン: v4.0

##### `ssl_cipher_whitelist`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: IANA名でSSL暗号スイートをホワイトリスト化する、正規表現をサポートするカンマ区切りのリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- 導入バージョン: v4.0

##### `task_runs_concurrency`

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時に実行されるTaskRunインスタンスのグローバル制限。`TaskRunScheduler`は、現在の実行数が`task_runs_concurrency`以上の場合、新しい実行のスケジューリングを停止するため、この値はスケジューラー全体の並列TaskRun実行を制限します。これは`MVPCTRefreshPartitioner`によっても使用され、TaskRunごとのパーティション更新粒度を計算します。値を増やすと並列処理とリソース使用量が増加し、減らすと並列処理が減少し、パーティション更新が実行ごとに大きくなります。スケジューリングを意図的に無効にする場合を除き、0または負の値に設定しないでください。0 (または負の値) は`TaskRunScheduler`による新しいTaskRunのスケジューリングを実質的に防止します。
- 導入バージョン: v3.2.0

##### `task_runs_queue_length`

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 保留中のキューに保持される保留中のTaskRunアイテムの最大数を制限します。`TaskRunManager`は現在の保留中の数をチェックし、有効な保留中のTaskRun数が`task_runs_queue_length`以上の場合、新しい送信を拒否します。マージ/承認されたTaskRunが追加される前にも同じ制限が再チェックされます。この値を調整して、メモリとスケジューリングバックログのバランスを取ります。大規模なバーストワークロードの場合は拒否を避けるために高く設定し、メモリを制限し保留中のバックログを減らすために低く設定します。
- 導入バージョン: v3.2.0

##### `thrift_backlog_num`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEノードのThriftサーバーが保持するバックログキューの長さ。
- 導入バージョン: -

##### `thrift_client_timeout_ms`

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: アイドル状態のクライアント接続がタイムアウトするまでの時間。
- 導入バージョン: -

##### `thrift_rpc_max_body_size`

- デフォルト: -1
- タイプ: Int
- 単位: バイト
- 変更可能: いいえ
- 説明: サーバーのThriftプロトコルを構築する際に使用されるThrift RPCメッセージ本体の最大許容サイズ (バイト単位) を制御します (ThriftServerのTBinaryProtocol.Factoryに渡されます)。`-1`の値は制限を無効にします (無制限)。正の値を設定すると上限が適用され、これよりも大きなメッセージはThriftレイヤーによって拒否されます。これはメモリ使用量を制限し、サイズ超過リクエストやDoSのリスクを軽減するのに役立ちます。正当なペイロード (大きな構造体やバッチデータ) に十分な大きさに設定して、正当なリクエストが拒否されるのを避けてください。
- 導入バージョン: v3.2.0

##### `thrift_server_max_worker_threads`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEノードのThriftサーバーがサポートするワーカー スレッドの最大数。
- 導入バージョン: -

##### `thrift_server_queue_size`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: リクエストが保留中のキューの長さ。thriftサーバーで処理中のスレッド数が`thrift_server_max_worker_threads`で指定された値を超えると、新しいリクエストが保留中のキューに追加されます。
- 導入バージョン: -

### メタデータとクラスター管理

##### `alter_max_worker_queue_size`

- デフォルト: 4096
- タイプ: Int
- 単位: タスク
- 変更可能: いいえ
- 説明: alterサブシステムが使用する内部ワーカー スレッドプールキューの容量を制御します。`alter_max_worker_threads`とともに`AlterHandler`の`ThreadPoolManager.newDaemonCacheThreadPool`に渡されます。保留中のalterタスクの数が`alter_max_worker_queue_size`を超えると、新しい送信は拒否され、`RejectedExecutionException`がスローされる可能性があります (`AlterHandler.handleFinishAlterTask`を参照)。この値を調整して、メモリ使用量と同時alterタスクに対して許容するバックログの量のバランスを取ります。
- 導入バージョン: v3.2.0

##### `alter_max_worker_threads`

- デフォルト: 4
- タイプ: Int
- 単位: スレッド
- 変更可能: いいえ
- 説明: AlterHandlerのスレッドプール内のワーカー スレッドの最大数を設定します。AlterHandlerは、この値でエグゼキューターを構築し、alter関連タスクを実行および完了します (例: handleFinishAlterTaskを介して`AlterReplicaTask`を送信)。この値はalter操作の同時実行を制限します。値を上げると並列処理とリソース使用量が増加し、下げると同時alterが制限され、ボトルネックになる可能性があります。エグゼキューターは`alter_max_worker_queue_size`とともに作成され、ハンドラースケジューリングは`alter_scheduler_interval_millisecond`を使用します。
- 導入バージョン: v3.2.0

##### `automated_cluster_snapshot_interval_seconds`

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 自動クラスター スナップショットタスクがトリガーされる間隔。
- 導入バージョン: v3.4.2

##### `background_refresh_metadata_interval_millis`

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: 連続する2つのHiveメタデータキャッシュ更新の間隔。
- 導入バージョン: v2.5.5

##### `background_refresh_metadata_time_secs_since_last_access_secs`

- デフォルト: 3600 * 24
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: Hiveメタデータキャッシュ更新タスクの有効期限。アクセスされたHiveカタログの場合、指定された時間以上アクセスされていない場合、StarRocksはそのキャッシュされたメタデータの更新を停止します。アクセスされていないHiveカタログの場合、StarRocksはそのキャッシュされたメタデータを更新しません。
- 導入バージョン: v2.5.5

##### `bdbje_cleaner_threads`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksジャーナルで使用されるBerkeley DB Java Edition (JE) 環境のバックグラウンドクリーナースレッド数。この値は、`BDBEnvironment.initConfigs`の環境初期化中に読み取られ、`Config.bdbje_cleaner_threads`を使用して`EnvironmentConfig.CLEANER_THREADS`に適用されます。JEログクリーニングとスペース再利用の並列処理を制御します。これを増やすと、フォアグラウンド操作との追加のCPUとI/Oの干渉を犠牲にして、クリーニングを高速化できます。変更はBDB環境が(再)初期化されるときにのみ有効になるため、新しい値を適用するにはフロントエンドの再起動が必要です。
- 導入バージョン: v3.2.0

##### `bdbje_heartbeat_timeout_second`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: StarRocksクラスターのリーダー、フォロワー、オブザーバーFE間のハートビートがタイムアウトするまでの時間。
- 導入バージョン: -

##### `bdbje_lock_timeout_second`

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: BDB JEベースのFE内のロックがタイムアウトするまでの時間。
- 導入バージョン: -

##### `bdbje_replay_cost_percent`

- デフォルト: 150
- タイプ: Int
- 単位: パーセント
- 変更可能: いいえ
- 説明: BDB JEログからトランザクションをリプレイする相対コスト (パーセンテージ) を、ネットワークリカバリを介して同じデータを取得するコストと比較して設定します。この値は基盤となるJEレプリケーションパラメータREPLAY_COST_PERCENTに提供され、通常はリプレイがネットワークリカバリよりも費用がかかることを示すために`>100`です。クリーンアップされたログファイルを潜在的なリプレイのために保持するかどうかを決定する際、システムはリプレイコストにログサイズを乗じた値をネットワークリカバリのコストと比較します。ネットワークリカバリの方が効率的と判断された場合、ファイルは削除されます。値が0の場合、このコスト比較に基づいた保持は無効になります。`REP_STREAM_TIMEOUT`内のレプリカに必要なログファイル、またはアクティブなレプリケーションに必要なログファイルは常に保持されます。
- 導入バージョン: v3.2.0

##### `bdbje_replica_ack_timeout_second`

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: リーダーFEからフォロワーFEにメタデータが書き込まれる際に、リーダーFEが指定された数のフォロワーFEからのACKメッセージを待機できる最大時間。単位: 秒。大量のメタデータが書き込まれている場合、フォロワーFEはリーダーFEにACKメッセージを返すまでに長い時間を要し、ACKタイムアウトが発生する可能性があります。この状況では、メタデータ書き込みが失敗し、FEプロセスが終了します。この状況を防ぐために、このパラメータの値を増やすことをお勧めします。
- 導入バージョン: -

##### `bdbje_reserved_disk_size`

- デフォルト: 512 * 1024 * 1024 (536870912)
- タイプ: Long
- 単位: バイト
- 変更可能: いいえ
- 説明: Berkeley DB JEが「保護されていない」(削除可能な) ログ/データファイルとして予約するバイト数を制限します。StarRocksは、BDBEnvironmentで`EnvironmentConfig.RESERVED_DISK`を介してこの値をJEに渡します。JEの組み込みのデフォルトは0 (無制限) です。StarRocksのデフォルト (512 MiB) は、JEが保護されていないファイルのために過剰なディスク領域を予約するのを防ぎながら、古いファイルを安全にクリーンアップできるようにします。ディスク制約のあるシステムでこの値を調整してください。値を減らすとJEはより多くのファイルをより早く解放でき、増やすとJEはより多くの予約領域を保持できます。変更を反映させるにはプロセスを再起動する必要があります。
- 導入バージョン: v3.2.0

##### `bdbje_reset_election_group`

- デフォルト: false
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: BDBJEレプリケーショングループをリセットするかどうか。このパラメータが`TRUE`に設定されている場合、FEはBDBJEレプリケーショングループをリセットし (つまり、すべての選挙可能なFEノードの情報を削除し)、リーダーFEとして起動します。リセット後、このFEはクラスター内で唯一のメンバーとなり、他のFEは`ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`を使用してこのクラスターに再参加できます。この設定は、ほとんどのフォロワーFEのデータが破損しているためにリーダーFEが選択できない場合にのみ使用してください。`reset_election_group`は`metadata_failure_recovery`を置き換えるために使用されます。
- 導入バージョン: -

##### `black_host_connect_failures_within_time`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ブラックリストに登録されたBEノードに許可される接続失敗のしきい値。BEノードが自動的にBEブラックリストに追加された場合、StarRocksはその接続性を評価し、BEブラックリストから削除できるかどうかを判断します。`black_host_history_sec`内で、ブラックリストに登録されたBEノードの接続失敗が`black_host_connect_failures_within_time`で設定されたしきい値よりも少ない場合にのみ、BEブラックリストから削除できます。
- 導入バージョン: v3.3.0

##### `black_host_history_sec`

- デフォルト: 2 * 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BEブラックリスト内のBEノードの過去の接続失敗を保持する期間。BEノードが自動的にBEブラックリストに追加された場合、StarRocksはその接続性を評価し、BEブラックリストから削除できるかどうかを判断します。`black_host_history_sec`内で、ブラックリストに登録されたBEノードの接続失敗が`black_host_connect_failures_within_time`で設定されたしきい値よりも少ない場合にのみ、BEブラックリストから削除できます。
- 導入バージョン: v3.3.0

##### `brpc_connection_pool_size`

- デフォルト: 16
- タイプ: Int
- 単位: 接続
- 変更可能: いいえ
- 説明: FEのBrpcProxyが使用するエンドポイントあたりのプールされたBRPC接続の最大数。この値は`setMaxTotoal`および`setMaxIdleSize`を介してRpcClientOptionsに適用されるため、各リクエストがプールから接続を借りる必要があるため、同時送信BRPCリクエストを直接制限します。高並列シナリオでは、リクエストのキューイングを避けるためにこれを増やしてください。増やすとソケットとメモリの使用量が増加し、リモートサーバーの負荷が増加する可能性があります。調整する際には、`brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr`、および`brpc_min_evictable_idle_time_ms`などの関連設定を考慮してください。この値の変更はホットリロード可能ではなく、再起動が必要です。
- 導入バージョン: v3.2.0

##### `brpc_short_connection`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: 基盤となるbrpc RpcClientが短命の接続を使用するかどうかを制御します。有効な場合 (`true`)、RpcClientOptions.setShortConnectionが設定され、リクエスト完了後に接続が閉じられ、より高い接続設定オーバーヘッドとレイテンシの増加を犠牲にして、長時間存続するソケットの数を減らします。無効な場合 (`false`、デフォルト)、永続接続と接続プールが使用されます。このオプションを有効にすると接続プールの動作に影響するため、`brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr`、および`brpc_inner_reuse_pool`と組み合わせて考慮する必要があります。一般的な高スループットデプロイメントでは無効のままにしてください。ソケットの寿命を制限する場合、またはネットワークポリシーで短命接続が必要な場合にのみ有効にしてください。
- 導入バージョン: v3.3.11, v3.4.1, v3.5.0

##### `catalog_try_lock_timeout_ms`

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: グローバルロックを取得するためのタイムアウト期間。
- 導入バージョン: -

##### `checkpoint_only_on_leader`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: `true`の場合、CheckpointControllerはリーダーFEのみをチェックポイントワーカーとして選択します。`false`の場合、コントローラーは任意のフロントエンドを選択する可能性があり、ヒープ使用量が少ないノードを優先します。`false`の場合、ワーカーは最近の失敗時刻と`heapUsedPercent`でソートされます (リーダーは無限のヒープ使用量を持つと見なされ、選択を避けます)。クラスター スナップショットメタデータを必要とする操作の場合、コントローラーはすでにこのフラグに関係なくリーダー選択を強制します。`true`を有効にすると、チェックポイント作業がリーダーに集中化されます (シンプルですが、リーダーのCPU/メモリとネットワーク負荷が増加します)。`false`のままにすると、チェックポイント負荷が負荷の少ないFEに分散されます。この設定はワーカーの選択と、`checkpoint_timeout_seconds`のようなタイムアウトや`thrift_rpc_timeout_ms`のようなRPC設定との相互作用に影響します。
- 導入バージョン: v3.4.0, v3.5.0

##### `checkpoint_timeout_seconds`

- デフォルト: 24 * 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: リーダーのCheckpointControllerがチェックポイントワーカーがチェックポイントを完了するのを待機する最大時間 (秒単位)。コントローラーはこの値をナノ秒に変換し、ワーカーの結果キューをポーリングします。このタイムアウト内に成功した完了が受信されない場合、チェックポイントは失敗と見なされ、createImageは失敗を返します。この値を増やすと、より長いチェックポイント実行に対応できますが、失敗検出と後続のイメージ伝播が遅れます。減らすと、より高速なフェイルオーバー/再試行が発生しますが、低速なワーカーに対して誤ったタイムアウトが発生する可能性があります。この設定は、チェックポイント作成中の`CheckpointController`での待機期間のみを制御し、ワーカーの内部チェックポイント動作は変更しません。
- 導入バージョン: v3.4.0, v3.5.0

##### `db_used_data_quota_update_interval_secs`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データベースのデータ使用量クォータが更新される間隔。StarRocksは定期的にすべてのデータベースのデータ使用量クォータを更新し、ストレージ消費量を追跡します。この値は、クォータの適用とメトリック収集に使用されます。過剰なシステム負荷を防ぐため、最小許容間隔は30秒です。30未満の値は拒否されます。
- 導入バージョン: -

##### `drop_backend_after_decommission`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: BEが廃止された後にBEを削除するかどうか。`TRUE`は、BEが廃止された直後に削除されることを示します。`FALSE`は、BEが廃止された後に削除されないことを示します。
- 導入バージョン: -

##### `edit_log_port`

- デフォルト: 9010
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: クラスター内のリーダー、フォロワー、オブザーバーFE間の通信に使用されるポート。
- 導入バージョン: -

##### `edit_log_roll_num`

- デフォルト: 50000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ログファイルが作成される前に書き込むことができるメタデータログエントリの最大数。このパラメータはログファイルのサイズを制御するために使用されます。新しいログファイルはBDBJEデータベースに書き込まれます。
- 導入バージョン: -

##### `edit_log_type`

- デフォルト: BDB
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 生成できる編集ログのタイプ。値を`BDB`に設定します。
- 導入バージョン: -

##### `enable_background_refresh_connector_metadata`

- デフォルト: v3.0以降はtrue、v2.5はfalse
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 定期的なHiveメタデータキャッシュ更新を有効にするかどうか。有効にすると、StarRocksはHiveクラスターのメタストア (Hive MetastoreまたはAWS Glue) をポーリングし、頻繁にアクセスされるHiveカタログのキャッシュされたメタデータを更新してデータ変更を感知します。`true`はHiveメタデータキャッシュ更新を有効にすることを示し、`false`は無効にすることを示します。
- 導入バージョン: v2.5.5

##### `enable_collect_query_detail_info`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリのプロファイルを収集するかどうか。このパラメータが`TRUE`に設定されている場合、システムはクエリのプロファイルを収集します。このパラメータが`FALSE`に設定されている場合、システムはクエリのプロファイルを収集しません。
- 導入バージョン: -

##### `enable_create_partial_partition_in_batch`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`false` (デフォルト) に設定されている場合、StarRocksはバッチ作成された範囲パーティションが標準の時間単位境界に整列するように強制します。穴の作成を避けるために、非整列の範囲は拒否されます。この項目を`true`に設定すると、その整列チェックが無効になり、バッチで部分的な (非標準の) パーティションを作成できるようになり、ギャップや誤ったパーティション範囲が生じる可能性があります。意図的に部分的なバッチパーティションが必要で、関連するリスクを受け入れる場合にのみ`true`に設定してください。
- 導入バージョン: v3.2.0

##### `enable_internal_sql`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: この項目が`true`に設定されている場合、内部コンポーネント (例: SimpleExecutor) によって実行される内部SQLステートメントは保持され、内部監査またはログメッセージに書き込まれます (そして、`enable_sql_desensitize_in_log`が設定されている場合、さらに非機密化できます)。`false`に設定されている場合、内部SQLテキストは抑制されます。フォーマットコード (SimpleExecutor.formatSQL) は"?"を返し、実際のステートメントは内部監査またはログメッセージに出力されません。この設定は内部ステートメントの実行セマンティクスを変更しません。プライバシーまたはセキュリティのために内部SQLのロギングと可視性のみを制御します。
- 導入バージョン: -

##### `enable_legacy_compatibility_for_replication`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: レプリケーションのレガシー互換性を有効にするかどうか。StarRocksは古いバージョンと新しいバージョンで異なる動作をする可能性があり、クラスター間データ移行中に問題を引き起こす可能性があります。そのため、データ移行前にターゲットクラスターのレガシー互換性を有効にし、データ移行完了後に無効にする必要があります。`true`はこのモードを有効にすることを示します。
- 導入バージョン: v3.1.10, v3.2.6

##### `enable_show_materialized_views_include_all_task_runs`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: SHOW MATERIALIZED VIEWSコマンドにTaskRunがどのように返されるかを制御します。この項目が`false`に設定されている場合、StarRocksはタスクごとに最新のTaskRunのみを返します (互換性のためのレガシー動作)。`true` (デフォルト) に設定されている場合、`TaskManager`は同じ開始TaskRun IDを共有する (例: 同じジョブに属する) 場合にのみ、同じタスクに対して追加のTaskRunを含めることができ、無関係な重複実行が表示されるのを防ぎながら、1つのジョブに関連付けられた複数のステータスを表示できるようにします。この項目を`false`に設定すると、単一実行出力が復元されるか、デバッグおよび監視のために複数実行ジョブ履歴が表示されます。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `enable_statistics_collect_profile`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 統計クエリのプロファイルを生成するかどうか。この項目を`true`に設定すると、StarRocksはシステム統計に関するクエリのプロファイルを生成できるようになります。
- 導入バージョン: v3.1.5

##### `enable_table_name_case_insensitive`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: カタログ名、データベース名、テーブル名、ビュー名、マテリアライズドビュー名の大文字小文字を区別しない処理を有効にするかどうか。現在、テーブル名はデフォルトで大文字小文字を区別します。
  - この機能を有効にすると、関連するすべての名前が小文字で保存され、これらの名前を含むすべてのSQLコマンドは自動的に小文字に変換されます。
  - この機能は、クラスター作成時にのみ有効にできます。**クラスター起動後、この設定の値をいかなる方法でも変更することはできません。** 変更しようとするとエラーが発生します。FEは、この設定値がクラスターが最初に起動されたときと一致しないことを検出すると、起動に失敗します。
  - 現在、この機能はJDBCカタログおよびテーブル名をサポートしていません。JDBCまたはODBCデータソースに対して大文字小文字を区別しない処理を実行したい場合は、この機能を有効にしないでください。
- 導入バージョン: v4.0

##### `enable_task_history_archive`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、完了したタスク実行レコードは永続タスク実行履歴テーブルにアーカイブされ、編集ログに記録されるため、ルックアップ (例: `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`) にアーカイブされた結果が含まれます。アーカイブはFEリーダーによって実行され、単体テスト中 (`FeConstants.runningUnitTest`) はスキップされます。有効な場合、メモリ内有効期限と強制GCパスはバイパスされます (コードは`removeExpiredRuns`と`forceGC`から早期に戻ります)。したがって、保持/排除は`task_runs_ttl_second`と`task_runs_max_history_number`ではなく永続アーカイブによって処理されます。無効な場合、履歴はメモリ内に保持され、これらの設定によって削除されます。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### `enable_task_run_fe_evaluation`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、FEは`TaskRunsSystemTable.supportFeEvaluation`でシステムテーブル`task_runs`に対してローカル評価を実行します。FE側の評価は、列を定数と比較する結合等価述語に対してのみ許可され、`QUERY_ID`および`TASK_NAME`列に限定されます。これを有効にすると、ターゲットを絞ったルックアップのパフォーマンスが向上し、より広範なスキャンや追加のリモート処理が回避されます。無効にすると、プランナーは`task_runs`のFE評価をスキップすることを強制され、述語の刈り込みが減少し、これらのフィルターのクエリレイテンシに影響する可能性があります。
- 導入バージョン: v3.3.13, v3.4.3, v3.5.0

##### `heartbeat_mgr_blocking_queue_size`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Heartbeat Managerによって実行されるハートビートタスクを格納するブロッキングキューのサイズ。
- 導入バージョン: -

##### `heartbeat_mgr_threads_num`

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: Heartbeat Managerがハートビートタスクを実行するために実行できるスレッドの数。
- 導入バージョン: -

##### `ignore_materialized_view_error`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEがマテリアライズドビューのエラーによって引き起こされるメタデータ例外を無視するかどうか。マテリアライズドビューのエラーによって引き起こされるメタデータ例外のためにFEが起動に失敗する場合、このパラメータを`true`に設定することでFEが例外を無視できるようにします。
- 導入バージョン: v2.5.10

##### `ignore_meta_check`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非リーダーFEがリーダーFEからのメタデータギャップを無視するかどうか。値がTRUEの場合、非リーダーFEはリーダーFEからのメタデータギャップを無視し、データ読み取りサービスを提供し続けます。このパラメータは、リーダーFEを長期間停止しても連続的なデータ読み取りサービスを保証します。値がFALSEの場合、非リーダーFEはリーダーFEからのメタデータギャップを無視せず、データ読み取りサービスを停止します。
- 導入バージョン: -

##### `ignore_task_run_history_replay_error`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: StarRocksが`information_schema.task_runs`のTaskRun履歴行を逆シリアル化する際、破損または無効なJSON行は通常、逆シリアル化が警告をログに記録し、RuntimeExceptionをスローします。この項目が`true`に設定されている場合、システムは逆シリアル化エラーをキャッチし、不正な形式のレコードをスキップし、クエリを失敗させる代わりに残りの行の処理を続行します。これにより、`information_schema.task_runs`クエリが`_statistics_.task_run_history`テーブル内の不正なエントリを許容できるようになります。ただし、これを有効にすると、破損した履歴レコードが明示的なエラーを表示する代わりにサイレントに削除される可能性があることに注意してください (潜在的なデータ損失)。
- 導入バージョン: v3.3.3, v3.4.0, v3.5.0

##### `lock_checker_interval_second`

- デフォルト: 30
- タイプ: long
- 単位: 秒
- 変更可能: はい
- 説明: LockCheckerフロントエンドデーモン ("deadlock-checker"という名前) の実行間隔 (秒単位)。デーモンはデッドロック検出と低速ロック スキャンを実行します。構成された値は1000倍され、ミリ秒単位でタイマーを設定します。この値を減らすと検出レイテンシは減少しますが、スケジューリングとCPUオーバーヘッドが増加します。増やすとオーバーヘッドは減少しますが、検出と低速ロックレポートが遅れます。デーモンは実行ごとに間隔をリセットするため、変更は実行時に有効になります。この設定は`lock_checker_enable_deadlock_check` (デッドロックチェックを有効にする) および`slow_lock_threshold_ms` (低速ロックを構成するものを定義する) と相互作用します。
- 導入バージョン: v3.2.0

##### `master_sync_policy`

- デフォルト: SYNC
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: リーダーFEがログをディスクにフラッシュする際のポリシー。このパラメータは現在のFEがリーダーFEである場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュが同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。

  フォロワーFEが1つしかデプロイされていない場合は、このパラメータを`SYNC`に設定することをお勧めします。フォロワーFEが3つ以上デプロイされている場合は、このパラメータと`replica_sync_policy`の両方を`WRITE_NO_SYNC`に設定することをお勧めします。

- 導入バージョン: -

##### `max_bdbje_clock_delta_ms`

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: StarRocksクラスターのリーダーFEとフォロワーまたはオブザーバーFEの間で許可される最大クロックオフセット。
- 導入バージョン: -

##### `meta_delay_toleration_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: フォロワーおよびオブザーバーFE上のメタデータがリーダーFE上のメタデータから遅延できる最大期間。単位: 秒。この期間を超えると、非リーダーFEはサービス提供を停止します。
- 導入バージョン: -

##### `meta_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: メタデータを格納するディレクトリ。
- 導入バージョン: -

##### `metadata_ignore_unknown_operation_type`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 不明なログIDを無視するかどうか。FEがロールバックされると、以前のバージョンのFEは一部のログIDを認識できない場合があります。値が`TRUE`の場合、FEは不明なログIDを無視します。値が`FALSE`の場合、FEは終了します。
- 導入バージョン: -

##### `profile_info_format`

- デフォルト: default
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: システムが出力するプロファイルの形式。有効な値: `default`と`json`。`default`に設定されている場合、プロファイルはデフォルト形式です。`json`に設定されている場合、システムはJSON形式でプロファイルを出力します。
- 導入バージョン: v2.5

##### `replica_ack_policy`

- デフォルト: SIMPLE_MAJORITY
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: ログエントリが有効と見なされる際のポリシー。デフォルト値`SIMPLE_MAJORITY`は、フォロワーFEの過半数がACKメッセージを返した場合にログエントリが有効と見なされることを指定します。
- 導入バージョン: -

##### `replica_sync_policy`

- デフォルト: SYNC
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: フォロワーFEがログをディスクにフラッシュする際のポリシー。このパラメータは現在のFEがフォロワーFEである場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュが同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。
- 導入バージョン: -

##### `start_with_incomplete_meta`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: trueの場合、イメージデータが存在するがBerkeley DB JE (BDB) ログファイルが欠落または破損している場合にFEの起動を許可します。`MetaHelper.checkMetaDir()`はこのフラグを使用して、対応するBDBログのないイメージからの起動を通常防止する安全チェックをバイパスします。このように起動すると、古いまたは一貫性のないメタデータが生成される可能性があり、緊急リカバリにのみ使用すべきです。`RestoreClusterSnapshotMgr`はクラスター スナップショットを復元する際にこのフラグを一時的にtrueに設定し、その後ロールバックします。このコンポーネントは復元中に`bdbje_reset_election_group`も切り替えます。通常の操作では有効にしないでください。破損したBDBデータからのリカバリ時、またはイメージベースのスナップショットを明示的に復元する場合にのみ有効にしてください。
- 導入バージョン: v3.2.0

##### `table_keeper_interval_second`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: TableKeeperデーモンの実行間隔 (秒単位)。TableKeeperDaemonはこの値 (1000倍されます) を使用して内部タイマーを設定し、履歴テーブルが存在すること、正しいテーブルプロパティ (レプリケーション数) とパーティションTTLの更新を保証するキーパータスクを定期的に実行します。デーモンはリーダーノードでのみ作業を実行し、`table_keeper_interval_second`が変更されると`setInterval`を介して実行時間隔を更新します。スケジューリング頻度と負荷を減らすには増やし、欠落または古い履歴テーブルへの反応を速くするには減らします。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### `task_runs_ttl_second`

- デフォルト: 7 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タスク実行履歴のTime-to-Live (TTL) を制御します。この値を下げると履歴の保持期間が短くなり、メモリ/ディスク使用量が減少します。上げると履歴が長く保持されますが、リソース使用量が増加します。`task_runs_max_history_number`および`enable_task_history_archive`と組み合わせて、予測可能な保持およびストレージ動作のために調整してください。
- 導入バージョン: v3.2.0

##### `task_ttl_second`

- デフォルト: 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タスクのTime-to-Live (TTL)。手動タスク (スケジュールが設定されていない場合) の場合、TaskBuilderはこの値を使用してタスクの`expireTime`を計算します (`expireTime = now + task_ttl_second * 1000L`)。TaskRunもこの値を実行タイムアウトの上限として使用します。実効実行タイムアウトは`min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`です。この値を調整すると、手動で作成されたタスクが有効なままになる期間が変更され、タスク実行の最大許容実行時間が間接的に制限される可能性があります。
- 導入バージョン: v3.2.0

##### `thrift_rpc_retry_times`

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: Thrift RPC呼び出しが実行する試行の合計回数を制御します。この値は`ThriftRPCRequestExecutor` (および`NodeMgr`や`VariableMgr`などの呼び出し元) によって再試行のループカウントとして使用されます。つまり、3という値は最初の試行を含めて最大3回までの試行を許可します。`TTransportException`が発生した場合、エグゼキューターは接続を再オープンしてこの回数まで再試行します。`SocketTimeoutException`が原因の場合や再オープンが失敗した場合は再試行しません。各試行は`thrift_rpc_timeout_ms`で設定された試行ごとのタイムアウトの対象となります。この値を増やすと一時的な接続障害に対する回復力が向上しますが、全体的なRPCレイテンシとリソース使用量が増加する可能性があります。
- 導入バージョン: v3.2.0

##### `thrift_rpc_strict_mode`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Thriftサーバーで使用されるTBinaryProtocolの「厳密な読み取り」モードを制御します。この値はThriftサーバー スタックのorg.apache.thrift.protocol.TBinaryProtocol.Factoryの最初の引数として渡され、受信Thriftメッセージの解析と検証方法に影響します。`true` (デフォルト) の場合、サーバーは厳密なThriftエンコーディング/バージョンチェックを適用し、設定された`thrift_rpc_max_body_size`制限を尊重します。`false`の場合、サーバーは非厳密な (レガシー/寛容な) メッセージ形式を受け入れます。これは古いクライアントとの互換性を向上させることができますが、一部のプロトコル検証をバイパスする可能性があります。実行中のクラスターでこれを変更する場合は注意してください。これは変更不可であり、相互運用性と解析の安全性に影響します。
- 導入バージョン: v3.2.0

##### `thrift_rpc_timeout_ms`

- デフォルト: 10000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: Thrift RPC呼び出しのデフォルトのネットワーク/ソケットタイムアウトとして使用されるタイムアウト (ミリ秒単位)。`ThriftConnectionPool`でThriftクライアントを作成する際にTSocketに渡され (フロントエンドおよびバックエンドプールで使用されます)、また、`ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr`、`CheckpointWorker`などの場所でRPC呼び出しタイムアウトを計算する際に、操作の実行タイムアウト (例: ExecTimeout*1000 + `thrift_rpc_timeout_ms`) に追加されます。この値を増やすと、RPC呼び出しがより長いネットワークまたはリモート処理の遅延を許容できるようになります。減らすと、低速なネットワークでのフェイルオーバーが高速化されます。この値を変更すると、Thrift RPCを実行するFEコードパス全体で接続作成とリクエストの期限に影響します。
- 導入バージョン: v3.2.0

##### `txn_latency_metric_report_groups`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: レポートするトランザクションレイテンシメトリックグループのカンマ区切りリスト。ロードタイプは監視のために論理グループに分類されます。グループが有効になると、その名前がトランザクションメトリックに「type」ラベルとして追加されます。有効な値: `stream_load`、`routine_load`、`broker_load`、`insert`、および`compaction` (共有データクラスターでのみ利用可能)。例: `"stream_load,routine_load"`。
- 導入バージョン: v4.0

##### `txn_rollback_limit`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: ロールバックできるトランザクションの最大数。
- 導入バージョン: -

### ユーザー、ロール、および権限

##### `enable_task_info_mask_credential`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: trueの場合、StarRocksは`information_schema.tasks`および`information_schema.task_runs`でタスクSQL定義を返す前に、`SqlCredentialRedactor.redact`をDEFINITION列に適用して、資格情報を編集します。`information_schema.task_runs`では、定義がタスク実行ステータスから来ているか、空の場合はタスク定義ルックアップから来ているかにかかわらず、同じ編集が適用されます。falseの場合、生のタスク定義が返されます (資格情報を公開する可能性があります)。マスキングはCPU/文字列処理作業であり、タスクまたはtask_runsの数が多い場合は時間がかかる可能性があります。編集されていない定義が必要であり、セキュリティリスクを受け入れる場合にのみ無効にしてください。
- 導入バージョン: v3.5.6

##### `privilege_max_role_depth`

- デフォルト: 16
- タイプ: Int
- 単位:
- 変更可能: はい
- 説明: ロールの最大ロール深度 (継承レベル)。
- 導入バージョン: v3.0.0

##### `privilege_max_total_roles_per_user`

- デフォルト: 64
- タイプ: Int
- 単位:
- 変更可能: はい
- 説明: ユーザーが持つことができるロールの最大数。
- 導入バージョン: v3.0.0

### クエリエンジン

##### `brpc_send_plan_fragment_timeout_ms`

- デフォルト: 60000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: プランフラグメントを送信する前にBRPC TalkTimeoutControllerに適用されるタイムアウト (ミリ秒単位)。`BackendServiceClient.sendPlanFragmentAsync`は、バックエンド`execPlanFragmentAsync`を呼び出す前にこの値を設定します。これは、BRPCがアイドル接続を接続プールから借りて送信を実行する際に待機する期間を制御します。これを超えると、RPCは失敗し、メソッドの再試行ロジックをトリガーする可能性があります。競合時にすぐに失敗させるにはこれを下げ、一時的なプール枯渇や低速ネットワークを許容するにはこれを上げてください。注意: 非常に大きな値は、障害検出を遅らせ、リクエストスレッドをブロックする可能性があります。
- 導入バージョン: v3.3.11, v3.4.1, v3.5.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- デフォルト: 12 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 大規模テーブルのクエリトリガーANALYZEタスクの間隔。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEで保留状態にあるクエリトリガーANALYZEタスクの最大数。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEで実行状態にあるクエリトリガーANALYZEタスクの最大数。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 小規模テーブルのクエリトリガーANALYZEタスクの間隔。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_rows`

- デフォルト: 10000000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: クエリトリガーANALYZEタスクのテーブルが小規模テーブルであるかどうかを判断するためのしきい値。
- 導入バージョン: v3.4.0

##### `connector_table_query_trigger_task_schedule_interval`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スケジューラースレッドがクエリトリガーバックグラウンドタスクをスケジューリングする間隔。この項目は、v3.4.0で導入された`connector_table_query_trigger_analyze_schedule_interval`を置き換えます。ここで、バックグラウンドタスクとは、v3.4の`ANALYZE`タスクと、v3.4以降のバージョンの低カーディナリティ列の辞書収集タスクを指します。
- 導入バージョン: v3.4.2

##### `create_table_max_serial_replicas`

- デフォルト: 128
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: シリアルで作成するレプリカの最大数。実際のレプリカ数がこの値を超えると、レプリカは並行して作成されます。テーブル作成に時間がかかっている場合は、この値を減らしてみてください。
- 導入バージョン: -

##### `default_mv_partition_refresh_number`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新が複数のパーティションを伴う場合、このパラメータはデフォルトで1つのバッチで更新されるパーティションの数を制御します。
バージョン3.3.0以降、潜在的なメモリ不足 (OOM) の問題を避けるため、システムはデフォルトで一度に1つのパーティションを更新します。以前のバージョンでは、デフォルトですべてのパーティションが一度に更新されていましたが、これによりメモリが枯渇し、タスクが失敗する可能性がありました。ただし、マテリアライズドビューの更新が多数のパーティションを伴う場合、一度に1つのパーティションしか更新しないと、過剰なスケジューリングオーバーヘッド、全体的な更新時間の延長、および多数の更新レコードが発生する可能性があることに注意してください。そのような場合、更新効率を向上させ、スケジューリングコストを削減するために、このパラメータを適切に調整することをお勧めします。
- 導入バージョン: v3.3.0

##### `default_mv_refresh_immediate`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非同期マテリアライズドビューを作成後すぐに更新するかどうか。この項目が`true`に設定されている場合、新しく作成されたマテリアライズドビューはすぐに更新されます。
- 導入バージョン: v3.2.3

##### `dynamic_partition_check_interval_seconds`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 新しいデータのチェック間隔。新しいデータが検出された場合、StarRocksは自動的にそのデータ用のパーティションを作成します。
- 導入バージョン: -

##### `dynamic_partition_enable`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 動的パーティショニング機能を有効にするかどうか。この機能が有効になっている場合、StarRocksは新しいデータ用のパーティションを動的に作成し、期限切れのパーティションを自動的に削除してデータの鮮度を確保します。
- 導入バージョン: -

##### `enable_active_materialized_view_schema_strict_check`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非アクティブなマテリアライズドビューをアクティブ化する際に、データ型の長さの一貫性を厳密にチェックするかどうか。この項目が`false`に設定されている場合、ベーステーブルでデータ型の長さが変更されていても、マテリアライズドビューのアクティブ化には影響しません。
- 導入バージョン: v3.3.4

##### `enable_auto_collect_array_ndv`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ARRAYタイプのNDV情報を自動的に収集することを有効にするかどうか。
- 導入バージョン: v4.0

##### `enable_backup_materialized_view`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 特定のデータベースをバックアップまたは復元する際に、非同期マテリアライズドビューのBACKUPとRESTOREを有効にするかどうか。この項目が`false`に設定されている場合、StarRocksは非同期マテリアライズドビューのバックアップをスキップします。
- 導入バージョン: v3.2.0

##### `enable_collect_full_statistic`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 自動フル統計収集を有効にするかどうか。この機能はデフォルトで有効になっています。
- 導入バージョン: -

##### `enable_colocate_mv_index`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 同期マテリアライズドビューを作成する際に、同期マテリアライズドビューインデックスをベーステーブルとコロケーションすることをサポートするかどうか。この項目が`true`に設定されている場合、タブレッドシンクは同期マテリアライズドビューの書き込みパフォーマンスを高速化します。
- 導入バージョン: v3.2.0

##### `enable_decimal_v3`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: DECIMAL V3データタイプをサポートするかどうか。
- 導入バージョン: -

##### `enable_experimental_mv`

- デフォルト: v2.5.2以降はtrue、v2.5.2以前はfalse
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 非同期マテリアライズドビュー機能を有効にするかどうか。TRUEは、この機能が有効であることを示します。v2.5.2以降、この機能はデフォルトで有効になっています。v2.5.2以前のバージョンでは、この機能はデフォルトで無効になっています。
- 導入バージョン: v2.4

##### `enable_local_replica_selection`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリに対してローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク転送コストを削減します。このパラメータがTRUEに設定されている場合、CBOは現在のFEと同じIPアドレスを持つBE上のタブレットレプリカを優先的に選択します。このパラメータが`FALSE`に設定されている場合、ローカルレプリカと非ローカルレプリカの両方を選択できます。
- 導入バージョン: -

##### `enable_manual_collect_array_ndv`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ARRAYタイプのNDV情報を手動で収集することを有効にするかどうか。
- 導入バージョン: v4.0

##### `enable_materialized_view`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの作成を有効にするかどうか。
- 導入バージョン: -

##### `enable_materialized_view_external_table_precise_refresh`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ベーステーブルが外部 (クラウドネイティブではない) テーブルである場合に、マテリアライズドビュー更新の内部最適化を有効にするには、この項目を`true`に設定します。有効にすると、マテリアライズドビュー更新プロセッサは候補パーティションを計算し、すべてのパーティションではなく影響を受けるベーステーブルパーティションのみを更新し、I/Oと更新コストを削減します。外部テーブルのフルパーティション更新を強制するには、`false`に設定します。
- 導入バージョン: v3.2.9

##### `enable_materialized_view_metrics_collect`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: デフォルトで非同期マテリアライズドビューの監視メトリックを収集するかどうか。
- 導入バージョン: v3.1.11, v3.2.5

##### `enable_materialized_view_spill`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビュー更新タスクのための中間結果スピリングを有効にするかどうか。
- 導入バージョン: v3.1.1

##### `enable_materialized_view_text_based_rewrite`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: デフォルトでテキストベースのクエリ書き換えを有効にするかどうか。この項目が`true`に設定されている場合、システムは非同期マテリアライズドビューの作成中に抽象構文ツリーを構築します。
- 導入バージョン: v3.2.5

##### `enable_mv_automatic_active_check`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ベーステーブル (ビュー) がスキーマ変更されたり、ドロップされて再作成されたりしたために非アクティブになった非同期マテリアライズドビューを、システムが自動的にチェックして再アクティブ化することを有効にするかどうか。この機能は、ユーザーが手動で非アクティブに設定したマテリアライズドビューは再アクティブ化しないことに注意してください。
- 導入バージョン: v3.1.6

##### `enable_mv_automatic_repairing_for_broken_base_tables`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、StarRocksは、ベース外部テーブルがドロップされて再作成された場合、またはテーブル識別子が変更された場合に、マテリアライズドビューベーステーブルメタデータを自動的に修復しようとします。修復フローは、マテリアライズドビューのベーステーブル情報を更新し、外部テーブルパーティションのパーティションレベルの修復情報を収集し、`autoRefreshPartitionsLimit`を尊重しながら、非同期自動更新マテリアライズドビューのパーティション更新決定を駆動することができます。現在、自動修復はHive外部テーブルをサポートしています。サポートされていないテーブルタイプは、マテリアライズドビューを非アクティブに設定し、修復例外を発生させます。パーティション情報収集は非ブロッキングであり、失敗はログに記録されます。
- 導入バージョン: v3.3.19, v3.4.8, v3.5.6

##### `enable_predicate_columns_collection`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 述語列の収集を有効にするかどうか。無効にすると、クエリ最適化中に述語列は記録されません。
- 導入バージョン: -

##### `enable_query_queue_v2`

- デフォルト: true
- タイプ: boolean
- 単位: -
- 変更可能: いいえ
- 説明: trueの場合、FEスロットベースクエリスケジューラをQuery Queue V2に切り替えます。このフラグは、スロットマネージャーとトラッカー (例: `BaseSlotManager.isEnableQueryQueueV2`および`SlotTracker#createSlotSelectionStrategy`) によって読み取られ、従来の戦略の代わりに`SlotSelectionStrategyV2`を選択します。`query_queue_v2_xxx`構成オプションと`QueryQueueOptions`は、このフラグが有効な場合にのみ有効になります。v4.1以降、デフォルト値は`false`から`true`に変更されました。
- 導入バージョン: v3.3.4, v3.4.0, v3.5.0

##### `enable_sql_blacklist`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: SQLクエリのブラックリストチェックを有効にするかどうか。この機能が有効な場合、ブラックリスト内のクエリは実行できません。
- 導入バージョン: -

##### `enable_statistic_collect`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: CBOの統計を収集するかどうか。この機能はデフォルトで有効になっています。
- 導入バージョン: -

##### `enable_statistic_collect_on_first_load`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: データロード操作によってトリガーされる自動統計収集とメンテナンスを制御します。これには以下が含まれます。
  - データが最初にパーティションにロードされたときの統計収集 (パーティションバージョンが2の場合)。
  - 複数パーティションテーブルの空のパーティションにデータがロードされたときの統計収集。
  - INSERT OVERWRITE操作の統計コピーと更新。

  **統計収集タイプの決定ポリシー:**

  - INSERT OVERWRITEの場合: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。既存の統計のみがコピーされます。
    - それ以外の場合、`targetRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE統計収集が使用されます。
    - それ以外の場合、FULL統計収集が使用されます。

  - First Loadの場合: `deltaRatio = loadRows / (totalRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。
    - それ以外の場合、`loadRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE統計収集が使用されます。
    - それ以外の場合、FULL統計収集が使用されます。

  **同期動作:**

  - DMLステートメント (INSERT INTO/INSERT OVERWRITE) の場合: テーブルロックを使用した同期モード。ロード操作は統計収集が完了するまで待機します (`semi_sync_collect_statistic_await_seconds`まで)。
  - Stream LoadおよびBroker Loadの場合: ロックなしの非同期モード。統計収集はロード操作をブロックせずにバックグラウンドで実行されます。

  :::note
  この設定を無効にすると、すべてのロードトリガー統計操作 (INSERT OVERWRITEの統計メンテナンスを含む) が防止され、テーブルに統計が不足する可能性があります。新しいテーブルが頻繁に作成され、データが頻繁にロードされる場合、この機能を有効にするとメモリとCPUオーバーヘッドが増加します。
  :::

- 導入バージョン: v3.1

##### `enable_statistic_collect_on_update`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: UPDATEステートメントが自動統計収集をトリガーできるかどうかを制御します。有効な場合、テーブルデータを変更するUPDATE操作は、`enable_statistic_collect_on_first_load`によって制御される同じインジェストベースの統計フレームワークを通じて統計収集をスケジュールする場合があります。この設定を無効にすると、UPDATEステートメントの統計収集がスキップされ、ロードトリガー統計収集の動作は変更されません。
- 導入バージョン: v3.5.11, v4.0.4

##### `enable_udf`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: UDFを有効にするかどうか。
- 導入バージョン: -

##### `expr_children_limit`

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 式内で許可される子式の最大数。
- 導入バージョン: -

##### `histogram_buckets_size`

- デフォルト: 64
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのデフォルトのバケット数。
- 導入バージョン: -

##### `histogram_max_sample_row_count`

- デフォルト: 10000000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのために収集する最大行数。
- 導入バージョン: -

##### `histogram_mcv_size`

- デフォルト: 100
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムの最も一般的な値 (MCV) の数。
- 導入バージョン: -

##### `histogram_sample_ratio`

- デフォルト: 0.1
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: ヒストグラムのサンプリング比率。
- 導入バージョン: -

##### `http_slow_request_threshold_ms`

- デフォルト: 5000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: HTTPリクエストの応答時間がこのパラメータで指定された値を超えると、このリクエストを追跡するためにログが生成されます。
- 導入バージョン: v2.5.15, v3.1.5

##### `lock_checker_enable_deadlock_check`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効な場合、LockCheckerスレッドはThreadMXBean.findDeadlockedThreads()を使用してJVMレベルのデッドロック検出を実行し、問題のあるスレッドのスタックトレースをログに記録します。このチェックはLockCheckerデーモン内 (頻度は`lock_checker_interval_second`によって制御されます) で実行され、詳細なスタック情報をログに書き込みます。これはCPUおよびI/O集中型になる可能性があります。ライブまたは再現可能なデッドロックの問題のトラブルシューティングにのみこのオプションを有効にしてください。通常の操作で有効のままにしておくと、オーバーヘッドとログ量が増加する可能性があります。
- 導入バージョン: v3.2.0

##### `low_cardinality_threshold`

- デフォルト: 255
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 低カーディナリティ辞書のしきい値。
- 導入バージョン: v3.5.0

##### `materialized_view_min_refresh_interval`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ASYNCマテリアライズドビュースケジュールの最小許容更新間隔 (秒単位)。マテリアライズドビューが時間ベースの間隔で作成された場合、間隔は秒に変換され、この値以上でなければなりません。そうでない場合、CREATE/ALTER操作はDDLエラーで失敗します。この値が0より大きい場合、チェックが強制されます。TaskManagerの過剰なスケジューリングと、頻繁すぎる更新によるFEのメモリ/CPU使用量の増加を防ぐために、0または負の値に設定して制限を無効にします。この項目はEVENT_TRIGGERED更新には適用されません。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `materialized_view_refresh_ascending`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、マテリアライズドビューパーティションの更新は、パーティションキーの昇順 (最も古いものから最も新しいもの) でパーティションを反復します。`false` (デフォルト) に設定されている場合、システムは降順 (最も新しいものから最も古いもの) で反復します。StarRocksは、パーティション更新の制限が適用される場合に処理するパーティションを選択し、後続のTaskRun実行のために次の開始/終了パーティション境界を計算するために、リストパーティション化されたマテリアライズドビューと範囲パーティション化されたマテリアライズドビューの両方の更新ロジックでこの項目を使用します。この項目を変更すると、最初に更新されるパーティションと、次のパーティション範囲がどのように導出されるかが変更されます。範囲パーティション化されたマテリアライズドビューの場合、スケジューラーは新しい開始/終了を検証し、変更が繰り返される境界 (デッドループ) を作成する場合、エラーを発生させるため、この項目は注意して設定してください。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### `max_allowed_in_element_num_of_delete`

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: DELETEステートメントのIN述語で許可される要素の最大数。
- 導入バージョン: -

##### `max_create_table_timeout_second`

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: テーブルを作成するための最大タイムアウト期間。
- 導入バージョン: -

##### `max_distribution_pruner_recursion_depth`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明:: パーティションプルーナーによって許可される最大再帰深度。再帰深度を増やすと、より多くの要素をプルーニングできますが、CPU消費量も増加します。
- 導入バージョン: -

##### `max_partitions_in_one_batch`

- デフォルト: 4096
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: パーティションを一括作成する際に作成できるパーティションの最大数。
- 導入バージョン: -

##### `max_planner_scalar_rewrite_num`

- デフォルト: 100000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: オプティマイザーがスカラー演算子を書き換えることができる最大回数。
- 導入バージョン: -

##### `max_query_queue_history_slots_number`

- デフォルト: 0
- タイプ: Int
- 単位: スロット
- 変更可能: はい
- 説明: 監視と可視性のために、クエリキューごとに保持される最近解放された (履歴) 割り当てスロットの数を制御します。`max_query_queue_history_slots_number`が`> 0`の値に設定されている場合、BaseSlotTrackerはメモリ内キューに最大その数の最も最近解放されたLogicalSlotエントリを保持し、制限を超えると最も古いものを削除します。これを有効にすると、getSlots()にこれらの履歴エントリが含まれるようになり (最新のものが最初に表示されます)、BaseSlotTrackerがConnectContextにスロットを登録してより豊富なExtraMessageデータを取得できるようになり、LogicalSlot.ConnectContextListenerがクエリ完了メタデータを履歴スロットにアタッチできるようになります。`max_query_queue_history_slots_number`が`<= 0`の場合、履歴メカニズムは無効になります (余分なメモリは使用されません)。可視性とメモリオーバーヘッドのバランスを取るために、適切な値を使用してください。
- 導入バージョン: v3.5.0

##### `max_query_retry_time`

- デフォルト: 2
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEでのクエリ再試行の最大回数。
- 導入バージョン: -

##### `max_running_rollup_job_num_per_table`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルごとに並列で実行できるロールアップジョブの最大数。
- 導入バージョン: -

##### `max_scalar_operator_flat_children`

- デフォルト：10000
- タイプ：Int
- 単位：-
- 変更可能: はい
- 説明：ScalarOperatorのフラットな子の最大数。オプティマイザーが過剰なメモリを使用するのを防ぐために、この制限を設定できます。
- 導入バージョン: -

##### `max_scalar_operator_optimize_depth`

- デフォルト：256
- タイプ：Int
- 単位：-
- 変更可能: はい
- 説明: ScalarOperator最適化が適用できる最大深度。
- 導入バージョン: -

##### `mv_active_checker_interval_seconds`

- デフォルト: 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: バックグラウンドのactive_checkerスレッドが有効になっている場合、システムは定期的に、スキーマ変更またはベーステーブル (またはビュー) の再構築のために非アクティブになったマテリアライズドビューを検出して自動的に再アクティブ化します。このパラメータは、チェッカースレッドのスケジューリング間隔を秒単位で制御します。デフォルト値はシステム定義です。
- 導入バージョン: v3.1.6

##### `mv_rewrite_consider_data_layout_mode`

- デフォルト: `enable`
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの書き換えが、最適なマテリアライズドビューを選択する際にベーステーブルのデータレイアウトを考慮に入れるべきかどうかを制御します。有効な値:
  - `disable`: 候補となるマテリアライズドビューを選択する際に、データレイアウト基準を使用しない。
  - `enable`: クエリがレイアウトを意識していると認識された場合にのみ、データレイアウト基準を使用する。
  - `force`: 最適なマテリアライズドビューを選択する際に、常にデータレイアウト基準を適用する。
  この項目を変更すると、`BestMvSelector`の動作に影響し、物理的なレイアウトがプランの正確性やパフォーマンスにとって重要であるかどうかに応じて、書き換えの適用性が向上または拡大する可能性があります。
- 導入バージョン: -

##### `publish_version_interval_ms`

- デフォルト: 10
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: リリース検証タスクが発行される時間間隔。
- 導入バージョン: -

##### `query_queue_slots_estimator_strategy`

- デフォルト: MAX
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: `enable_query_queue_v2`がtrueの場合にキューベースクエリに使用されるスロット推定戦略を選択します。有効な値: MBE (メモリベース)、PBE (並列処理ベース)、MAX (MBEとPBEの最大値を取る)、MIN (MBEとPBEの最小値を取る)。MBEは、予測されたメモリまたはプランコストをスロットあたりのメモリターゲットで割ってスロットを推定し、`totalSlots`で制限されます。PBEは、フラグメント並列処理 (スキャン範囲カウントまたはカーディナリティ/スロットあたりの行数) とCPUコストベースの計算 (スロットあたりのCPUコストを使用) からスロットを導出し、結果を[numSlots/2, numSlots]の範囲に制限します。MAXとMINは、MBEとPBEの最大値または最小値をそれぞれ取ることによって組み合わされます。設定された値が無効な場合、デフォルト (`MAX`) が使用されます。
- 導入バージョン: v3.5.0

##### `query_queue_v2_concurrency_level`

- デフォルト: 4
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: システムの合計クエリ スロットを計算する際に使用される論理的な並列処理「レイヤー」の数を制御します。共有なしモードでは、合計スロット = `query_queue_v2_concurrency_level` * BEの数 * BEごとのコア数 (BackendResourceStatから派生)。マルチウェアハウスモードでは、実効並列処理はmax(1, `query_queue_v2_concurrency_level` / 4) にスケールダウンされます。設定された値が非正の値の場合、`4`として扱われます。この値を変更すると、totalSlots (ひいては同時クエリ容量) が増減し、スロットごとのリソースに影響します。memBytesPerSlotは、ワーカーごとのメモリを (ワーカーごとのコア数 * 並列処理) で割って導出され、CPU会計は`query_queue_v2_cpu_costs_per_slot`を使用します。クラスターのサイズに比例して設定してください。非常に大きな値は、スロットごとのメモリを削減し、リソースの断片化を引き起こす可能性があります。
- 導入バージョン: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_cpu_costs_per_slot`

- デフォルト: 1000000000
- タイプ: Long
- 単位: プランナーCPUコスト単位
- 変更可能: はい
- 説明: クエリが必要とするスロット数をそのプランナーCPUコストから推定するために使用されるスロットごとのCPUコストしきい値。スケジューラーは、slots = integer(plan_cpu_costs / `query_queue_v2_cpu_costs_per_slot`) を計算し、その結果を [1, totalSlots] の範囲にクランプし、計算された値が非正の場合は最小値を1とします。V2コードは非正の設定を1に正規化するため (Math.max(1, value))、非正の値は実質的に`1`になります。この値を増やすと、クエリごとに割り当てられるスロットが減少し (より少なく、より大きなスロットのクエリを優先)、減らすとクエリごとのスロットが増加します。並列処理とリソース粒度のバランスを制御するために、`query_queue_v2_num_rows_per_slot`と並列処理設定と組み合わせて調整してください。
- 導入バージョン: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_num_rows_per_slot`

- デフォルト: 4096
- タイプ: Int
- 単位: 行
- 変更可能: はい
- 説明: クエリごとのスロット数を推定する際に、単一のスケジューリングスロットに割り当てられるソース行レコードの目標数。StarRocksは、estimated_slots = (ソースノードのカーディナリティ) / `query_queue_v2_num_rows_per_slot`を計算し、その結果を[1, totalSlots]の範囲にクランプし、計算された値が非正の場合は最小値を1とします。totalSlotsは利用可能なリソース (おおよそDOP * `query_queue_v2_concurrency_level` * ワーカー/BEの数) から派生するため、クラスター/コア数に依存します。この値を増やすと、スロット数が減少し (各スロットがより多くの行を処理)、スケジューリングオーバーヘッドが減少します。減らすと、並列処理が増加し (より多くの小さなスロット)、リソース制限まで増加します。
- 導入バージョン: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_schedule_strategy`

- デフォルト: SWRR
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: Query Queue V2が保留中のクエリを並べ替えるために使用するスケジューリングポリシーを選択します。サポートされる値 (大文字と小文字を区別しない): `SWRR` (Smooth Weighted Round Robin) — デフォルトで、公平な重み付き共有を必要とする混合/ハイブリッドワークロードに適しています。`SJF` (Short Job First + Aging) — 短いジョブを優先し、エージングを使用してスタベーションを回避します。値はケースインセンシティブな列挙型ルックアップで解析されます。認識されない値はエラーとしてログに記録され、デフォルトポリシーが使用されます。この設定は、Query Queue V2が有効な場合にのみ動作に影響し、`query_queue_v2_concurrency_level`などのV2サイジング設定と相互作用します。
- 導入バージョン: v3.3.12, v3.4.2, v3.5.0

##### `semi_sync_collect_statistic_await_seconds`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: DML操作 (INSERT INTOおよびINSERT OVERWRITEステートメント) 中の半同期統計収集の最大待機時間。Stream LoadおよびBroker Loadは非同期モードを使用するため、この設定の影響を受けません。統計収集時間がこの値を超えると、ロード操作は収集完了を待たずに続行されます。この設定は`enable_statistic_collect_on_first_load`と連携して機能します。
- 導入バージョン: v3.1

##### `slow_query_analyze_threshold`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明:: クエリがQuery Feedbackの分析をトリガーするための実行時間しきい値。
- 導入バージョン: v3.4.0

##### `statistic_analyze_status_keep_second`

- デフォルト: 3 * 24 * 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 収集タスクの履歴を保持する期間。デフォルト値は3日です。
- 導入バージョン: -

##### `statistic_auto_analyze_end_time`

- デフォルト: 23:59:59
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 自動収集の終了時刻。値の範囲: `00:00:00` - `23:59:59`。
- 導入バージョン: -

##### `statistic_auto_analyze_start_time`

- デフォルト: 00:00:00
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 自動収集の開始時刻。値の範囲: `00:00:00` - `23:59:59`。
- 導入バージョン: -

##### `statistic_auto_collect_ratio`

- デフォルト: 0.8
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 自動収集の統計が健全であるかどうかを判断するためのしきい値。統計の健全性がこのしきい値を下回ると、自動収集がトリガーされます。
- 導入バージョン: -

##### `statistic_auto_collect_small_table_rows`

- デフォルト: 10000000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 自動収集中に外部データソース (Hive、Iceberg、Hudi) のテーブルが小規模テーブルであるかどうかを判断するためのしきい値。テーブルの行数がこの値よりも少ない場合、そのテーブルは小規模テーブルと見なされます。
- 導入バージョン: v3.2

##### `statistic_cache_columns`

- デフォルト: 100000
- タイプ: Long
- 単位: -
- 変更可能: いいえ
- 説明: 統計テーブルのためにキャッシュできる行数。
- 導入バージョン: -

##### `statistic_cache_thread_pool_size`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 統計キャッシュを更新するために使用されるスレッドプールのサイズ。
- 導入バージョン: -

##### `statistic_collect_interval_sec`

- デフォルト: 5 * 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 自動収集中のデータ更新をチェックする間隔。
- 導入バージョン: -

##### `statistic_max_full_collect_data_size`

- デフォルト: 100 * 1024 * 1024 * 1024
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: 統計の自動収集のデータサイズしきい値。合計サイズがこの値を超えると、フル収集の代わりにサンプリング収集が実行されます。
- 導入バージョン: -

##### `statistic_sample_collect_rows`

- デフォルト: 200000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: ロードトリガー統計操作中にSAMPLE統計収集とFULL統計収集を決定するための行数しきい値。ロードまたは変更された行数がこのしきい値 (デフォルト200,000) を超える場合、SAMPLE統計収集が使用されます。それ以外の場合、FULL統計収集が使用されます。この設定は`enable_statistic_collect_on_first_load`および`statistic_sample_collect_ratio_threshold_of_first_load`と連携して機能します。
- 導入バージョン: -

##### `statistic_update_interval_sec`

- デフォルト: 24 * 60 * 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 統計情報のキャッシュが更新される間隔。
- 導入バージョン: -

##### `task_check_interval_second`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タスクバックグラウンドジョブの実行間隔。GlobalStateMgrはこの値を使用して`doTaskBackgroundJob()`を呼び出すTaskCleaner FrontendDaemonをスケジュールします。この値は1000倍されてデーモンの間隔をミリ秒単位で設定します。この値を減らすと、バックグラウンドメンテナンス (タスククリーンアップ、チェック) がより頻繁に実行され、より迅速に反応しますが、CPU/IOオーバーヘッドが増加します。増やすとオーバーヘッドは減少しますが、クリーンアップと古いタスクの検出が遅れます。この値を調整して、メンテナンスの応答性とリソース使用量のバランスを取ってください。
- 導入バージョン: v3.2.0

##### `task_min_schedule_interval_s`

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: SQLレイヤーによってチェックされるタスクスケジュールの最小許容スケジュール間隔 (秒単位)。タスクが送信されると、TaskAnalyzerはスケジュール期間を秒に変換し、期間が`task_min_schedule_interval_s`よりも小さい場合、ERR_INVALID_PARAMETERで送信を拒否します。これにより、頻繁すぎるタスクの作成が防止され、スケジューラーが高頻度タスクから保護されます。スケジュールに明示的な開始時間がない場合、TaskAnalyzerは開始時間を現在のエポック秒に設定します。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `task_runs_timeout_second`

- デフォルト: 4 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: TaskRunのデフォルト実行タイムアウト (秒単位)。この項目はTaskRun実行でベースラインタイムアウトとして使用されます。タスク実行のプロパティに、正の整数値のセッション変数`query_timeout`または`insert_timeout`が含まれている場合、ランタイムはセッションタイムアウトと`task_runs_timeout_second`の大きい方の値を使用します。実効タイムアウトは、設定された`task_runs_ttl_second`および`task_ttl_second`を超えないように制限されます。この項目を設定して、タスク実行が実行できる期間を制限します。非常に大きな値は、タスク/タスク実行TTL設定によって切り捨てられる可能性があります。
- 導入バージョン: -

### ロードとアンロード

##### `broker_load_default_timeout_second`

- デフォルト: 14400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Broker Loadジョブのタイムアウト期間。
- 導入バージョン: -

##### `desired_max_waiting_jobs`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEにおける保留中のジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FEの保留中のジョブ数がこの値に達すると、FEは新しいロードリクエストを拒否します。このパラメータは非同期ロードにのみ有効です。v2.5以降、デフォルト値は100から1024に変更されました。
- 導入バージョン: -

##### `disable_load_job`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラスターでエラーが発生した場合にロードを無効にするかどうか。これにより、クラスターエラーによる損失を防ぎます。デフォルト値は`FALSE`で、ロードが無効になっていないことを示します。`TRUE`はロードが無効であり、クラスターが読み取り専用状態であることを示します。
- 導入バージョン: -

##### `empty_load_as_error`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: データがロードされていない場合に「すべてのパーティションにロードデータがありません」というエラーメッセージを返すかどうか。有効な値:
  - `true`: データがロードされていない場合、システムは失敗メッセージを表示し、「すべてのパーティションにロードデータがありません」というエラーを返します。
  - `false`: データがロードされていない場合、システムは成功メッセージを表示し、エラーではなくOKを返します。
- 導入バージョン: -

##### `enable_file_bundling`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クラウドネイティブテーブルのファイルバンドリング最適化を有効にするかどうか。この機能が有効になっている場合 (trueに設定されている場合)、システムはロード、Compaction、またはPublish操作によって生成されたデータファイルを自動的にバンドルし、それによって外部ストレージシステムへの高頻度アクセスによって引き起こされるAPIコストを削減します。この動作は、CREATE TABLEプロパティ`file_bundling`を使用してテーブルレベルでも制御できます。詳細な手順については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)を参照してください。
- 導入バージョン: v4.0

##### `enable_routine_load_lag_metrics`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ルーチンロードKafkaパーティションオフセットラグメトリックを収集するかどうか。この項目を`true`に設定すると、Kafka APIを呼び出してパーティションの最新オフセットを取得することに注意してください。
- 導入バージョン: -

##### `enable_sync_publish`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ロードトランザクションの公開フェーズで適用タスクを同期的に実行するかどうか。このパラメータは、プライマリキーテーブルにのみ適用されます。有効な値:
  - `TRUE` (デフォルト): 適用タスクはロードトランザクションの公開フェーズで同期的に実行されます。これは、適用タスクが完了し、ロードされたデータが実際にクエリ可能になった後にのみ、ロードトランザクションが成功として報告されることを意味します。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりする場合、このパラメータを`true`に設定すると、クエリパフォーマンスと安定性を向上させることができますが、ロードレイテンシが増加する可能性があります。
  - `FALSE`: 適用タスクはロードトランザクションの公開フェーズで非同期的に実行されます。これは、適用タスクが送信された後にロードトランザクションが成功として報告されますが、ロードされたデータはすぐにクエリできないことを意味します。この場合、同時クエリは適用タスクが完了するかタイムアウトするまで待機する必要があります。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりする場合、このパラメータを`false`に設定すると、クエリパフォーマンスと安定性に影響する可能性があります。
- 導入バージョン: v3.2.0

##### `export_checker_interval_second`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがスケジュールされる時間間隔。
- 導入バージョン: -

##### `export_max_bytes_per_be_per_task`

- デフォルト: 268435456
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: 単一のデータアンロードタスクによって単一のBEからエクスポートできるデータの最大量。
- 導入バージョン: -

##### `export_running_job_num_limit`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 並行して実行できるデータエクスポートタスクの最大数。
- 導入バージョン: -

##### `export_task_default_timeout_second`

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データエクスポートタスクのタイムアウト期間。
- 導入バージョン: -

##### `export_task_pool_size`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: アンロードタスクスレッドプールのサイズ。
- 導入バージョン: -

##### `external_table_commit_timeout_ms`

- デフォルト: 10000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: StarRocks外部テーブルへの書き込みトランザクションをコミット (発行) するためのタイムアウト期間。デフォルト値`10000`は10秒のタイムアウト期間を示します。
- 導入バージョン: -

##### `finish_transaction_default_lock_timeout_ms`

- デフォルト: 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: トランザクション完了時にdbおよびテーブルロックを取得するためのデフォルトのタイムアウト。
- 導入バージョン: v4.0.0, v3.5.8

##### `history_job_keep_max_second`

- デフォルト: 7 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スキーマ変更ジョブなどの履歴ジョブを保持できる最大期間。
- 導入バージョン: -

##### `insert_load_default_timeout_second`

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: データをロードするために使用されるINSERT INTOステートメントのタイムアウト期間。
- 導入バージョン: -

##### `label_clean_interval_second`

- デフォルト: 4 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### `label_keep_max_num`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ある期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報は削除されます。
- 導入バージョン: -

##### `label_keep_max_second`

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 完了済みでFINISHEDまたはCANCELLED状態にあるロードジョブのラベルを保持する最大期間 (秒単位)。デフォルト値は3日です。この期間が経過すると、ラベルは削除されます。このパラメータはすべての種類のロードジョブに適用されます。値が大きすぎると、大量のメモリを消費します。
- 導入バージョン: -

##### `load_checker_interval_second`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: ロードジョブがローリングベースで処理される時間間隔。
- 導入バージョン: -

##### `load_parallel_instance_num`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: ブローカーおよびストリームロードのために、単一ホスト上に作成される並列ロードフラグメントインスタンスの数を制御します。LoadPlannerは、セッションがアダプティブシンクDOPを有効にしない限り、この値をホストあたりの並列度の値として使用します。セッション変数`enable_adaptive_sink_dop`がtrueの場合、セッションの`sink_degree_of_parallelism`がこの設定を上書きします。シャッフルが必要な場合、この値はフラグメント並列実行 (スキャンフラグメントとシンクフラグメントの並列実行インスタンス) に適用されます。シャッフルが不要な場合、シンクパイプラインDOPとして使用されます。注: ローカルファイルからのロードは、ローカルディスク競合を避けるため、単一インスタンス (パイプラインDOP = 1、並列実行 = 1) に強制されます。この値を増やすと、ホストあたりの並列処理とスループットが向上しますが、CPU、メモリ、I/O競合が増加する可能性があります。
- 導入バージョン: v3.2.0

##### `load_straggler_wait_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: BEレプリカが許容できる最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするクローニングが実行されます。
- 導入バージョン: -

##### `loads_history_retained_days`

- デフォルト: 30
- タイプ: Int
- 単位: 日
- 変更可能: はい
- 説明: 内部`_statistics_.loads_history`テーブルにロード履歴を保持する日数。この値はテーブル作成時にテーブルプロパティ`partition_live_number`を設定するために使用され、何日分のパーティションを保持するかを決定するために`TableKeeper`に渡されます (最小1に制限されます)。この値を増減すると、完了したロードジョブが日次パーティションに保持される期間が調整されます。これは新しいテーブル作成とキーパーの削除動作に影響しますが、過去のパーティションを自動的に再作成することはありません。`LoadsHistorySyncer`は、ロード履歴のライフサイクルを管理する際にこの保持に依存します。その同期頻度は`loads_history_sync_interval_second`によって制御されます。
- 導入バージョン: v3.3.6, v3.4.0, v3.5.0

##### `loads_history_sync_interval_second`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: `LoadsHistorySyncer`が、`information_schema.loads`から内部`_statistics_.loads_history`テーブルへの完了済みロードジョブの定期的な同期をスケジュールするために使用する間隔 (秒単位)。コンストラクタで値が1000倍され、FrontendDaemonの間隔を設定します。シンクロナイザーは最初の実行をスキップし (テーブル作成を許可するため)、1分以上前に完了したロードのみをインポートします。値が小さいとDMLとエグゼキューターの負荷が増加し、値が大きいと履歴ロードレコードの可用性が遅れます。ターゲットテーブルの保持/パーティショニング動作については`loads_history_retained_days`を参照してください。
- 導入バージョン: v3.3.6, v3.4.0, v3.5.0

##### `max_broker_load_job_concurrency`

- デフォルト: 5
- エイリアス: async_load_task_pool_size
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocksクラスター内で許可される同時Broker Loadジョブの最大数。このパラメータはBroker Loadにのみ有効です。このパラメータの値は`max_running_txn_num_per_db`の値よりも小さくなければなりません。v2.5以降、デフォルト値は10から5に変更されました。
- 導入バージョン: -

##### `max_load_timeout_second`

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべての種類のロードジョブに適用されます。
- 導入バージョン: -

##### `max_routine_load_batch_size`

- デフォルト: 4294967296
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: ルーチンロードタスクによってロードできるデータの最大量。
- 導入バージョン: -

##### `max_routine_load_task_concurrent_num`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各ルーチンロードジョブの同時タスクの最大数。
- 導入バージョン: -

##### `max_routine_load_task_num_per_be`

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各BEでの同時ルーチンロードタスクの最大数。v3.1.0以降、このパラメータのデフォルト値は5から16に増加し、BE静的パラメータ`routine_load_thread_pool_size` (非推奨) の値以下である必要はなくなりました。
- 導入バージョン: -

##### `max_running_txn_num_per_db`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocksクラスター内のデータベースごとに実行が許可されるロードトランザクションの最大数。デフォルト値は`1000`です。v3.1以降、デフォルト値は100から1000に変更されました。データベースで実行されているロードトランザクションの実際の数がこのパラメータの値を超えると、新しいロードリクエストは処理されません。同期ロードジョブの新しいリクエストは拒否され、非同期ロードジョブの新しいリクエストはキューに入れられます。この値を増やすとシステム負荷が増加するため、増やすことはお勧めしません。
- 導入バージョン: -

##### `max_stream_load_timeout_second`

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Stream Loadジョブに許可される最大タイムアウト期間。
- 導入バージョン: -

##### `max_tolerable_backend_down_num`

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許容される障害のあるBEノードの最大数。この数を超えると、ルーチンロードジョブは自動的に回復できません。
- 導入バージョン: -

##### `min_bytes_per_broker_scanner`

- デフォルト: 67108864
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: Broker Loadインスタンスによって処理できるデータの最小許容量。
- 導入バージョン: -

##### `min_load_timeout_second`

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: ロードジョブに許可される最小タイムアウト期間。この制限はすべての種類のロードジョブに適用されます。
- 導入バージョン: -

##### `min_routine_load_lag_for_metrics`

- デフォルト: 10000
- タイプ: INT
- 単位: -
- 変更可能: はい
- 説明: モニタリングメトリックに表示されるルーチンロードジョブの最小オフセット遅延。オフセット遅延がこの値よりも大きいルーチンロードジョブは、メトリックに表示されます。
- 導入バージョン: -

##### `period_of_auto_resume_min`

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 変更可能: はい
- 説明: ルーチンロードジョブが自動的に回復される間隔。
- 導入バージョン: -

##### `prepared_transaction_default_timeout_second`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 準備されたトランザクションのデフォルトのタイムアウト期間。
- 導入バージョン: -

##### `routine_load_task_consume_second`

- デフォルト: 15
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: クラスター内の各ルーチンロードタスクがデータを消費するための最大時間。v3.1.0以降、ルーチンロードジョブは[job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties)で新しいパラメータ`task_consume_second`をサポートしています。このパラメータは、ルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### `routine_load_task_timeout_second`

- デフォルト: 60
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: クラスター内の各ルーチンロードタスクのタイムアウト期間。v3.1.0以降、ルーチンロードジョブは[job_properties](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties)で新しいパラメータ`task_timeout_second`をサポートしています。このパラメータは、ルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### `routine_load_unstable_threshold_second`

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: ルーチンロードジョブ内のいずれかのタスクが遅延した場合、ルーチンロードジョブはUNSTABLE状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在の時刻との差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。
- 導入バージョン: -

##### `spark_dpp_version`

- デフォルト: 1.0.0
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用されるSpark Dynamic Partition Pruning (DPP) のバージョン。
- 導入バージョン: -

##### `spark_home_default_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Sparkクライアントのルートディレクトリ。
- 導入バージョン: -

##### `spark_launcher_log_dir`

- デフォルト: sys_log_dir + "/spark_launcher_log"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Sparkログファイルを格納するディレクトリ。
- 導入バージョン: -

##### `spark_load_default_timeout_second`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各Spark Loadジョブのタイムアウト期間。
- 導入バージョン: -

##### `spark_load_submit_timeout_second`

- デフォルト: 300
- タイプ: long
- 単位: 秒
- 変更可能: いいえ
- 説明: Sparkアプリケーションを送信した後、YARNからの応答を待機する最大時間 (秒単位)。`SparkLauncherMonitor.LogMonitor`はこの値をミリ秒に変換し、ジョブがUNKNOWN/CONNECTED/SUBMITTEDのままこのタイムアウトより長く残っている場合、監視を停止し、spark launcherプロセスを強制終了します。`SparkLoadJob`はこの設定をデフォルトとして読み取り、`LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT`プロパティを介してロードごとの上書きを許可します。YARNのキューイング遅延に対応できる十分な高さを設定してください。低すぎると、正当にキューイングされたジョブが中止される可能性があり、高すぎると、障害処理とリソースクリーンアップが遅延する可能性があります。
- 導入バージョン: v3.2.0

##### `spark_resource_path`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Spark依存関係パッケージのルートディレクトリ。
- 導入バージョン: -

##### `stream_load_default_timeout_second`

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 各Stream Loadジョブのデフォルトのタイムアウト期間。
- 導入バージョン: -

##### `stream_load_max_txn_num_per_be`

- デフォルト: -1
- タイプ: Int
- 単位: トランザクション
- 変更可能: はい
- 説明: 単一のBE (バックエンド) ホストから受け入れられる同時ストリームロードトランザクションの数を制限します。非負の整数に設定されている場合、FrontendServiceImplはBE (クライアントIP別) の現在のトランザクション数をチェックし、数がこの制限`>=`を超えている場合、新しいストリームロード開始リクエストを拒否します。`< 0`の値は制限を無効にします (無制限)。このチェックはストリームロード開始時に発生し、超えた場合、`streamload txn num per be exceeds limit`エラーを引き起こす可能性があります。関連するランタイム動作では、リクエストタイムアウトのフォールバックに`stream_load_default_timeout_second`を使用します。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `stream_load_task_keep_max_num`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StreamLoadMgrがメモリに保持するStream Loadタスクの最大数 (すべてのデータベースにわたるグローバル)。追跡されているタスク数 (`idToStreamLoadTask`) がこのしきい値を超えると、StreamLoadMgrは最初に`cleanSyncStreamLoadTasks()`を呼び出して完了した同期ストリームロードタスクを削除します。サイズがまだこのしきい値の半分よりも大きい場合、`cleanOldStreamLoadTasks(true)`を呼び出して、古いタスクまたは完了したタスクの強制削除を実行します。メモリにより多くのタスク履歴を保持するにはこの値を増やし、メモリ使用量を減らし、クリーンアップをより積極的に行うにはこの値を減らします。この値はメモリ内保持のみを制御し、永続化/リプレイされたタスクには影響しません。
- 導入バージョン: v3.2.0

##### `stream_load_task_keep_max_second`

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 完了またはキャンセルされたStream Loadタスクの保持期間。タスクが最終状態に達し、終了タイムスタンプがこのしきい値よりも前 (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`) である場合、`StreamLoadMgr.cleanOldStreamLoadTasks`による削除の対象となり、永続化された状態をロードする際に破棄されます。`StreamLoadTask`と`StreamLoadMultiStmtTask`の両方に適用されます。合計タスク数が`stream_load_task_keep_max_num`を超えると、クリーンアップが早期にトリガーされる可能性があります (同期タスクは`cleanSyncStreamLoadTasks`によって優先されます)。履歴/デバッグ可能性とメモリ使用量のバランスを取るためにこれを設定してください。
- 導入バージョン: v3.2.0

##### `transaction_clean_interval_second`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: 完了したトランザクションがクリーンアップされる時間間隔。単位: 秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### `transaction_stream_load_coordinator_cache_capacity`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: トランザクションラベルからコーディネーターノードへのマッピングを格納するキャッシュの容量。
- 導入バージョン: -

##### `transaction_stream_load_coordinator_cache_expire_seconds`

- デフォルト: 900
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: キャッシュ内のコーディネーターマッピングが削除されるまでの時間 (TTL)。
- 導入バージョン: -

##### `yarn_client_path`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Yarnクライアントパッケージのルートディレクトリ。
- 導入バージョン: -

##### `yarn_config_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Yarn構成ファイルを格納するディレクトリ。
- 導入バージョン: -

### 統計レポート

##### `enable_collect_warehouse_metrics`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、システムはウェアハウスごとのメトリックを収集してエクスポートします。これを有効にすると、ウェアハウスレベルのメトリック (スロット/使用量/可用性) がメトリック出力に追加され、メトリックカーディナリティと収集オーバーヘッドが増加します。ウェアハウス固有のメトリックを省略し、CPU/ネットワークおよび監視ストレージコストを削減するには、これを無効にします。
- 導入バージョン: v3.5.0

##### `enable_http_detail_metrics`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: はい
- 説明: trueの場合、HTTPサーバーは詳細なHTTPワーカーメトリック (特に`HTTP_WORKER_PENDING_TASKS_NUM`ゲージ) を計算して公開します。これを有効にすると、サーバーはNettyワーカーエグゼキューターを反復処理し、各`NioEventLoop`で`pendingTasks()`を呼び出して保留中のタスクカウントを合計します。無効にすると、そのコストを回避するためにゲージは0を返します。この追加の収集はCPUおよびレイテンシに敏感になる可能性があるため、デバッグまたは詳細な調査のためにのみ有効にしてください。
- 導入バージョン: v3.2.3

##### `proc_profile_collect_time_s`

- デフォルト: 120
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 単一のプロセスプロファイル収集の期間 (秒単位)。`proc_profile_cpu_enable`または`proc_profile_mem_enable`が`true`に設定されている場合、AsyncProfilerが起動され、コレクタースレッドはこの期間スリープし、その後プロファイラーが停止されプロファイルが書き込まれます。値が大きいほどサンプルカバレッジとファイルサイズが増加しますが、プロファイラーの実行時間が長くなり、後続の収集が遅れます。値が小さいほどオーバーヘッドは減少しますが、不十分なサンプルが生成される可能性があります。この値が`proc_profile_file_retained_days`や`proc_profile_file_retained_size_bytes`などの保持設定と一致していることを確認してください。
- 導入バージョン: v3.2.12

### ストレージ

##### `alter_table_timeout_second`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: スキーマ変更操作 (ALTER TABLE) のタイムアウト期間。
- 導入バージョン: -

##### `capacity_used_percent_high_water`

- デフォルト: 0.75
- タイプ: double
- 単位: 比率 (0.0–1.0)
- 変更可能: はい
- 説明: バックエンド負荷スコアの計算時に使用されるディスク容量使用率 (総容量の割合) の高水位しきい値。`BackendLoadStatistic.calcSore`は、`capacity_used_percent_high_water`を使用して`LoadScore.capacityCoefficient`を設定します。バックエンドの使用率が0.5未満の場合、係数は0.5に等しくなります。使用率が`capacity_used_percent_high_water`より大きい場合、係数は1.0になります。それ以外の場合、係数は`2 * usedPercent - 0.5`を介して使用率とともに線形に変化します。係数が1.0の場合、負荷スコアは容量比率によって完全に駆動されます。値が低いほど、レプリカ数の重みが増加します。この値を調整すると、高ディスク使用率のバックエンドをバランサーがどれだけ積極的にペナルティを科すかが変わります。
- 導入バージョン: v3.2.0

##### `catalog_trash_expire_second`

- デフォルト: 86400
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: データベース、テーブル、またはパーティションがドロップされた後にメタデータが保持できる最長期間。この期間が経過すると、データは削除され、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md)コマンドで回復することはできません。
- 導入バージョン: -

##### `check_consistency_default_timeout_second`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカ整合性チェックのタイムアウト期間。タブレットのサイズに基づいてこのパラメータを設定できます。
- 導入バージョン: -

##### `consistency_check_cooldown_time_second`

- デフォルト: 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 同じタブレットの整合性チェックの間で必要な最小間隔 (秒単位) を制御します。タブレットの選択中、タブレットは`tablet.getLastCheckTime()`が`(currentTimeMillis - consistency_check_cooldown_time_second * 1000)`よりも小さい場合にのみ適格と見なされます。デフォルト値 (24 * 3600) は、バックエンドディスクI/Oを削減するために、タブレットあたり約1日1回のチェックを強制します。この値を減らすとチェック頻度とリソース使用量が増加し、増やすと不整合の検出が遅れる代わりにI/Oが減少します。この値は、インデックスのタブレットリストからクールダウンされたタブレットをフィルタリングする際にグローバルに適用されます。
- 導入バージョン: v3.5.5

##### `consistency_check_end_time`

- デフォルト: "4"
- タイプ: String
- 単位: 時刻 (0-23)
- 変更可能: いいえ
- 説明: ConsistencyCheckerの作業ウィンドウの終了時刻 (日の時刻) を指定します。値はシステムタイムゾーンでSimpleDateFormat("HH")で解析され、0～23 (1桁または2桁) として受け入れられます。StarRocksは、`consistency_check_start_time`と`consistency_check_end_time`を使用して、整合性チェックジョブをいつスケジュールして追加するかを決定します。`consistency_check_start_time`が`consistency_check_end_time`よりも大きい場合、ウィンドウは深夜をまたぎます (例: デフォルトは`consistency_check_start_time` = "23"から`consistency_check_end_time` = "4")。`consistency_check_start_time`が`consistency_check_end_time`と等しい場合、チェッカーは実行されません。解析エラーはFEの起動時にエラーをログに記録して終了するため、有効な時間文字列を提供してください。
- 導入バージョン: v3.2.0

##### `consistency_check_start_time`

- デフォルト: "23"
- タイプ: String
- 単位: 時刻 (00-23)
- 変更可能: いいえ
- 説明: ConsistencyCheckerの作業ウィンドウの開始時刻 (日の時刻) を指定します。値はシステムタイムゾーンでSimpleDateFormat("HH")で解析され、0～23 (1桁または2桁) として受け入れられます。StarRocksは、`consistency_check_start_time`と`consistency_check_end_time`を使用して、整合性チェックジョブをいつスケジュールして追加するかを決定します。`consistency_check_start_time`が`consistency_check_end_time`よりも大きい場合、ウィンドウは深夜をまたぎます (例: デフォルトは`consistency_check_start_time` = "23"から`consistency_check_end_time` = "4")。`consistency_check_start_time`が`consistency_check_end_time`と等しい場合、チェッカーは実行されません。解析エラーはFEの起動時にエラーをログに記録して終了するため、有効な時間文字列を提供してください。
- 導入バージョン: v3.2.0

##### `consistency_tablet_meta_check_interval_ms`

- デフォルト: 2 * 3600 * 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: ConsistencyCheckerが`TabletInvertedIndex`と`LocalMetastore`の間でフルタブレットメタ整合性スキャンを実行する間隔。`runAfterCatalogReady`内のデーモンは、`current time - lastTabletMetaCheckTime`がこの値を超えたときにcheckTabletMetaConsistencyをトリガーします。無効なタブレットが最初に検出されると、その`toBeCleanedTime`は`now + (consistency_tablet_meta_check_interval_ms / 2)`に設定されるため、実際の削除は後続のスキャンまで遅延します。この値を増やすとスキャン頻度と負荷が減少します (クリーンアップが遅くなります)。減らすと古いタブレットをより速く検出して削除します (オーバーヘッドが増加します)。
- 導入バージョン: v3.2.0

##### `default_replication_num`

- デフォルト: 3
- タイプ: Short
- 単位: -
- 変更可能: はい
- 説明: StarRocksでテーブルを作成する際に、各データパーティションのデフォルトのレプリカ数を設定します。この設定は、CREATE TABLE DDLで`replication_num=x`を指定することで、テーブル作成時に上書きできます。
- 導入バージョン: -

##### `enable_auto_tablet_distribution`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: バケット数を自動設定するかどうか。
  - このパラメータが`TRUE`に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を指定する必要はありません。StarRocksは自動的にバケット数を決定します。
  - このパラメータが`FALSE`に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を手動で指定する必要があります。テーブルに新しいパーティションを追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションのバケット数を手動で指定することもできます。
- 導入バージョン: v2.5.7

##### `enable_experimental_rowstore`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: [ハイブリッド行-列ストレージ](../../table_design/hybrid_table.md)機能を有効にするかどうか。
- 導入バージョン: v3.2.3

##### `enable_fast_schema_evolution`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: StarRocksクラスター内のすべてのテーブルで高速スキーマ進化を有効にするかどうか。有効な値は`TRUE`と`FALSE` (デフォルト) です。高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加や削除時のリソース使用量が削減されます。
- 導入バージョン: v3.2.0

> **NOTE**
>
> - StarRocks共有データクラスターはv3.3.0からこのパラメータをサポートします。
> - 特定のテーブルの高速スキーマ進化を設定する必要がある場合 (例: 特定のテーブルの高速スキーマ進化を無効にする場合) は、テーブル作成時にテーブルプロパティ[`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution)を設定できます。

##### `enable_online_optimize_table`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 最適化ジョブを作成する際に、StarRocksが非ブロッキングのオンライン最適化パスを使用するかどうかを制御します。`enable_online_optimize_table`がtrueで、ターゲットテーブルが互換性チェックを満たしている場合 (パーティション/キー/ソート指定なし、分散が`RandomDistributionDesc`ではない、ストレージタイプが`COLUMN_WITH_ROW`ではない、レプリケーションストレージが有効、テーブルがクラウドネイティブテーブルまたはマテリアライズドビューではない)、プランナーは`OnlineOptimizeJobV2`を作成して、書き込みをブロックせずに最適化を実行します。falseの場合、または互換性条件のいずれかが失敗した場合、StarRocksは`OptimizeJobV2`にフォールバックします。これは最適化中に書き込み操作をブロックする可能性があります。
- 導入バージョン: v3.3.3, v3.4.0, v3.5.0

##### `enable_strict_storage_medium_check`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ユーザーがテーブルを作成する際に、FEがBEのストレージ媒体を厳密にチェックするかどうか。このパラメータが`TRUE`に設定されている場合、FEはユーザーがテーブルを作成する際にBEのストレージ媒体をチェックし、BEのストレージ媒体がCREATE TABLEステートメントで指定された`storage_medium`パラメータと異なる場合、エラーを返します。例えば、CREATE TABLEステートメントで指定されたストレージ媒体がSSDであるのに、BEの実際のストレージ媒体がHDDである場合、テーブル作成は失敗します。このパラメータが`FALSE`の場合、FEはユーザーがテーブルを作成する際にBEのストレージ媒体をチェックしません。
- 導入バージョン: -

##### `max_bucket_number_per_partition`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: パーティションで作成できるバケットの最大数。
- 導入バージョン: v3.3.2

##### `max_column_number_per_table`

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルで作成できる列の最大数。
- 導入バージョン: v3.3.2

##### `max_dynamic_partition_num`

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 動的パーティションテーブルを分析または作成する際に、一度に作成できるパーティションの最大数を制限します。動的パーティションプロパティの検証中に、システムはtask_runs_max_history_numberが予想されるパーティション数 (終了オフセット + 履歴パーティション数) を計算し、合計が`max_dynamic_partition_num`を超えるとDDLエラーをスローします。正当に大きなパーティション範囲を予想する場合にのみこの値を上げてください。これを増やすとより多くのパーティションを作成できますが、メタデータサイズ、スケジューリング作業、および運用上の複雑さが増加する可能性があります。
- 導入バージョン: v3.2.0

##### `max_partition_number_per_table`

- デフォルト: 100000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: テーブルで作成できるパーティションの最大数。
- 導入バージョン: v3.3.2

##### `max_task_consecutive_fail_count`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: タスクがスケジューラーによって自動的に一時停止されるまでの連続失敗の最大数。`TaskSource.MV.equals(task.getSource())`と`max_task_consecutive_fail_count`が0より大きい場合、タスクの連続失敗カウンターが`max_task_consecutive_fail_count`に達するか超えると、タスクはTaskManagerを介して一時停止され、マテリアライズドビュータスクの場合、マテリアライズドビューは非アクティブ化されます。一時停止と再アクティブ化の方法を示す例外がスローされます (例: `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`)。自動一時停止を無効にするには、この項目を0または負の値に設定します。
- 導入バージョン: -

##### `partition_recycle_retention_period_secs`

- デフォルト: 1800
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: INSERT OVERWRITEまたはマテリアライズドビュー更新操作によって削除されたパーティションのメタデータ保持時間。このようなメタデータは、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md)を実行しても回復できないことに注意してください。
- 導入バージョン: v3.5.9

##### `recover_with_empty_tablet`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 紛失または破損したタブレットレプリカを空のレプリカで置き換えるかどうか。タブレットレプリカが紛失または破損した場合、このタブレットまたは他の健全なタブレットに対するデータクエリが失敗する可能性があります。紛失または破損したタブレットレプリカを空のタブレットで置き換えることで、クエリは引き続き実行できます。ただし、データが失われているため、結果が正しくない可能性があります。デフォルト値は`FALSE`で、紛失または破損したタブレットレプリカが空のレプリカで置き換えられず、クエリが失敗することを意味します。
- 導入バージョン: -

##### `storage_usage_hard_limit_percent`

- デフォルト: 95
- エイリアス: storage_flood_stage_usage_percent
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BEディレクトリのストレージ使用率のハードリミット。BEストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが`storage_usage_hard_limit_reserve_bytes`未満の場合、ロードジョブとリカバリジョブは拒否されます。この項目はBE構成項目`storage_flood_stage_usage_percent`と組み合わせて設定する必要があります。
- 導入バージョン: -

##### `storage_usage_hard_limit_reserve_bytes`

- デフォルト: 100 * 1024 * 1024 * 1024
- エイリアス: storage_flood_stage_left_capacity_bytes
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BEディレクトリの残りのストレージスペースのハードリミット。BEストレージディレクトリの残りのストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が`storage_usage_hard_limit_percent`を超える場合、ロードジョブとリカバリジョブは拒否されます。この項目はBE構成項目`storage_flood_stage_left_capacity_bytes`と組み合わせて設定する必要があります。
- 導入バージョン: -

##### `storage_usage_soft_limit_percent`

- デフォルト: 90
- エイリアス: storage_high_watermark_usage_percent
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BEディレクトリのストレージ使用率のソフトリミット。BEストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが`storage_usage_soft_limit_reserve_bytes`未満の場合、タブレットをこのディレクトリにクローンすることはできません。
- 導入バージョン: -

##### `storage_usage_soft_limit_reserve_bytes`

- デフォルト: 200 * 1024 * 1024 * 1024
- エイリアス: storage_min_left_capacity_bytes
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BEディレクトリの残りのストレージスペースのソフトリミット。BEストレージディレクトリの残りのストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が`storage_usage_soft_limit_percent`を超える場合、タブレットをこのディレクトリにクローンすることはできません。
- 導入バージョン: -

##### `tablet_checker_lock_time_per_cycle_ms`

- デフォルト: 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: タブレットチェッカーがテーブルロックを解放して再取得するまでの、サイクルあたりの最大ロック保持時間。100未満の値は100として扱われます。
- 導入バージョン: v3.5.9, v4.0.2

##### `tablet_create_timeout_second`

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレットを作成するためのタイムアウト期間。v3.1以降、デフォルト値は1から10に変更されました。
- 導入バージョン: -

##### `tablet_delete_timeout_second`

- デフォルト: 2
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレットを削除するためのタイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_balance_load_disk_safe_threshold`

- デフォルト: 0.5
- エイリアス: balance_load_disk_safe_threshold
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BEのディスク使用量がバランスしているかどうかを判断するためのパーセンテージしきい値。すべてのBEのディスク使用量がこの値よりも低い場合、バランスしていると見なされます。ディスク使用量がこの値よりも大きく、最高と最低のBEディスク使用量の差が10%よりも大きい場合、ディスク使用量が不均衡と見なされ、タブレットの再バランスがトリガーされます。
- 導入バージョン: -

##### `tablet_sched_balance_load_score_threshold`

- デフォルト: 0.1
- エイリアス: balance_load_score_threshold
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BEの負荷がバランスしているかどうかを判断するためのパーセンテージしきい値。BEの負荷がすべてのBEの平均負荷よりも低く、その差がこの値よりも大きい場合、このBEは低負荷状態です。逆に、BEの負荷が平均負荷よりも高く、その差がこの値よりも大きい場合、このBEは高負荷状態です。
- 導入バージョン: -

##### `tablet_sched_be_down_tolerate_time_s`

- デフォルト: 900
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: スケジューラがBEノードを非アクティブのまま許容する最大期間。この時間しきい値に達すると、そのBEノード上のタブレットは他のアクティブなBEノードに移行されます。
- 導入バージョン: v2.5.7

##### `tablet_sched_disable_balance`

- デフォルト: false
- エイリアス: disable_balance
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: タブレットバランシングを無効にするかどうか。`TRUE`はタブレットバランシングが無効になっていることを示します。`FALSE`はタブレットバランシングが有効になっていることを示します。
- 導入バージョン: -

##### `tablet_sched_disable_colocate_balance`

- デフォルト: false
- エイリアス: disable_colocate_balance
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Colocate Tableのレプリカバランシングを無効にするかどうか。`TRUE`はレプリカバランシングが無効になっていることを示します。`FALSE`はレプリカバランシングが有効になっていることを示します。
- 導入バージョン: -

##### `tablet_sched_max_balancing_tablets`

- デフォルト: 500
- エイリアス: max_balancing_tablets
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にバランスできるタブレットの最大数。この値を超えると、タブレットの再バランスはスキップされます。
- 導入バージョン: -

##### `tablet_sched_max_clone_task_timeout_sec`

- デフォルト: 2 * 60 * 60
- エイリアス: max_clone_task_timeout_sec
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: タブレットのクローン作成の最大タイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_max_not_being_scheduled_interval_ms`

- デフォルト: 15 * 60 * 1000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: タブレットクローンタスクがスケジュールされている際に、タブレットがこのパラメータで指定された時間スケジュールされていない場合、StarRocksはそれをできるだけ早くスケジュールするために高い優先度を与えます。
- 導入バージョン: -

##### `tablet_sched_max_scheduling_tablets`

- デフォルト: 10000
- エイリアス: max_scheduling_tablets
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にスケジュールできるタブレットの最大数。この値を超えると、タブレットのバランス調整と修復チェックはスキップされます。
- 導入バージョン: -

##### `tablet_sched_min_clone_task_timeout_sec`

- デフォルト: 3 * 60
- エイリアス: min_clone_task_timeout_sec
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: タブレットのクローン作成の最小タイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_num_based_balance_threshold_ratio`

- デフォルト: 0.5
- エイリアス: -
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 数値ベースのバランスはディスクサイズバランスを損なう可能性がありますが、ディスク間の最大ギャップは`tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold`を超えることはできません。クラスター内にAからB、BからAへと常にバランスしているタブレットがある場合、この値を減らしてください。タブレットの分散をよりバランスさせたい場合は、この値を増やしてください。
- 導入バージョン: - 3.1

##### `tablet_sched_repair_delay_factor_second`

- デフォルト: 60
- エイリアス: tablet_repair_delay_factor_second
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカが修復される間隔 (秒単位)。
- 導入バージョン: -

##### `tablet_sched_slot_num_per_path`

- デフォルト: 8
- エイリアス: schedule_slot_num_per_path
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BEストレージディレクトリで同時に実行できるタブレット関連タスクの最大数。v2.5以降、このパラメータのデフォルト値は4から8に変更されました。
- 導入バージョン: -

##### `tablet_sched_storage_cooldown_second`

- デフォルト: -1
- エイリアス: storage_cooldown_second
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: テーブル作成時からの自動冷却のレイテンシ。デフォルト値`-1`は自動冷却が無効になっていることを指定します。自動冷却を有効にする場合は、このパラメータを`-1`より大きい値に設定してください。
- 導入バージョン: -

##### `tablet_stat_update_interval_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: FEが各BEからタブレット統計を取得する時間間隔。
- 導入バージョン: -

### 共有データ

##### `aws_s3_access_key`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用するAccess Key ID。
- 導入バージョン: v3.0

##### `aws_s3_endpoint`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用するエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。
- 導入バージョン: v3.0

##### `aws_s3_external_id`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットへのクロスアカウントアクセスに使用されるAWSアカウントの外部ID。
- 導入バージョン: v3.0

##### `aws_s3_iam_role_arn`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データファイルが格納されているS3バケットに対する権限を持つIAMロールのARN。
- 導入バージョン: v3.0

##### `aws_s3_path`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるS3パス。S3バケットの名前と、その下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.0

##### `aws_s3_region`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットが存在するリージョン。例: `us-west-2`。
- 導入バージョン: v3.0

##### `aws_s3_secret_key`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用するSecret Access Key。
- 導入バージョン: v3.0

##### `aws_s3_use_aws_sdk_default_behavior`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: AWS SDKのデフォルトの認証資格情報を使用するかどうか。有効な値: trueおよびfalse (デフォルト)。
- 導入バージョン: v3.0

##### `aws_s3_use_instance_profile`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: S3にアクセスするための認証方法として、インスタンスプロファイルと引き受けロールを使用するかどうか。有効な値: trueおよびfalse (デフォルト)。
  - IAMユーザーベースの資格情報 (Access KeyとSecret Key) を使用してS3にアクセスする場合は、この項目を`false`に指定し、`aws_s3_access_key`と`aws_s3_secret_key`を指定する必要があります。
  - インスタンスプロファイルを使用してS3にアクセスする場合は、この項目を`true`に指定する必要があります。
  - 引き受けロールを使用してS3にアクセスする場合は、この項目を`true`に指定し、`aws_s3_iam_role_arn`を指定する必要があります。
  - また、外部AWSアカウントを使用する場合は、`aws_s3_external_id`も指定する必要があります。
- 導入バージョン: v3.0

##### `azure_adls2_endpoint`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2アカウントのエンドポイント。例: `https://test.dfs.core.windows.net`。
- 導入バージョン: v3.4.1

##### `azure_adls2_oauth2_client_id`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるマネージドIDのクライアントID。
- 導入バージョン: v3.4.4

##### `azure_adls2_oauth2_tenant_id`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるマネージドIDのテナントID。
- 導入バージョン: v3.4.4

##### `azure_adls2_oauth2_use_managed_identity`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: マネージドIDを使用してAzure Data Lake Storage Gen2へのリクエストを認証するかどうか。
- 導入バージョン: v3.4.4

##### `azure_adls2_path`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるAzure Data Lake Storage Gen2パス。ファイルシステム名とディレクトリ名で構成されます。例: `testfilesystem/starrocks`。
- 導入バージョン: v3.4.1

##### `azure_adls2_sas_token`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用される共有アクセス署名 (SAS)。
- 導入バージョン: v3.4.1

##### `azure_adls2_shared_key`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用される共有キー。
- 導入バージョン: v3.4.1

##### `azure_blob_endpoint`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageアカウントのエンドポイント。例: `https://test.blob.core.windows.net`。
- 導入バージョン: v3.1

##### `azure_blob_path`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるAzure Blob Storageパス。ストレージアカウント内のコンテナの名前と、その下のサブパス (存在する場合) で構成されます。例: `testcontainer/subpath`。
- 導入バージョン: v3.1

##### `azure_blob_sas_token`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageへのリクエストを認証するために使用される共有アクセス署名 (SAS)。
- 導入バージョン: v3.1

##### `azure_blob_shared_key`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageへのリクエストを認証するために使用される共有キー。
- 導入バージョン: v3.1

##### `azure_use_native_sdk`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Azure Blob StorageにアクセスするためにネイティブSDKを使用するかどうか。これにより、マネージドIDとサービスプリンシパルでの認証が可能になります。この項目が`false`に設定されている場合、共有キーとSASトークンでの認証のみが許可されます。
- 導入バージョン: v3.4.4

##### `cloud_native_hdfs_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: HDFSストレージのURL。例: `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 導入バージョン: -

##### `cloud_native_meta_port`

- デフォルト: 6090
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: FEクラウドネイティブメタデータサーバーRPCリスンポート。
- 導入バージョン: -

##### `cloud_native_storage_type`

- デフォルト: S3
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocksはHDFS、Azure Blob (v3.1.1以降サポート)、Azure Data Lake Storage Gen2 (v3.4.1以降サポート)、Google Storage (ネイティブSDK、v3.5.1以降サポート)、およびS3プロトコルと互換性のあるオブジェクトストレージシステム (AWS S3、MinIOなど) へのデータ保存をサポートします。有効な値: `S3` (デフォルト)、`HDFS`、`AZBLOB`、`ADLS2`、および`GS`。このパラメータを`S3`と指定した場合、`aws_s3`で始まるパラメータを追加する必要があります。`AZBLOB`と指定した場合、`azure_blob`で始まるパラメータを追加する必要があります。`ADLS2`と指定した場合、`azure_adls2`で始まるパラメータを追加する必要があります。`GS`と指定した場合、`gcp_gcs`で始まるパラメータを追加する必要があります。`HDFS`と指定した場合、`cloud_native_hdfs_url`のみを指定する必要があります。
- 導入バージョン: -

##### `enable_load_volume_from_conf`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがFE構成ファイルで指定されたオブジェクトストレージ関連プロパティを使用して、組み込みのストレージボリュームを作成することを許可するかどうか。v3.4.1以降、デフォルト値は`true`から`false`に変更されました。
- 導入バージョン: v3.1.0

##### `gcp_gcs_impersonation_service_account`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Google Storageにアクセスするために偽装ベースの認証を使用する場合に偽装するサービスアカウント。
- 導入バージョン: v3.5.1

##### `gcp_gcs_path`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるGoogle Cloudパス。Google Cloudバケットの名前と、その下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_email`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: サービスアカウント作成時に生成されたJSONファイル内のメールアドレス。例: `user@hello.iam.gserviceaccount.com`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_private_key`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: サービスアカウント作成時に生成されたJSONファイル内の秘密鍵。例: `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_private_key_id`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: サービスアカウント作成時に生成されたJSONファイル内の秘密鍵ID。
- 導入バージョン: v3.5.1

##### `gcp_gcs_use_compute_engine_service_account`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Compute Engineにバインドされているサービスアカウントを使用するかどうか。
- 導入バージョン: v3.5.1

##### `hdfs_file_system_expire_seconds`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: HdfsFsManagerによって管理される未使用のキャッシュされたHDFS/ObjectStore FileSystemのTime-to-Live (秒単位)。FileSystemExpirationChecker (60秒ごとに実行) はこの値を使用して各HdfsFs.isExpired(...) を呼び出します。期限切れになると、マネージャーは基盤となるFileSystemを閉じ、キャッシュから削除します。アクセサーメソッド (例: `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`) は最終アクセス時刻を更新するため、有効期限は非アクティブ状態に基づいています。値が低いとアイドルリソースの保持が減少しますが、再オープンオーバーヘッドが増加します。値が高いとハンドルが長く保持され、より多くのリソースを消費する可能性があります。
- 導入バージョン: v3.2.0

##### `lake_autovacuum_grace_period_minutes`

- デフォルト: 30
- タイプ: Long
- 単位: 分
- 変更可能: はい
- 説明: 共有データクラスター内の履歴データバージョンを保持する時間範囲。この時間範囲内の履歴データバージョンは、Compaction後にAutoVacuumによって自動的にクリーンアップされません。実行中のクエリによってアクセスされるデータがクエリ完了前に削除されないように、この値を最大クエリ時間よりも大きく設定する必要があります。デフォルト値はv3.3.0、v3.2.5、v3.1.10以降、`5`から`30`に変更されました。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_parallel_partitions`

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: 共有データクラスターで同時にAutoVacuumを受けることができるパーティションの最大数。AutoVacuumはCompaction後のガベージコレクションです。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_partition_naptime_seconds`

- デフォルト: 180
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 共有データクラスター内の同じパーティションでのAutoVacuum操作間の最小間隔。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_stale_partition_threshold`

- デフォルト: 12
- タイプ: Long
- 単位: 時間
- 変更可能: はい
- 説明: この時間範囲内でパーティションに更新 (ロード、DELETE、またはCompaction) がない場合、システムはこのパーティションに対してAutoVacuumを実行しません。
- 導入バージョン: v3.1.0

##### `lake_compaction_allow_partial_success`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、システムは共有データクラスターでのCompaction操作を、サブタスクの1つが成功した場合に成功と見なします。
- 導入バージョン: v3.5.2

##### `lake_compaction_disable_ids`

- デフォルト: ""
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: 共有データモードでCompactionが無効になっているテーブルまたはパーティションのリスト。形式はセミコロン区切りの`tableId1;partitionId2`です。例: `12345;98765`。
- 導入バージョン: v3.4.4

##### `lake_compaction_history_size`

- デフォルト: 20
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターのリーダーFEノードのメモリに保持する最近成功したCompactionタスクレコードの数。`SHOW PROC '/compactions'`コマンドを使用して、最近成功したCompactionタスクレコードを表示できます。Compaction履歴はFEプロセスのメモリに格納され、FEプロセスが再起動されると失われることに注意してください。
- 導入バージョン: v3.1.0

##### `lake_compaction_max_tasks`

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターで許可される同時Compactionタスクの最大数。この項目を`-1`に設定すると、同時タスク数が適応的に計算されることを示します。この値を`0`に設定すると、Compactionは無効になります。
- 導入バージョン: v3.1.0

##### `lake_compaction_score_selector_min_score`

- デフォルト: 10.0
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターでCompaction操作をトリガーするCompactionスコアのしきい値。パーティションのCompactionスコアがこの値以上の場合、システムはそのパーティションに対してCompactionを実行します。
- 導入バージョン: v3.1.0

##### `lake_compaction_score_upper_bound`

- デフォルト: 2000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスター内のパーティションのCompactionスコアの上限。`0`は上限なしを示します。この項目は`lake_enable_ingest_slowdown`が`true`に設定されている場合にのみ有効になります。パーティションのCompactionスコアがこの上限に達するか超えると、受信ロードタスクは拒否されます。v3.3.6以降、デフォルト値は`0`から`2000`に変更されました。
- 導入バージョン: v3.2.0

##### `lake_enable_balance_tablets_between_workers`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターのタブレット移行中に、コンピュートノード間でタブレットの数をバランスさせるかどうか。`true`はコンピュートノード間でタブレットのバランスを取ることを示し、`false`はこの機能を無効にすることを示します。
- 導入バージョン: v3.3.4

##### `lake_enable_ingest_slowdown`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターでデータ取り込みの減速を有効にするかどうか。データ取り込みの減速が有効になっている場合、パーティションのCompactionスコアが`lake_ingest_slowdown_threshold`を超えると、そのパーティションでのロードタスクがスロットリングされます。この設定は、`run_mode`が`shared_data`に設定されている場合にのみ有効になります。v3.3.6以降、デフォルト値は`false`から`true`に変更されました。
- 導入バージョン: v3.2.0

##### `lake_ingest_slowdown_threshold`

- デフォルト: 100
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスターでデータ取り込みの減速をトリガーするCompactionスコアのしきい値。この設定は、`lake_enable_ingest_slowdown`が`true`に設定されている場合にのみ有効になります。
- 導入バージョン: v3.2.0

##### `lake_publish_version_max_threads`

- デフォルト: 512
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 共有データクラスター内のバージョン公開タスクの最大スレッド数。
- 導入バージョン: v3.2.0

##### `meta_sync_force_delete_shard_meta`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: リモートストレージファイルをクリーンアップせずに、共有データクラスターのメタデータを直接削除することを許可するかどうか。この項目は、クリーンアップするシャードが過剰に多く、FE JVMに極端なメモリ圧力がかかる場合にのみ`true`に設定することをお勧めします。この機能が有効になった後、シャードまたはタブレットに属するデータファイルが自動的にクリーンアップされないことに注意してください。
- 導入バージョン: v3.2.10, v3.3.3

##### `run_mode`

- デフォルト: shared_nothing
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksクラスターの実行モード。有効な値: `shared_data`と`shared_nothing` (デフォルト)。
  - `shared_data`はStarRocksが共有データモードで実行されていることを示します。
  - `shared_nothing`はStarRocksが共有なしモードで実行されていることを示します。

  > **CAUTION**
  >
  > - StarRocksクラスターに対して`shared_data`モードと`shared_nothing`モードを同時に採用することはできません。混合デプロイメントはサポートされていません。
  > - クラスターのデプロイ後に`run_mode`を変更しないでください。変更すると、クラスターが再起動に失敗します。共有なしクラスターから共有データクラスターへの変換、またはその逆の変換はサポートされていません。

- 導入バージョン: -

##### `shard_group_clean_threshold_sec`

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: 共有データクラスターでFEが未使用のタブレットおよびシャードグループをクリーンアップするまでの時間。このしきい値内で作成されたタブレットおよびシャードグループはクリーンアップされません。
- 導入バージョン: -

##### `star_mgr_meta_sync_interval_sec`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: FEが共有データクラスターでStarMgrと定期的なメタデータ同期を実行する間隔。
- 導入バージョン: -

##### `starmgr_grpc_server_max_worker_threads`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE starmgrモジュールのgrpcサーバーが使用するワーカー スレッドの最大数。
- 導入バージョン: v4.0.0, v3.5.8

##### `starmgr_grpc_timeout_seconds`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明:
- 導入バージョン: -

### データレイク

##### `files_enable_insert_push_down_schema`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、アナライザーはINSERT ... FROM files() 操作のために、ターゲットテーブルスキーマを`files()`テーブル関数にプッシュしようとします。これは、ソースがFileTableFunctionRelationであり、ターゲットがネイティブテーブルであり、SELECTリストに対応するスロット参照列 (または*) が含まれている場合にのみ適用されます。アナライザーは、選択された列をターゲット列に一致させ (カウントが一致する必要があります)、ターゲットテーブルを一時的にロックし、非複合型 (Parquet JSON `->` `array<varchar>`などの複合型はスキップされます) のファイル列タイプをディープコピーされたターゲット列タイプに置き換えます。元のファイルテーブルからの列名は保持されます。これにより、取り込み中のファイルベースの型推論による型不一致と緩みが減少します。
- 導入バージョン: v3.4.0, v3.5.0

##### `hdfs_read_buffer_size_kb`

- デフォルト: 8192
- タイプ: Int
- 単位: キロバイト
- 変更可能: はい
- 説明: HDFS読み取りバッファのサイズ (キロバイト単位)。StarRocksはこの値をバイトに変換し (`<< 10`)、HdfsFsManagerでHDFS読み取りバッファを初期化し、ブローカーアクセスが使用されていない場合にBEタスクに送信されるthriftフィールド`hdfs_read_buffer_size_kb` (例: `TBrokerScanRangeParams`、`TDownloadReq`) を設定するために使用します。`hdfs_read_buffer_size_kb`を増やすと、シーケンシャル読み取りスループットが向上し、syscallオーバーヘッドが減少しますが、ストリームあたりのメモリ使用量が増加します。減らすとメモリフットプリントが減少しますが、IO効率が低下する可能性があります。調整する際には、ワークロード (多くの小さなストリームと少数の大きなシーケンシャル読み取り) を考慮してください。
- 導入バージョン: v3.2.0

##### `hdfs_write_buffer_size_kb`

- デフォルト: 1024
- タイプ: Int
- 単位: キロバイト
- 変更可能: はい
- 説明: ブローカーを使用せずにHDFSまたはオブジェクトストアに直接書き込む際に使用されるHDFS書き込みバッファサイズ (KB単位) を設定します。FEはこの値をバイトに変換し (`<< 10`)、HdfsFsManagerでローカル書き込みバッファを初期化し、Thriftリクエスト (例: TUploadReq、TExportSink、シンクオプション) に伝播されるため、バックエンド/エージェントは同じバッファサイズを使用します。この値を増やすと、大規模なシーケンシャル書き込みのスループットが向上しますが、ライターあたりのメモリが増加します。減らすと、ストリームあたりのメモリ使用量が減少し、小さな書き込みのレイテンシが低下する可能性があります。`hdfs_read_buffer_size_kb`と並行して調整し、利用可能なメモリと同時ライターを考慮してください。
- 導入バージョン: v3.2.0

##### `lake_batch_publish_max_version_num`

- デフォルト: 10
- タイプ: Int
- 単位: カウント
- 変更可能: はい
- 説明: レイク (クラウドネイティブ) テーブルの公開バッチを構築する際に、連続するトランザクションバージョンをグループ化できる数の上限を設定します。この値はトランザクショングラフバッチルーチン (getReadyToPublishTxnListBatchを参照) に渡され、`lake_batch_publish_min_version_num`と連携してTransactionStateBatchの候補範囲サイズを決定します。値が大きいほど、より多くのコミットをバッチ処理することで公開スループットを向上させることができますが、アトミック公開の範囲が増加し (可視性のレイテンシが長くなり、ロールバックの表面積が大きくなります)、バージョンが連続していない場合、実行時に制限される可能性があります。ワークロードと可視性/レイテンシの要件に従って調整してください。
- 導入バージョン: v3.2.0

##### `lake_batch_publish_min_version_num`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: レイクテーブルの公開バッチを形成するために必要な連続トランザクションバージョンの最小数を設定します。DatabaseTransactionMgr.getReadyToPublishTxnListBatchはこの値を`lake_batch_publish_max_version_num`とともにtransactionGraph.getTxnsWithTxnDependencyBatchに渡し、依存するトランザクションを選択します。`1`の値は単一トランザクション公開 (バッチ処理なし) を許可します。`>1`の値は、少なくともその数の連続したバージョンを持つ単一テーブル、非レプリケーショントランザクションが利用可能である必要があります。バージョンが連続していない場合、レプリケーショントランザクションが表示された場合、またはスキーマ変更がバージョンを消費した場合、バッチ処理は中止されます。この値を増やすと、コミットをグループ化することで公開スループットが向上する可能性がありますが、十分な連続トランザクションを待機している間に公開が遅延する可能性があります。
- 導入バージョン: v3.2.0

##### `lake_enable_batch_publish_version`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、PublishVersionDaemonは同じレイク (共有データ) テーブル/パーティションの準備完了トランザクションをバッチ処理し、トランザクションごとの公開を発行する代わりに、それらのバージョンをまとめて公開します。RunModeがshared-dataの場合、デーモンはgetReadyPublishTransactionsBatch()を呼び出し、publishVersionForLakeTableBatch(...) を使用してグループ化された公開操作を実行します (RPCを削減し、スループットを向上させます)。無効にすると、デーモンはpublishVersionForLakeTable(...) を介してトランザクションごとの公開にフォールバックします。実装は、スイッチが切り替えられた場合の重複公開を避けるために内部セットを使用して進行中の作業を調整し、`lake_publish_version_max_threads`を介したスレッドプールのサイジングの影響を受けます。
- 導入バージョン: v3.2.0

##### `lake_enable_tablet_creation_optimization`

- デフォルト: false
- タイプ: boolean
- 単位: -
- 変更可能: はい
- 説明: 有効にすると、StarRocksは共有データモードのクラウドネイティブテーブルとマテリアライズドビューのタブレット作成を最適化し、タブレットごとに個別のメタデータではなく、物理パーティション下のすべてのタブレットに単一の共有タブレットメタデータを作成します。これにより、テーブル作成、ロールアップ、スキーマ変更ジョブ中に作成されるタブレット作成タスクとメタデータ/ファイルの数が削減されます。この最適化はクラウドネイティブテーブル/マテリアライズドビューにのみ適用され、`file_bundling`と組み合わされます (後者は同じ最適化ロジックを再利用します)。注: スキーマ変更およびロールアップジョブは、`file_bundling`を使用するテーブルの最適化を明示的に無効にして、同じ名前のファイルが上書きされるのを防ぎます。注意して有効にしてください。作成されるタブレットメタデータの粒度が変更され、レプリカ作成とファイル命名の動作に影響する可能性があります。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### `lake_use_combined_txn_log`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、システムはLakeテーブルが関連トランザクションに結合されたトランザクションログパスを使用することを許可します。共有データクラスターでのみ利用可能です。
- 導入バージョン: v3.3.7, v3.4.0, v3.5.0

##### `enable_iceberg_commit_queue`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Icebergテーブルのコミットキューを有効にして、同時コミット競合を回避するかどうか。Icebergは、メタデータコミットに楽観的同時実行制御 (OCC) を使用します。複数のスレッドが同じテーブルに同時にコミットすると、「Cannot commit: Base metadata location is not same as the current table metadata location」のようなエラーで競合が発生する可能性があります。有効にすると、各Icebergテーブルはコミット操作のために独自のシングルスレッドエグゼキューターを持ち、同じテーブルへのコミットがシリアライズされ、OCC競合が防止されます。異なるテーブルは同時にコミットでき、全体的なスループットが維持されます。これは信頼性を向上させるためのシステムレベルの最適化であり、デフォルトで有効にすべきです。無効にすると、楽観的ロック競合により同時コミットが失敗する可能性があります。
- 導入バージョン: v4.1.0

##### `iceberg_commit_queue_timeout_seconds`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: Icebergコミット操作が完了するまで待機するタイムアウト (秒単位)。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、各コミット操作はこのタイムアウト内に完了する必要があります。コミットがこのタイムアウトよりも長くかかった場合、キャンセルされ、エラーが発生します。コミット時間に影響する要因には、コミットされるデータファイルの数、テーブルのメタデータサイズ、基盤となるストレージ (S3、HDFSなど) のパフォーマンスが含まれます。
- 導入バージョン: v4.1.0

##### `iceberg_commit_queue_max_size`

- デフォルト: 1000
- タイプ: Int
- 単位: カウント
- 変更可能: いいえ
- 説明: Icebergテーブルあたりの保留中のコミット操作の最大数。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、これは単一のテーブルに対してキューに格納できるコミット操作の数を制限します。制限に達すると、追加のコミット操作は呼び出し元のスレッドで実行されます (容量が利用可能になるまでブロックされます)。この設定はFE起動時に読み取られ、新しく作成されたテーブルエグゼキューターに適用されます。変更を有効にするにはFEの再起動が必要です。同じテーブルへの同時コミットが多いと予想される場合は、この値を増やしてください。この値が低すぎると、高コンカレンシー時に呼び出し元のスレッドでコミットがブロックされる可能性があります。
- 導入バージョン: v4.1.0

### その他

##### `agent_task_resend_wait_time_ms`

- デフォルト: 5000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: FEがエージェントタスクを再送信するまでに待機する必要がある期間。エージェントタスクは、タスク作成時間と現在の時間のギャップがこのパラメータの値を超えた場合にのみ再送信できます。このパラメータは、エージェントタスクの繰り返し送信を防ぐために使用されます。
- 導入バージョン: -

##### `allow_system_reserved_names`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ユーザーが`__op`および`__row`で始まる名前の列を作成することを許可するかどうか。この機能を有効にするには、このパラメータを`TRUE`に設定します。これらの名前形式はStarRocksで特別な目的のために予約されており、そのような列を作成すると未定義の動作になる可能性があるため、この機能はデフォルトで無効になっています。
- 導入バージョン: v3.2.0

##### `auth_token`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEが属するStarRocksクラスター内でID認証に使用されるトークン。このパラメータが指定されていない場合、StarRocksはクラスターのリーダーFEが最初に起動されたときにクラスターのランダムなトークンを生成します。
- 導入バージョン: -

##### `authentication_ldap_simple_bind_base_dn`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAPサーバーがユーザーの認証情報の検索を開始する基底DN。
- 導入バージョン: -

##### `authentication_ldap_simple_bind_root_dn`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: ユーザーの認証情報を検索するために使用される管理者DN。
- 導入バージョン: -

##### `authentication_ldap_simple_bind_root_pwd`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: ユーザーの認証情報を検索するために使用される管理者のパスワード。
- 導入バージョン: -

##### `authentication_ldap_simple_server_host`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAPサーバーが実行されているホスト。
- 導入バージョン: -

##### `authentication_ldap_simple_server_port`

- デフォルト: 389
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: LDAPサーバーのポート。
- 導入バージョン: -

##### `authentication_ldap_simple_user_search_attr`

- デフォルト: uid
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: LDAPオブジェクトでユーザーを識別する属性の名前。
- 導入バージョン: -

##### `backup_job_default_timeout_ms`

- デフォルト: 86400 * 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: バックアップジョブのタイムアウト期間。この値を超えると、バックアップジョブは失敗します。
- 導入バージョン: -

##### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: `SHOW PROC /BACKENDS/{id}`コマンドで、各ディスクのタブレット数の収集を有効にするかどうか。
- 導入バージョン: v4.0.1, v3.5.8

##### `enable_colocate_restore`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Colocate Tableのバックアップと復元を有効にするかどうか。`true`はColocate Tableのバックアップと復元を有効にすることを示し、`false`は無効にすることを示します。
- 導入バージョン: v3.2.10, v3.3.3

##### `enable_materialized_view_concurrent_prepare`

- デフォルト: true
- タイプ: Boolean
- 単位:
- 変更可能: はい
- 説明: パフォーマンスを向上させるために、マテリアライズドビューを同時に準備するかどうか。
- 導入バージョン: v3.4.4

##### `enable_metric_calculator`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: メトリックを定期的に収集する機能を有効にするかどうかを指定します。有効な値: `TRUE`と`FALSE`。`TRUE`は有効にすることを指定し、`FALSE`は無効にすることを指定します。
- 導入バージョン: -

##### `enable_table_metrics_collect`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEでテーブルレベルのメトリックをエクスポートするかどうか。無効にすると、FEはテーブルメトリック (テーブルスキャン/ロードカウンター、テーブルサイズメトリックなど) のエクスポートをスキップしますが、カウンターはメモリに記録されます。
- 導入バージョン: -

##### `enable_mv_post_image_reload_cache`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEがイメージをロードした後、リロードフラグチェックを実行するかどうか。ベースマテリアライズドビューでチェックが実行された場合、それに関連する他のマテリアライズドビューでは不要です。
- 導入バージョン: v3.5.0

##### `enable_mv_query_context_cache`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリ書き換えパフォーマンスを向上させるために、クエリレベルのマテリアライズドビュー書き換えキャッシュを有効にするかどうか。
- 導入バージョン: v3.3

##### `enable_mv_refresh_collect_profile`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: すべてのマテリアライズドビューのデフォルトで、マテリアライズドビュー更新時にプロファイルを有効にするかどうか。
- 導入バージョン: v3.3.0

##### `enable_mv_refresh_extra_prefix_logging`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: より良いデバッグのために、ログにマテリアライズドビュー名を持つプレフィックスを有効にするかどうか。
- 導入バージョン: v3.4.0

##### `enable_mv_refresh_query_rewrite`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: クエリ書き換えをマテリアライズドビュー更新中に有効にするかどうか。これにより、クエリはベーステーブルではなく、書き換えられたマテリアライズドビューを直接使用してクエリパフォーマンスを向上させることができます。
- 導入バージョン: v3.3

##### `enable_trace_historical_node`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: システムが履歴ノードをトレースすることを許可するかどうか。この項目を`true`に設定することで、キャッシュ共有機能を有効にし、エラスティック スケーリング中にシステムが適切なキャッシュノードを選択できるようにします。
- 導入バージョン: v3.5.1

##### `es_state_sync_interval_second`

- デフォルト: 10
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: FEがElasticsearchインデックスを取得し、StarRocks外部テーブルのメタデータを同期する時間間隔。
- 導入バージョン: -

##### `hive_meta_cache_refresh_interval_s`

- デフォルト: 3600 * 2
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: Hive外部テーブルのキャッシュされたメタデータが更新される時間間隔。
- 導入バージョン: -

##### `hive_meta_store_timeout_s`

- デフォルト: 10
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: Hiveメタストアへの接続がタイムアウトするまでの時間。
- 導入バージョン: -

##### `jdbc_connection_idle_timeout_ms`

- デフォルト: 600000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: JDBCカタログへのアクセスに使用される接続がタイムアウトする最大時間。タイムアウトした接続はアイドル状態と見なされます。
- 導入バージョン: -

##### `jdbc_connection_timeout_ms`

- デフォルト: 10000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: いいえ
- 説明: HikariCP接続プールが接続を取得するためのタイムアウト (ミリ秒)。この時間内にプールから接続を取得できない場合、操作は失敗します。
- 導入バージョン: v3.5.13

##### `jdbc_query_timeout_ms`

- デフォルト: 30000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: JDBCステートメントクエリ実行のタイムアウト (ミリ秒)。このタイムアウトは、JDBCカタログを介して実行されるすべてのSQLクエリ (パーティションメタデータクエリなど) に適用されます。値はJDBCドライバーに渡される際に秒に変換されます。
- 導入バージョン: v3.5.13

##### `jdbc_network_timeout_ms`

- デフォルト: 30000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: JDBCネットワーク操作 (ソケット読み取り) のタイムアウト (ミリ秒)。このタイムアウトは、外部データベースが応答しない場合に無期限のブロックを防ぐために、データベースメタデータ呼び出し (例: getSchemas()、getTables()、getColumns()) に適用されます。
- 導入バージョン: v3.5.13

##### `jdbc_connection_pool_size`

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBCカタログにアクセスするためのJDBC接続プールの最大容量。
- 導入バージョン: -

##### `jdbc_meta_default_cache_enable`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: JDBCカタログメタデータキャッシュが有効になっているかどうかのデフォルト値。Trueに設定すると、新しく作成されたJDBCカタログはデフォルトでメタデータキャッシュが有効になります。
- 導入バージョン: -

##### `jdbc_meta_default_cache_expire_sec`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: JDBCカタログメタデータキャッシュのデフォルトの有効期限。`jdbc_meta_default_cache_enable`がtrueに設定されている場合、新しく作成されたJDBCカタログはデフォルトでメタデータキャッシュの有効期限を設定します。
- 導入バージョン: -

##### `jdbc_minimum_idle_connections`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: JDBCカタログにアクセスするためのJDBC接続プール内のアイドル接続の最小数。
- 導入バージョン: -

##### `jwt_jwks_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JSON Web Key Set (JWKS) サービスへのURL、または`fe/conf`ディレクトリ下の公開鍵ローカルファイルへのパス。
- 導入バージョン: v3.5.0

##### `jwt_principal_field`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTのサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は`sub`です。このフィールドの値は、StarRocksにログインするユーザー名と同一である必要があります。
- 導入バージョン: v3.5.0

##### `jwt_required_audience`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTのオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWTオーディエンスと一致する場合にのみ有効と見なされます。
- 導入バージョン: v3.5.0

##### `jwt_required_issuer`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTの発行者 (`iss`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWT発行者と一致する場合にのみ有効と見なされます。
- 導入バージョン: v3.5.0

##### locale

- デフォルト: zh_CN.UTF-8
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: FEが使用する文字セット。
- 導入バージョン: -

##### `max_agent_task_threads_num`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: エージェントタスクスレッドプールで許可されるスレッドの最大数。
- 導入バージョン: -

##### `max_download_task_per_be`

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各RESTORE操作で、StarRocksがBEノードに割り当てるダウンロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限はありません。
- 導入バージョン: v3.1.0

##### `max_mv_check_base_table_change_retry_times`

- デフォルト: 10
- タイプ: -
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新時にベーステーブルの変更を検出するための最大再試行回数。
- 導入バージョン: v3.3.0

##### `max_mv_refresh_failure_retry_times`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新が失敗した場合の最大再試行回数。
- 導入バージョン: v3.3.0

##### `max_mv_refresh_try_lock_failure_retry_times`

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューの更新が失敗した際のロック試行の最大再試行回数。
- 導入バージョン: v3.3.0

##### `max_small_file_number`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FEディレクトリに保存できる小ファイルの最大数。
- 導入バージョン: -

##### `max_small_file_size_bytes`

- デフォルト: 1024 * 1024
- タイプ: Int
- 単位: バイト
- 変更可能: はい
- 説明: 小ファイルの最大サイズ。
- 導入バージョン: -

##### `max_upload_task_per_be`

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 各BACKUP操作で、StarRocksがBEノードに割り当てるアップロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限はありません。
- 導入バージョン: v3.1.0

##### `mv_create_partition_batch_interval_ms`

- デフォルト: 1000
- タイプ: Int
- 単位: ms
- 変更可能: はい
- 説明: マテリアライズドビューの更新中に、複数のパーティションを一括作成する必要がある場合、システムはそれらを64パーティションのバッチに分割します。頻繁なパーティション作成による障害のリスクを軽減するため、各バッチ間にデフォルトの間隔 (ミリ秒単位) が設定され、作成頻度を制御します。
- 導入バージョン: v3.3

##### `mv_plan_cache_max_size`

- デフォルト: 1000
- タイプ: Long
- 単位:
- 変更可能: はい
- 説明: マテリアライズドビュープランキャッシュ (マテリアライズドビュー書き換えに使用される) の最大サイズ。透過的なクエリ書き換えに多くのマテリアライズドビューが使用される場合、この値を増やすことができます。
- 導入バージョン: v3.2

##### `mv_plan_cache_thread_pool_size`

- デフォルト: 3
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビュープランキャッシュ (マテリアライズドビュー書き換えに使用される) のデフォルトスレッドプールサイズ。
- 導入バージョン: v3.2

##### `mv_refresh_default_planner_optimize_timeout`

- デフォルト: 30000
- タイプ: -
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビューを更新する際のオプティマイザーの計画フェーズのデフォルトのタイムアウト。
- 導入バージョン: v3.3.0

##### `mv_refresh_fail_on_filter_data`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 更新中にデータがフィルタリングされた場合、MV更新は失敗します (デフォルトはtrue)。そうでない場合、フィルタリングされたデータを無視して成功を返します。
- 導入バージョン: -

##### `mv_refresh_try_lock_timeout_ms`

- デフォルト: 30000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: マテリアライズドビューの更新がベーステーブル/マテリアライズドビューのDBロックを試行する際のデフォルトの試行ロックタイムアウト。
- 導入バージョン: v3.3.0

##### `oauth2_auth_server_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 認証URL。OAuth 2.0認証プロセスを開始するためにユーザーのブラウザがリダイレクトされるURL。
- 導入バージョン: v3.5.0

##### `oauth2_client_id`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksクライアントの公開識別子。
- 導入バージョン: v3.5.0

##### `oauth2_client_secret`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksクライアントを認証サーバーで認証するために使用されるシークレット。
- 導入バージョン: v3.5.0

##### `oauth2_jwks_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JSON Web Key Set (JWKS) サービスへのURL、または`conf`ディレクトリ下のローカルファイルへのパス。
- 導入バージョン: v3.5.0

##### `oauth2_principal_field`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTのサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は`sub`です。このフィールドの値は、StarRocksにログインするユーザー名と同一である必要があります。
- 導入バージョン: v3.5.0

##### `oauth2_redirect_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: OAuth 2.0認証が成功した後、ユーザーのブラウザがリダイレクトされるURL。認証コードはこのURLに送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`として構成する必要があります。
- 導入バージョン: v3.5.0

##### `oauth2_required_audience`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTのオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWTオーディエンスと一致する場合にのみ有効と見なされます。
- 導入バージョン: v3.5.0

##### `oauth2_required_issuer`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: JWTの発行者 (`iss`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWT発行者と一致する場合にのみ有効と見なされます。
- 導入バージョン: v3.5.0

##### `oauth2_token_server_url`

- デフォルト: 空文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがアクセストークンを取得する認証サーバーのエンドポイントのURL。
- 導入バージョン: v3.5.0

##### `plugin_dir`

- デフォルト: System.getenv("STARROCKS_HOME") + "/plugins"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: プラグインインストールパッケージを格納するディレクトリ。
- 導入バージョン: -

##### `plugin_enable`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: FEにプラグインをインストールできるかどうか。プラグインはリーダーFEにのみインストールまたはアンインストールできます。
- 導入バージョン: -

##### `proc_profile_jstack_depth`

- デフォルト: 128
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: システムがCPUおよびメモリプロファイルを収集する際のJavaスタック深度の最大値。この値は、サンプリングされた各スタックについてキャプチャされるJavaスタックフレームの数を制御します。値が大きいほどトレースの詳細度と出力サイズが増加し、プロファイリングオーバーヘッドが増加する可能性があります。値が小さいほど詳細度は減少します。この設定は、CPUプロファイリングとメモリプロファイリングの両方でプロファイラーが起動されるときに使用されるため、診断ニーズとパフォーマンスへの影響のバランスを取るために調整してください。
- 導入バージョン: -

##### `proc_profile_mem_enable`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: プロセスメモリ割り当てプロファイルの収集を有効にするかどうか。この項目が`true`に設定されている場合、システムは`sys_log_dir/proc_profile`の下に`mem-profile-<timestamp>.html`というHTMLプロファイルを生成し、`proc_profile_collect_time_s`秒間スリープしながらサンプリングし、Javaスタック深度には`proc_profile_jstack_depth`を使用します。生成されたファイルは圧縮され、`proc_profile_file_retained_days`と`proc_profile_file_retained_size_bytes`に従ってパージされます。ネイティブ抽出パスは`/tmp`でのnoexecの問題を回避するために`STARROCKS_HOME_DIR`を使用します。この項目は、メモリ割り当てのホットスポットのトラブルシューティングを目的としています。これを有効にすると、CPU、I/O、ディスク使用量が増加し、大きなファイルが生成される可能性があります。
- 導入バージョン: v3.2.12

##### `query_detail_explain_level`

- デフォルト: COSTS
- タイプ: String
- 単位: -
- 変更可能: true
- 説明: EXPLAINステートメントによって返されるクエリ計画の詳細レベル。有効な値: COSTS、NORMAL、VERBOSE。
- 導入バージョン: v3.2.12, v3.3.5

##### `replication_interval_ms`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: レプリケーションタスクがスケジュールされる最小時間間隔。
- 導入バージョン: v3.3.5

##### `replication_max_parallel_data_size_mb`

- デフォルト: 1048576
- タイプ: Int
- 単位: MB
- 変更可能: はい
- 説明: 同時同期に許可されるデータの最大サイズ。
- 導入バージョン: v3.3.5

##### `replication_max_parallel_replica_count`

- デフォルト: 10240
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時同期に許可されるタブレットレプリカの最大数。
- 導入バージョン: v3.3.5

##### `replication_max_parallel_table_count`

- デフォルト: 100
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 許可される同時データ同期タスクの最大数。StarRocksはテーブルごとに1つの同期タスクを作成します。
- 導入バージョン: v3.3.5

##### `replication_transaction_timeout_sec`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 同期タスクのタイムアウト期間。
- 導入バージョン: v3.3.5

##### `skip_whole_phase_lock_mv_limit`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: StarRocksが関連するマテリアライズドビューを持つテーブルに「非ロック」最適化を適用するタイミングを制御します。この項目が0未満に設定されている場合、システムは常に非ロック最適化を適用し、クエリのために関連するマテリアライズドビューをコピーしません (FEメモリ使用量とメタデータコピー/ロック競合は減少しますが、メタデータの並列性問題のリスクが増加する可能性があります)。0に設定されている場合、非ロック最適化は無効になります (システムは常に安全なコピーアンドロックパスを使用します)。0より大きい値に設定されている場合、非ロック最適化は、関連するマテリアライズドビューの数が設定されたしきい値以下であるテーブルにのみ適用されます。さらに、値が0以上の場合、プランナーはクエリOLAPテーブルをオプティマイザーコンテキストに記録して、マテリアライズドビュー関連の書き換えパスを有効にします。0未満の場合、このステップはスキップされます。
- 導入バージョン: v3.2.1

##### `small_file_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 小ファイルのルートディレクトリ。
- 導入バージョン: -

##### `task_runs_max_history_number`

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: メモリに保持し、アーカイブされたタスク実行履歴をクエリする際のデフォルトのLIMITとして使用するタスク実行レコードの最大数。`enable_task_history_archive`がfalseの場合、この値はメモリ内の履歴を制限します。強制GCは、最新の`task_runs_max_history_number`のみが残るように古いエントリを削除します。アーカイブ履歴がクエリされた場合 (明示的なLIMITが指定されていない場合)、この値が0より大きい場合、`TaskRunHistoryTable.lookup`は`"ORDER BY create_time DESC LIMIT <value>"`を使用します。注: これを0に設定すると、クエリ側のLIMITが無効になります (上限なし) が、メモリ内履歴はゼロに切り捨てられます (アーカイブが有効になっていない場合)。
- 導入バージョン: v3.2.0

##### `tmp_dir`

- デフォルト: StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: バックアップおよび復元手順中に生成されるファイルなど、一時ファイルを格納するディレクトリ。これらの手順が完了すると、生成された一時ファイルは削除されます。
- 導入バージョン: -

##### `transform_type_prefer_string_for_varchar`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: マテリアライズドビュー作成およびCTAS操作で固定長varchar列に文字列型を優先するかどうか。
- 導入バージョン: v4.0.0

<EditionSpecificFEItem />
