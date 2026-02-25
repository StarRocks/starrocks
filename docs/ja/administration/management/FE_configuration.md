---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 設定

<FEConfigMethod />

## FE 設定項目の表示

FEが起動した後、MySQLクライアントでADMIN SHOW FRONTEND CONFIGコマンドを実行して、パラメータ設定を確認できます。特定のパラメータの設定をクエリしたい場合は、以下のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細については、[`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)を参照してください。

:::note
クラスター管理関連のコマンドを実行するには、管理者権限が必要です。
:::

## FE パラメータの設定

### FE動的パラメータの設定

FE動的パラメータの設定は、[`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md)を使用して構成または変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE静的パラメータを設定する

<StaticFEConfigNote />

## FEパラメータを理解する

### ログ

##### `audit_log_delete_age`

- Default: 30d
- Type: String
- Unit: -
- Is mutable: No
- Description: 監査ログファイルの保持期間。デフォルト値 `30d` は、各監査ログファイルが30日間保持されることを指定します。StarRocks は各監査ログファイルをチェックし、30日前に生成されたファイルを削除します。
- Introduced in: -

##### `audit_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: 監査ログファイルを保存するディレクトリ。
- Introduced in: -

##### `audit_log_enable_compress`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: No
- Description: true の場合、生成された Log4j2 設定は、ローテーションされた監査ログファイル名 (fe.audit.log.*) に ".gz" 接尾辞を追加し、Log4j2 がロールオーバー時に圧縮された (.gz) アーカイブ監査ログファイルを生成するようにします。この設定は、FE 起動時に Log4jConfig.initLogging で読み取られ、監査ログの RollingFile アペンダーに適用されます。これは、アクティブな監査ログではなく、ローテーション/アーカイブされたファイルのみに影響します。値は起動時に初期化されるため、変更を有効にするには FE の再起動が必要です。監査ログのローテーション設定 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) と併用してください。
- Introduced in: 3.2.12

##### `audit_log_json_format`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: true の場合、FE 監査イベントは、デフォルトのパイプ区切り "key=value" 文字列ではなく、構造化された JSON (Jackson ObjectMapper が注釈付き AuditEvent フィールドの Map をシリアル化) として出力されます。この設定は、AuditLogBuilder が処理するすべての組み込み監査シンクに影響します。接続監査、クエリ監査、ビッグクエリ監査 (イベントが条件を満たす場合、ビッグクエリしきい値フィールドが JSON に追加されます)、およびスロー監査出力です。ビッグクエリしきい値と "features" フィールドに注釈が付けられたフィールドは特別に扱われます (通常の監査エントリから除外され、該当する場合にビッグクエリまたは機能ログに含まれます)。ログコレクターまたは SIEMs がログを機械で解析できるようにするには、これを有効にしてください。ログ形式が変更され、従来のパイプ区切り形式を期待する既存のパーサーの更新が必要になる場合があることに注意してください。
- Introduced in: 3.2.7

##### `audit_log_modules`

- Default: `slow_query`, query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks が監査ログエントリを生成するモジュール。デフォルトでは、StarRocks は `slow_query` モジュールと `query` モジュールに対して監査ログを生成します。`connection` モジュールは v3.0 からサポートされています。モジュール名をコンマ (,) とスペースで区切ってください。
- Introduced in: -

##### `audit_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks が監査ログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、監査ログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、監査ログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `audit_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: `audit_log_roll_interval` パラメータで指定された各保持期間内に保持できる監査ログファイルの最大数。
- Introduced in: -

##### `bdbje_log_level`

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks で Berkeley DB Java Edition (BDB JE) が使用するロギングレベルを制御します。BDB 環境の初期化中に BDBEnvironment.initConfigs() はこの値を `com.sleepycat.je` パッケージの Java ロガーと BDB JE 環境ファイルロギングレベル (`EnvironmentConfig.FILE_LOGGING_LEVEL`) に適用します。SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF などの標準的な java.util.logging.Level 名を受け入れます。ALL に設定すると、すべてのログメッセージが有効になります。詳細度を上げるとログ量が増加し、ディスク I/O とパフォーマンスに影響を与える可能性があります。この値は BDB 環境が初期化されるときに読み取られるため、環境の (再) 初期化後にのみ有効になります。
- Introduced in: v3.2.0

##### `big_query_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ビッグクエリログファイル (`fe.big_query.log.*`) が自動削除されるまでの保持期間を制御します。この値は Log4j の削除ポリシーに IfLastModified age として渡されます。最終変更時刻がこの値よりも古いローテーションされたビッグクエリログは削除されます。`d` (日)、`h` (時間)、`m` (分)、`s` (秒) のサフィックスをサポートします。例: `7d` (7日間)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。この項目は `big_query_log_roll_interval` および `big_query_log_roll_num` と連携して、どのファイルを保持またはパージするかを決定します。
- Introduced in: v3.2.0

##### `big_query_log_dir`

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: FE がビッグクエリダンプログ (`fe.big_query.log.*`) を書き込むディレクトリ。Log4j 設定はこのパスを使用して `fe.big_query.log` とそのローテーションされたファイル用の RollingFile アペンダーを作成します。ローテーションと保持は、`big_query_log_roll_interval` (時間ベースのサフィックス)、`log_roll_size_mb` (サイズトリガー)、`big_query_log_roll_num` (最大ファイル数)、および `big_query_log_delete_age` (年齢ベースの削除) によって管理されます。ビッグクエリレコードは、`big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold`、または `big_query_log_scan_bytes_threshold` などのユーザー定義のしきい値を超えるクエリに対してログに記録されます。`big_query_log_modules` を使用して、どのモジュールがこのファイルにログを記録するかを制御します。
- Introduced in: v3.2.0

##### `big_query_log_modules`

- Default: `{"query"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: モジュールごとのビッグクエリロギングを有効にするモジュール名サフィックスのリスト。一般的な値は論理コンポーネント名です。たとえば、デフォルトの `query` は `big_query.query` を生成します。
- Introduced in: v3.2.0

##### `big_query_log_roll_interval`

- Default: `"DAY"`
- Type: String
- Unit: -
- Is mutable: No
- Description: `big_query` ログアペンダーのローリングファイル名の日付コンポーネントを構築するために使用される時間間隔を指定します。有効な値 (大文字と小文字を区別しない) は `DAY` (デフォルト) と `HOUR` です。`DAY` は日次パターン (`"%d{yyyyMMdd}"`) を生成し、`HOUR` は時間パターン (`"%d{yyyyMMddHH}"`) を生成します。この値は、サイズベースのロールオーバー (`big_query_roll_maxsize`) およびインデックスベースのロールオーバー (`big_query_log_roll_num`) と組み合わされて RollingFile の filePattern を形成します。無効な値はログ設定の生成を失敗させ (IOException)、ログの初期化または再設定を妨げる可能性があります。`big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num`、および `big_query_log_delete_age` と併用してください。
- Introduced in: v3.2.0

##### `big_query_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `big_query_log_roll_interval` ごとに保持するローテーションされた FE ビッグクエリログファイルの最大数。この値は、`fe.big_query.log` の RollingFile アペンダーの DefaultRolloverStrategy `max` 属性にバインドされます。ログがロール (時間または `log_roll_size_mb` によって) されると、StarRocks は最大 `big_query_log_roll_num` 個のインデックス付きファイル (filePattern は時間サフィックスとインデックスを使用) を保持します。この数よりも古いファイルはロールオーバーによって削除される可能性があり、`big_query_log_delete_age` は最終変更時刻によってファイルをさらに削除できます。
- Introduced in: v3.2.0

##### `dump_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: ダンプログファイルの保持期間。デフォルト値 `7d` は、各ダンプログファイルが7日間保持されることを指定します。StarRocks は各ダンプログファイルをチェックし、7日前に生成されたファイルを削除します。
- Introduced in: -

##### `dump_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: ダンプログファイルを保存するディレクトリ。
- Introduced in: -

##### `dump_log_modules`

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks がダンプログエントリを生成するモジュール。デフォルトでは、StarRocks はクエリモジュールに対してダンプログを生成します。モジュール名をコンマ (,) とスペースで区切ってください。
- Introduced in: -

##### `dump_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks がダンプログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、ダンプログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、ダンプログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `dump_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `dump_log_roll_interval` パラメータで指定された各保持期間内に保持できるダンプログファイルの最大数。
- Introduced in: -

##### `edit_log_write_slow_log_threshold_ms`

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: JournalWriter が遅い編集ログバッチ書き込みを検出してログに記録するために使用するしきい値 (ミリ秒単位)。バッチコミット後、バッチ期間がこの値を超えると、JournalWriter はバッチサイズ、期間、および現在のジャーナルキューサイズを含む WARN を出力します (約2秒に1回にレート制限されます)。この設定は、FE リーダーでの潜在的な IO またはレプリケーションの遅延に関するロギング/アラートのみを制御します。コミットまたはロールの動作は変更しません (`edit_log_roll_num` およびコミット関連の設定を参照)。このしきい値に関係なく、メトリックの更新は引き続き行われます。
- Introduced in: v3.2.3

##### `enable_audit_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、FE 監査サブシステムは、ConnectProcessor によって処理される FE 監査ログ (`fe.audit.log`) にステートメントの SQL テキストを記録します。保存されるステートメントは他の制御を尊重します。暗号化されたステートメントは編集され (`AuditEncryptionChecker`)、`enable_sql_desensitize_in_log` が設定されている場合、機密性の高い資格情報は編集または非機密化され、ダイジェスト記録は `enable_sql_digest` によって制御されます。`false` に設定されている場合、ConnectProcessor は監査イベントのステートメントテキストを "?" に置き換えます。他の監査フィールド (ユーザー、ホスト、期間、ステータス、`qe_slow_log_ms` を介したスロークエリ検出、およびメトリック) は引き続き記録されます。SQL 監査を有効にすると、フォレンジックおよびトラブルシューティングの可視性が向上しますが、機密性の高い SQL コンテンツが公開され、ログ量と I/O が増加する可能性があります。無効にすると、監査ログでの完全なステートメントの可視性が失われる代わりにプライバシーが向上します。
- Introduced in: -

##### `enable_profile_log`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: プロファイルロギングを有効にするかどうか。この機能が有効になっている場合、FE はクエリごとのプロファイルログ (ProfileManager によって生成されるシリアル化された `queryDetail` JSON) をプロファイルログシンクに書き込みます。このロギングは、`enable_collect_query_detail_info` も有効になっている場合にのみ実行されます。`enable_profile_log_compress` が有効になっている場合、JSON はロギング前に gzip 圧縮される場合があります。プロファイルログファイルは、`profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` によって管理され、`profile_log_delete_age` ( `7d`、`10h`、`60m`、`120s` などの形式をサポート) に従ってローテーション/削除されます。この機能を無効にすると、プロファイルログの書き込みが停止します (ディスク I/O、圧縮 CPU、ストレージ使用量が削減されます)。
- Introduced in: v3.2.5

##### `enable_qe_slow_log`

- Default: true
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: 有効にすると、FE 組み込み監査プラグイン (AuditLogBuilder) は、測定された実行時間 ("Time" フィールド) が `qe_slow_log_ms` で設定されたしきい値を超えるクエリイベントをスロークエリ監査ログ (AuditLog.getSlowAudit) に書き込みます。無効にすると、これらのスロークエリエントリは抑制されます (通常のクエリおよび接続監査ログは影響を受けません)。スロー監査エントリは、グローバルな `audit_log_json_format` 設定 (JSON vs. プレーン文字列) に従います。このフラグを使用して、通常の監査ロギングとは独立してスロークエリ監査量を制御します。`qe_slow_log_ms` が低い場合や、ワークロードが多くの長時間実行クエリを生成する場合に、これをオフにするとログ I/O を削減できます。
- Introduced in: 3.2.11

##### `enable_sql_desensitize_in_log`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムは機密性の高い SQL コンテンツをログおよびクエリ詳細レコードに書き込む前に置き換えたり非表示にしたりします。この設定を尊重するコードパスには、ConnectProcessor.formatStmt (監査ログ)、StmtExecutor.addRunningQueryDetail (クエリ詳細)、および SimpleExecutor.formatSQL (内部エグゼキュータログ) が含まれます。この機能が有効になっている場合、無効な SQL は固定の非機密化メッセージに置き換えられる可能性があり、資格情報 (ユーザー/パスワード) は非表示になり、SQL フォーマッターはサニタイズされた表現を生成する必要があります (ダイジェスト形式の出力を有効にすることもできます)。これにより、監査/内部ログでの機密リテラルや資格情報の漏洩が減少しますが、ログやクエリ詳細に元の完全な SQL テキストが含まれなくなることも意味します (これはリプレイやデバッグに影響を与える可能性があります)。
- Introduced in: -

##### `internal_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 内部ログファイル (`internal_log_dir` に書き込まれる) の保持期間を指定します。値は期間文字列です。サポートされるサフィックス: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7日間)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。この項目は、Log4j 設定に RollingFile Delete ポリシーで使用される `<IfLastModified age="..."/>` 述語として代入されます。最終変更時刻がこの期間よりも古いファイルは、ログロールオーバー中に削除されます。この値を増やすとディスクスペースが早く解放され、減らすと内部マテリアライズドビューまたは統計ログをより長く保持できます。
- Introduced in: v3.2.4

##### `internal_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ロギングサブシステムが内部ログ (`fe.internal.log`) を保存するために使用するディレクトリ。この設定は Log4j 設定に代入され、InternalFile アペンダーが内部/マテリアライズドビュー/統計ログを書き込む場所、および `internal.<module>` の下のモジュールごとのロガーがファイルを配置する場所を決定します。ディレクトリが存在し、書き込み可能であり、十分なディスクスペースがあることを確認してください。このディレクトリ内のファイルのログローテーションと保持は、`log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age`、および `internal_log_roll_interval` によって制御されます。`sys_log_to_console` が有効になっている場合、内部ログはこのディレクトリではなくコンソールに書き込まれる場合があります。
- Introduced in: v3.2.4

##### `internal_log_json_format`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、内部統計/監査エントリはコンパクトな JSON オブジェクトとして統計監査ロガーに書き込まれます。JSON には、キー "executeType" (InternalType: QUERY または DML)、"queryId"、"sql"、および "time" (経過ミリ秒) が含まれます。`false` に設定されている場合、同じ情報は単一のフォーマットされたテキスト行 ("statistic execute: ... | QueryId: [...] | SQL: ...") としてログに記録されます。JSON を有効にすると、機械解析とログプロセッサとの統合が向上しますが、生の SQL テキストがログに含まれるため、機密情報が公開され、ログサイズが増加する可能性があります。
- Introduced in: -

##### `internal_log_modules`

- Default: `{"base", "statistic"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: 専用の内部ロギングを受け取るモジュール識別子のリスト。各エントリ X について、Log4j はレベル INFO および additivity="false" の `internal.<X>` という名前のロガーを作成します。これらのロガーは、内部アペンダー (`fe.internal.log` に書き込まれる) または `sys_log_to_console` が有効になっている場合はコンソールにルーティングされます。必要に応じて短い名前またはパッケージフラグメントを使用してください。正確なロガー名は `internal.` + 設定された文字列になります。内部ログファイルのローテーションと保持は、`internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval`、および `log_roll_size_mb` に従います。モジュールを追加すると、そのランタイムメッセージが内部ロガーストリームに分離され、デバッグと監査が容易になります。
- Introduced in: v3.2.4

##### `internal_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 内部ログアペンダーの時間ベースのロール間隔を制御します。受け入れられる値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は時間ごとのファイルパターン (`"%d{yyyyMMddHH}"`) を生成し、`DAY` は日ごとのファイルパターン (`"%d{yyyyMMdd}"`) を生成します。これらは RollingFile TimeBasedTriggeringPolicy によってローテーションされた `fe.internal.log` ファイルの名前付けに使用されます。無効な値は初期化を失敗させます (アクティブな Log4j 設定を構築するときに IOException がスローされます)。ロール動作は、`internal_log_dir`、`internal_roll_maxsize`、`internal_log_roll_num`、および `internal_log_delete_age` などの関連設定にも依存します。
- Introduced in: v3.2.4

##### `internal_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: 内部アペンダー (`fe.internal.log`) のために保持するローテーションされた内部 FE ログファイルの最大数。この値は Log4j DefaultRolloverStrategy の `max` 属性として使用されます。ロールオーバーが発生すると、StarRocks は最大 `internal_log_roll_num` 個のアーカイブファイルを保持し、古いファイルを削除します (`internal_log_delete_age` によっても管理されます)。値が低いとディスク使用量が削減されますが、ログ履歴が短くなります。値が高いと、より多くの履歴内部ログが保持されます。この項目は `internal_log_dir`、`internal_log_roll_interval`、および `internal_roll_maxsize` と連携して機能します。
- Introduced in: v3.2.4

##### `log_cleaner_audit_log_min_retention_days`

- Default: 3
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: 監査ログファイルの最小保持日数。これより新しい監査ログファイルは、ディスク使用量が高くても削除されません。これにより、監査ログがコンプライアンスおよびトラブルシューティングの目的で保持されることが保証されます。
- Introduced in: -

##### `log_cleaner_check_interval_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: ディスク使用量をチェックし、ログをクリーンアップする間隔 (秒単位)。クリーナーは定期的に各ログディレクトリのディスク使用量をチェックし、必要に応じてクリーンアップをトリガーします。デフォルトは300秒 (5分) です。
- Introduced in: -

##### `log_cleaner_disk_usage_target`

- Default: 60
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: ログクリーンアップ後の目標ディスク使用量 (パーセンテージ)。ログクリーンアップは、ディスク使用量がこのしきい値を下回るまで続行されます。クリーナーは、目標に達するまで最も古いログファイルを1つずつ削除します。
- Introduced in: -

##### `log_cleaner_disk_usage_threshold`

- Default: 80
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: ログクリーンアップをトリガーするディスク使用量しきい値 (パーセンテージ)。ディスク使用量がこのしきい値を超えると、ログクリーンアップが開始されます。クリーナーは、設定された各ログディレクトリを個別にチェックし、このしきい値を超えるディレクトリを処理します。
- Introduced in: -

##### `log_cleaner_disk_util_based_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ディスク使用量に基づく自動ログクリーンアップを有効にします。有効にすると、ディスク使用量がしきい値を超えたときにログがクリーンアップされます。ログクリーナーは FE ノードでバックグラウンドデーモンとして実行され、ログファイルの蓄積によるディスクスペースの枯渇を防ぐのに役立ちます。
- Introduced in: -

##### `log_plan_cancelled_by_crash_be`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: BE クラッシュまたは RPC 例外によりクエリがキャンセルされたときに、クエリ実行計画のロギングを有効にするかどうか。この機能が有効になっている場合、StarRocks は、BE クラッシュまたは `RpcException` によりクエリがキャンセルされたときに、クエリ実行計画 (`TExplainLevel.COSTS` レベル) を WARN エントリとしてログに記録します。ログエントリには QueryId、SQL、および COSTS プランが含まれます。ExecuteExceptionHandler パスでは、例外スタックトレースもログに記録されます。`enable_collect_query_detail_info` が有効になっている場合 (プランはクエリ詳細に保存される)、ロギングはスキップされます。コードパスでは、クエリ詳細が null であることを確認することでチェックが実行されます。ExecuteExceptionHandler では、プランは最初の再試行 (`retryTime == 0`) でのみログに記録されることに注意してください。これを有効にすると、完全な COSTS プランが大きくなる可能性があるため、ログ量が増加する可能性があります。
- Introduced in: v3.2.0

##### `log_register_and_unregister_query_id`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE が QeProcessorImpl からクエリ登録および登録解除メッセージ (例: `"register query id = {}"` および `"deregister query id = {}"`) をログに記録することを許可するかどうか。ログは、クエリが null ではない ConnectContext を持ち、コマンドが `COM_STMT_EXECUTE` ではないか、セッション変数 `isAuditExecuteStmt()` が true の場合にのみ出力されます。これらのメッセージはすべてのクエリライフサイクルイベントに対して書き込まれるため、この機能を有効にすると、高並行環境で高いログ量が発生し、スループットのボトルネックになる可能性があります。デバッグまたは監査のために有効にし、ロギングオーバーヘッドを削減し、パフォーマンスを向上させるために無効にしてください。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `log_roll_size_mb`

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: システムログファイルまたは監査ログファイルの最大サイズ。
- Introduced in: -

##### `proc_profile_file_retained_days`

- Default: 1
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: `sys_log_dir/proc_profile` の下に生成されたプロセスプロファイリングファイル (CPU およびメモリ) を保持する日数。ProcProfileCollector は、現在時刻から `proc_profile_file_retained_days` 日を引いたカットオフ (yyyyMMdd-HHmmss 形式) を計算し、タイムスタンプ部分がそのカットオフよりも辞書順で早いプロファイルファイル (つまり、`timePart.compareTo(timeToDelete) < 0`) を削除します。ファイルの削除は、`proc_profile_file_retained_size_bytes` によって制御されるサイズベースのカットオフも尊重します。プロファイルファイルは `cpu-profile-` および `mem-profile-` のプレフィックスを使用し、収集後に圧縮されます。
- Introduced in: v3.2.12

##### `proc_profile_file_retained_size_bytes`

- Default: 2L * 1024 * 1024 * 1024 (2147483648)
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: プロファイルディレクトリの下に保持する収集された CPU およびメモリプロファイルファイル (`cpu-profile-` および `mem-profile-` のプレフィックスを持つファイル) の最大合計バイト数。有効なプロファイルファイルの合計が `proc_profile_file_retained_size_bytes` を超えると、コレクターは残りの合計サイズが `proc_profile_file_retained_size_bytes` 以下になるまで最も古いプロファイルファイルを削除します。`proc_profile_file_retained_days` よりも古いファイルもサイズに関係なく削除されます。この設定はプロファイルアーカイブのディスク使用量を制御し、`proc_profile_file_retained_days` と連携して削除順序と保持を決定します。
- Introduced in: v3.2.12

##### `profile_log_delete_age`

- Default: 1d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE プロファイルログファイルが削除の対象となるまでの保持期間を制御します。この値は Log4j の `<IfLastModified age="..."/>` ポリシー ( `Log4jConfig` 経由) に挿入され、`profile_log_roll_interval` や `profile_log_roll_num` などのローテーション設定と併せて適用されます。サポートされるサフィックス: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7日間)、`10h` (10時間)、`60m` (60分)、`120s` (120秒)。
- Introduced in: v3.2.5

##### `profile_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE プロファイルログが書き込まれるディレクトリ。Log4jConfig はこの値を使用してプロファイル関連のアペンダーを配置します (このディレクトリの下に `fe.profile.log` や `fe.features.log` などのファイルを作成します)。これらのファイルのローテーションと保持は、`profile_log_roll_size_mb`、`profile_log_roll_num`、および `profile_log_delete_age` によって管理されます。タイムスタンプサフィックス形式は `profile_log_roll_interval` (DAY または HOUR をサポート) によって制御されます。デフォルトのディレクトリは `STARROCKS_HOME_DIR` の下にあるため、FE プロセスがこのディレクトリに対する書き込みおよびローテーション/削除権限を持っていることを確認してください。
- Introduced in: v3.2.5

##### `profile_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: プロファイルログファイル名の日付部分を生成するために使用される時間粒度を制御します。有効な値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は `"%d{yyyyMMddHH}"` (時間ごとのタイムバケット) のパターンを生成し、`DAY` は `"%d{yyyyMMdd}"` (日ごとのタイムバケット) を生成します。この値は Log4j 設定で `profile_file_pattern` を計算するときに使用され、ロールオーバーファイル名の時間ベースのコンポーネントのみに影響します。サイズベースのロールオーバーは `profile_log_roll_size_mb` によって、保持は `profile_log_roll_num` / `profile_log_delete_age` によって引き続き制御されます。無効な値はロギング初期化中に IOException を引き起こします (エラーメッセージ: `"profile_log_roll_interval config error: <value>"`)。大量のプロファイリングを行う場合は、ファイルごとのサイズを時間ごとに制限するために `HOUR` を選択し、日ごとの集計には `DAY` を選択してください。
- Introduced in: v3.2.5

##### `profile_log_roll_num`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: プロファイルロガーの Log4j の DefaultRolloverStrategy によって保持されるローテーションされたプロファイルログファイルの最大数を指定します。この値は、ロギング XML に `${profile_log_roll_num}` として挿入されます (例: `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`)。ローテーションは `profile_log_roll_size_mb` または `profile_log_roll_interval` によってトリガーされます。ローテーションが発生すると、Log4j は最大でこれらのインデックス付きファイルを保持し、古いインデックスファイルは削除の対象となります。ディスク上の実際の保持は、`profile_log_delete_age` および `profile_log_dir` の場所にも影響されます。値が低いとディスク使用量が削減されますが、保持される履歴が制限されます。値が高いと、より多くの履歴プロファイルログが保持されます。
- Introduced in: v3.2.5

##### `profile_log_roll_size_mb`

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: FE プロファイルログファイルのサイズベースのロールオーバーをトリガーするサイズしきい値 (メガバイト単位) を設定します。この値は、`ProfileFile` アペンダーの Log4j RollingFile SizeBasedTriggeringPolicy によって使用されます。プロファイルログが `profile_log_roll_size_mb` を超えると、ローテーションされます。`profile_log_roll_interval` に達すると、時間によってもローテーションが発生する可能性があります。どちらの条件もロールオーバーをトリガーします。`profile_log_roll_num` および `profile_log_delete_age` と組み合わせて、この項目は保持される履歴プロファイルファイルの数と古いファイルが削除される時期を制御します。ローテーションされたファイルの圧縮は `enable_profile_log_compress` によって制御されます。
- Introduced in: v3.2.5

##### `qe_slow_log_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: クエリがスロークエリであるかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超えると、**fe.audit.log** にスロークエリとして記録されます。
- Introduced in: -

##### `slow_lock_log_every_ms`

- Default: 3000L
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: 同じ SlowLockLogStats インスタンスに対して別の「スローロック」警告を出力するまでに待機する最小間隔 (ミリ秒単位)。LockUtils は、ロック待機が `slow_lock_threshold_ms` を超えた後にこの値をチェックし、最後にログに記録されたスローロックイベントから `slow_lock_log_every_ms` ミリ秒が経過するまで追加の警告を抑制します。長時間の競合中にログ量を減らすには大きな値を、より頻繁な診断を得るには小さな値を使用してください。変更は、その後のチェックに対して実行時に有効になります。
- Introduced in: v3.2.0

##### `slow_lock_print_stack`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: LockManager が `logSlowLockTrace` によって出力されるスローロック警告の JSON ペイロードに、所有スレッドの完全なスタックトレースを含めることを許可するかどうか ("stack" 配列は `LogUtil.getStackTraceToJsonArray` を使用して `start=0` および `max=Short.MAX_VALUE` で設定されます)。この設定は、ロック取得が `slow_lock_threshold_ms` で設定されたしきい値を超えたときに表示されるロック所有者の追加のスタック情報のみを制御します。この機能を有効にすると、ロックを保持している正確なスレッドスタックが提供されるため、デバッグに役立ちます。無効にすると、高並行環境でスタックトレースをキャプチャしてシリアル化することによって発生するログ量と CPU/メモリオーバーヘッドが削減されます。
- Introduced in: v3.3.16, v3.4.5, v3.5.1

##### `slow_lock_threshold_ms`

- Default: 3000L
- Type: long
- Unit: Milliseconds
- Is mutable: Yes
- Description: ロック操作または保持されているロックを「スロー」として分類するために使用されるしきい値 (ミリ秒単位)。ロックの経過待機時間または保持時間がこの値を超えると、StarRocks は (コンテキストに応じて) 診断ログを出力し、スタックトレースまたは待機者/所有者情報を含め、LockManager ではこの遅延後にデッドロック検出を開始します。これは LockUtils (スローロックロギング)、QueryableReentrantReadWriteLock (スローリーダーのフィルタリング)、LockManager (デッドロック検出遅延とスローロックトレース)、LockChecker (定期的なスローロック検出)、およびその他の呼び出し元 (例: DiskAndTabletLoadReBalancer ロギング) によって使用されます。値を下げると感度とロギング/診断オーバーヘッドが増加します。0 または負の値を設定すると、初期の待機ベースのデッドロック検出遅延動作が無効になります。`slow_lock_log_every_ms`、`slow_lock_print_stack`、および `slow_lock_stack_trace_reserve_levels` と組み合わせて調整してください。
- Introduced in: 3.2.0

##### `sys_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: システムログファイルの保持期間。デフォルト値 `7d` は、各システムログファイルが7日間保持されることを指定します。StarRocks は各システムログファイルをチェックし、7日前に生成されたファイルを削除します。
- Introduced in: -

##### `sys_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: システムログファイルを保存するディレクトリ。
- Introduced in: -

##### `sys_log_enable_compress`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムはローテーションされたシステムログファイル名に ".gz" 接尾辞を追加し、Log4j が gzip 圧縮されたローテーションされた FE システムログ (例: fe.log.*) を生成するようにします。この値は Log4j 設定生成中 (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) に読み取られ、RollingFile の filePattern で使用される `sys_file_postfix` プロパティを制御します。この機能を有効にすると、保持されるログのディスク使用量が削減されますが、ロールオーバー中の CPU と I/O が増加し、ログファイル名が変更されるため、ログを読み取るツールやスクリプトは .gz ファイルを処理できる必要があります。監査ログは圧縮に別の設定 (`audit_log_enable_compress`) を使用することに注意してください。
- Introduced in: v3.2.12

##### `sys_log_format`

- Default: "plaintext"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ログに使用される Log4j レイアウトを選択します。有効な値: `"plaintext"` (デフォルト) と `"json"`。値は大文字と小文字を区別しません。`"plaintext"` は、人間が読めるタイムスタンプ、レベル、スレッド、クラス.メソッド:行、および WARN/ERROR のスタックトレースを含む PatternLayout を設定します。`"json"` は JsonTemplateLayout を設定し、ログアグリゲーター (ELK、Splunk) に適した構造化 JSON イベント (UTC タイムスタンプ、レベル、スレッド ID/名前、ソースファイル/メソッド/行、メッセージ、例外スタックトレース) を出力します。JSON 出力は、最大文字列長について `sys_log_json_max_string_length` および `sys_log_json_profile_max_string_length` に従います。
- Introduced in: v3.2.10

##### `sys_log_json_max_string_length`

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: JSON 形式のシステムログに使用される JsonTemplateLayout の "maxStringLength" 値を設定します。`sys_log_format` が `"json"` に設定されている場合、文字列値フィールド (例: "message" および文字列化された例外スタックトレース) は、その長さがこの制限を超えると切り捨てられます。この値は、生成された Log4j XML の `Log4jConfig.generateActiveLog4jXmlConfig()` に挿入され、デフォルト、警告、監査、ダンプ、およびビッグクエリのレイアウトに適用されます。プロファイルレイアウトは別の設定 (`sys_log_json_profile_max_string_length`) を使用します。この値を下げるとログサイズは削減されますが、有用な情報が切り捨てられる可能性があります。
- Introduced in: 3.2.11

##### `sys_log_json_profile_max_string_length`

- Default: 104857600 (100 MB)
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: `sys_log_format` が "json" の場合、プロファイル (および関連機能) ログアペンダーの JsonTemplateLayout の maxStringLength を設定します。JSON 形式のプロファイルログの文字列フィールド値は、このバイト長に切り捨てられます。非文字列フィールドは影響を受けません。この項目は Log4jConfig の `JsonTemplateLayout maxStringLength` に適用され、`plaintext` ロギングが使用されている場合は無視されます。必要な完全なメッセージに対して十分な大きさの値を保持しますが、値が大きいほどログサイズと I/O が増加することに注意してください。
- Introduced in: v3.2.11

##### `sys_log_level`

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: システムログエントリが分類される重大度レベル。有効な値: `INFO`、`WARN`、`ERROR`、および `FATAL`。
- Introduced in: -

##### `sys_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks がシステムログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメータが `DAY` に設定されている場合、システムログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメータが `HOUR` に設定されている場合、システムログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `sys_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `sys_log_roll_interval` パラメータで指定された各保持期間内に保持できるシステムログファイルの最大数。
- Introduced in: -

##### `sys_log_to_console`

- Default: false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムは Log4j を設定して、すべてのログをファイルベースのアペンダーではなくコンソール (ConsoleErr アペンダー) に送信します。この値は、アクティブな Log4j XML 設定を生成するときに読み取られ (ルートロガーとモジュールごとのロガーアペンダーの選択に影響します)、プロセス起動時に `SYS_LOG_TO_CONSOLE` 環境変数から取得されます。実行時に変更しても効果はありません。この設定は、stdout/stderr ログ収集がログファイルの書き込みよりも優先されるコンテナ化された環境や CI 環境で一般的に使用されます。
- Introduced in: v3.2.0

##### `sys_log_verbose_modules`

- Default: Empty string
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks がシステムログを生成するモジュール。このパラメータが `org.apache.starrocks.catalog` に設定されている場合、StarRocks はカタログモジュールに対してのみシステムログを生成します。モジュール名をコンマ (,) とスペースで区切ってください。
- Introduced in: -

##### `sys_log_warn_modules`

- Default: {}
- Type: String[]
- Unit: -
- Is mutable: No
- Description: システムが起動時に WARN レベルのロガーとして設定し、警告アペンダー (SysWF) — `fe.warn.log` ファイルにルーティングするロガー名またはパッケージプレフィックスのリスト。エントリは生成された Log4j 設定 (org.apache.kafka、org.apache.hudi、org.apache.hadoop.io.compress などの組み込み警告モジュールと共に) に挿入され、`<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` のようなロガー要素を生成します。完全修飾パッケージおよびクラスプレフィックス (例: "com.example.lib") は、通常のログへのノイズの多い INFO/DEBUG 出力を抑制し、警告を個別にキャプチャできるようにするために推奨されます。
- Introduced in: v3.2.13

### サーバー

##### `brpc_idle_wait_max_time`

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: いいえ
- Description: bRPCクライアントがアイドル状態で待機する最大時間。
- Introduced in: -

##### `brpc_inner_reuse_pool`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: いいえ
- Description: 基盤となるBRPCクライアントが接続/チャネルに内部共有再利用プールを使用するかどうかを制御します。StarRocksは、RpcClientOptionsを構築する際にBrpcProxyで`brpc_inner_reuse_pool`を読み取ります（`rpcOptions.setInnerResuePool(...)`経由）。有効（true）の場合、RPCクライアントは内部プールを再利用して、呼び出しごとの接続作成を減らし、FE-to-BE / LakeService RPCの接続チャーン、メモリ、ファイルディスクリプタの使用量を削減します。無効（false）の場合、クライアントはより分離されたプールを作成する可能性があります（より高いリソース使用量を犠牲にして並行性分離を増加させます）。この値を変更するには、プロセスを再起動して適用する必要があります。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_min_evictable_idle_time_ms`

- Default: 120000
- Type: Int
- Unit: ミリ秒
- Is mutable: いいえ
- Description: アイドル状態のBRPC接続が接続プールに留まり、削除の対象となるまでのミリ秒単位の時間。`BrpcProxy`が使用するRpcClientOptionsに適用されます（RpcClientOptions.setMinEvictableIdleTime経由）。この値を上げると、アイドル接続を長く保持できます（再接続のチャーンを減らします）。この値を下げると、未使用のソケットをより速く解放できます（リソース使用量を減らします）。`brpc_connection_pool_size`および`brpc_idle_wait_max_time`と合わせて調整し、接続の再利用、プールの成長、および削除動作のバランスを取ります。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_reuse_addr`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: trueの場合、StarRocksはbrpc RpcClientによって作成されたクライアントソケットのローカルアドレス再利用を許可するようにソケットオプションを設定します（RpcClientOptions.setReuseAddress経由）。これを有効にすると、バインドの失敗が減り、ソケットが閉じられた後のローカルポートの再バインドが速くなるため、高レートの接続チャーンや迅速な再起動に役立ちます。falseの場合、アドレス/ポートの再利用は無効になり、意図しないポート共有の可能性を減らすことができますが、一時的なバインドエラーが増加する可能性があります。このオプションは、クライアントソケットがどれだけ迅速に再バインドおよび再利用できるかに影響するため、`brpc_connection_pool_size`および`brpc_short_connection`によって構成される接続動作と相互作用します。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `cluster_name`

- Default: StarRocks クラスター
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: FEが属するStarRocksクラスターの名前。クラスター名はウェブページの`Title`に表示されます。
- Introduced in: -

##### `dns_cache_ttl_seconds`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: いいえ
- Description: 成功したDNSルックアップのDNSキャッシュTTL（Time-To-Live）を秒単位で指定します。これは、JVMが成功したDNSルックアップをキャッシュする期間を制御するJavaセキュリティプロパティ`networkaddress.cache.ttl`を設定します。システムが常に情報をキャッシュするようにするにはこの項目を`-1`に設定し、キャッシュを無効にするには`0`に設定します。これは、Kubernetesデプロイメントや動的DNSが使用されている場合など、IPアドレスが頻繁に変更される環境で特に役立ちます。
- Introduced in: v3.5.11, v4.0.4

##### `enable_http_async_handler`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: システムがHTTPリクエストを非同期で処理することを許可するかどうか。この機能が有効になっている場合、Nettyワーカー スレッドによって受信されたHTTPリクエストは、HTTPサーバーのブロックを回避するために、サービスロジック処理用の別のスレッドプールに送信されます。無効になっている場合、Nettyワーカーがサービスロジックを処理します。
- Introduced in: 4.0.0

##### `enable_http_validate_headers`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: NettyのHttpServerCodecが厳密なHTTPヘッダー検証を実行するかどうかを制御します。この値は、`HttpServer`でHTTPパイプラインが初期化されるときにHttpServerCodecに渡されます（UseLocationsを参照）。新しいNettyバージョンではより厳密なヘッダー規則が適用されるため（https://github.com/netty/netty/pull/12760）、後方互換性のためにデフォルトはfalseです。RFC準拠のヘッダーチェックを強制するにはtrueに設定します。そうすると、レガシーなクライアントやプロキシからの不正な形式または非準拠のリクエストが拒否される可能性があります。変更を有効にするにはHTTPサーバーの再起動が必要です。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_https`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: FEノードでHTTPサーバーと並行してHTTPSサーバーを有効にするかどうか。
- Introduced in: v4.0

##### `frontend_address`

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: FEノードのIPアドレス。
- Introduced in: -

##### `http_async_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 非同期HTTPリクエスト処理用のスレッドプールのサイズ。エイリアスは`max_http_sql_service_task_threads_num`です。
- Introduced in: 4.0.0

##### `http_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのHTTPサーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `http_max_chunk_size`

- Default: 8192
- Type: Int
- Unit: バイト
- Is mutable: いいえ
- Description: FE HTTPサーバーのNettyのHttpServerCodecによって処理される単一のHTTPチャンクの最大許容サイズ（バイト単位）を設定します。これはHttpServerCodecの3番目の引数として渡され、チャンク転送またはストリーミングリクエスト/レスポンス中のチャンクの長さを制限します。受信チャンクがこの値を超えると、Nettyはフレームが大きすぎるエラー（例：TooLongFrameException）を発生させ、リクエストが拒否される可能性があります。正当な大きなチャンクアップロードの場合はこれを増やし、メモリ負荷とDoS攻撃の表面積を減らすために小さく保ちます。この設定は、`http_max_initial_line_length`、`http_max_header_size`、および`enable_http_validate_headers`と併用されます。
- Introduced in: v3.2.0

##### `http_max_header_size`

- Default: 32768
- Type: Int
- Unit: バイト
- Is mutable: いいえ
- Description: Nettyの`HttpServerCodec`によって解析されるHTTPリクエストヘッダーブロックの最大許容サイズ（バイト単位）。StarRocksはこの値を`HttpServerCodec`に渡します（`Config.http_max_header_size`として）。受信リクエストのヘッダー（名前と値の組み合わせ）がこの制限を超えると、コーデックはリクエストを拒否し（デコーダー例外）、接続/リクエストは失敗します。クライアントが正当に非常に大きなヘッダー（大きなCookieまたは多数のカスタムヘッダー）を送信する場合にのみ増やしてください。値が大きいほど、接続ごとのメモリ使用量が増加します。`http_max_initial_line_length`および`http_max_chunk_size`と組み合わせて調整してください。変更にはFEの再起動が必要です。
- Introduced in: v3.2.0

##### `http_max_initial_line_length`

- Default: 4096
- Type: Int
- Unit: バイト
- Is mutable: いいえ
- Description: HttpServerで使用されるNetty `HttpServerCodec`が受け入れるHTTP初期リクエストライン（メソッド + リクエストターゲット + HTTPバージョン）の最大許容長（バイト単位）を設定します。この値はNettyのデコーダーに渡され、これより長い初期ラインを持つリクエストは拒否されます（TooLongFrameException）。非常に長いリクエストURIをサポートする必要がある場合にのみこれを増やしてください。値が大きいほどメモリ使用量が増加し、不正な形式のリクエスト/リクエスト乱用への露出が高まる可能性があります。`http_max_header_size`および`http_max_chunk_size`と組み合わせて調整してください。
- Introduced in: v3.2.0

##### `http_port`

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのHTTPサーバーがリッスンするポート。
- Introduced in: -

##### `http_web_page_display_hardware`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: trueの場合、HTTPインデックスページ（/index）には、oshiライブラリ（CPU、メモリ、プロセス、ディスク、ファイルシステム、ネットワークなど）を介して入力されたハードウェア情報セクションが含まれます。oshiはシステムユーティリティを呼び出したり、システムファイルを間接的に読み取ったりする可能性があり（たとえば、`getent passwd`などのコマンドを実行できます）、機密性の高いシステムデータが表面化する可能性があります。より厳格なセキュリティが必要な場合、またはホストでこれらの間接コマンドの実行を避けたい場合は、この設定をfalseに設定して、Web UIでのハードウェア詳細の収集と表示を無効にしてください。
- Introduced in: v3.2.0

##### `http_worker_threads_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: HTTPリクエストを処理するためのHTTPサーバーのワーカー スレッド数。負の値または0の場合、スレッド数はCPUコア数の2倍になります。
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### `https_port`

- Default: 8443
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのHTTPSサーバーがリッスンするポート。
- Introduced in: v4.0

##### `max_mysql_service_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのMySQLサーバーがタスクを処理するために実行できるスレッドの最大数。
- Introduced in: -

##### `max_task_runs_threads_num`

- Default: 512
- Type: Int
- Unit: スレッド
- Is mutable: いいえ
- Description: タスク実行エグゼキュータースレッドプール内の最大スレッド数を制御します。この値は、同時タスク実行の上限です。これを増やすと並列処理が向上しますが、CPU、メモリ、ネットワークの使用量も増加します。一方、これを減らすと、タスク実行のバックログとレイテンシの増加を引き起こす可能性があります。この値は、予想される同時スケジュール済みジョブと利用可能なシステムリソースに応じて調整してください。
- Introduced in: v3.2.0

##### `memory_tracker_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: FEメモリトラッカーサブシステムを有効にします。`memory_tracker_enable`が`true`に設定されている場合、`MemoryUsageTracker`は登録されたメタデータモジュールを定期的にスキャンし、インメモリの`MemoryUsageTracker.MEMORY_USAGE`マップを更新し、合計をログに記録し、`MetricRepo`がメトリクス出力でメモリ使用量とオブジェクト数ゲージを公開するようにします。サンプリング間隔を制御するには`memory_tracker_interval_seconds`を使用します。この機能を有効にすると、メモリ消費量の監視とデバッグに役立ちますが、CPUとI/Oのオーバーヘッド、および追加のメトリクスカーディナリティが発生します。
- Introduced in: v3.2.4

##### `memory_tracker_interval_seconds`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: はい
- Description: FE `MemoryUsageTracker`デーモンがFEプロセスおよび登録された`MemoryTrackable`モジュールのメモリ使用量をポーリングして記録する間隔（秒単位）。`memory_tracker_enable`が`true`に設定されている場合、トラッカーはこの周期で実行され、`MEMORY_USAGE`を更新し、集計されたJVMおよび追跡対象モジュールの使用量をログに記録します。
- Introduced in: v3.2.4

##### `mysql_nio_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのMySQLサーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `mysql_server_version`

- Default: 8.0.33
- Type: String
- Unit: -
- Is mutable: はい
- Description: クライアントに返されるMySQLサーバーのバージョン。このパラメータを変更すると、以下の状況でバージョン情報に影響します。
  1. `select version();`
  2. ハンドシェイクパケットのバージョン
  3. グローバル変数`version`の値（`show variables like 'version';`）
- Introduced in: -

##### `mysql_service_io_threads_num`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのMySQLサーバーがI/Oイベントを処理するために実行できるスレッドの最大数。
- Introduced in: -

##### `mysql_service_kill_after_disconnect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: MySQL TCP接続が閉じられたと検出された場合（読み取り時のEOF）、サーバーがセッションをどのように処理するかを制御します。`true`に設定されている場合、サーバーはその接続で実行中のクエリを直ちに終了し、即座にクリーンアップを実行します。`false`の場合、サーバーは切断時に実行中のクエリを終了せず、保留中のリクエストタスクがない場合にのみクリーンアップを実行し、クライアントが切断した後も長時間実行されるクエリを続行できるようにします。注：TCPキープアライブを示唆する短いコメントにもかかわらず、このパラメータは切断後の終了動作を具体的に管理しており、孤立したクエリを終了させるか（信頼性の低い/ロードバランスされたクライアントの背後で推奨）、完了させるかを希望するかどうかに応じて設定する必要があります。
- Introduced in: -

##### `mysql_service_nio_enable_keep_alive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: MySQL接続のTCP Keep-Aliveを有効にします。ロードバランサーの背後にある長時間アイドル状態の接続に役立ちます。
- Introduced in: -

##### `net_use_ipv6_when_priority_networks_empty`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: `priority_networks`が指定されていない場合にIPv6アドレスを優先的に使用するかどうかを制御するブール値。`true`は、ノードをホストするサーバーがIPv4とIPv6の両方のアドレスを持ち、`priority_networks`が指定されていない場合に、システムがIPv6アドレスを優先的に使用することを許可することを示します。
- Introduced in: v3.3.0

##### `priority_networks`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: 複数のIPアドレスを持つサーバーの選択戦略を宣言します。このパラメータで指定されたリストには、最大で1つのIPアドレスが一致する必要があることに注意してください。このパラメータの値は、CIDR表記でセミコロン（;）で区切られたエントリ（例：10.10.10.0/24）で構成されるリストです。このリストのエントリに一致するIPアドレスがない場合、サーバーの利用可能なIPアドレスがランダムに選択されます。v3.3.0以降、StarRocksはIPv6ベースのデプロイメントをサポートしています。サーバーがIPv4とIPv6の両方のアドレスを持ち、このパラメータが指定されていない場合、システムはデフォルトでIPv4アドレスを使用します。`net_use_ipv6_when_priority_networks_empty`を`true`に設定することで、この動作を変更できます。
- Introduced in: -

##### `proc_profile_cpu_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: この項目が`true`に設定されている場合、バックグラウンドの`ProcProfileCollector`は`AsyncProfiler`を使用してCPUプロファイルを収集し、`sys_log_dir/proc_profile`の下にHTMLレポートを書き込みます。各収集実行は、`proc_profile_collect_time_s`で構成された期間のCPUスタックを記録し、Javaスタック深度には`proc_profile_jstack_depth`を使用します。生成されたプロファイルは圧縮され、古いファイルは`proc_profile_file_retained_days`および`proc_profile_file_retained_size_bytes`に従って削除されます。`AsyncProfiler`にはネイティブライブラリ（`libasyncProfiler.so`）が必要です。`/tmp`でのnoexecの問題を回避するために、`one.profiler.extractPath`は`STARROCKS_HOME_DIR/bin`に設定されています。
- Introduced in: v3.2.12

##### `qe_max_connection`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: すべてのユーザーがFEノードに確立できる接続の最大数。v3.1.12およびv3.2.7以降、デフォルト値は`1024`から`4096`に変更されました。
- Introduced in: -

##### `query_port`

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのMySQLサーバーがリッスンするポート。
- Introduced in: -

##### `rpc_port`

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのThriftサーバーがリッスンするポート。
- Introduced in: -

##### `slow_lock_stack_trace_reserve_levels`

- Default: 15
- Type: Int
- Unit: -
- Is mutable: はい
- Description: StarRocksが遅いロックまたは保持されているロックのロックデバッグ情報をダンプするときに、いくつのスタックトレースフレームがキャプチャされ出力されるかを制御します。この値は、排他ロック所有者、現在のスレッド、および最も古い/共有リーダーのJSONを生成する際に、`QueryableReentrantReadWriteLock`によって`LogUtil.getStackTraceToJsonArray`に渡されます。この値を増やすと、より大きなJSONペイロードとスタックキャプチャのためのわずかに高いCPU/メモリを犠牲にして、遅いロックまたはデッドロックの問題を診断するためのより多くのコンテキストが提供されます。減らすとオーバーヘッドが削減されます。注：遅いロックのみをログに記録する場合、リーダーエントリは`slow_lock_threshold_ms`でフィルタリングできます。
- Introduced in: v3.4.0, v3.5.0

##### `ssl_cipher_blacklist`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: IANA名でSSL暗号スイートをブラックリスト化するための、正規表現をサポートするカンマ区切りのリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- Introduced in: v4.0

##### `ssl_cipher_whitelist`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: IANA名でSSL暗号スイートをホワイトリスト化するための、正規表現をサポートするカンマ区切りのリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- Introduced in: v4.0

##### `task_runs_concurrency`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 同時に実行されるTaskRunインスタンスのグローバル制限。現在の実行数が`task_runs_concurrency`以上の場合、`TaskRunScheduler`は新しい実行のスケジュールを停止するため、この値はスケジューラー全体での並列TaskRun実行を制限します。また、`MVPCTRefreshPartitioner`によって、TaskRunごとのパーティション更新粒度を計算するためにも使用されます。値を増やすと並列処理とリソース使用量が増加し、減らすと並行処理が減少し、実行ごとのパーティション更新が大きくなります。意図的にスケジューリングを無効にする場合を除き、0または負の値に設定しないでください。0（または負の値）は、`TaskRunScheduler`による新しいTaskRunのスケジュールを事実上妨げます。
- Introduced in: v3.2.0

##### `task_runs_queue_length`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 保留中のキューに保持される保留中のTaskRunアイテムの最大数を制限します。`TaskRunManager`は現在の保留中の数をチェックし、有効な保留中のTaskRun数が`task_runs_queue_length`以上の場合、新しい送信を拒否します。マージ/承認されたTaskRunが追加される前に、同じ制限が再チェックされます。メモリとスケジューリングのバックログのバランスを取るためにこの値を調整してください。大量のバースト的なワークロードの場合は拒否を避けるために高く設定し、メモリを制限し保留中のバックログを減らすために低く設定します。
- Introduced in: v3.2.0

##### `thrift_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: FEノードのThriftサーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `thrift_client_timeout_ms`

- Default: 5000
- Type: Int
- Unit: ミリ秒
- Is mutable: いいえ
- Description: アイドル状態のクライアント接続がタイムアウトするまでの時間。
- Introduced in: -

##### `thrift_rpc_max_body_size`

- Default: -1
- Type: Int
- Unit: バイト
- Is mutable: いいえ
- Description: サーバーのThriftプロトコルを構築する際に使用されるThrift RPCメッセージボディの最大許容サイズ（バイト単位）を制御します（`ThriftServer`のTBinaryProtocol.Factoryに渡されます）。`-1`の値は制限を無効にします（無制限）。正の値を設定すると上限が強制され、これより大きいメッセージはThriftレイヤーによって拒否されます。これにより、メモリ使用量を制限し、過大なリクエストやDoSのリスクを軽減するのに役立ちます。正当なリクエストが拒否されないように、予想されるペイロード（大きな構造体やバッチデータ）に十分な大きさに設定してください。
- Introduced in: v3.2.0

##### `thrift_server_max_worker_threads`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: はい
- Description: FEノードのThriftサーバーがサポートするワーカー スレッドの最大数。
- Introduced in: -

##### `thrift_server_queue_size`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: リクエストが保留されているキューの長さ。Thriftサーバーで処理されているスレッドの数が`thrift_server_max_worker_threads`で指定された値を超えると、新しいリクエストが保留中のキューに追加されます。
- Introduced in: -

### メタデータとクラスター管理

##### `alter_max_worker_queue_size`

- Default: 4096
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: alterサブシステムが使用する内部ワーカー スレッド プール キューの容量を制御します。`AlterHandler`の`ThreadPoolManager.newDaemonCacheThreadPool`に`alter_max_worker_threads`とともに渡されます。保留中のalterタスクの数が`alter_max_worker_queue_size`を超えると、新しい送信は拒否され、`RejectedExecutionException`がスローされる可能性があります（`AlterHandler.handleFinishAlterTask`を参照）。この値を調整して、メモリ使用量と同時alterタスクに許可するバックログの量のバランスを取ります。
- Introduced in: v3.2.0

##### `alter_max_worker_threads`

- Default: 4
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: AlterHandlerのスレッドプールにおけるワーカー スレッドの最大数を設定します。AlterHandlerはこの値を使用してエグゼキュータを構築し、alter関連タスク（例: `handleFinishAlterTask`を介した`AlterReplicaTask`の送信）を実行および完了します。この値はalter操作の同時実行を制限します。値を上げると並列処理とリソース使用量が増加し、値を下げると同時alterが制限され、ボトルネックになる可能性があります。エグゼキュータは`alter_max_worker_queue_size`とともに作成され、ハンドラ スケジューリングは`alter_scheduler_interval_millisecond`を使用します。
- Introduced in: v3.2.0

##### `automated_cluster_snapshot_interval_seconds`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 自動クラスター スナップショット タスクがトリガーされる間隔。
- Introduced in: v3.4.2

##### `background_refresh_metadata_interval_millis`

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: 2つの連続するHiveメタデータ キャッシュ更新の間隔。
- Introduced in: v2.5.5

##### `background_refresh_metadata_time_secs_since_last_access_secs`

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Hiveメタデータ キャッシュ更新タスクの有効期限。アクセスされたHiveカタログの場合、指定された時間以上アクセスされていない場合、StarRocksはそのキャッシュされたメタデータの更新を停止します。アクセスされていないHiveカタログの場合、StarRocksはそのキャッシュされたメタデータを更新しません。
- Introduced in: v2.5.5

##### `bdbje_cleaner_threads`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: StarRocksジャーナルが使用するBerkeley DB Java Edition (JE) 環境のバックグラウンド クリーナー スレッド数。この値は`BDBEnvironment.initConfigs`での環境初期化中に読み取られ、`Config.bdbje_cleaner_threads`を使用して`EnvironmentConfig.CLEANER_THREADS`に適用されます。JEログのクリーンアップとスペース再利用の並列処理を制御します。値を増やすと、フォアグラウンド操作との追加のCPUおよびI/O干渉を犠牲にして、クリーンアップを高速化できます。変更はBDB環境が(再)初期化されたときにのみ有効になるため、新しい値を適用するにはフロントエンドの再起動が必要です。
- Introduced in: v3.2.0

##### `bdbje_heartbeat_timeout_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: StarRocksクラスター内のリーダー、フォロワー、オブザーバーFE間のハートビートがタイムアウトするまでの時間。
- Introduced in: -

##### `bdbje_lock_timeout_second`

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: BDB JEベースのFEにおけるロックがタイムアウトするまでの時間。
- Introduced in: -

##### `bdbje_replay_cost_percent`

- Default: 150
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: BDB JEログからトランザクションをリプレイする相対コスト（パーセンテージ）と、ネットワーク リストアを介して同じデータを取得するコストを設定します。この値は、基盤となるJEレプリケーション パラメータ`REPLAY_COST_PERCENT`に供給され、通常は`>100`であり、リプレイがネットワーク リストアよりも高価であることを示します。潜在的なリプレイのためにクリーンアップされたログファイルを保持するかどうかを決定する際、システムはリプレイ コストにログ サイズを乗じたものをネットワーク リストアのコストと比較します。ネットワーク リストアの方が効率的と判断された場合、ファイルは削除されます。値が0の場合、このコスト比較に基づく保持は無効になります。`REP_STREAM_TIMEOUT`内のレプリカ、またはアクティブなレプリケーションに必要なログファイルは常に保持されます。
- Introduced in: v3.2.0

##### `bdbje_replica_ack_timeout_second`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: リーダーFEからフォロワーFEにメタデータが書き込まれる際に、リーダーFEが指定された数のフォロワーFEからのACKメッセージを待機できる最大時間。単位: 秒。大量のメタデータが書き込まれている場合、フォロワーFEがリーダーFEにACKメッセージを返すまでに時間がかかり、ACKタイムアウトが発生する可能性があります。この状況では、メタデータの書き込みが失敗し、FEプロセスが終了します。この状況を防ぐために、このパラメータの値を増やすことをお勧めします。
- Introduced in: -

##### `bdbje_reserved_disk_size`

- Default: 512 * 1024 * 1024 (536870912)
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Berkeley DB JEが「保護されていない」（削除可能な）ログ/データファイルとして予約するバイト数を制限します。StarRocksはこの値をBDBEnvironmentの`EnvironmentConfig.RESERVED_DISK`を介してJEに渡します。JEの組み込みデフォルトは0（無制限）です。StarRocksのデフォルト（512 MiB）は、JEが保護されていないファイルのために過剰なディスクスペースを予約するのを防ぎつつ、古いファイルを安全にクリーンアップできるようにします。ディスク容量が制約されているシステムではこの値を調整してください。値を減らすとJEはより早くファイルを解放でき、値を増やすとJEはより多くの予約スペースを保持できます。変更を有効にするにはプロセスの再起動が必要です。
- Introduced in: v3.2.0

##### `bdbje_reset_election_group`

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: BDBJEレプリケーション グループをリセットするかどうか。このパラメータが`TRUE`に設定されている場合、FEはBDBJEレプリケーション グループをリセットし（つまり、すべての選出可能なFEノードの情報を削除し）、リーダーFEとして起動します。リセット後、このFEはクラスター内の唯一のメンバーとなり、他のFEは`ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`を使用してこのクラスターに再参加できます。この設定は、ほとんどのフォロワーFEのデータが破損しているためにリーダーFEを選出できない場合にのみ使用してください。`reset_election_group`は`metadata_failure_recovery`を置き換えるために使用されます。
- Introduced in: -

##### `black_host_connect_failures_within_time`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: ブラックリストに登録されたBEノードに許可される接続失敗のしきい値。BEノードが自動的にBEブラックリストに追加された場合、StarRocksはその接続性を評価し、BEブラックリストから削除できるかどうかを判断します。`black_host_history_sec`内で、ブラックリストに登録されたBEノードの接続失敗が`black_host_connect_failures_within_time`で設定されたしきい値よりも少ない場合にのみ、BEブラックリストから削除できます。
- Introduced in: v3.3.0

##### `black_host_history_sec`

- Default: 2 * 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: BEブラックリスト内のBEノードの過去の接続失敗を保持する期間。BEノードが自動的にBEブラックリストに追加された場合、StarRocksはその接続性を評価し、BEブラックリストから削除できるかどうかを判断します。`black_host_history_sec`内で、ブラックリストに登録されたBEノードの接続失敗が`black_host_connect_failures_within_time`で設定されたしきい値よりも少ない場合にのみ、BEブラックリストから削除できます。
- Introduced in: v3.3.0

##### `brpc_connection_pool_size`

- Default: 16
- Type: Int
- Unit: Connections
- Is mutable: No
- Description: FEのBrpcProxyが使用するエンドポイントごとのプールされたBRPC接続の最大数。この値は`setMaxTotoal`と`setMaxIdleSize`を介してRpcClientOptionsに適用されるため、各リクエストがプールから接続を借りる必要があるため、同時発信BRPCリクエストを直接制限します。高並列シナリオでは、リクエストのキューイングを避けるためにこれを増やしてください。増やすとソケットとメモリの使用量が増加し、リモートサーバーの負荷が増加する可能性があります。調整する際は、`brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr`、`brpc_min_evictable_idle_time_ms`などの関連設定を考慮してください。この値の変更はホットリロード可能ではなく、再起動が必要です。
- Introduced in: v3.2.0

##### `brpc_short_connection`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 基盤となるbrpc RpcClientが短命の接続を使用するかどうかを制御します。有効（`true`）にすると、RpcClientOptions.setShortConnectionが設定され、リクエスト完了後に接続が閉じられ、接続設定のオーバーヘッド増加とレイテンシ増加を犠牲にして、長寿命ソケットの数を減らします。無効（`false`、デフォルト）にすると、永続接続と接続プールが使用されます。このオプションを有効にすると接続プールの動作に影響するため、`brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr`、`brpc_inner_reuse_pool`と合わせて考慮する必要があります。一般的な高スループット展開では無効のままにしてください。ソケットの寿命を制限する場合、またはネットワークポリシーによって短命接続が必要な場合にのみ有効にしてください。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `catalog_try_lock_timeout_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: グローバルロックを取得するためのタイムアウト期間。
- Introduced in: -

##### `checkpoint_only_on_leader`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `true`の場合、CheckpointControllerはリーダーFEのみをチェックポイントワーカーとして選択します。`false`の場合、コントローラーは任意のフロントエンドを選択でき、ヒープ使用量が少ないノードを優先します。`false`の場合、ワーカーは最近の失敗時間と`heapUsedPercent`でソートされます（リーダーは選択を避けるために無限の使用量を持つものとして扱われます）。クラスター スナップショット メタデータを必要とする操作の場合、コントローラーはこのフラグに関係なくリーダー選択を強制します。`true`を有効にすると、チェックポイント作業がリーダーに集中します（より単純ですが、リーダーのCPU/メモリとネットワーク負荷が増加します）。`false`のままにすると、チェックポイント負荷が負荷の少ないFEに分散されます。この設定は、ワーカーの選択と、`checkpoint_timeout_seconds`などのタイムアウトや`thrift_rpc_timeout_ms`などのRPC設定との相互作用に影響します。
- Introduced in: v3.4.0, v3.5.0

##### `checkpoint_timeout_seconds`

- Default: 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: リーダーのCheckpointControllerがチェックポイントワーカーがチェックポイントを完了するのを待機する最大時間（秒単位）。コントローラーはこの値をナノ秒に変換し、ワーカーの結果キューをポーリングします。このタイムアウト内に正常な完了が受信されない場合、チェックポイントは失敗と見なされ、`createImage`は失敗を返します。この値を増やすと、実行時間の長いチェックポイントに対応できますが、障害検出とそれに続くイメージ伝播が遅れます。値を減らすと、より高速なフェイルオーバー/再試行が発生しますが、遅いワーカーに対して誤ったタイムアウトを生成する可能性があります。この設定は、チェックポイント作成中の`CheckpointController`での待機期間のみを制御し、ワーカーの内部チェックポイント動作は変更しません。
- Introduced in: v3.4.0, v3.5.0

##### `db_used_data_quota_update_interval_secs`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: データベースの使用済みデータクォータが更新される間隔。StarRocksは、ストレージ消費を追跡するために、すべてのデータベースの使用済みデータクォータを定期的に更新します。この値は、クォータの適用とメトリクスの収集に使用されます。過剰なシステム負荷を防ぐため、許可される最小間隔は30秒です。30未満の値は拒否されます。
- Introduced in: -

##### `drop_backend_after_decommission`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: BEが廃止された後にBEを削除するかどうか。`TRUE`は、BEが廃止された直後に削除されることを示します。`FALSE`は、BEが廃止された後に削除されないことを示します。
- Introduced in: -

##### `edit_log_port`

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: クラスター内のリーダー、フォロワー、オブザーバーFE間の通信に使用されるポート。
- Introduced in: -

##### `edit_log_roll_num`

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: メタデータログエントリのログファイルが作成されるまでに書き込むことができるメタデータログエントリの最大数。このパラメータはログファイルのサイズを制御するために使用されます。新しいログファイルはBDBJEデータベースに書き込まれます。
- Introduced in: -

##### `edit_log_type`

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: 生成できる編集ログのタイプ。値を`BDB`に設定します。
- Introduced in: -

##### `enable_background_refresh_connector_metadata`

- Default: true in v3.0 and later and false in v2.5
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 定期的なHiveメタデータ キャッシュ更新を有効にするかどうか。有効にすると、StarRocksはHiveクラスターのメタストア（Hive MetastoreまたはAWS Glue）をポーリングし、頻繁にアクセスされるHiveカタログのキャッシュされたメタデータを更新してデータ変更を認識します。`true`はHiveメタデータ キャッシュ更新を有効にすることを示し、`false`は無効にすることを示します。
- Introduced in: v2.5.5

##### `enable_collect_query_detail_info`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クエリのプロファイルを収集するかどうか。このパラメータが`TRUE`に設定されている場合、システムはクエリのプロファイルを収集します。このパラメータが`FALSE`に設定されている場合、システムはクエリのプロファイルを収集しません。
- Introduced in: -

##### `enable_create_partial_partition_in_batch`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が`false`（デフォルト）に設定されている場合、StarRocksはバッチ作成された範囲パーティションが標準の時間単位境界に揃うように強制します。穴の作成を避けるために、非アラインメント範囲は拒否されます。この項目を`true`に設定すると、そのアラインメントチェックが無効になり、バッチで部分的な（非標準の）パーティションを作成できるようになります。これにより、ギャップやアラインメントされていないパーティション範囲が生成される可能性があります。部分的なバッチパーティションが意図的に必要であり、関連するリスクを受け入れる場合にのみ、これを`true`に設定してください。
- Introduced in: v3.2.0

##### `enable_internal_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が`true`に設定されている場合、内部コンポーネント（例: SimpleExecutor）によって実行される内部SQLステートメントは保持され、内部監査またはログメッセージに書き込まれます（`enable_sql_desensitize_in_log`が設定されている場合はさらに非機密化できます）。`false`に設定されている場合、内部SQLテキストは抑制されます。フォーマットコード（SimpleExecutor.formatSQL）は「?」を返し、実際のステートメントは内部監査またはログメッセージに出力されません。この設定は内部ステートメントの実行セマンティクスを変更しません。プライバシーまたはセキュリティのために内部SQLのログ記録と可視性を制御するだけです。
- Introduced in: -

##### `enable_legacy_compatibility_for_replication`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: レプリケーションのレガシー互換性を有効にするかどうか。StarRocksは旧バージョンと新バージョンで動作が異なる場合があり、クラスター間のデータ移行中に問題を引き起こす可能性があります。そのため、データ移行前にターゲットクラスターのレガシー互換性を有効にし、データ移行完了後に無効にする必要があります。`true`はこのモードを有効にすることを示します。
- Introduced in: v3.1.10, v3.2.6

##### `enable_show_materialized_views_include_all_task_runs`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `SHOW MATERIALIZED VIEWS`コマンドにTaskRunsがどのように返されるかを制御します。この項目が`false`に設定されている場合、StarRocksはタスクごとに最新のTaskRunのみを返します（互換性のためのレガシー動作）。`true`（デフォルト）に設定されている場合、`TaskManager`は、同じ開始TaskRun IDを共有する場合にのみ（たとえば、同じジョブに属する場合）、同じタスクの追加のTaskRunを含めることができ、無関係な重複実行が表示されるのを防ぎながら、1つのジョブに関連付けられた複数のステータスを表示できます。単一実行の出力を復元するため、またはデバッグと監視のために複数実行のジョブ履歴を表示するには、この項目を`false`に設定します。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_statistics_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 統計クエリのプロファイルを生成するかどうか。この項目を`true`に設定すると、StarRocksがシステム統計に関するクエリのクエリプロファイルを生成できるようになります。
- Introduced in: v3.1.5

##### `enable_table_name_case_insensitive`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: カタログ名、データベース名、テーブル名、ビュー名、マテリアライズドビュー名の大文字と小文字を区別しない処理を有効にするかどうか。現在、テーブル名は大文字と小文字を区別します（デフォルト）。
  - この機能を有効にすると、関連するすべての名前は小文字で保存され、これらの名前を含むすべてのSQLコマンドは自動的に小文字に変換されます。
  - この機能は、クラスターの作成時にのみ有効にできます。**クラスターが起動した後、この設定の値をいかなる方法でも変更することはできません**。変更しようとするとエラーが発生します。FEは、この設定項目の値がクラスターが最初に起動したときと矛盾していることを検出すると、起動に失敗します。
  - 現在、この機能はJDBCカタログおよびテーブル名をサポートしていません。JDBCまたはODBCデータソースに対して大文字と小文字を区別しない処理を実行したい場合は、この機能を有効にしないでください。
- Introduced in: v4.0

##### `enable_task_history_archive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、完了したタスク実行レコードは永続的なタスク実行履歴テーブルにアーカイブされ、編集ログに記録されるため、ルックアップ（例: `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`）にはアーカイブされた結果が含まれます。アーカイブはFEリーダーによって実行され、単体テスト（`FeConstants.runningUnitTest`）中はスキップされます。有効にすると、インメモリの有効期限と強制GCパスはバイパスされ（コードは`removeExpiredRuns`と`forceGC`から早期にリターンします）、保持/削除は`task_runs_ttl_second`と`task_runs_max_history_number`ではなく永続アーカイブによって処理されます。無効にすると、履歴はメモリに残り、これらの設定によって剪定されます。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `enable_task_run_fe_evaluation`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、FEは`TaskRunsSystemTable.supportFeEvaluation`内のシステムテーブル`task_runs`に対してローカル評価を実行します。FE側の評価は、列を定数と比較する結合等価述語にのみ許可され、`QUERY_ID`と`TASK_NAME`列に限定されます。これを有効にすると、より広範なスキャンや追加のリモート処理を回避することで、ターゲットを絞ったルックアップのパフォーマンスが向上します。無効にすると、プランナーは`task_runs`のFE評価をスキップせざるを得なくなり、述語の剪定が減少し、これらのフィルターのクエリレイテンシに影響を与える可能性があります。
- Introduced in: v3.3.13, v3.4.3, v3.5.0

##### `heartbeat_mgr_blocking_queue_size`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: ハートビートマネージャーによって実行されるハートビートタスクを格納するブロッキングキューのサイズ。
- Introduced in: -

##### `heartbeat_mgr_threads_num`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: ハートビートマネージャーがハートビートタスクを実行できるスレッド数。
- Introduced in: -

##### `ignore_materialized_view_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FEがマテリアライズドビューエラーによって引き起こされるメタデータ例外を無視するかどうか。マテリアライズドビューエラーによって引き起こされるメタデータ例外のためにFEが起動に失敗した場合、このパラメータを`true`に設定してFEが例外を無視できるようにすることができます。
- Introduced in: v2.5.10

##### `ignore_meta_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非リーダーFEがリーダーFEからのメタデータギャップを無視するかどうか。値が`TRUE`の場合、非リーダーFEはリーダーFEからのメタデータギャップを無視し、データ読み取りサービスを提供し続けます。このパラメータは、リーダーFEを長期間停止した場合でも、継続的なデータ読み取りサービスを保証します。値が`FALSE`の場合、非リーダーFEはリーダーFEからのメタデータギャップを無視せず、データ読み取りサービスを停止します。
- Introduced in: -

##### `ignore_task_run_history_replay_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: StarRocksが`information_schema.task_runs`のTaskRun履歴行を逆シリアル化する際、破損または無効なJSON行は通常、逆シリアル化が警告をログに記録し、RuntimeExceptionをスローする原因となります。この項目が`true`に設定されている場合、システムは逆シリアル化エラーをキャッチし、不正な形式のレコードをスキップして、クエリを失敗させる代わりに残りの行の処理を続行します。これにより、`information_schema.task_runs`クエリは`_statistics_.task_run_history`テーブル内の不正なエントリに対して耐性を持つようになります。ただし、これを有効にすると、明示的なエラーを表示する代わりに、破損した履歴レコードがサイレントに破棄される可能性があることに注意してください（潜在的なデータ損失）。
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### `lock_checker_interval_second`

- Default: 30
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: LockCheckerフロントエンドデーモン（「deadlock-checker」という名前）の実行間隔（秒単位）。デーモンはデッドロック検出とスローロック スキャンを実行します。設定された値は1000倍されてタイマーがミリ秒単位で設定されます。この値を減らすと検出レイテンシは減少しますが、スケジューリングとCPUオーバーヘッドが増加します。値を増やすとオーバーヘッドは減少しますが、検出とスローロックの報告が遅れます。デーモンは実行ごとに間隔をリセットするため、変更は実行時に有効になります。この設定は`lock_checker_enable_deadlock_check`（デッドロックチェックを有効にする）および`slow_lock_threshold_ms`（スローロックを構成するものを定義する）と相互作用します。
- Introduced in: v3.2.0

##### `master_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: リーダーFEがログをディスクにフラッシュするポリシー。このパラメータは、現在のFEがリーダーFEである場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュが同時に行われません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。

  フォロワーFEを1つだけデプロイしている場合は、このパラメータを`SYNC`に設定することをお勧めします。フォロワーFEを3つ以上デプロイしている場合は、このパラメータと`replica_sync_policy`の両方を`WRITE_NO_SYNC`に設定することをお勧めします。

- Introduced in: -

##### `max_bdbje_clock_delta_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: StarRocksクラスター内のリーダーFEとフォロワーまたはオブザーバーFE間で許容される最大クロックオフセット。
- Introduced in: -

##### `meta_delay_toleration_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: フォロワーFEおよびオブザーバーFE上のメタデータがリーダーFE上のメタデータから遅延できる最大期間。単位: 秒。この期間を超えると、非リーダーFEはサービスの提供を停止します。
- Introduced in: -

##### `meta_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- Type: String
- Unit: -
- Is mutable: No
- Description: メタデータを格納するディレクトリ。
- Introduced in: -

##### `metadata_ignore_unknown_operation_type`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 不明なログIDを無視するかどうか。FEがロールバックされると、以前のバージョンのFEは一部のログIDを認識できない場合があります。値が`TRUE`の場合、FEは不明なログIDを無視します。値が`FALSE`の場合、FEは終了します。
- Introduced in: -

##### `profile_info_format`

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description: システムが出力するプロファイルの形式。有効な値: `default`と`json`。`default`に設定すると、プロファイルはデフォルト形式になります。`json`に設定すると、システムはプロファイルをJSON形式で出力します。
- Introduced in: v2.5

##### `replica_ack_policy`

- Default: `SIMPLE_MAJORITY`
- Type: String
- Unit: -
- Is mutable: No
- Description: ログエントリが有効と見なされるポリシー。デフォルト値`SIMPLE_MAJORITY`は、フォロワーFEの過半数がACKメッセージを返した場合にログエントリが有効と見なされることを指定します。
- Introduced in: -

##### `replica_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: フォロワーFEがログをディスクにフラッシュするポリシー。このパラメータは、現在のFEがフォロワーFEである場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされるときに、ログエントリの生成とフラッシュが同時に行われません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。
- Introduced in: -

##### `start_with_incomplete_meta`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: `true`の場合、イメージデータは存在するがBerkeley DB JE (BDB) ログファイルが欠落または破損している場合に、FEは起動を許可します。`MetaHelper.checkMetaDir()`はこのフラグを使用して、対応するBDBログなしでイメージから起動するのを防ぐ安全チェックをバイパスします。この方法で起動すると、古いまたは一貫性のないメタデータが生成される可能性があり、緊急復旧にのみ使用すべきです。`RestoreClusterSnapshotMgr`はクラスター スナップショットを復元する際に一時的にこのフラグを`true`に設定し、その後ロールバックします。このコンポーネントは復元中に`bdbje_reset_election_group`も切り替えます。通常の操作では有効にしないでください。破損したBDBデータから回復する場合、またはイメージベースのスナップショットを明示的に復元する場合にのみ有効にしてください。
- Introduced in: v3.2.0

##### `table_keeper_interval_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: TableKeeperデーモンの実行間隔（秒単位）。TableKeeperDaemonはこの値（1000倍）を使用して内部タイマーを設定し、履歴テーブルの存在確認、テーブルプロパティ（レプリケーション数）の修正、パーティションTTLの更新を行うキーパータスクを定期的に実行します。デーモンはリーダーノードでのみ作業を実行し、`table_keeper_interval_second`が変更されたときに`setInterval`を介して実行時間隔を更新します。スケジューリング頻度と負荷を減らすには値を増やし、欠落または古い履歴テーブルへのより迅速な反応のためには値を減らします。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `task_runs_ttl_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タスク実行履歴の有効期間（TTL）を制御します。この値を下げると履歴の保持期間が短くなり、メモリ/ディスク使用量が減少します。値を上げると履歴が長く保持されますが、リソース使用量が増加します。予測可能な保持とストレージ動作のために、`task_runs_max_history_number`および`enable_task_history_archive`と合わせて調整してください。
- Introduced in: v3.2.0

##### `task_ttl_second`

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タスクの有効期間（TTL）。手動タスク（スケジュールが設定されていない場合）の場合、TaskBuilderはこの値を使用してタスクの`expireTime`を計算します（`expireTime = now + task_ttl_second * 1000L`）。TaskRunも、実行の実行タイムアウトを計算する際の上限としてこの値を使用します。有効な実行タイムアウトは`min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)`です。この値を調整すると、手動で作成されたタスクが有効なままである期間が変更され、タスク実行の最大許容実行時間を間接的に制限できます。
- Introduced in: v3.2.0

##### `thrift_rpc_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Thrift RPC呼び出しが行う試行の合計回数を制御します。この値は`ThriftRPCRequestExecutor`（および`NodeMgr`や`VariableMgr`などの呼び出し元）によって再試行のループカウントとして使用されます。つまり、値が3の場合、最初の試行を含めて最大3回の試行が許可されます。`TTransportException`が発生した場合、エグゼキュータはこの回数まで接続を再オープンして再試行します。原因が`SocketTimeoutException`である場合、または再オープンが失敗した場合は再試行しません。各試行は`thrift_rpc_timeout_ms`で設定された試行ごとのタイムアウトの対象となります。この値を増やすと、一時的な接続障害に対する回復力が向上しますが、RPC全体のレイテンシとリソース使用量が増加する可能性があります。
- Introduced in: v3.2.0

##### `thrift_rpc_strict_mode`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Thriftサーバーが使用するTBinaryProtocolの「厳密読み取り」モードを制御します。この値はThriftサーバー スタックのorg.apache.thrift.protocol.TBinaryProtocol.Factoryの最初の引数として渡され、受信Thriftメッセージの解析と検証方法に影響します。`true`（デフォルト）の場合、サーバーは厳密なThriftエンコーディング/バージョンチェックを強制し、設定された`thrift_rpc_max_body_size`制限を尊重します。`false`の場合、サーバーは非厳密な（レガシー/寛容な）メッセージ形式を受け入れます。これは古いクライアントとの互換性を向上させる可能性がありますが、一部のプロトコル検証をバイパスする可能性があります。実行中のクラスターでこれを変更する際は注意が必要です。これは変更不可であり、相互運用性と解析の安全性に影響するためです。
- Introduced in: v3.2.0

##### `thrift_rpc_timeout_ms`

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Thrift RPC呼び出しのデフォルトのネットワーク/ソケットタイムアウトとして使用されるタイムアウト（ミリ秒単位）。`ThriftConnectionPool`（フロントエンドおよびバックエンドプールで使用）でThriftクライアントを作成する際にTSocketに渡され、`ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr`、`CheckpointWorker`などの場所でRPC呼び出しタイムアウトを計算する際に、操作の実行タイムアウト（例: ExecTimeout*1000 + `thrift_rpc_timeout_ms`）にも追加されます。この値を増やすと、RPC呼び出しがより長いネットワークまたはリモート処理の遅延を許容できるようになります。値を減らすと、低速ネットワークでのフェイルオーバーが高速化されます。この値を変更すると、Thrift RPCを実行するFEコードパス全体で接続作成とリクエストの期限に影響します。
- Introduced in: v3.2.0

##### `txn_latency_metric_report_groups`

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 報告するトランザクションレイテンシメトリックグループのコンマ区切りリスト。ロードタイプは監視のために論理グループに分類されます。グループが有効になっている場合、その名前はトランザクションメトリックに「type」ラベルとして追加されます。有効な値: `stream_load`、`routine_load`、`broker_load`、`insert`、および`compaction`（共有データクラスターでのみ利用可能）。例: `"stream_load,routine_load"`。
- Introduced in: v4.0

##### `txn_rollback_limit`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: ロールバックできるトランザクションの最大数。
- Introduced in: -

### ユーザー、ロール、および権限

##### `enable_task_info_mask_credential`

- デフォルト: true
- タイプ: ブール
- 単位: -
- 変更可能: はい
- 説明: trueの場合、StarRocksは`information_schema.tasks`および`information_schema.task_runs`で資格情報を返す前に、`DEFINITION`列に`SqlCredentialRedactor.redact`を適用して、タスクSQL定義から資格情報を編集（秘匿）します。`information_schema.task_runs`では、定義がタスク実行ステータスから来るか、または空の場合にタスク定義ルックアップから来るかにかかわらず、同じ編集が適用されます。falseの場合、生のタスク定義が返されます（資格情報が公開される可能性があります）。マスキングはCPU/文字列処理作業であり、タスクまたは`task_runs`の数が多い場合は時間がかかることがあります。編集されていない定義が必要で、セキュリティリスクを受け入れる場合にのみ無効にしてください。
- 導入バージョン: v3.5.6

##### `privilege_max_role_depth`

- デフォルト: 16
- タイプ: 整数
- 単位:
- 変更可能: はい
- 説明: ロールの最大ロール深度（継承レベル）。
- 導入バージョン: v3.0.0

##### `privilege_max_total_roles_per_user`

- デフォルト: 64
- タイプ: 整数
- 単位:
- 変更可能: はい
- 説明: ユーザーが持つことができるロールの最大数。
- 導入バージョン: v3.0.0

### クエリエンジン

##### `brpc_send_plan_fragment_timeout_ms`

- Default: 60000
- Type: Int
- Unit: ミリ秒
- Is mutable: Yes
- Description: プランフラグメントを送信する前にBRPC TalkTimeoutControllerに適用されるミリ秒単位のタイムアウト。`BackendServiceClient.sendPlanFragmentAsync` は、バックエンドの `execPlanFragmentAsync` を呼び出す前にこの値を設定します。これは、BRPCが接続プールからアイドル接続を借りる際、および送信を実行する際に待機する時間を制御します。この値を超えると、RPCは失敗し、メソッドのリトライロジックがトリガーされる可能性があります。競合下で迅速に失敗させるにはこの値を低く設定し、一時的なプール枯渇や低速ネットワークを許容するには高く設定してください。注意: 非常に大きな値は、障害検出を遅らせ、リクエストスレッドをブロックする可能性があります。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- Default: 12 * 3600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 大規模テーブルのクエリトリガー `ANALYZE` タスクの間隔。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE上でPending状態にあるクエリトリガー `ANALYZE` タスクの最大数。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE上でRunning状態にあるクエリトリガー `ANALYZE` タスクの最大数。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- Default: 2 * 3600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 小規模テーブルのクエリトリガー `ANALYZE` タスクの間隔。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_rows`

- Default: 10000000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: クエリトリガー `ANALYZE` タスクにおいて、テーブルが小規模テーブルであるかどうかを判断するためのしきい値。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_task_schedule_interval`

- Default: 30
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: スケジューラスレッドがクエリトリガーバックグラウンドタスクをスケジュールする間隔。この項目は、v3.4.0で導入された `connector_table_query_trigger_analyze_schedule_interval` を置き換えるものです。ここでいうバックグラウンドタスクとは、v3.4では `ANALYZE` タスクを指し、v3.4以降のバージョンでは低カーディナリティ列の辞書収集タスクを指します。
- Introduced in: v3.4.2

##### `create_table_max_serial_replicas`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: シリアルに作成されるレプリカの最大数。実際のレプリカ数がこの値を超えると、レプリカは並行して作成されます。テーブル作成に時間がかかっている場合は、この値を減らしてみてください。
- Introduced in: -

##### `default_mv_partition_refresh_number`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新が複数のパーティションに関わる場合、このパラメータはデフォルトで1つのバッチでいくつのパーティションが更新されるかを制御します。
バージョン3.3.0以降、システムは潜在的なメモリ不足 (OOM) の問題を避けるため、一度に1つのパーティションを更新することをデフォルトとしています。以前のバージョンでは、デフォルトですべてのパーティションが一度に更新され、メモリ枯渇やタスク失敗につながる可能性がありました。ただし、マテリアライズドビューの更新が多数のパーティションに関わる場合、一度に1つのパーティションのみを更新すると、過剰なスケジューリングオーバーヘッド、全体の更新時間の延長、および多数の更新レコードが発生する可能性があります。このような場合は、更新効率を向上させ、スケジューリングコストを削減するために、このパラメータを適切に調整することをお勧めします。
- Introduced in: v3.3.0

##### `default_mv_refresh_immediate`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非同期マテリアライズドビューを、作成後すぐに更新するかどうか。この項目が `true` に設定されている場合、新しく作成されたマテリアライズドビューはすぐに更新されます。
- Introduced in: v3.2.3

##### `dynamic_partition_check_interval_seconds`

- Default: 600
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: 新しいデータがチェックされる間隔。新しいデータが検出されると、StarRocksはそのデータのために自動的にパーティションを作成します。
- Introduced in: -

##### `dynamic_partition_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 動的パーティショニング機能を有効にするかどうか。この機能が有効な場合、StarRocksは新しいデータのために動的にパーティションを作成し、期限切れのパーティションを自動的に削除して、データの鮮度を確保します。
- Introduced in: -

##### `enable_active_materialized_view_schema_strict_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非アクティブなマテリアライズドビューをアクティブ化する際に、データ型の長さの一貫性を厳密にチェックするかどうか。この項目が `false` に設定されている場合、ベーステーブルでデータ型の長さが変更されても、マテリアライズドビューのアクティブ化には影響しません。
- Introduced in: v3.3.4

##### `enable_auto_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `ARRAY` 型の `NDV` 情報の自動収集を有効にするかどうか。
- Introduced in: v4.0

##### `enable_backup_materialized_view`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 特定のデータベースをバックアップまたは復元する際に、非同期マテリアライズドビューの `BACKUP` および `RESTORE` を有効にするかどうか。この項目が `false` に設定されている場合、StarRocksは非同期マテリアライズドビューのバックアップをスキップします。
- Introduced in: v3.2.0

##### `enable_collect_full_statistic`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 自動的な完全統計収集を有効にするかどうか。この機能はデフォルトで有効です。
- Introduced in: -

##### `enable_colocate_mv_index`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 同期マテリアライズドビューを作成する際に、同期マテリアライズドビューインデックスをベーステーブルとコロケートすることをサポートするかどうか。この項目が `true` に設定されている場合、タブレットシンクは同期マテリアライズドビューの書き込みパフォーマンスを高速化します。
- Introduced in: v3.2.0

##### `enable_decimal_v3`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `DECIMAL V3` データ型をサポートするかどうか。
- Introduced in: -

##### `enable_experimental_mv`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非同期マテリアライズドビュー機能を有効にするかどうか。`TRUE` はこの機能が有効であることを示します。v2.5.2以降、この機能はデフォルトで有効です。v2.5.2より前のバージョンでは、この機能はデフォルトで無効です。
- Introduced in: v2.4

##### `enable_local_replica_selection`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クエリに対してローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク転送コストを削減します。このパラメータが `TRUE` に設定されている場合、`CBO` は現在の `FE` と同じIPアドレスを持つ `BE` 上のタブレットレプリカを優先的に選択します。このパラメータが `FALSE` に設定されている場合、ローカルレプリカと非ローカルレプリカの両方を選択できます。
- Introduced in: -

##### `enable_manual_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `ARRAY` 型の `NDV` 情報の手動収集を有効にするかどうか。
- Introduced in: v4.0

##### `enable_materialized_view`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの作成を有効にするかどうか。
- Introduced in: -

##### `enable_materialized_view_external_table_precise_refresh`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ベーステーブルが外部（非クラウドネイティブ）テーブルである場合に、マテリアライズドビューの更新に対する内部最適化を有効にするには、この項目を `true` に設定します。有効にすると、マテリアライズドビューの更新プロセッサは候補パーティションを計算し、すべてのパーティションではなく、影響を受けるベーステーブルパーティションのみを更新するため、I/Oと更新コストが削減されます。外部テーブルの全パーティション更新を強制するには、`false` に設定します。
- Introduced in: v3.2.9

##### `enable_materialized_view_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: デフォルトで非同期マテリアライズドビューの監視メトリクスを収集するかどうか。
- Introduced in: v3.1.11, v3.2.5

##### `enable_materialized_view_spill`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新タスクに対して中間結果のスピルを有効にするかどうか。
- Introduced in: v3.1.1

##### `enable_materialized_view_text_based_rewrite`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: デフォルトでテキストベースのクエリ書き換えを有効にするかどうか。この項目が `true` に設定されている場合、システムは非同期マテリアライズドビューの作成中に抽象構文木を構築します。
- Introduced in: v3.2.5

##### `enable_mv_automatic_active_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ベーステーブル（ビュー）が `Schema Change` を受けた、または削除されて再作成されたために非アクティブに設定された非同期マテリアライズドビューを、システムが自動的にチェックして再アクティブ化することを有効にするかどうか。この機能は、ユーザーによって手動で非アクティブに設定されたマテリアライズドビューを再アクティブ化しないことに注意してください。
- Introduced in: v3.1.6

##### `enable_mv_automatic_repairing_for_broken_base_tables`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、ベースの外部テーブルが削除されて再作成されたり、そのテーブル識別子が変更されたりしたときに、StarRocksはマテリアライズドビューのベーステーブルメタデータを自動的に修復しようとします。修復フローは、マテリアライズドビューのベーステーブル情報を更新し、外部テーブルパーティションのパーティションレベルの修復情報を収集し、`autoRefreshPartitionsLimit` を尊重しながら、非同期自動更新マテリアライズドビューのパーティション更新決定を駆動できます。現在、自動修復は `Hive` 外部テーブルをサポートしています。サポートされていないテーブルタイプは、マテリアライズドビューが非アクティブに設定され、修復例外が発生します。パーティション情報収集は非ブロッキングであり、失敗はログに記録されます。
- Introduced in: v3.3.19, v3.4.8, v3.5.6

##### `enable_predicate_columns_collection`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 述語列の収集を有効にするかどうか。無効にすると、クエリ最適化中に述語列は記録されません。
- Introduced in: -

##### `enable_query_queue_v2`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: `true` の場合、`FE` のスロットベースのクエリスケジューラを `Query Queue V2` に切り替えます。このフラグは、スロットマネージャーとトラッカー（例: `BaseSlotManager.isEnableQueryQueueV2` および `SlotTracker#createSlotSelectionStrategy`）によって読み取られ、従来の戦略ではなく `SlotSelectionStrategyV2` を選択します。`query_queue_v2_xxx` 設定オプションと `QueryQueueOptions` は、このフラグが有効な場合にのみ有効になります。v4.1以降、デフォルト値は `false` から `true` に変更されました。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `enable_sql_blacklist`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `SQL` クエリのブラックリストチェックを有効にするかどうか。この機能が有効な場合、ブラックリスト内のクエリは実行できません。
- Introduced in: -

##### `enable_statistic_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `CBO` の統計情報を収集するかどうか。この機能はデフォルトで有効です。
- Introduced in: -

##### `enable_statistic_collect_on_first_load`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: データロード操作によってトリガーされる自動統計収集とメンテナンスを制御します。これには以下が含まれます。
  - データがパーティションに初めてロードされたとき（パーティションバージョンが2の場合）の統計収集。
  - マルチパーティションテーブルの空のパーティションにデータがロードされたときの統計収集。
  - `INSERT OVERWRITE` 操作のための統計のコピーと更新。

  **統計収集タイプの決定ポリシー:**
  
  - `INSERT OVERWRITE` の場合: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。既存の統計のみがコピーされます。
    - それ以外で、`targetRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、`SAMPLE` 統計収集が使用されます。
    - それ以外の場合、`FULL` 統計収集が使用されます。
  
  - 初回ロードの場合: `deltaRatio = loadRows / (totalRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。
    - それ以外で、`loadRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、`SAMPLE` 統計収集が使用されます。
    - それ以外の場合、`FULL` 統計収集が使用されます。
  
  **同期動作:**
  
  - `DML` ステートメント (`INSERT INTO`/`INSERT OVERWRITE`) の場合: テーブルロックを伴う同期モード。ロード操作は統計収集が完了するまで待機します（最大 `semi_sync_collect_statistic_await_seconds`）。
  - `Stream Load` および `Broker Load` の場合: ロックなしの非同期モード。統計収集はロード操作をブロックせずにバックグラウンドで実行されます。
  
  :::note
  この設定を無効にすると、`INSERT OVERWRITE` の統計メンテナンスを含む、ロードによってトリガーされるすべての統計操作が防止され、テーブルに統計が不足する可能性があります。新しいテーブルが頻繁に作成され、データが頻繁にロードされる場合、この機能を有効にするとメモリとCPUのオーバーヘッドが増加します。
  :::

- Introduced in: v3.1

##### `enable_statistic_collect_on_update`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `UPDATE` ステートメントが自動統計収集をトリガーできるかどうかを制御します。有効にすると、テーブルデータを変更する `UPDATE` 操作は、`enable_statistic_collect_on_first_load` によって制御されるのと同じ取り込みベースの統計フレームワークを通じて統計収集をスケジュールする場合があります。この設定を無効にすると、`UPDATE` ステートメントの統計収集はスキップされますが、ロードによってトリガーされる統計収集の動作は変更されません。
- Introduced in: v3.5.11, v4.0.4

##### `enable_udf`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: `UDF` を有効にするかどうか。
- Introduced in: -

##### `expr_children_limit`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 式で許可される子式の最大数。
- Introduced in: -

##### `histogram_buckets_size`

- Default: 64
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ヒストグラムのデフォルトのバケット数。
- Introduced in: -

##### `histogram_max_sample_row_count`

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ヒストグラムのために収集する最大行数。
- Introduced in: -

##### `histogram_mcv_size`

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ヒストグラムの最頻値（MCV）の数。
- Introduced in: -

##### `histogram_sample_ratio`

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: ヒストグラムのサンプリング比率。
- Introduced in: -

##### `http_slow_request_threshold_ms`

- Default: 5000
- Type: Int
- Unit: ミリ秒
- Is mutable: Yes
- Description: HTTPリクエストの応答時間がこのパラメータで指定された値を超えた場合、このリクエストを追跡するためのログが生成されます。
- Introduced in: v2.5.15, v3.1.5

##### `lock_checker_enable_deadlock_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、`LockChecker` スレッドは `ThreadMXBean.findDeadlockedThreads()` を使用して `JVM` レベルのデッドロック検出を実行し、問題のあるスレッドのスタックトレースをログに記録します。このチェックは `LockChecker` デーモン（その頻度は `lock_checker_interval_second` によって制御されます）内で実行され、詳細なスタック情報をログに書き込みますが、これはCPUとI/Oを大量に消費する可能性があります。このオプションは、ライブまたは再現可能なデッドロック問題のトラブルシューティングのためにのみ有効にしてください。通常の操作で有効にしたままにすると、オーバーヘッドとログ量が増加する可能性があります。
- Introduced in: v3.2.0

##### `low_cardinality_threshold`

- Default: 255
- Type: Int
- Unit: -
- Is mutable: No
- Description: 低カーディナリティ辞書のしきい値。
- Introduced in: v3.5.0

##### `materialized_view_min_refresh_interval`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: 非同期マテリアライズドビューのスケジュールで許可される最小更新間隔（秒単位）。時間ベースの間隔でマテリアライズドビューが作成される場合、その間隔は秒に変換され、この値より小さくてはなりません。そうでない場合、`CREATE`/`ALTER` 操作は `DDL` エラーで失敗します。この値が0より大きい場合、チェックが強制されます。制限を無効にするには0または負の値を設定します。これにより、過度な `TaskManager` スケジューリングや、頻繁すぎる更新による `FE` の高いメモリ/CPU使用率が防止されます。この項目は `EVENT_TRIGGERED` 更新には適用されません。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `materialized_view_refresh_ascending`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、マテリアライズドビューのパーティション更新は、パーティションキーの昇順（最も古いものから最も新しいものへ）でパーティションを反復処理します。`false`（デフォルト）に設定されている場合、システムは降順（最も新しいものから最も古いものへ）で反復処理します。StarRocksは、パーティション更新制限が適用される場合に処理するパーティションを選択し、後続の `TaskRun` 実行のための次の開始/終了パーティション境界を計算するために、リストパーティションおよび範囲パーティションのマテリアライズドビュー更新ロジックの両方でこの項目を使用します。この項目を変更すると、どのパーティションが最初に更新されるか、および次のパーティション範囲がどのように導出されるかが変わります。範囲パーティションのマテリアライズドビューの場合、スケジューラは新しい開始/終了を検証し、変更が繰り返される境界（デッドループ）を作成する場合にエラーを発生させるため、この項目は注意して設定してください。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `max_allowed_in_element_num_of_delete`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: `DELETE` ステートメントの `IN` 述語で許可される要素の最大数。
- Introduced in: -

##### `max_create_table_timeout_second`

- Default: 600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: テーブル作成の最大タイムアウト期間。
- Introduced in: -

##### `max_distribution_pruner_recursion_depth`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: パーティションプルーナーによって許可される最大再帰深度。再帰深度を増やすと、より多くの要素をプルーニングできますが、CPU消費も増加します。
- Introduced in: -

##### `max_partitions_in_one_batch`

- Default: 4096
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: パーティションを一括作成する際に作成できるパーティションの最大数。
- Introduced in: -

##### `max_planner_scalar_rewrite_num`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: オプティマイザがスカラー演算子を書き換えられる最大回数。
- Introduced in: -

##### `max_query_queue_history_slots_number`

- Default: 0
- Type: Int
- Unit: スロット
- Is mutable: Yes
- Description: 監視と可観測性のために、クエリキューごとに最近解放された（履歴）割り当てスロットをいくつ保持するかを制御します。`max_query_queue_history_slots_number` が `> 0` の値に設定されている場合、`BaseSlotTracker` は、その数の最も最近解放された `LogicalSlot` エントリをインメモリキューに保持し、制限を超えると最も古いものを削除します。これを有効にすると、`getSlots()` がこれらの履歴エントリ（最新のものが最初）を含むようになり、`BaseSlotTracker` がより豊富な `ExtraMessage` データのために `ConnectContext` にスロットを登録しようとすることを許可し、`LogicalSlot.ConnectContextListener` がクエリ完了メタデータを履歴スロットに添付できるようになります。`max_query_queue_history_slots_number` が `<= 0` の場合、履歴メカニズムは無効になります（追加のメモリは使用されません）。可観測性とメモリオーバーヘッドのバランスを取るために、適切な値を使用してください。
- Introduced in: v3.5.0

##### `max_query_retry_time`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: `FE` でのクエリ再試行の最大回数。
- Introduced in: -

##### `max_running_rollup_job_num_per_table`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: テーブルに対して並行して実行できるロールアップジョブの最大数。
- Introduced in: -

##### `max_scalar_operator_flat_children`

- Default：10000
- Type：Int
- Unit：-
- Is mutable: Yes
- Description：`ScalarOperator` のフラットな子要素の最大数。この制限を設定することで、オプティマイザが過剰なメモリを使用するのを防ぐことができます。
- Introduced in: -

##### `max_scalar_operator_optimize_depth`

- Default：256
- Type：Int
- Unit：-
- Is mutable: Yes
- Description: `ScalarOperator` の最適化が適用できる最大深度。
- Introduced in: -

##### `mv_active_checker_interval_seconds`

- Default: 60
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: バックグラウンドの `active_checker` スレッドが有効な場合、システムは定期的に、ベーステーブル（またはビュー）のスキーマ変更や再構築により非アクティブになったマテリアライズドビューを検出し、自動的に再アクティブ化します。このパラメータは、チェッカースレッドのスケジューリング間隔を秒単位で制御します。デフォルト値はシステム定義です。
- Introduced in: v3.1.6

##### `mv_rewrite_consider_data_layout_mode`

- Default: `enable`
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 最適なマテリアライズドビューを選択する際に、マテリアライズドビューの書き換えがベーステーブルのデータレイアウトを考慮すべきかどうかを制御します。有効な値:
  - `disable`: 候補となるマテリアライズドビューを選択する際に、データレイアウト基準を一切使用しません。
  - `enable`: クエリがレイアウトセンシティブであると認識された場合にのみ、データレイアウト基準を使用します。
  - `force`: 最適なマテリアライズドビューを選択する際に、常にデータレイアウト基準を適用します。
  この項目を変更すると、`BestMvSelector` の動作に影響を与え、物理レイアウトがプランの正確性やパフォーマンスに影響するかどうかに応じて、書き換えの適用性を改善または拡大できます。
- Introduced in: -

##### `publish_version_interval_ms`

- Default: 10
- Type: Int
- Unit: ミリ秒
- Is mutable: No
- Description: リリース検証タスクが発行される時間間隔。
- Introduced in: -

##### `query_queue_slots_estimator_strategy`

- Default: MAX
- Type: String
- Unit: -
- Is mutable: Yes
- Description: `enable_query_queue_v2` が `true` の場合に、キューベースのクエリに使用されるスロット推定戦略を選択します。有効な値: `MBE` (メモリベース)、`PBE` (並列性ベース)、`MAX` (`MBE` と `PBE` の最大値を取る)、`MIN` (`MBE` と `PBE` の最小値を取る)。`MBE` は、予測メモリまたはプランコストをスロットあたりのメモリターゲットで割ってスロットを推定し、`totalSlots` で上限が設定されます。`PBE` は、フラグメントの並列性（スキャン範囲のカウントまたはカーディナリティ / スロットあたりの行数）とCPUコストベースの計算（スロットあたりのCPUコストを使用）からスロットを導出し、結果を [numSlots/2, numSlots] の範囲内に制限します。`MAX` と `MIN` は、それぞれ最大値または最小値を取ることによって `MBE` と `PBE` を組み合わせます。設定値が無効な場合、デフォルト (`MAX`) が使用されます。
- Introduced in: v3.5.0

##### `query_queue_v2_concurrency_level`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: システムの総クエリスロットを計算する際に、いくつの論理的な並列性「レイヤー」が使用されるかを制御します。シェアードナッシングモードでは、総スロット = `query_queue_v2_concurrency_level` * `BE` の数 * `BE` あたりのコア数（`BackendResourceStat` から導出されます）。マルチウェアハウスモードでは、実効並列性は `max(1, query_queue_v2_concurrency_level / 4)` にスケールダウンされます。設定値が非正の場合、`4` として扱われます。この値を変更すると、`totalSlots`（したがって同時クエリ容量）が増減し、スロットあたりのリソースに影響します。`memBytesPerSlot` は、ワーカーあたりのメモリを（ワーカーあたりのコア数 * 並列性）で割って導出され、CPUアカウンティングは `query_queue_v2_cpu_costs_per_slot` を使用します。クラスターサイズに比例して設定してください。非常に大きな値は、スロットあたりのメモリを減らし、リソースの断片化を引き起こす可能性があります。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_cpu_costs_per_slot`

- Default: 1000000000
- Type: Long
- Unit: プランナーCPUコスト単位
- Is mutable: Yes
- Description: クエリがそのプランナーCPUコストから必要とするスロット数を推定するために使用される、スロットあたりのCPUコストしきい値。スケジューラは、スロットを整数(`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`)として計算し、結果を [1, totalSlots] の範囲にクランプします（`totalSlots` はクエリキュー `V2` パラメータから導出されます）。`V2` コードは非正の設定を1に正規化するため（`Math.max(1, value)`）、非正の値は実質的に `1` になります。この値を増やすと、クエリごとに割り当てられるスロットが減少し（より少ない、より大きなスロットのクエリを優先）、減らすとクエリごとのスロットが増加します。並列性とリソースの粒度を制御するために、`query_queue_v2_num_rows_per_slot` および並列性設定と合わせて調整してください。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_num_rows_per_slot`

- Default: 4096
- Type: Int
- Unit: 行
- Is mutable: Yes
- Description: クエリごとのスロット数を推定する際に、単一のスケジューリングスロットに割り当てられるソース行レコードの目標数。StarRocksは `estimated_slots` = (ソースノードのカーディナリティ) / `query_queue_v2_num_rows_per_slot` を計算し、結果を [1, totalSlots] の範囲にクランプし、計算値が非正の場合は最小値1を強制します。`totalSlots` は利用可能なリソース（おおよそ `DOP` * `query_queue_v2_concurrency_level` * ワーカー/`BE` の数）から導出されるため、クラスター/コア数に依存します。この値を増やすと、スロット数が減少し（各スロットがより多くの行を処理）、スケジューリングオーバーヘッドが低減されます。減らすと、リソース制限まで並列性が増加します（より多くの、より小さなスロット）。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_schedule_strategy`

- Default: SWRR
- Type: String
- Unit: -
- Is mutable: Yes
- Description: `Query Queue V2` が保留中のクエリを順序付けるために使用するスケジューリングポリシーを選択します。サポートされる値（大文字小文字を区別しない）は、`SWRR` (Smooth Weighted Round Robin) — デフォルトであり、公平な重み付き共有が必要な混合/ハイブリッドワークロードに適しています — と `SJF` (Short Job First + Aging) — 短いジョブを優先し、エージングを使用して飢餓状態を回避します — です。値は大文字小文字を区別しない列挙型ルックアップで解析されます。認識されない値はエラーとしてログに記録され、デフォルトポリシーが使用されます。この設定は、`Query Queue V2` が有効な場合にのみ動作に影響し、`query_queue_v2_concurrency_level` などの `V2` サイジング設定と相互作用します。
- Introduced in: v3.3.12, v3.4.2, v3.5.0

##### `semi_sync_collect_statistic_await_seconds`

- Default: 30
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: `DML` 操作（`INSERT INTO` および `INSERT OVERWRITE` ステートメント）中の半同期統計収集の最大待機時間。`Stream Load` および `Broker Load` は非同期モードを使用するため、この設定の影響を受けません。統計収集時間がこの値を超えると、ロード操作は収集の完了を待たずに続行されます。この設定は `enable_statistic_collect_on_first_load` と連携して機能します。
- Introduced in: v3.1

##### `slow_query_analyze_threshold`

- Default: 5
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: クエリフィードバックの分析をトリガーするためのクエリの実行時間しきい値。
- Introduced in: v3.4.0

##### `statistic_analyze_status_keep_second`

- Default: 3 * 24 * 3600
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: 収集タスクの履歴を保持する期間。デフォルト値は3日間です。
- Introduced in: -

##### `statistic_auto_analyze_end_time`

- Default: 23:59:59
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 自動収集の終了時刻。値の範囲: `00:00:00` - `23:59:59`。
- Introduced in: -

##### `statistic_auto_analyze_start_time`

- Default: 00:00:00
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 自動収集の開始時刻。値の範囲: `00:00:00` - `23:59:59`。
- Introduced in: -

##### `statistic_auto_collect_ratio`

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: 自動収集の統計が健全であるかどうかを判断するためのしきい値。統計の健全性がこのしきい値を下回ると、自動収集がトリガーされます。
- Introduced in: -

##### `statistic_auto_collect_small_table_rows`

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: 自動収集中に、外部データソース（`Hive`、`Iceberg`、`Hudi`）内のテーブルが小規模テーブルであるかどうかを判断するためのしきい値。テーブルの行数がこの値より少ない場合、そのテーブルは小規模テーブルと見なされます。
- Introduced in: v3.2

##### `statistic_cache_columns`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: No
- Description: 統計テーブルのためにキャッシュできる行数。
- Introduced in: -

##### `statistic_cache_thread_pool_size`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: 統計キャッシュを更新するために使用されるスレッドプールのサイズ。
- Introduced in: -

##### `statistic_collect_interval_sec`

- Default: 5 * 60
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: 自動収集中のデータ更新をチェックする間隔。
- Introduced in: -

##### `statistic_max_full_collect_data_size`

- Default: 100 * 1024 * 1024 * 1024
- Type: Long
- Unit: バイト
- Is mutable: Yes
- Description: 統計の自動収集におけるデータサイズしきい値。合計サイズがこの値を超えると、完全収集ではなくサンプリング収集が実行されます。
- Introduced in: -

##### `statistic_sample_collect_rows`

- Default: 200000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ロードによってトリガーされる統計操作中に、`SAMPLE` 統計収集と `FULL` 統計収集のどちらを選択するかを決定するための行数しきい値。ロードまたは変更された行数がこのしきい値（デフォルト200,000）を超えると、`SAMPLE` 統計収集が使用されます。それ以外の場合は、`FULL` 統計収集が使用されます。この設定は、`enable_statistic_collect_on_first_load` および `statistic_sample_collect_ratio_threshold_of_first_load` と連携して機能します。
- Introduced in: -

##### `statistic_update_interval_sec`

- Default: 24 * 60 * 60
- Type: Long
- Unit: 秒
- Is mutable: Yes
- Description: 統計情報のキャッシュが更新される間隔。
- Introduced in: -

##### `task_check_interval_second`

- Default: 60
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: タスクバックグラウンドジョブの実行間隔。`GlobalStateMgr` はこの値を使用して、`doTaskBackgroundJob()` を呼び出す `TaskCleaner FrontendDaemon` をスケジュールします。この値は1000倍されて、デーモン間隔がミリ秒単位で設定されます。値を減らすと、バックグラウンドメンテナンス（タスククリーンアップ、チェック）がより頻繁に実行され、より迅速に反応しますが、CPU/IOオーバーヘッドが増加します。値を増やすと、オーバーヘッドは減少しますが、クリーンアップと古いタスクの検出が遅れます。この値を調整して、メンテナンスの応答性とリソース使用量のバランスを取ってください。
- Introduced in: v3.2.0

##### `task_min_schedule_interval_s`

- Default: 10
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: `SQL` レイヤーによってチェックされるタスクスケジュールの最小許容スケジュール間隔（秒単位）。タスクが送信されると、`TaskAnalyzer` はスケジュール期間を秒に変換し、期間が `task_min_schedule_interval_s` より小さい場合、`ERR_INVALID_PARAMETER` で送信を拒否します。これにより、頻繁に実行されるタスクの作成が防止され、スケジューラが高頻度タスクから保護されます。スケジュールに明示的な開始時刻がない場合、`TaskAnalyzer` は開始時刻を現在のエポック秒に設定します。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `task_runs_timeout_second`

- Default: 4 * 3600
- Type: Int
- Unit: 秒
- Is mutable: Yes
- Description: `TaskRun` のデフォルトの実行タイムアウト（秒単位）。この項目は、`TaskRun` 実行のベースラインタイムアウトとして使用されます。タスク実行のプロパティに、正の整数値を持つセッション変数 `query_timeout` または `insert_timeout` が含まれている場合、ランタイムは、そのセッションタイムアウトと `task_runs_timeout_second` のうち大きい方の値を使用します。実効タイムアウトは、設定された `task_runs_ttl_second` および `task_ttl_second` を超えないように制限されます。この項目を設定して、タスク実行が実行できる期間を制限します。非常に大きな値は、タスク/タスク実行の `TTL` 設定によって切り詰められる場合があります。
- Introduced in: -

### ロードとアンロード

##### `broker_load_default_timeout_second`

- デフォルト: 14400
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: Broker Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### `desired_max_waiting_jobs`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: FE 内の保留中のジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FE 内の保留中のジョブの数がこの値に達すると、FE は新しいロードリクエストを拒否します。このパラメーターは、非同期ロードにのみ有効です。v2.5 以降、デフォルト値は 100 から 1024 に変更されました。
- 導入バージョン: -

##### `disable_load_job`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: クラスターがエラーに遭遇したときにロードを無効にするかどうか。これにより、クラスターエラーによって引き起こされる損失を防ぎます。デフォルト値は `FALSE` で、ロードが無効になっていないことを示します。`TRUE` はロードが無効になっており、クラスターが読み取り専用状態であることを示します。
- 導入バージョン: -

##### `empty_load_as_error`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: データがロードされなかった場合に、「すべてのパーティションにロードデータがありません」というエラーメッセージを返すかどうか。有効な値:
  - `true`: データがロードされなかった場合、システムは失敗メッセージを表示し、「すべてのパーティションにロードデータがありません」というエラーを返します。
  - `false`: データがロードされなかった場合、システムは成功メッセージを表示し、エラーではなく OK を返します。
- 導入バージョン: -

##### `enable_file_bundling`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: クラウドネイティブテーブルの File Bundling 最適化を有効にするかどうか。この機能が有効になっている場合 (`true` に設定されている場合)、システムはロード、Compaction、または Publish 操作によって生成されたデータファイルを自動的にバンドルし、外部ストレージシステムへの高頻度アクセスによって発生する API コストを削減します。この動作は、CREATE TABLE プロパティ `file_bundling` を使用してテーブルレベルで制御することもできます。詳細な手順については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。
- 導入バージョン: v4.0

##### `enable_routine_load_lag_metrics`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: Routine Load Kafka パーティションオフセットラグメトリクスを収集するかどうか。この項目を `true` に設定すると、Kafka API を呼び出してパーティションの最新オフセットを取得することに注意してください。
- 導入バージョン: -

##### `enable_sync_publish`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: Yes
- 説明: ロードトランザクションのパブリッシュフェーズで適用タスクを同期的に実行するかどうか。このパラメーターは Primary Key テーブルにのみ適用されます。有効な値:
  - `TRUE` (デフォルト): ロードトランザクションのパブリッシュフェーズで適用タスクが同期的に実行されます。これは、適用タスクが完了した後にのみロードトランザクションが成功として報告され、ロードされたデータが実際にクエリ可能になることを意味します。タスクが一度に大量のデータをロードする場合、または頻繁にデータをロードする場合、このパラメーターを `true` に設定すると、クエリのパフォーマンスと安定性が向上しますが、ロードのレイテンシーが増加する可能性があります。
  - `FALSE`: ロードトランザクションのパブリッシュフェーズで適用タスクが非同期的に実行されます。これは、適用タスクが送信された後にロードトランザクションが成功として報告されますが、ロードされたデータはすぐにクエリできないことを意味します。この場合、同時クエリは、適用タスクが完了するかタイムアウトするまで待機してから続行する必要があります。タスクが一度に大量のデータをロードする場合、または頻繁にデータをロードする場合、このパラメーターを `false` に設定すると、クエリのパフォーマンスと安定性に影響を与える可能性があります。
- 導入バージョン: v3.2.0

##### `export_checker_interval_second`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: No
- 説明: ロードジョブがスケジュールされる時間間隔。
- 導入バージョン: -

##### `export_max_bytes_per_be_per_task`

- デフォルト: 268435456
- タイプ: Long
- 単位: バイト
- 変更可能: Yes
- 説明: 単一のデータアンロードタスクによって単一の BE からエクスポートできるデータの最大量。
- 導入バージョン: -

##### `export_running_job_num_limit`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: 並行して実行できるデータエクスポートタスクの最大数。
- 導入バージョン: -

##### `export_task_default_timeout_second`

- デフォルト: 2 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: データエクスポートタスクのタイムアウト期間。
- 導入バージョン: -

##### `export_task_pool_size`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: No
- 説明: アンロードタスクスレッドプールのサイズ。
- 導入バージョン: -

##### `external_table_commit_timeout_ms`

- デフォルト: 10000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: Yes
- 説明: StarRocks 外部テーブルへの書き込みトランザクションをコミット (パブリッシュ) する際のタイムアウト期間。デフォルト値 `10000` は 10 秒のタイムアウト期間を示します。
- 導入バージョン: -

##### `finish_transaction_default_lock_timeout_ms`

- デフォルト: 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: Yes
- 説明: トランザクション完了中に DB およびテーブルロックを取得するためのデフォルトのタイムアウト。
- 導入バージョン: v4.0.0, v3.5.8

##### `history_job_keep_max_second`

- デフォルト: 7 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: スキーマ変更ジョブなど、履歴ジョブを保持できる最大期間。
- 導入バージョン: -

##### `insert_load_default_timeout_second`

- デフォルト: 3600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: データをロードするために使用される INSERT INTO ステートメントのタイムアウト期間。
- 導入バージョン: -

##### `label_clean_interval_second`

- デフォルト: 4 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: No
- 説明: ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### `label_keep_max_num`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: 一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報は削除されます。
- 導入バージョン: -

##### `label_keep_max_second`

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: 完了し、FINISHED または CANCELLED 状態にあるロードジョブのラベルを保持する最大期間 (秒単位)。デフォルト値は 3 日です。この期間が経過すると、ラベルは削除されます。このパラメーターは、すべての種類のロードジョブに適用されます。値が大きすぎると、多くのメモリを消費します。
- 導入バージョン: -

##### `load_checker_interval_second`

- デフォルト: 5
- タイプ: Int
- 単位: 秒
- 変更可能: No
- 説明: ロードジョブがローリングベースで処理される時間間隔。
- 導入バージョン: -

##### `load_parallel_instance_num`

- デフォルト: 1
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: Broker および Stream Load の単一ホストで作成される並列ロードフラグメントインスタンスの数を制御します。セッションが適応型シンク DOP を有効にしない限り、LoadPlanner はこの値をホストごとの並列度として使用します。セッション変数 `enable_adaptive_sink_dop` が true の場合、セッションの `sink_degree_of_parallelism` がこの設定を上書きします。シャッフルが必要な場合、この値はフラグメントの並列実行 (スキャンフラグメントとシンクフラグメントの並列実行インスタンス) に適用されます。シャッフルが不要な場合、シンクパイプライン DOP として使用されます。注: ローカルディスクの競合を避けるため、ローカルファイルからのロードは単一インスタンス (パイプライン DOP = 1、並列実行 = 1) に強制されます。この数を増やすと、ホストごとの同時実行性とスループットが向上しますが、CPU、メモリ、I/O の競合が増加する可能性があります。
- 導入バージョン: v3.2.0

##### `load_straggler_wait_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: BE レプリカが許容できる最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするためにクローニングが実行されます。
- 導入バージョン: -

##### `loads_history_retained_days`

- デフォルト: 30
- タイプ: Int
- 単位: 日
- 変更可能: Yes
- 説明: 内部 `_statistics_.loads_history` テーブルにロード履歴を保持する日数。この値は、テーブル作成時にテーブルプロパティ `partition_live_number` を設定するために使用され、`TableKeeper` (最小 1 にクランプ) に渡されて、保持する日次パーティションの数を決定します。この値を増減すると、完了したロードジョブが日次パーティションに保持される期間が調整されます。これは新しいテーブル作成とキーパーのプルーニング動作に影響しますが、過去のパーティションを自動的に再作成することはありません。`LoadsHistorySyncer` は、ロード履歴のライフサイクルを管理する際にこの保持に依存します。その同期頻度は `loads_history_sync_interval_second` によって制御されます。
- 導入バージョン: v3.3.6, v3.4.0, v3.5.0

##### `loads_history_sync_interval_second`

- デフォルト: 60
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: LoadsHistorySyncer が `information_schema.loads` から内部 `_statistics_.loads_history` テーブルへの完了したロードジョブの定期的な同期をスケジュールするために使用する間隔 (秒単位)。この値はコンストラクタで 1000 倍され、FrontendDaemon の間隔を設定します。シンクロナイザーは最初の実行をスキップし (テーブル作成を許可するため)、1 分以上前に完了したロードのみをインポートします。小さい値は DML とエグゼキュータの負荷を増加させ、大きい値は履歴ロードレコードの可用性を遅らせます。ターゲットテーブルの保持/パーティショニング動作については、`loads_history_retained_days` を参照してください。
- 導入バージョン: v3.3.6, v3.4.0, v3.5.0

##### `max_broker_load_job_concurrency`

- デフォルト: 5
- エイリアス: `async_load_task_pool_size`
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: StarRocks クラスター内で許可される同時 Broker Load ジョブの最大数。このパラメーターは Broker Load にのみ有効です。このパラメーターの値は、`max_running_txn_num_per_db` の値よりも小さくなければなりません。v2.5 以降、デフォルト値は `10` から `5` に変更されました。
- 導入バージョン: -

##### `max_load_timeout_second`

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限は、すべての種類のロードジョブに適用されます。
- 導入バージョン: -

##### `max_routine_load_batch_size`

- デフォルト: 4294967296
- タイプ: Long
- 単位: バイト
- 変更可能: Yes
- 説明: Routine Load タスクによってロードできるデータの最大量。
- 導入バージョン: -

##### `max_routine_load_task_concurrent_num`

- デフォルト: 5
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: 各 Routine Load ジョブの同時タスクの最大数。
- 導入バージョン: -

##### `max_routine_load_task_num_per_be`

- デフォルト: 16
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: 各 BE での同時 Routine Load タスクの最大数。v3.1.0 以降、このパラメーターのデフォルト値は 5 から 16 に増加し、BE 静的パラメーター `routine_load_thread_pool_size` (非推奨) の値以下である必要はなくなりました。
- 導入バージョン: -

##### `max_running_txn_num_per_db`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: StarRocks クラスター内の各データベースで実行が許可されるロードトランザクションの最大数。デフォルト値は `1000` です。v3.1 以降、デフォルト値は `100` から `1000` に変更されました。データベースで実行中のロードトランザクションの実際の数がこのパラメーターの値を超えると、新しいロードリクエストは処理されません。同期ロードジョブの新しいリクエストは拒否され、非同期ロードジョブの新しいリクエストはキューに入れられます。このパラメーターの値を増やすとシステム負荷が増加するため、増やすことはお勧めしません。
- 導入バージョン: -

##### `max_stream_load_timeout_second`

- デフォルト: 259200
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: Stream Load ジョブに許可される最大タイムアウト期間。
- 導入バージョン: -

##### `max_tolerable_backend_down_num`

- デフォルト: 0
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: 許容される障害のある BE ノードの最大数。この数を超えると、Routine Load ジョブは自動的に回復できません。
- 導入バージョン: -

##### `min_bytes_per_broker_scanner`

- デフォルト: 67108864
- タイプ: Long
- 単位: バイト
- 変更可能: Yes
- 説明: Broker Load インスタンスによって処理できるデータの最小許容量。
- 導入バージョン: -

##### `min_load_timeout_second`

- デフォルト: 1
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: ロードジョブに許可される最小タイムアウト期間。この制限は、すべての種類のロードジョブに適用されます。
- 導入バージョン: -

##### `min_routine_load_lag_for_metrics`

- デフォルト: 10000
- タイプ: INT
- 単位: -
- 変更可能: Yes
- 説明: 監視メトリクスに表示される Routine Load ジョブの最小オフセットラグ。オフセットラグがこの値よりも大きい Routine Load ジョブは、メトリクスに表示されます。
- 導入バージョン: -

##### `period_of_auto_resume_min`

- デフォルト: 5
- タイプ: Int
- 単位: 分
- 変更可能: Yes
- 説明: Routine Load ジョブが自動的に回復される間隔。
- 導入バージョン: -

##### `prepared_transaction_default_timeout_second`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: 準備済みトランザクションのデフォルトのタイムアウト期間。
- 導入バージョン: -

##### `routine_load_task_consume_second`

- デフォルト: 15
- タイプ: Long
- 単位: 秒
- 変更可能: Yes
- 説明: クラスター内の各 Routine Load タスクがデータを消費する最大時間。v3.1.0 以降、Routine Load ジョブは [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメーター `task_consume_second` をサポートしています。このパラメーターは、Routine Load ジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### `routine_load_task_timeout_second`

- デフォルト: 60
- タイプ: Long
- 単位: 秒
- 変更可能: Yes
- 説明: クラスター内の各 Routine Load タスクのタイムアウト期間。v3.1.0 以降、Routine Load ジョブは [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメーター `task_timeout_second` をサポートしています。このパラメーターは、Routine Load ジョブ内の個々のロードタスクに適用され、より柔軟です。
- 導入バージョン: -

##### `routine_load_unstable_threshold_second`

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: Yes
- 説明: Routine Load ジョブ内のいずれかのタスクが遅延した場合、Routine Load ジョブは UNSTABLE 状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在の時刻との差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。
- 導入バージョン: -

##### `spark_dpp_version`

- デフォルト: 1.0.0
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: 使用される Spark Dynamic Partition Pruning (DPP) のバージョン。
- 導入バージョン: -

##### `spark_home_default_dir`

- デフォルト: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: Spark クライアントのルートディレクトリ。
- 導入バージョン: -

##### `spark_launcher_log_dir`

- デフォルト: `sys_log_dir` + "/spark_launcher_log"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: Spark ログファイルを保存するディレクトリ。
- 導入バージョン: -

##### `spark_load_default_timeout_second`

- デフォルト: 86400
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: 各 Spark Load ジョブのタイムアウト期間。
- 導入バージョン: -

##### `spark_load_submit_timeout_second`

- デフォルト: 300
- タイプ: long
- 単位: 秒
- 変更可能: No
- 説明: Spark アプリケーションを送信した後、YARN の応答を待機する最大時間 (秒単位)。`SparkLauncherMonitor.LogMonitor` はこの値をミリ秒に変換し、ジョブがこのタイムアウトよりも長く UNKNOWN/CONNECTED/SUBMITTED 状態のままである場合、監視を停止し、Spark ランチャープロセスを強制終了します。`SparkLoadJob` はこの設定をデフォルトとして読み取り、`LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` プロパティを介してロードごとの上書きを許可します。YARN のキューイング遅延に対応できる十分な高さに設定してください。低すぎると正当にキューに入れられたジョブが中止される可能性があり、高すぎると障害処理とリソースクリーンアップが遅れる可能性があります。
- 導入バージョン: v3.2.0

##### `spark_resource_path`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: Spark 依存関係パッケージのルートディレクトリ。
- 導入バージョン: -

##### `stream_load_default_timeout_second`

- デフォルト: 600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: 各 Stream Load ジョブのデフォルトのタイムアウト期間。
- 導入バージョン: -

##### `stream_load_max_txn_num_per_be`

- デフォルト: -1
- タイプ: Int
- 単位: トランザクション
- 変更可能: Yes
- 説明: 単一の BE (バックエンド) ホストから受け入れられる同時ストリームロードトランザクションの数を制限します。非負の整数に設定すると、FrontendServiceImpl は BE (クライアント IP 別) の現在のトランザクション数をチェックし、カウントがこの制限 `>=` を超える場合、新しいストリームロード開始リクエストを拒否します。`< 0` の値は制限を無効にします (無制限)。このチェックはストリームロード開始時に発生し、超過すると `streamload txn num per be exceeds limit` エラーを引き起こす可能性があります。関連するランタイム動作では、リクエストタイムアウトのフォールバックに `stream_load_default_timeout_second` を使用します。
- 導入バージョン: v3.3.0, v3.4.0, v3.5.0

##### `stream_load_task_keep_max_num`

- デフォルト: 1000
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: StreamLoadMgr がメモリに保持する Stream Load タスクの最大数 (すべてのデータベースでグローバル)。追跡されるタスク (`idToStreamLoadTask`) の数がこのしきい値を超えると、StreamLoadMgr はまず `cleanSyncStreamLoadTasks()` を呼び出して完了した同期ストリームロードタスクを削除します。サイズがこのしきい値の半分よりも大きいままである場合、`cleanOldStreamLoadTasks(true)` を呼び出して、古いタスクまたは完了したタスクの削除を強制します。この値を増やすと、より多くのタスク履歴をメモリに保持できます。減らすと、メモリ使用量が削減され、クリーンアップがより積極的になります。この値はメモリ内保持のみを制御し、永続化/リプレイされたタスクには影響しません。
- 導入バージョン: v3.2.0

##### `stream_load_task_keep_max_second`

- デフォルト: 3 * 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: 完了またはキャンセルされた Stream Load タスクの保持期間。タスクが最終状態に達し、その終了タイムスタンプがこのしきい値 (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`) よりも早い場合、`StreamLoadMgr.cleanOldStreamLoadTasks` による削除の対象となり、永続化された状態をロードする際に破棄されます。`StreamLoadTask` と `StreamLoadMultiStmtTask` の両方に適用されます。合計タスク数が `stream_load_task_keep_max_num` を超える場合、クリーンアップが早期にトリガーされる可能性があります (同期タスクは `cleanSyncStreamLoadTasks` によって優先されます)。履歴/デバッグ可能性とメモリ使用量のバランスを取るためにこれを設定してください。
- 導入バージョン: v3.2.0

##### `transaction_clean_interval_second`

- デフォルト: 30
- タイプ: Int
- 単位: 秒
- 変更可能: No
- 説明: 完了したトランザクションがクリーンアップされる時間間隔。単位: 秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- 導入バージョン: -

##### `transaction_stream_load_coordinator_cache_capacity`

- デフォルト: 4096
- タイプ: Int
- 単位: -
- 変更可能: Yes
- 説明: トランザクションラベルからコーディネーターノードへのマッピングを格納するキャッシュの容量。
- 導入バージョン: -

##### `transaction_stream_load_coordinator_cache_expire_seconds`

- デフォルト: 900
- タイプ: Int
- 単位: 秒
- 変更可能: Yes
- 説明: コーディネーターマッピングがキャッシュから削除されるまでの保持時間 (TTL)。
- 導入バージョン: -

##### `yarn_client_path`

- デフォルト: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: Yarn クライアントパッケージのルートディレクトリ。
- 導入バージョン: -

##### `yarn_config_dir`

- デフォルト: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- タイプ: String
- 単位: -
- 変更可能: No
- 説明: Yarn 設定ファイルを保存するディレクトリ。
- 導入バージョン: -

### 統計レポート

##### `enable_collect_warehouse_metrics`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: この項目が`true`に設定されている場合、システムはウェアハウスごとのメトリクスを収集し、エクスポートします。これを有効にすると、ウェアハウスレベルのメトリクス（スロット/使用量/可用性）がメトリクス出力に追加され、メトリクスのカーディナリティと収集オーバーヘッドが増加します。無効にすると、ウェアハウス固有のメトリクスが省略され、CPU/ネットワークおよび監視ストレージのコストが削減されます。
- Introduced in: v3.5.0

##### `enable_http_detail_metrics`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: はい
- Description: `true`の場合、HTTPサーバーは詳細なHTTPワーカーメトリクス（特に`HTTP_WORKER_PENDING_TASKS_NUM`ゲージ）を計算し、公開します。これを有効にすると、サーバーはNettyワーカーエグゼキュータを反復処理し、各`NioEventLoop`で`pendingTasks()`を呼び出して保留中のタスク数を合計します。無効にすると、そのコストを避けるためにゲージは0を返します。この追加の収集はCPUおよびレイテンシに影響を与える可能性があるため、デバッグまたは詳細な調査のためにのみ有効にしてください。
- Introduced in: v3.2.3

##### `proc_profile_collect_time_s`

- Default: 120
- Type: Int
- Unit: 秒
- Is mutable: はい
- Description: 単一のプロセスプロファイル収集の期間を秒単位で指定します。`proc_profile_cpu_enable`または`proc_profile_mem_enable`が`true`に設定されている場合、AsyncProfilerが開始され、コレクタースレッドはこの期間スリープし、その後プロファイラーが停止されてプロファイルが書き込まれます。値が大きいほどサンプルカバレッジとファイルサイズは増加しますが、プロファイラーの実行時間が長くなり、後続の収集が遅れます。値が小さいほどオーバーヘッドは減少しますが、十分なサンプルが得られない可能性があります。この値が`proc_profile_file_retained_days`や`proc_profile_file_retained_size_bytes`などの保持設定と一致していることを確認してください。
- Introduced in: v3.2.12

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
- 単位: 割合 (0.0–1.0)
- 変更可能: はい
- 説明: バックエンドの負荷スコアを計算する際に使用される、ディスク使用容量の割合（総容量に対する割合）のハイウォーターしきい値です。`BackendLoadStatistic.calcSore` は `capacity_used_percent_high_water` を使用して `LoadScore.capacityCoefficient` を設定します。バックエンドの使用率が 0.5 未満の場合、係数は 0.5 になります。使用率が `capacity_used_percent_high_water` を超える場合、係数は 1.0 になります。それ以外の場合、係数は (2 * usedPercent - 0.5) を介して使用率と線形に変化します。係数が 1.0 の場合、負荷スコアは完全に容量の割合によって決定されます。値が低いほど、レプリカ数の重みが増加します。この値を調整すると、バランサーが高ディスク使用率のバックエンドにペナルティを課す積極性が変わります。
- 導入バージョン: v3.2.0

##### `catalog_trash_expire_second`

- デフォルト: 86400
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: データベース、テーブル、またはパーティションが削除された後、メタデータを保持できる最長期間です。この期間が経過すると、データは削除され、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) コマンドで復元することはできません。
- 導入バージョン: -

##### `check_consistency_default_timeout_second`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカの一貫性チェックのタイムアウト期間です。このパラメータは、タブレットのサイズに基づいて設定できます。
- 導入バージョン: -

##### `consistency_check_cooldown_time_second`

- デフォルト: 24 * 3600
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: 同じタブレットの一貫性チェック間に必要な最小間隔（秒単位）を制御します。タブレット選択中、`tablet.getLastCheckTime()` が `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` より小さい場合にのみ、タブレットは適格と見なされます。デフォルト値 (24 * 3600) は、バックエンドのディスク I/O を削減するために、1日あたり約1回のタブレットチェックを強制します。この値を下げるとチェック頻度とリソース使用量が増加し、上げると一貫性の検出が遅くなる代わりに I/O が減少します。この値は、インデックスのタブレットリストからクールダウンされたタブレットをフィルタリングする際にグローバルに適用されます。
- 導入バージョン: v3.5.5

##### `consistency_check_end_time`

- デフォルト: "4"
- タイプ: String
- 単位: 時刻 (0-23)
- 変更可能: いいえ
- 説明: ConsistencyChecker の作業ウィンドウの終了時刻（時）を指定します。この値はシステムタイムゾーンで SimpleDateFormat("HH") を使用して解析され、0～23（1桁または2桁）として受け入れられます。StarRocks は `consistency_check_start_time` とともにこれを使用して、一貫性チェックジョブをいつスケジュールし、追加するかを決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます（例：デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4"）。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると、FE の起動時にエラーがログに記録され、終了するため、有効な時刻文字列を指定してください。
- 導入バージョン: v3.2.0

##### `consistency_check_start_time`

- デフォルト: "23"
- タイプ: String
- 単位: 時刻 (00-23)
- 変更可能: いいえ
- 説明: ConsistencyChecker の作業ウィンドウの開始時刻（時）を指定します。この値はシステムタイムゾーンで SimpleDateFormat("HH") を使用して解析され、0～23（1桁または2桁）として受け入れられます。StarRocks は `consistency_check_end_time` とともにこれを使用して、一貫性チェックジョブをいつスケジュールし、追加するかを決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます（例：デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4"）。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると、FE の起動時にエラーがログに記録され、終了するため、有効な時刻文字列を指定してください。
- 導入バージョン: v3.2.0

##### `consistency_tablet_meta_check_interval_ms`

- デフォルト: 2 * 3600 * 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: ConsistencyChecker が `TabletInvertedIndex` と `LocalMetastore` の間で完全なタブレットメタ一貫性スキャンを実行するために使用する間隔です。`runAfterCatalogReady` のデーモンは、`current time - lastTabletMetaCheckTime` がこの値を超えたときに `checkTabletMetaConsistency` をトリガーします。無効なタブレットが最初に検出されると、その `toBeCleanedTime` は `now + (consistency_tablet_meta_check_interval_ms / 2)` に設定され、実際の削除は後続のスキャンまで遅延されます。この値を増やすとスキャン頻度と負荷が減少し（クリーンアップが遅くなる）、減らすと古いタブレットの検出と削除が速くなります（オーバーヘッドが増加する）。
- 導入バージョン: v3.2.0

##### `default_replication_num`

- デフォルト: 3
- タイプ: Short
- 単位: -
- 変更可能: はい
- 説明: StarRocks でテーブルを作成する際に、各データパーティションのデフォルトのレプリカ数を設定します。この設定は、CREATE TABLE DDL で `replication_num=x` を指定することで、テーブル作成時に上書きできます。
- 導入バージョン: -

##### `enable_auto_tablet_distribution`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: バケット数を自動的に設定するかどうか。
  - このパラメータが `TRUE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際に、バケット数を指定する必要はありません。StarRocks が自動的にバケット数を決定します。
  - このパラメータが `FALSE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際に、バケット数を手動で指定する必要があります。テーブルに新しいパーティションを追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションのバケット数を手動で指定することもできます。
- 導入バージョン: v2.5.7

##### `enable_experimental_rowstore`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: [ハイブリッド行・列ストレージ](../../table_design/hybrid_table.md) 機能を有効にするかどうか。
- 導入バージョン: v3.2.3

##### `enable_fast_schema_evolution`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: StarRocks クラスター内のすべてのテーブルで高速スキーマ進化を有効にするかどうか。有効な値は `TRUE` と `FALSE`（デフォルト）です。高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加または削除時のリソース使用量を削減できます。
- 導入バージョン: v3.2.0

> **注記**
>
> - StarRocks shared-data クラスターは、v3.3.0 以降でこのパラメータをサポートしています。
> - 特定のテーブルに対して高速スキーマ進化を設定する必要がある場合（例：特定のテーブルの高速スキーマ進化を無効にする場合）、テーブル作成時にテーブルプロパティ [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) を設定できます。

##### `enable_online_optimize_table`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 最適化ジョブを作成する際に、StarRocks が非ブロッキングのオンライン最適化パスを使用するかどうかを制御します。`enable_online_optimize_table` が true で、ターゲットテーブルが互換性チェック（パーティション/キー/ソート指定なし、ディストリビューションが `RandomDistributionDesc` ではない、ストレージタイプが `COLUMN_WITH_ROW` ではない、レプリケートストレージが有効、かつテーブルがクラウドネイティブテーブルまたはマテリアライズドビューではない）を満たす場合、プランナーは書き込みをブロックせずに最適化を実行するために `OnlineOptimizeJobV2` を作成します。false の場合、または互換性条件のいずれかが失敗した場合、StarRocks は `OptimizeJobV2` にフォールバックし、最適化中に書き込み操作をブロックする可能性があります。
- 導入バージョン: v3.3.3, v3.4.0, v3.5.0

##### `enable_strict_storage_medium_check`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: ユーザーがテーブルを作成する際に、FE が BE のストレージメディアを厳密にチェックするかどうか。このパラメータが `TRUE` に設定されている場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックし、BE のストレージメディアが CREATE TABLE ステートメントで指定された `storage_medium` パラメータと異なる場合、エラーを返します。例えば、CREATE TABLE ステートメントで指定されたストレージメディアが SSD であっても、BE の実際のストレージメディアが HDD の場合、テーブル作成は失敗します。このパラメータが `FALSE` の場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックしません。
- 導入バージョン: -

##### `max_bucket_number_per_partition`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 1つのパーティションで作成できるバケットの最大数。
- 導入バージョン: v3.3.2

##### `max_column_number_per_table`

- デフォルト: 10000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 1つのテーブルで作成できる列の最大数。
- 導入バージョン: v3.3.2

##### `max_dynamic_partition_num`

- デフォルト: 500
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 動的パーティションテーブルを分析または作成する際に、一度に作成できるパーティションの最大数を制限します。動的パーティションプロパティの検証中、`systemtask_runs_max_history_number` は予想されるパーティション数（終了オフセット + 履歴パーティション数）を計算し、その合計が `max_dynamic_partition_num` を超える場合、DDL エラーをスローします。正当に大きなパーティション範囲を期待する場合にのみこの値を増やしてください。増やすとより多くのパーティションを作成できますが、メタデータサイズ、スケジューリング作業、および運用上の複雑さが増加する可能性があります。
- 導入バージョン: v3.2.0

##### `max_partition_number_per_table`

- デフォルト: 100000
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 1つのテーブルで作成できるパーティションの最大数。
- 導入バージョン: v3.3.2

##### `max_task_consecutive_fail_count`

- デフォルト: 10
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: スケジューラがタスクを自動的に中断するまでに、タスクが連続して失敗する可能性のある最大回数。`TaskSource.MV.equals(task.getSource())` であり、`max_task_consecutive_fail_count` が 0 より大きい場合、タスクの連続失敗カウンタが `max_task_consecutive_fail_count` に達するか超えると、タスクは TaskManager を介して中断され、マテリアライズドビュータスクの場合、マテリアライズドビューは非アクティブ化されます。中断と再アクティブ化の方法（例：`ALTER MATERIALIZED VIEW <mv_name> ACTIVE`）を示す例外がスローされます。自動中断を無効にするには、この項目を 0 または負の値に設定します。
- 導入バージョン: -

##### `partition_recycle_retention_period_secs`

- デフォルト: 1800
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: INSERT OVERWRITE またはマテリアライズドビューの更新操作によって削除されたパーティションのメタデータ保持期間です。このようなメタデータは、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) を実行しても復元できないことに注意してください。
- 導入バージョン: v3.5.9

##### `recover_with_empty_tablet`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: 紛失または破損したタブレットレプリカを空のレプリカに置き換えるかどうか。タブレットレプリカが紛失または破損した場合、このタブレットまたは他の正常なタブレットに対するデータクエリが失敗する可能性があります。紛失または破損したタブレットレプリカを空のタブレットに置き換えることで、クエリは引き続き実行できます。ただし、データが失われるため、結果が正しくない可能性があります。デフォルト値は `FALSE` であり、紛失または破損したタブレットレプリカは空のレプリカに置き換えられず、クエリは失敗します。
- 導入バージョン: -

##### `storage_usage_hard_limit_percent`

- デフォルト: 95
- エイリアス: `storage_flood_stage_usage_percent`
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ディレクトリ内のストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、かつ残りのストレージスペースが `storage_usage_hard_limit_reserve_bytes` 未満の場合、ロードおよびリストアジョブは拒否されます。この項目は、BE 設定項目 `storage_flood_stage_usage_percent` とともに設定して、設定を有効にする必要があります。
- 導入バージョン: -

##### `storage_usage_hard_limit_reserve_bytes`

- デフォルト: 100 * 1024 * 1024 * 1024
- エイリアス: `storage_flood_stage_left_capacity_bytes`
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BE ディレクトリ内の残りのストレージスペースのハードリミット。BE ストレージディレクトリ内の残りのストレージスペースがこの値より少なく、かつストレージ使用率（パーセンテージ）が `storage_usage_hard_limit_percent` を超える場合、ロードおよびリストアジョブは拒否されます。この項目は、BE 設定項目 `storage_flood_stage_left_capacity_bytes` とともに設定して、設定を有効にする必要があります。
- 導入バージョン: -

##### `storage_usage_soft_limit_percent`

- デフォルト: 90
- エイリアス: `storage_high_watermark_usage_percent`
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ディレクトリ内のストレージ使用率のソフトリミット。BE ストレージディレクトリのストレージ使用率（パーセンテージ）がこの値を超え、かつ残りのストレージスペースが `storage_usage_soft_limit_reserve_bytes` 未満の場合、タブレットはこのディレクトリにクローンできません。
- 導入バージョン: -

##### `storage_usage_soft_limit_reserve_bytes`

- デフォルト: 200 * 1024 * 1024 * 1024
- エイリアス: `storage_min_left_capacity_bytes`
- タイプ: Long
- 単位: バイト
- 変更可能: はい
- 説明: BE ディレクトリ内の残りのストレージスペースのソフトリミット。BE ストレージディレクトリ内の残りのストレージスペースがこの値より少なく、かつストレージ使用率（パーセンテージ）が `storage_usage_soft_limit_percent` を超える場合、タブレットはこのディレクトリにクローンできません。
- 導入バージョン: -

##### `tablet_checker_lock_time_per_cycle_ms`

- デフォルト: 1000
- タイプ: Int
- 単位: ミリ秒
- 変更可能: はい
- 説明: タブレットチェッカーがテーブルロックを解放して再取得するまでの、サイクルあたりの最大ロック保持時間。100 未満の値は 100 として扱われます。
- 導入バージョン: v3.5.9, v4.0.2

##### `tablet_create_timeout_second`

- デフォルト: 10
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレット作成のタイムアウト期間。デフォルト値は v3.1 以降、1 から 10 に変更されました。
- 導入バージョン: -

##### `tablet_delete_timeout_second`

- デフォルト: 2
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: タブレット削除のタイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_balance_load_disk_safe_threshold`

- デフォルト: 0.5
- エイリアス: `balance_load_disk_safe_threshold`
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BE のディスク使用率がバランスしているかどうかを判断するためのパーセンテージしきい値。すべての BE のディスク使用率がこの値より低い場合、バランスしていると見なされます。ディスク使用率がこの値より大きく、かつ最高と最低の BE ディスク使用率の差が 10% を超える場合、ディスク使用率はアンバランスと見なされ、タブレットのリバランスがトリガーされます。
- 導入バージョン: -

##### `tablet_sched_balance_load_score_threshold`

- デフォルト: 0.1
- エイリアス: `balance_load_score_threshold`
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: BE の負荷がバランスしているかどうかを判断するためのパーセンテージしきい値。BE の負荷がすべての BE の平均負荷よりも低く、その差がこの値より大きい場合、この BE は低負荷状態にあります。逆に、BE の負荷が平均負荷よりも高く、その差がこの値より大きい場合、この BE は高負荷状態にあります。
- 導入バージョン: -

##### `tablet_sched_be_down_tolerate_time_s`

- デフォルト: 900
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: スケジューラが BE ノードが非アクティブ状態を維持することを許可する最大期間。この時間しきい値に達すると、その BE ノード上のタブレットは他のアクティブな BE ノードに移行されます。
- 導入バージョン: v2.5.7

##### `tablet_sched_disable_balance`

- デフォルト: false
- エイリアス: `disable_balance`
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: タブレットのバランシングを無効にするかどうか。`TRUE` はタブレットのバランシングが無効であることを示します。`FALSE` はタブレットのバランシングが有効であることを示します。
- 導入バージョン: -

##### `tablet_sched_disable_colocate_balance`

- デフォルト: false
- エイリアス: `disable_colocate_balance`
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Colocate Table のレプリカバランシングを無効にするかどうか。`TRUE` はレプリカバランシングが無効であることを示します。`FALSE` はレプリカバランシングが有効であることを示します。
- 導入バージョン: -

##### `tablet_sched_max_balancing_tablets`

- デフォルト: 500
- エイリアス: `max_balancing_tablets`
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にバランスできるタブレットの最大数。この値を超えると、タブレットのリバランスはスキップされます。
- 導入バージョン: -

##### `tablet_sched_max_clone_task_timeout_sec`

- デフォルト: 2 * 60 * 60
- エイリアス: `max_clone_task_timeout_sec`
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: タブレットをクローンする際の最大タイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_max_not_being_scheduled_interval_ms`

- デフォルト: 15 * 60 * 1000
- タイプ: Long
- 単位: ミリ秒
- 変更可能: はい
- 説明: タブレットクローンタスクがスケジュールされている際に、このパラメータで指定された時間、タブレットがスケジュールされていない場合、StarRocks はそのタブレットをできるだけ早くスケジュールするために高い優先度を与えます。
- 導入バージョン: -

##### `tablet_sched_max_scheduling_tablets`

- デフォルト: 10000
- エイリアス: `max_scheduling_tablets`
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: 同時にスケジュールできるタブレットの最大数。この値を超えると、タブレットのバランシングと修復チェックはスキップされます。
- 導入バージョン: -

##### `tablet_sched_min_clone_task_timeout_sec`

- デフォルト: 3 * 60
- エイリアス: `min_clone_task_timeout_sec`
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: タブレットをクローンする際の最小タイムアウト期間。
- 導入バージョン: -

##### `tablet_sched_num_based_balance_threshold_ratio`

- デフォルト: 0.5
- エイリアス: -
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: 数値ベースのバランシングを行うとディスクサイズのバランスが崩れる可能性がありますが、ディスク間の最大ギャップは `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold` を超えることはできません。クラスター内でタブレットが常に A から B、B から A へとバランシングを繰り返している場合、この値を減らしてください。タブレットの分散をよりバランスさせたい場合は、この値を増やしてください。
- 導入バージョン: - 3.1

##### `tablet_sched_repair_delay_factor_second`

- デフォルト: 60
- エイリアス: `tablet_repair_delay_factor_second`
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: レプリカが修復される間隔（秒単位）。
- 導入バージョン: -

##### `tablet_sched_slot_num_per_path`

- デフォルト: 8
- エイリアス: `schedule_slot_num_per_path`
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: BE ストレージディレクトリで同時に実行できるタブレット関連タスクの最大数。v2.5 以降、このパラメータのデフォルト値は `4` から `8` に変更されました。
- 導入バージョン: -

##### `tablet_sched_storage_cooldown_second`

- デフォルト: -1
- エイリアス: `storage_cooldown_second`
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: テーブル作成時から始まる自動冷却の遅延時間。デフォルト値 `-1` は、自動冷却が無効であることを指定します。自動冷却を有効にするには、このパラメータを `-1` より大きい値に設定してください。
- 導入バージョン: -

##### `tablet_stat_update_interval_second`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: いいえ
- 説明: FE が各 BE からタブレット統計を取得する時間間隔。
- 導入バージョン: -

### 共有データ

##### `aws_s3_access_key`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用されるアクセスキーID。
- 導入バージョン: v3.0

##### `aws_s3_endpoint`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用されるエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。
- 導入バージョン: v3.0

##### `aws_s3_external_id`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットへのクロスアカウントアクセスに使用されるAWSアカウントの外部ID。
- 導入バージョン: v3.0

##### `aws_s3_iam_role_arn`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データファイルが保存されているS3バケットに対する権限を持つIAMロールのARN。
- 導入バージョン: v3.0

##### `aws_s3_path`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるS3パス。S3バケットの名前と、その下のサブパス（もしあれば）で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.0

##### `aws_s3_region`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットが存在するリージョン。例: `us-west-2`。
- 導入バージョン: v3.0

##### `aws_s3_secret_key`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: S3バケットにアクセスするために使用されるシークレットアクセスキー。
- 導入バージョン: v3.0

##### `aws_s3_use_aws_sdk_default_behavior`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: AWS SDKのデフォルト認証情報を使用するかどうか。有効な値: trueおよびfalse（デフォルト）。
- 導入バージョン: v3.0

##### `aws_s3_use_instance_profile`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: S3にアクセスするための認証方法としてInstance ProfileとAssumed Roleを使用するかどうか。有効な値: trueおよびfalse（デフォルト）。
  - IAMユーザーベースの認証情報（Access KeyとSecret Key）を使用してS3にアクセスする場合、この項目を`false`に設定し、`aws_s3_access_key`と`aws_s3_secret_key`を指定する必要があります。
  - Instance Profileを使用してS3にアクセスする場合、この項目を`true`に設定する必要があります。
  - Assumed Roleを使用してS3にアクセスする場合、この項目を`true`に設定し、`aws_s3_iam_role_arn`を指定する必要があります。
  - 外部AWSアカウントを使用する場合、`aws_s3_external_id`も指定する必要があります。
- 導入バージョン: v3.0

##### `azure_adls2_endpoint`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2アカウントのエンドポイント。例: `https://test.dfs.core.windows.net`。
- 導入バージョン: v3.4.1

##### `azure_adls2_oauth2_client_id`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるManaged IdentityのクライアントID。
- 導入バージョン: v3.4.4

##### `azure_adls2_oauth2_tenant_id`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるManaged IdentityのテナントID。
- 導入バージョン: v3.4.4

##### `azure_adls2_oauth2_use_managed_identity`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するためにManaged Identityを使用するかどうか。
- 導入バージョン: v3.4.4

##### `azure_adls2_path`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるAzure Data Lake Storage Gen2パス。ファイルシステム名とディレクトリ名で構成されます。例: `testfilesystem/starrocks`。
- 導入バージョン: v3.4.1

##### `azure_adls2_sas_token`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるShared Access Signatures (SAS)。
- 導入バージョン: v3.4.1

##### `azure_adls2_shared_key`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Data Lake Storage Gen2へのリクエストを認証するために使用されるShared Key。
- 導入バージョン: v3.4.1

##### `azure_blob_endpoint`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageアカウントのエンドポイント。例: `https://test.blob.core.windows.net`。
- 導入バージョン: v3.1

##### `azure_blob_path`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるAzure Blob Storageパス。ストレージアカウント内のコンテナ名と、その下のサブパス（もしあれば）で構成されます。例: `testcontainer/subpath`。
- 導入バージョン: v3.1

##### `azure_blob_sas_token`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageへのリクエストを認証するために使用されるShared Access Signatures (SAS)。
- 導入バージョン: v3.1

##### `azure_blob_shared_key`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Azure Blob Storageへのリクエストを認証するために使用されるShared Key。
- 導入バージョン: v3.1

##### `azure_use_native_sdk`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: Azure Blob StorageにアクセスするためにネイティブSDKを使用するかどうか。これにより、Managed IdentitiesとService Principalsによる認証が可能になります。この項目が`false`に設定されている場合、Shared KeyとSAS Tokenによる認証のみが許可されます。
- 導入バージョン: v3.4.4

##### `cloud_native_hdfs_url`

- デフォルト: 空の文字列
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
- 説明: FEクラウドネイティブメタデータサーバーのRPCリスニングポート。
- 導入バージョン: -

##### `cloud_native_storage_type`

- デフォルト: S3
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: 使用するオブジェクトストレージのタイプ。shared-dataモードでは、StarRocksはHDFS、Azure Blob（v3.1.1以降でサポート）、Azure Data Lake Storage Gen2（v3.4.1以降でサポート）、Google Storage（ネイティブSDKを使用、v3.5.1以降でサポート）、およびS3プロトコルと互換性のあるオブジェクトストレージシステム（AWS S3、MinIOなど）にデータを保存することをサポートしています。有効な値: `S3`（デフォルト）、`HDFS`、`AZBLOB`、`ADLS2`、および`GS`。このパラメータを`S3`として指定する場合、`aws_s3`で始まるパラメータを追加する必要があります。このパラメータを`AZBLOB`として指定する場合、`azure_blob`で始まるパラメータを追加する必要があります。このパラメータを`ADLS2`として指定する場合、`azure_adls2`で始まるパラメータを追加する必要があります。このパラメータを`GS`として指定する場合、`gcp_gcs`で始まるパラメータを追加する必要があります。このパラメータを`HDFS`として指定する場合、`cloud_native_hdfs_url`のみを指定する必要があります。
- 導入バージョン: -

##### `enable_load_volume_from_conf`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksがFE設定ファイルで指定されたオブジェクトストレージ関連のプロパティを使用して、組み込みストレージボリュームを作成することを許可するかどうか。デフォルト値はv3.4.1以降、`true`から`false`に変更されました。
- 導入バージョン: v3.1.0

##### `gcp_gcs_impersonation_service_account`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Google Storageにアクセスするために偽装ベースの認証を使用する場合に偽装するService Account。
- 導入バージョン: v3.5.1

##### `gcp_gcs_path`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: データを保存するために使用されるGoogle Cloudパス。Google Cloudバケットの名前と、その下のサブパス（もしあれば）で構成されます。例: `testbucket/subpath`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_email`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account作成時に生成されるJSONファイル内のメールアドレス。例: `user@hello.iam.gserviceaccount.com`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_private_key`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account作成時に生成されるJSONファイル内の秘密鍵。例: `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 導入バージョン: v3.5.1

##### `gcp_gcs_service_account_private_key_id`

- デフォルト: 空の文字列
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: Service Account作成時に生成されるJSONファイル内の秘密鍵ID。
- 導入バージョン: v3.5.1

##### `gcp_gcs_use_compute_engine_service_account`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: いいえ
- 説明: Compute EngineにバインドされているService Accountを使用するかどうか。
- 導入バージョン: v3.5.1

##### `hdfs_file_system_expire_seconds`

- デフォルト: 300
- タイプ: Int
- 単位: 秒
- 変更可能: はい
- 説明: HdfsFsManagerによって管理される未使用のキャッシュされたHDFS/ObjectStore FileSystemの生存期間（秒単位）。FileSystemExpirationChecker（60秒ごとに実行）は、この値を使用して各HdfsFs.isExpired(...)を呼び出します。期限切れになると、マネージャーは基になるFileSystemを閉じ、キャッシュから削除します。アクセサーメソッド（例: `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`）は最終アクセス時刻を更新するため、有効期限は非アクティブ状態に基づいています。値が低いとアイドルリソースの保持は減りますが、再オープンのオーバーヘッドが増加します。値が高いとハンドルを長く保持し、より多くのリソースを消費する可能性があります。
- 導入バージョン: v3.2.0

##### `lake_autovacuum_grace_period_minutes`

- デフォルト: 30
- タイプ: Long
- 単位: 分
- 変更可能: はい
- 説明: shared-dataクラスターで履歴データバージョンを保持する時間範囲。この時間範囲内の履歴データバージョンは、Compaction後にAutoVacuumによって自動的にクリーンアップされません。実行中のクエリによってアクセスされるデータがクエリ完了前に削除されるのを避けるために、この値を最大クエリ時間よりも大きく設定する必要があります。デフォルト値はv3.3.0、v3.2.5、v3.1.10以降、`5`から`30`に変更されました。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_parallel_partitions`

- デフォルト: 8
- タイプ: Int
- 単位: -
- 変更可能: いいえ
- 説明: shared-dataクラスターでAutoVacuumを同時に実行できるパーティションの最大数。AutoVacuumはCompaction後のガベージコレクションです。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_partition_naptime_seconds`

- デフォルト: 180
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: shared-dataクラスターで同じパーティションに対するAutoVacuum操作間の最小間隔。
- 導入バージョン: v3.1.0

##### `lake_autovacuum_stale_partition_threshold`

- デフォルト: 12
- タイプ: Long
- 単位: 時間
- 変更可能: はい
- 説明: この時間範囲内にパーティションに更新（ロード、DELETE、またはCompaction）がない場合、システムはこのパーティションに対してAutoVacuumを実行しません。
- 導入バージョン: v3.1.0

##### `lake_compaction_allow_partial_success`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: この項目が`true`に設定されている場合、shared-dataクラスターでのCompaction操作は、サブタスクのいずれかが成功したときに成功と見なされます。
- 導入バージョン: v3.5.2

##### `lake_compaction_disable_ids`

- デフォルト: ""
- タイプ: String
- 単位: -
- 変更可能: はい
- 説明: shared-dataモードでCompactionが無効になっているテーブルまたはパーティションのリスト。フォーマットはセミコロンで区切られた`tableId1;partitionId2`です。例: `12345;98765`。
- 導入バージョン: v3.4.4

##### `lake_compaction_history_size`

- デフォルト: 20
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターのLeader FEノードのメモリに保持する最近成功したCompactionタスクレコードの数。`SHOW PROC '/compactions'`コマンドを使用して、最近成功したCompactionタスクレコードを表示できます。Compaction履歴はFEプロセスメモリに保存されるため、FEプロセスが再起動されると失われることに注意してください。
- 導入バージョン: v3.1.0

##### `lake_compaction_max_tasks`

- デフォルト: -1
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターで許可される同時Compactionタスクの最大数。この項目を`-1`に設定すると、同時タスク数が適応的に計算されることを示します。この値を`0`に設定すると、Compactionが無効になります。
- 導入バージョン: v3.1.0

##### `lake_compaction_score_selector_min_score`

- デフォルト: 10.0
- タイプ: Double
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターでCompaction操作をトリガーするCompaction Scoreのしきい値。パーティションのCompaction Scoreがこの値以上の場合、システムはそのパーティションに対してCompactionを実行します。
- 導入バージョン: v3.1.0

##### `lake_compaction_score_upper_bound`

- デフォルト: 2000
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターにおけるパーティションのCompaction Scoreの上限。`0`は上限なしを示します。この項目は`lake_enable_ingest_slowdown`が`true`に設定されている場合にのみ有効です。パーティションのCompaction Scoreがこの上限に達するか超えると、受信するロードタスクは拒否されます。v3.3.6以降、デフォルト値は`0`から`2000`に変更されました。
- 導入バージョン: v3.2.0

##### `lake_enable_balance_tablets_between_workers`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターにおけるクラウドネイティブテーブルのタブレット移行中に、Compute Node間でタブレットの数をバランスさせるかどうか。`true`はCompute Node間でタブレットをバランスさせることを示し、`false`はこの機能を無効にすることを示します。
- 導入バージョン: v3.3.4

##### `lake_enable_ingest_slowdown`

- デフォルト: true
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターでData Ingestion Slowdownを有効にするかどうか。Data Ingestion Slowdownが有効になっている場合、パーティションのCompaction Scoreが`lake_ingest_slowdown_threshold`を超えると、そのパーティションでのロードタスクはスロットリングされます。この設定は、`run_mode`が`shared_data`に設定されている場合にのみ有効です。v3.3.6以降、デフォルト値は`false`から`true`に変更されました。
- 導入バージョン: v3.2.0

##### `lake_ingest_slowdown_threshold`

- デフォルト: 100
- タイプ: Long
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターでData Ingestion SlowdownをトリガーするCompaction Scoreのしきい値。この設定は、`lake_enable_ingest_slowdown`が`true`に設定されている場合にのみ有効です。
- 導入バージョン: v3.2.0

##### `lake_publish_version_max_threads`

- デフォルト: 512
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: shared-dataクラスターにおけるVersion Publishタスクの最大スレッド数。
- 導入バージョン: v3.2.0

##### `meta_sync_force_delete_shard_meta`

- デフォルト: false
- タイプ: Boolean
- 単位: -
- 変更可能: はい
- 説明: リモートストレージファイルのクリーンアップをバイパスして、shared-dataクラスターのメタデータを直接削除することを許可するかどうか。この項目は、クリーンアップするシャードが過剰に多く、FE JVMに極端なメモリ負荷がかかる場合にのみ`true`に設定することをお勧めします。この機能が有効になった後、シャードまたはタブレットに属するデータファイルは自動的にクリーンアップされないことに注意してください。
- 導入バージョン: v3.2.10, v3.3.3

##### `run_mode`

- デフォルト: `shared_nothing`
- タイプ: String
- 単位: -
- 変更可能: いいえ
- 説明: StarRocksクラスターの実行モード。有効な値: `shared_data`および`shared_nothing`（デフォルト）。
  - `shared_data`は、StarRocksをshared-dataモードで実行することを示します。
  - `shared_nothing`は、StarRocksをshared-nothingモードで実行することを示します。

  > **注意**
  >
  > - StarRocksクラスターで`shared_data`モードと`shared_nothing`モードを同時に採用することはできません。混合デプロイメントはサポートされていません。
  > - クラスターがデプロイされた後で`run_mode`を変更しないでください。そうしないと、クラスターの再起動に失敗します。shared-nothingクラスターからshared-dataクラスターへの変換、またはその逆はサポートされていません。

- 導入バージョン: -

##### `shard_group_clean_threshold_sec`

- デフォルト: 3600
- タイプ: Long
- 単位: 秒
- 変更可能: はい
- 説明: shared-dataクラスターでFEが未使用のタブレットとシャードグループをクリーンアップするまでの時間。このしきい値内で作成されたタブレットとシャードグループはクリーンアップされません。
- 導入バージョン: -

##### `star_mgr_meta_sync_interval_sec`

- デフォルト: 600
- タイプ: Long
- 単位: 秒
- 変更可能: いいえ
- 説明: shared-dataクラスターでFEがStarMgrとの定期的なメタデータ同期を実行する間隔。
- 導入バージョン: -

##### `starmgr_grpc_server_max_worker_threads`

- デフォルト: 1024
- タイプ: Int
- 単位: -
- 変更可能: はい
- 説明: FE starmgrモジュールのgrpcサーバーによって使用されるワーカー最大スレッド数。
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
- 型: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、アナライザーは `INSERT ... FROM files()` 操作のために、ターゲットテーブルのスキーマを `files()` テーブル関数にプッシュしようとします。これは、ソースが FileTableFunctionRelation であり、ターゲットがネイティブテーブルであり、SELECT リストに対応するスロット参照カラム (または *) が含まれている場合にのみ適用されます。アナライザーは、選択されたカラムをターゲットカラムに一致させ (カウントが一致する必要がある)、ターゲットテーブルを一時的にロックし、非複合型 (Parquet JSON `->` `array<varchar>` のような複合型はスキップされます) のファイルカラム型をディープコピーされたターゲットカラム型に置き換えます。元のファイルテーブルのカラム名は保持されます。これにより、取り込み時のファイルベースの型推論による型不一致と緩さが軽減されます。
- 導入バージョン: v3.4.0, v3.5.0

##### `hdfs_read_buffer_size_kb`

- デフォルト: 8192
- 型: Int
- 単位: Kilobytes
- 変更可能: Yes
- 説明: HDFS 読み取りバッファのサイズをキロバイト単位で指定します。StarRocks はこの値をバイト (`<< 10`) に変換し、`HdfsFsManager` で HDFS 読み取りバッファを初期化するために使用し、ブローカーアクセスが使用されない場合に BE タスク (例: `TBrokerScanRangeParams`、`TDownloadReq`) に送信される Thrift フィールド `hdfs_read_buffer_size_kb` を設定します。`hdfs_read_buffer_size_kb` を増やすと、ストリームあたりのメモリ使用量が増加する代わりに、シーケンシャル読み取りスループットが向上し、システムコールオーバーヘッドが削減されます。減らすとメモリフットプリントは減少しますが、IO 効率が低下する可能性があります。チューニング時にはワークロード (多数の小さなストリームか、少数の大きなシーケンシャル読み取りか) を考慮してください。
- 導入バージョン: v3.2.0

##### `hdfs_write_buffer_size_kb`

- デフォルト: 1024
- 型: Int
- 単位: Kilobytes
- 変更可能: Yes
- 説明: ブローカーを使用しない場合に HDFS またはオブジェクトストレージへの直接書き込みに使用される HDFS 書き込みバッファサイズ (KB 単位) を設定します。FE はこの値をバイト (`<< 10`) に変換し、HdfsFsManager でローカル書き込みバッファを初期化します。また、Thrift リクエスト (例: TUploadReq、TExportSink、シンクオプション) で伝播されるため、バックエンド/エージェントは同じバッファサイズを使用します。この値を増やすと、ライターあたりのメモリが増加する代わりに、大規模なシーケンシャル書き込みのスループットが向上します。減らすとストリームあたりのメモリ使用量が減少し、小さな書き込みのレイテンシが低下する可能性があります。`hdfs_read_buffer_size_kb` と合わせてチューニングし、利用可能なメモリと同時ライター数を考慮してください。
- 導入バージョン: v3.2.0

##### `lake_batch_publish_max_version_num`

- デフォルト: 10
- 型: Int
- 単位: Count
- 変更可能: Yes
- 説明: レイク (クラウドネイティブ) テーブルのパブリッシュバッチを構築する際に、いくつの連続したトランザクションバージョンをグループ化できるかの上限を設定します。この値はトランザクショングラフのバッチ処理ルーチン (getReadyToPublishTxnListBatch を参照) に渡され、`lake_batch_publish_min_version_num` と連携して TransactionStateBatch の候補範囲サイズを決定します。値が大きいほど、より多くのコミットをバッチ処理することでパブリッシュスループットを向上させることができますが、アトミックなパブリッシュの範囲が広がり (可視性レイテンシが長くなり、ロールバックの対象が大きくなる)、バージョンが連続していない場合は実行時に制限される可能性があります。ワークロードと可視性/レイテンシの要件に応じてチューニングしてください。
- 導入バージョン: v3.2.0

##### `lake_batch_publish_min_version_num`

- デフォルト: 1
- 型: Int
- 単位: -
- 変更可能: Yes
- 説明: レイクテーブルのパブリッシュバッチを形成するために必要な連続したトランザクションバージョンの最小数を設定します。DatabaseTransactionMgr.getReadyToPublishTxnListBatch は、この値を `lake_batch_publish_max_version_num` とともに transactionGraph.getTxnsWithTxnDependencyBatch に渡し、依存するトランザクションを選択します。値が `1` の場合、単一トランザクションのパブリッシュ (バッチ処理なし) が許可されます。値が `>1` の場合、少なくともその数の連続したバージョンを持つ、単一テーブルの非レプリケーショントランザクションが利用可能である必要があります。バージョンが連続していない場合、レプリケーショントランザクションが出現した場合、またはスキーマ変更がバージョンを消費した場合、バッチ処理は中止されます。この値を増やすと、コミットをグループ化することでパブリッシュスループットを向上させることができますが、十分な連続トランザクションを待つ間、パブリッシュが遅延する可能性があります。
- 導入バージョン: v3.2.0

##### `lake_enable_batch_publish_version`

- デフォルト: true
- 型: Boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、PublishVersionDaemon は同じレイク (共有データ) テーブル/パーティションの準備ができたトランザクションをバッチ処理し、トランザクションごとのパブリッシュを発行する代わりに、それらのバージョンをまとめてパブリッシュします。RunMode shared-data では、デーモンは getReadyPublishTransactionsBatch() を呼び出し、publishVersionForLakeTableBatch(...) を使用してグループ化されたパブリッシュ操作を実行します (RPC を削減し、スループットを向上させます)。無効にすると、デーモンは publishVersionForLakeTable(...) を介したトランザクションごとのパブリッシュにフォールバックします。実装は、スイッチが切り替えられたときに重複するパブリッシュを避けるために内部セットを使用して進行中の作業を調整し、`lake_publish_version_max_threads` を介したスレッドプールサイズの影響を受けます。
- 導入バージョン: v3.2.0

##### `lake_enable_tablet_creation_optimization`

- デフォルト: false
- 型: boolean
- 単位: -
- 変更可能: Yes
- 説明: 有効にすると、StarRocks は共有データモードのクラウドネイティブテーブルおよびマテリアライズドビューのタブレット作成を最適化し、タブレットごとに個別のメタデータを作成する代わりに、物理パーティション下のすべてのタブレットに対して単一の共有タブレットメタデータを作成します。これにより、テーブル作成、ロールアップ、スキーマ変更ジョブ中に生成されるタブレット作成タスクとメタデータ/ファイルの数が削減されます。この最適化はクラウドネイティブテーブル/マテリアライズドビューにのみ適用され、`file_bundling` (後者は同じ最適化ロジックを再利用します) と組み合わされます。注: スキーマ変更およびロールアップジョブは、`file_bundling` を使用するテーブルに対して、同一名のファイルを上書きしないようにこの最適化を明示的に無効にします。慎重に有効にしてください — 作成されるタブレットメタデータの粒度が変更され、レプリカ作成とファイル命名の動作に影響を与える可能性があります。
- 導入バージョン: v3.3.1, v3.4.0, v3.5.0

##### `lake_use_combined_txn_log`

- デフォルト: false
- 型: Boolean
- 単位: -
- 変更可能: Yes
- 説明: この項目が `true` に設定されている場合、システムはレイクテーブルが関連するトランザクションに対して結合されたトランザクションログパスを使用することを許可します。共有データクラスターでのみ利用可能です。
- 導入バージョン: v3.3.7, v3.4.0, v3.5.0

##### `enable_iceberg_commit_queue`

- デフォルト: true
- 型: Boolean
- 単位: -
- 変更可能: Yes
- 説明: Iceberg テーブルのコミットキューを有効にして、同時コミットの競合を回避するかどうか。Iceberg はメタデータコミットに楽観的並行性制御 (OCC) を使用します。複数のスレッドが同じテーブルに同時にコミットすると、「Cannot commit: Base metadata location is not same as the current table metadata location」のようなエラーで競合が発生する可能性があります。有効にすると、各 Iceberg テーブルはコミット操作用に独自のシングルスレッドエグゼキュータを持ち、同じテーブルへのコミットがシリアル化され、OCC 競合が防止されます。異なるテーブルは同時にコミットでき、全体のスループットを維持します。これは信頼性を向上させるためのシステムレベルの最適化であり、デフォルトで有効にする必要があります。無効にすると、楽観的ロックの競合により同時コミットが失敗する可能性があります。
- 導入バージョン: v4.1.0

##### `iceberg_commit_queue_timeout_seconds`

- デフォルト: 300
- 型: Int
- 単位: Seconds
- 変更可能: Yes
- 説明: Iceberg コミット操作が完了するのを待つタイムアウトを秒単位で指定します。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、各コミット操作はこのタイムアウト内に完了する必要があります。コミットがこのタイムアウトよりも長くかかった場合、キャンセルされ、エラーが発生します。コミット時間に影響を与える要因には、コミットされるデータファイルの数、テーブルのメタデータサイズ、基盤となるストレージ (例: S3、HDFS) のパフォーマンスなどがあります。
- 導入バージョン: v4.1.0

##### `iceberg_commit_queue_max_size`

- デフォルト: 1000
- 型: Int
- 単位: Count
- 変更可能: No
- 説明: Iceberg テーブルあたりの保留中のコミット操作の最大数。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、これは単一のテーブルに対してキューに入れられるコミット操作の数を制限します。制限に達すると、追加のコミット操作は呼び出し元のスレッドで実行されます (容量が利用可能になるまでブロックされます)。この設定は FE 起動時に読み込まれ、新しく作成されるテーブルエグゼキュータに適用されます。有効にするには FE の再起動が必要です。同じテーブルへの同時コミットが多数発生すると予想される場合は、この値を増やしてください。この値が低すぎると、高並行性時にコミットが呼び出し元のスレッドでブロックされる可能性があります。
- 導入バージョン: v4.1.0

### その他

##### `agent_task_resend_wait_time_ms`

- Default: 5000
- Type: Long
- Unit: ミリ秒
- Is mutable: はい
- Description: FEがエージェントタスクを再送信する前に待機しなければならない期間。エージェントタスクは、タスク作成時間と現在時間の間のギャップがこのパラメータの値を超えた場合にのみ再送信できます。このパラメータは、エージェントタスクの繰り返し送信を防ぐために使用されます。
- Introduced in: -

##### `allow_system_reserved_names`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: ユーザーが`__op`および`__row`で始まる名前の列を作成することを許可するかどうか。この機能を有効にするには、このパラメータを`TRUE`に設定します。これらの名前形式はStarRocksで特別な目的のために予約されており、そのような列を作成すると未定義の動作を引き起こす可能性があることに注意してください。そのため、この機能はデフォルトで無効になっています。
- Introduced in: v3.2.0

##### `auth_token`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: FEが属するStarRocksクラスター内でID認証に使用されるトークン。このパラメータが指定されていない場合、StarRocksはクラスターのリーダーFEが初めて起動されたときに、クラスター用のランダムなトークンを生成します。
- Introduced in: -

##### `authentication_ldap_simple_bind_base_dn`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: はい
- Description: LDAPサーバーがユーザーの認証情報の検索を開始する基点となるベースDN。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_dn`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: はい
- Description: ユーザーの認証情報を検索するために使用される管理者DN。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_pwd`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: はい
- Description: ユーザーの認証情報を検索するために使用される管理者のパスワード。
- Introduced in: -

##### `authentication_ldap_simple_server_host`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: はい
- Description: LDAPサーバーが実行されているホスト。
- Introduced in: -

##### `authentication_ldap_simple_server_port`

- Default: 389
- Type: Int
- Unit: -
- Is mutable: はい
- Description: LDAPサーバーのポート。
- Introduced in: -

##### `authentication_ldap_simple_user_search_attr`

- Default: uid
- Type: String
- Unit: -
- Is mutable: はい
- Description: LDAPオブジェクトでユーザーを識別する属性の名前。
- Introduced in: -

##### `backup_job_default_timeout_ms`

- Default: 86400 * 1000
- Type: Int
- Unit: ミリ秒
- Is mutable: はい
- Description: バックアップジョブのタイムアウト期間。この値を超えると、バックアップジョブは失敗します。
- Introduced in: -

##### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: `SHOW PROC /BACKENDS/{id}`コマンドで各ディスクのタブレット数の収集を有効にするかどうか。
- Introduced in: v4.0.1, v3.5.8

##### `enable_colocate_restore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: Colocate Tableのバックアップと復元を有効にするかどうか。`true`はColocate Tableのバックアップと復元を有効にすることを意味し、`false`は無効にすることを意味します。
- Introduced in: v3.2.10, v3.3.3

##### `enable_materialized_view_concurrent_prepare`

- Default: true
- Type: Boolean
- Unit:
- Is mutable: はい
- Description: パフォーマンスを向上させるために、マテリアライズドビューを並行して準備するかどうか。
- Introduced in: v3.4.4

##### `enable_metric_calculator`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: いいえ
- Description: メトリクスを定期的に収集する機能を有効にするかどうかを指定します。有効な値は`TRUE`と`FALSE`です。`TRUE`はこの機能を有効にし、`FALSE`はこの機能を無効にします。
- Introduced in: -

##### `enable_table_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: FEでテーブルレベルのメトリクスをエクスポートするかどうか。無効にすると、FEはテーブルメトリクス（テーブルスキャン/ロードカウンターやテーブルサイズメトリクスなど）のエクスポートをスキップしますが、カウンターはメモリに記録されます。
- Introduced in: -

##### `enable_mv_post_image_reload_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: FEがイメージをロードした後、リロードフラグのチェックを実行するかどうか。ベースマテリアライズドビューに対してチェックが実行された場合、それに関連する他のマテリアライズドビューに対しては不要です。
- Introduced in: v3.5.0

##### `enable_mv_query_context_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: クエリ書き換えのパフォーマンスを向上させるために、クエリレベルのマテリアライズドビュー書き換えキャッシュを有効にするかどうか。
- Introduced in: v3.3

##### `enable_mv_refresh_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: すべてのマテリアライズドビューについて、デフォルトでマテリアライズドビューのリフレッシュ時にプロファイルを有効にするかどうか。
- Introduced in: v3.3.0

##### `enable_mv_refresh_extra_prefix_logging`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: デバッグを容易にするために、ログにマテリアライズドビュー名を含むプレフィックスを有効にするかどうか。
- Introduced in: v3.4.0

##### `enable_mv_refresh_query_rewrite`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュ中にクエリ書き換えを有効にするかどうか。これにより、クエリはベーステーブルではなく書き換えられたMVを直接使用してクエリパフォーマンスを向上させることができます。
- Introduced in: v3.3

##### `enable_trace_historical_node`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: システムが履歴ノードをトレースすることを許可するかどうか。この項目を`true`に設定することで、キャッシュ共有機能を有効にし、システムがエラスティック・スケーリング中に適切なキャッシュノードを選択できるようにします。
- Introduced in: v3.5.1

##### `es_state_sync_interval_second`

- Default: 10
- Type: Long
- Unit: 秒
- Is mutable: いいえ
- Description: FEがElasticsearchインデックスを取得し、StarRocks外部テーブルのメタデータを同期する時間間隔。
- Introduced in: -

##### `hive_meta_cache_refresh_interval_s`

- Default: 3600 * 2
- Type: Long
- Unit: 秒
- Is mutable: いいえ
- Description: Hive外部テーブルのキャッシュされたメタデータが更新される時間間隔。
- Introduced in: -

##### `hive_meta_store_timeout_s`

- Default: 10
- Type: Long
- Unit: 秒
- Is mutable: いいえ
- Description: Hive metastoreへの接続がタイムアウトするまでの時間。
- Introduced in: -

##### `jdbc_connection_idle_timeout_ms`

- Default: 600000
- Type: Int
- Unit: ミリ秒
- Is mutable: いいえ
- Description: JDBCカタログにアクセスするための接続がタイムアウトするまでの最大時間。タイムアウトした接続はアイドル状態と見なされます。
- Introduced in: -

##### `jdbc_connection_timeout_ms`

- Default: 10000
- Type: Long
- Unit: ミリ秒
- Is mutable: いいえ
- Description: HikariCP接続プールが接続を取得するためのミリ秒単位のタイムアウト。この時間内にプールから接続を取得できない場合、操作は失敗します。
- Introduced in: v3.5.13

##### `jdbc_query_timeout_ms`

- Default: 30000
- Type: Long
- Unit: ミリ秒
- Is mutable: はい
- Description: JDBCステートメントクエリ実行のミリ秒単位のタイムアウト。このタイムアウトは、JDBCカタログを介して実行されるすべてのSQLクエリ（パーティションメタデータクエリなど）に適用されます。値はJDBCドライバーに渡されるときに秒に変換されます。
- Introduced in: v3.5.13

##### `jdbc_network_timeout_ms`

- Default: 30000
- Type: Long
- Unit: ミリ秒
- Is mutable: はい
- Description: JDBCネットワーク操作（ソケット読み取り）のミリ秒単位のタイムアウト。このタイムアウトは、外部データベースが応答しない場合に無期限のブロックを防ぐために、データベースメタデータ呼び出し（getSchemas()、getTables()、getColumns()など）に適用されます。
- Introduced in: v3.5.13

##### `jdbc_connection_pool_size`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: JDBCカタログにアクセスするためのJDBC接続プールの最大容量。
- Introduced in: -

##### `jdbc_meta_default_cache_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: JDBCカタログのメタデータキャッシュが有効になっているかどうかのデフォルト値。Trueに設定すると、新しく作成されたJDBCカタログはデフォルトでメタデータキャッシュが有効になります。
- Introduced in: -

##### `jdbc_meta_default_cache_expire_sec`

- Default: 600
- Type: Long
- Unit: 秒
- Is mutable: はい
- Description: JDBCカタログのメタデータキャッシュのデフォルトの有効期限。`jdbc_meta_default_cache_enable`がtrueに設定されている場合、新しく作成されたJDBCカタログはデフォルトでメタデータキャッシュの有効期限を設定します。
- Introduced in: -

##### `jdbc_minimum_idle_connections`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: JDBCカタログにアクセスするためのJDBC接続プール内のアイドル接続の最小数。
- Introduced in: -

##### `jwt_jwks_url`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JSON Web Key Set (JWKS) サービスへのURL、または`fe/conf`ディレクトリ下の公開鍵ローカルファイルへのパス。
- Introduced in: v3.5.0

##### `jwt_principal_field`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTでサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は`sub`です。このフィールドの値は、StarRocksにログインするためのユーザー名と同一である必要があります。
- Introduced in: v3.5.0

##### `jwt_required_audience`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTでオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWTオーディエンスと一致する場合にのみ有効と見なされます。
- Introduced in: v3.5.0

##### `jwt_required_issuer`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTで発行者 (`iss`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWT発行者と一致する場合にのみ有効と見なされます。
- Introduced in: v3.5.0

##### locale

- Default: `zh_CN.UTF-8`
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: FEが使用する文字セット。
- Introduced in: -

##### `max_agent_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: エージェントタスクスレッドプールで許可されるスレッドの最大数。
- Introduced in: -

##### `max_download_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 各RESTORE操作において、StarRocksがBEノードに割り当てるダウンロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限は課されません。
- Introduced in: v3.1.0

##### `max_mv_check_base_table_change_retry_times`

- Default: 10
- Type: -
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュ時にベーステーブルの変更を検出するための最大再試行回数。
- Introduced in: v3.3.0

##### `max_mv_refresh_failure_retry_times`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュが失敗した場合の最大再試行回数。
- Introduced in: v3.3.0

##### `max_mv_refresh_try_lock_failure_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュが失敗した場合のロック試行の最大再試行回数。
- Introduced in: v3.3.0

##### `max_small_file_number`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: はい
- Description: FEディレクトリに保存できる小さなファイルの最大数。
- Introduced in: -

##### `max_small_file_size_bytes`

- Default: 1024 * 1024
- Type: Int
- Unit: バイト
- Is mutable: はい
- Description: 小さなファイルの最大サイズ。
- Introduced in: -

##### `max_upload_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 各BACKUP操作において、StarRocksがBEノードに割り当てるアップロードタスクの最大数。この項目が0以下に設定されている場合、タスク数に制限は課されません。
- Introduced in: v3.1.0

##### `mv_create_partition_batch_interval_ms`

- Default: 1000
- Type: Int
- Unit: ミリ秒
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュ中に、複数のパーティションを一括で作成する必要がある場合、システムはそれらをそれぞれ64パーティションのバッチに分割します。頻繁なパーティション作成による障害のリスクを軽減するため、各バッチ間にデフォルトの間隔（ミリ秒単位）が設定され、作成頻度を制御します。
- Introduced in: v3.3

##### `mv_plan_cache_max_size`

- Default: 1000
- Type: Long
- Unit:
- Is mutable: はい
- Description: マテリアライズドビュープランキャッシュ（マテリアライズドビューの書き換えに使用される）の最大サイズ。透過的なクエリ書き換えに使用されるマテリアライズドビューが多い場合、この値を増やすことができます。
- Introduced in: v3.2

##### `mv_plan_cache_thread_pool_size`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビュープランキャッシュ（マテリアライズドビューの書き換えに使用される）のデフォルトのスレッドプールサイズ。
- Introduced in: v3.2

##### `mv_refresh_default_planner_optimize_timeout`

- Default: 30000
- Type: -
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュ時に、オプティマイザの計画フェーズのデフォルトのタイムアウト。
- Introduced in: v3.3.0

##### `mv_refresh_fail_on_filter_data`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: リフレッシュ中にフィルタリングされたデータがある場合、MVのリフレッシュは失敗します（デフォルトはtrue）。そうでない場合は、フィルタリングされたデータを無視して成功を返します。
- Introduced in: -

##### `mv_refresh_try_lock_timeout_ms`

- Default: 30000
- Type: Int
- Unit: ミリ秒
- Is mutable: はい
- Description: マテリアライズドビューのリフレッシュが、そのベーステーブル/マテリアライズドビューのDBロックを試行する際のデフォルトのロック試行タイムアウト。
- Introduced in: v3.3.0

##### `oauth2_auth_server_url`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: 認証URL。OAuth 2.0認証プロセスを開始するために、ユーザーのブラウザがリダイレクトされるURL。
- Introduced in: v3.5.0

##### `oauth2_client_id`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: StarRocksクライアントの公開識別子。
- Introduced in: v3.5.0

##### `oauth2_client_secret`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: 認証サーバーでStarRocksクライアントを認証するために使用されるシークレット。
- Introduced in: v3.5.0

##### `oauth2_jwks_url`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JSON Web Key Set (JWKS) サービスへのURL、または`conf`ディレクトリ下のローカルファイルへのパス。
- Introduced in: v3.5.0

##### `oauth2_principal_field`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTでサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は`sub`です。このフィールドの値は、StarRocksにログインするためのユーザー名と同一である必要があります。
- Introduced in: v3.5.0

##### `oauth2_redirect_url`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: OAuth 2.0認証が成功した後、ユーザーのブラウザがリダイレクトされるURL。認証コードはこのURLに送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`として設定する必要があります。
- Introduced in: v3.5.0

##### `oauth2_required_audience`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTでオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWTオーディエンスと一致する場合にのみ有効と見なされます。
- Introduced in: v3.5.0

##### `oauth2_required_issuer`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: JWTで発行者 (`iss`) を識別するために使用される文字列のリスト。JWTは、リスト内のいずれかの値がJWT発行者と一致する場合にのみ有効と見なされます。
- Introduced in: v3.5.0

##### `oauth2_token_server_url`

- Default: 空の文字列
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: StarRocksがアクセストークンを取得する認証サーバー上のエンドポイントのURL。
- Introduced in: v3.5.0

##### `plugin_dir`

- Default: `System.getenv("STARROCKS_HOME")` + "/plugins"
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: プラグインインストールパッケージを保存するディレクトリ。
- Introduced in: -

##### `plugin_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: FEにプラグインをインストールできるかどうか。プラグインはLeader FEにのみインストールまたはアンインストールできます。
- Introduced in: -

##### `proc_profile_jstack_depth`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: はい
- Description: システムがCPUおよびメモリプロファイルを収集する際のJavaスタックの最大深度。この値は、サンプリングされた各スタックに対してキャプチャされるJavaスタックフレームの数を制御します。値が大きいほどトレースの詳細度と出力サイズが増加し、プロファイリングのオーバーヘッドが増える可能性がありますが、値が小さいほど詳細度が減少します。この設定は、CPUとメモリの両方のプロファイリングでプロファイラが開始されるときに使用されるため、診断の必要性とパフォーマンスへの影響のバランスを取るように調整してください。
- Introduced in: -

##### `proc_profile_mem_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: プロセスメモリ割り当てプロファイルの収集を有効にするかどうか。この項目が`true`に設定されている場合、システムは`sys_log_dir/proc_profile`の下に`mem-profile-<timestamp>.html`という名前のHTMLプロファイルを生成し、サンプリング中に`proc_profile_collect_time_s`秒間スリープし、Javaスタック深度に`proc_profile_jstack_depth`を使用します。生成されたファイルは`proc_profile_file_retained_days`と`proc_profile_file_retained_size_bytes`に従って圧縮およびパージされます。ネイティブ抽出パスは`/tmp`のnoexec問題を回避するために`STARROCKS_HOME_DIR`を使用します。この項目は、メモリ割り当てのホットスポットのトラブルシューティングを目的としています。これを有効にすると、CPU、I/O、ディスク使用量が増加し、大きなファイルが生成される可能性があります。
- Introduced in: v3.2.12

##### `query_detail_explain_level`

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: はい
- Description: EXPLAINステートメントによって返されるクエリプランの詳細レベル。有効な値：COSTS、NORMAL、VERBOSE。
- Introduced in: v3.2.12, v3.3.5

##### `replication_interval_ms`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: いいえ
- Description: レプリケーションタスクがスケジュールされる最小時間間隔。
- Introduced in: v3.3.5

##### `replication_max_parallel_data_size_mb`

- Default: 1048576
- Type: Int
- Unit: メガバイト
- Is mutable: はい
- Description: 同時同期に許可されるデータの最大サイズ。
- Introduced in: v3.3.5

##### `replication_max_parallel_replica_count`

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 同時同期に許可されるタブレットレプリカの最大数。
- Introduced in: v3.3.5

##### `replication_max_parallel_table_count`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 許可される同時データ同期タスクの最大数。StarRocksはテーブルごとに1つの同期タスクを作成します。
- Introduced in: v3.3.5

##### `replication_transaction_timeout_sec`

- Default: 86400
- Type: Int
- Unit: 秒
- Is mutable: はい
- Description: 同期タスクのタイムアウト期間。
- Introduced in: v3.3.5

##### `skip_whole_phase_lock_mv_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: はい
- Description: 関連するマテリアライズドビューを持つテーブルに対してStarRocksが「非ロック」最適化を適用するタイミングを制御します。この項目が0未満に設定されている場合、システムは常に非ロック最適化を適用し、クエリのために関連するマテリアライズドビューをコピーしません（FEのメモリ使用量とメタデータのコピー/ロック競合は減少しますが、メタデータの同時実行性の問題のリスクが増加する可能性があります）。0に設定されている場合、非ロック最適化は無効になります（システムは常に安全なコピーアンドロックパスを使用します）。0より大きい値に設定されている場合、非ロック最適化は、関連するマテリアライズドビューの数が設定されたしきい値以下であるテーブルにのみ適用されます。さらに、値が0以上の場合、プランナーはクエリOLAPテーブルをオプティマイザコンテキストに記録して、マテリアライズドビュー関連の書き換えパスを有効にします。0未満の場合、このステップはスキップされます。
- Introduced in: v3.2.1

##### `small_file_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/small_files"
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: 小さなファイルのルートディレクトリ。
- Introduced in: -

##### `task_runs_max_history_number`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: はい
- Description: メモリに保持し、アーカイブされたタスク実行履歴をクエリする際のデフォルトのLIMITとして使用するタスク実行レコードの最大数。`enable_task_history_archive`がfalseの場合、この値はメモリ内履歴を制限します。強制GCは古いエントリを削除し、最新の`task_runs_max_history_number`のみが残ります。アーカイブ履歴がクエリされた場合（明示的なLIMITが提供されていない場合）、この値が0より大きい場合、`TaskRunHistoryTable.lookup`は`"ORDER BY create_time DESC LIMIT <value>"`を使用します。注：これを0に設定すると、クエリ側のLIMITは無効になりますが（上限なし）、メモリ内履歴はゼロに切り詰められます（アーカイブが有効でない限り）。
- Introduced in: v3.2.0

##### `tmp_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/temp_dir"
- Type: String
- Unit: -
- Is mutable: いいえ
- Description: バックアップおよび復元手順中に生成されるファイルなどの一時ファイルを保存するディレクトリ。これらの手順が完了すると、生成された一時ファイルは削除されます。
- Introduced in: -

##### `transform_type_prefer_string_for_varchar`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: はい
- Description: マテリアライズドビューの作成およびCTAS操作において、固定長varchar列に文字列型を優先するかどうか。
- Introduced in: v4.0.0

<EditionSpecificFEItem />
