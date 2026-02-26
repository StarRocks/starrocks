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

FE の起動後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行して、パラメーター設定を確認できます。特定のパラメーターの設定をクエリするには、次のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスター管理関連コマンドを実行するには、管理者権限が必要です。
:::

## FE パラメーターの設定

### FE 動的パラメーターの設定

[`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して、FE 動的パラメーターの設定を構成または変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメーターの設定

<StaticFEConfigNote />

## FE パラメーターについて

### ロギング

##### `audit_log_delete_age`

- Default: 30d
- Type: String
- Unit: -
- Is mutable: No
- Description: 監査ログファイルの保持期間。デフォルト値 `30d` は、各監査ログファイルが 30 日間保持できることを指定します。StarRocks は各監査ログファイルをチェックし、30 日以上前に生成されたファイルを削除します。
- Introduced in: -

##### `audit_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: 監査ログファイルを格納するディレクトリ。
- Introduced in: -

##### `audit_log_enable_compress`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: No
- Description: true の場合、生成された Log4j2 設定は、ローテーションされた監査ログファイル名 (fe.audit.log.*) に ".gz" 接尾辞を追加し、Log4j2 がロールオーバー時に圧縮された (.gz) アーカイブ監査ログファイルを生成するようにします。この設定は、FE 起動時に Log4jConfig.initLogging で読み込まれ、監査ログの RollingFile アペンダーに適用されます。アクティブな監査ログではなく、ローテーション/アーカイブされたファイルにのみ影響します。値は起動時に初期化されるため、変更を有効にするには FE の再起動が必要です。監査ログのローテーション設定 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) とともに使用します。
- Introduced in: 3.2.12

##### `audit_log_json_format`

- Default: false
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: true の場合、FE 監査イベントは、デフォルトのパイプ区切り "key=value" 文字列ではなく、構造化された JSON (Jackson ObjectMapper が注釈付き AuditEvent フィールドの Map をシリアル化) として出力されます。この設定は、AuditLogBuilder が処理するすべての組み込み監査シンクに影響します。接続監査、クエリ監査、大容量クエリ監査 (イベントが条件を満たす場合、大容量クエリしきい値フィールドが JSON に追加されます)、および低速監査出力です。大容量クエリしきい値および "features" フィールドに注釈が付けられたフィールドは特別に扱われます (通常の監査エントリから除外され、該当する場合、大容量クエリまたは機能ログに含まれます)。これを有効にすると、ログコレクターまたは SIEM のログが機械で解析可能になります。ログ形式が変更されるため、従来のパイプ区切り形式を期待する既存のパーサーを更新する必要がある場合があります。
- Introduced in: 3.2.7

##### `audit_log_modules`

- Default: `slow_query`, query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks が監査ログエントリを生成するモジュール。デフォルトでは、StarRocks は `slow_query` モジュールと `query` モジュールの監査ログを生成します。`connection` モジュールは v3.0 以降でサポートされています。モジュール名をコンマ (,) とスペースで区切ります。
- Introduced in: -

##### `audit_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks が監査ログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメーターが `DAY` に設定されている場合、監査ログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、監査ログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `audit_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: `audit_log_roll_interval` パラメーターで指定された各保持期間内に保持できる監査ログファイルの最大数。
- Introduced in: -

##### `bdbje_log_level`

- Default: INFO
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks で Berkeley DB Java Edition (BDB JE) が使用するロギングレベルを制御します。BDB 環境の初期化中に BDBEnvironment.initConfigs() は、この値を `com.sleepycat.je` パッケージの Java ロガーと BDB JE 環境ファイルロギングレベル (`EnvironmentConfig.FILE_LOGGING_LEVEL`) に適用します。SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF などの標準的な java.util.logging.Level 名を受け入れます。ALL に設定すると、すべてのログメッセージが有効になります。詳細度を上げると、ログのボリュームが増加し、ディスク I/O とパフォーマンスに影響を与える可能性があります。この値は BDB 環境が初期化されるときに読み込まれるため、環境の (再) 初期化後にのみ有効になります。
- Introduced in: v3.2.0

##### `big_query_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE の大容量クエリログファイル (`fe.big_query.log.*`) が自動削除されるまでの保持期間を制御します。この値は、Log4j の削除ポリシーに IfLastModified age として渡されます。最終更新時刻がこの値よりも古いローテーションされた大容量クエリログは削除されます。`d` (日)、`h` (時間)、`m` (分)、`s` (秒) などの接尾辞をサポートしています。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。この項目は `big_query_log_roll_interval` および `big_query_log_roll_num` と連携して、どのファイルを保持またはパージするかを決定します。
- Introduced in: v3.2.0

##### `big_query_log_dir`

- Default: `Config.STARROCKS_HOME_DIR + "/log"`
- Type: String
- Unit: -
- Is mutable: No
- Description: FE が大容量クエリダンプログ (`fe.big_query.log.*`) を書き込むディレクトリ。Log4j 設定はこのパスを使用して、`fe.big_query.log` とそのローテーションされたファイル用の RollingFile アペンダーを作成します。ローテーションと保持は、`big_query_log_roll_interval` (時刻ベースのサフィックス)、`log_roll_size_mb` (サイズトリガー)、`big_query_log_roll_num` (最大ファイル数)、および `big_query_log_delete_age` (年齢ベースの削除) によって管理されます。大容量クエリレコードは、`big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold`、または `big_query_log_scan_bytes_threshold` などのユーザー定義のしきい値を超えるクエリに対してログに記録されます。このファイルにログを記録するモジュールを制御するには、`big_query_log_modules` を使用します。
- Introduced in: v3.2.0

##### `big_query_log_modules`

- Default: `{"query"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: モジュールごとの大容量クエリロギングを有効にするモジュール名サフィックスのリスト。一般的な値は論理コンポーネント名です。たとえば、デフォルトの `query` は `big_query.query` を生成します。
- Introduced in: v3.2.0

##### `big_query_log_roll_interval`

- Default: `"DAY"`
- Type: String
- Unit: -
- Is mutable: No
- Description: `big_query` ログアペンダーのローリングファイル名の日付コンポーネントを構築するために使用される時間間隔を指定します。有効な値 (大文字と小文字を区別しない) は `DAY` (デフォルト) と `HOUR` です。`DAY` は日次パターン (`"%d{yyyyMMdd}"`) を生成し、`HOUR` は時間別パターン (`"%d{yyyyMMddHH}"`) を生成します。この値は、サイズベースのロールオーバー (`big_query_roll_maxsize`) およびインデックスベースのロールオーバー (`big_query_log_roll_num`) と組み合わせて、RollingFile の filePattern を形成します。無効な値は、ログ設定の生成が失敗し (IOException)、ログの初期化または再構成を妨げる可能性があります。`big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num`、および `big_query_log_delete_age` とともに使用します。
- Introduced in: v3.2.0

##### `big_query_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `big_query_log_roll_interval` ごとに保持するローテーションされた FE 大容量クエリログファイルの最大数。この値は、`fe.big_query.log` の RollingFile アペンダーの DefaultRolloverStrategy `max` 属性にバインドされます。ログが (時間または `log_roll_size_mb` によって) ロールオーバーすると、StarRocks は `big_query_log_roll_num` 個のインデックス付きファイル (filePattern は時刻サフィックスとインデックスを使用) を保持します。この数よりも古いファイルはロールオーバーによって削除される可能性があり、`big_query_log_delete_age` は最終更新時刻によってさらにファイルを削除できます。
- Introduced in: v3.2.0

##### `dump_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: ダンプログファイルの保持期間。デフォルト値 `7d` は、各ダンプログファイルが 7 日間保持できることを指定します。StarRocks は各ダンプログファイルをチェックし、7 日以上前に生成されたファイルを削除します。
- Introduced in: -

##### `dump_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: ダンプログファイルを格納するディレクトリ。
- Introduced in: -

##### `dump_log_modules`

- Default: query
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks がダンプログエントリを生成するモジュール。デフォルトでは、StarRocks はクエリモジュールのダンプログを生成します。モジュール名をコンマ (,) とスペースで区切ります。
- Introduced in: -

##### `dump_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks がダンプログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメーターが `DAY` に設定されている場合、ダンプログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、ダンプログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `dump_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `dump_log_roll_interval` パラメーターで指定された各保持期間内に保持できるダンプログファイルの最大数。
- Introduced in: -

##### `edit_log_write_slow_log_threshold_ms`

- Default: 2000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: JournalWriter が低速な編集ログバッチ書き込みを検出してログに記録するために使用するしきい値 (ミリ秒)。バッチコミット後、バッチ期間がこの値を超えると、JournalWriter はバッチサイズ、期間、現在のジャーナルキューサイズを伴う WARN を出力します (約 2 秒に 1 回にレート制限)。この設定は、FE リーダーでの潜在的な I/O またはレプリケーションの遅延に対するロギング/アラートのみを制御します。コミットまたはロールの動作は変更しません (`edit_log_roll_num` およびコミット関連の設定を参照)。このしきい値に関係なく、メトリック更新は引き続き発生します。
- Introduced in: v3.2.3

##### `enable_audit_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、FE 監査サブシステムは、ConnectProcessor によって処理されたステートメントの SQL テキストを FE 監査ログ (`fe.audit.log`) に記録します。格納されたステートメントは、他の制御に従います。暗号化されたステートメントは編集され (`AuditEncryptionChecker`)、`enable_sql_desensitize_in_log` が設定されている場合、機密性の高い資格情報は編集または非機密化される可能性があり、ダイジェストレコーディングは `enable_sql_digest` によって制御されます。`false` に設定されている場合、ConnectProcessor は監査イベントのステートメントテキストを "?" に置き換えます。他の監査フィールド (ユーザー、ホスト、期間、ステータス、`qe_slow_log_ms` を介した低速クエリ検出、およびメトリック) は引き続き記録されます。SQL 監査を有効にすると、フォレンジックとトラブルシューティングの可視性が向上しますが、機密性の高い SQL コンテンツが公開され、ログのボリュームと I/O が増加する可能性があります。無効にすると、監査ログでの完全なステートメントの可視性を失う代わりにプライバシーが向上します。
- Introduced in: -

##### `enable_profile_log`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: プロファイルロギングを有効にするかどうか。この機能が有効になっている場合、FE はクエリごとのプロファイルログ (ProfileManager によって生成されたシリアル化された `queryDetail` JSON) をプロファイルログシンクに書き込みます。このロギングは `enable_collect_query_detail_info` も有効になっている場合にのみ実行されます。`enable_profile_log_compress` が有効になっている場合、JSON はロギング前に gzipped されることがあります。プロファイルログファイルは `profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` によって管理され、`profile_log_delete_age` ( `7d`、`10h`、`60m`、`120s` などの形式をサポート) に従ってローテーション/削除されます。この機能を無効にすると、プロファイルログの書き込みが停止します (ディスク I/O、圧縮 CPU、ストレージ使用量の削減)。
- Introduced in: v3.2.5

##### `enable_qe_slow_log`

- Default: true
- Type: Boolean
- Unit: N/A
- Is mutable: Yes
- Description: 有効にすると、FE 組み込み監査プラグイン (AuditLogBuilder) は、測定された実行時間 ("Time" フィールド) が `qe_slow_log_ms` で設定されたしきい値を超えるクエリイベントを低速クエリ監査ログ (AuditLog.getSlowAudit) に書き込みます。無効にすると、これらの低速クエリエントリは抑制されます (通常のクエリおよび接続監査ログは影響を受けません)。低速監査エントリは、グローバルな `audit_log_json_format` 設定 (JSON とプレーン文字列) に従います。このフラグを使用して、通常の監査ロギングとは独立して低速クエリ監査ボリュームの生成を制御します。無効にすると、`qe_slow_log_ms` が低い場合やワークロードが多くの長時間実行クエリを生成する場合にログ I/O を削減できます。
- Introduced in: 3.2.11

##### `enable_sql_desensitize_in_log`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムはログとクエリ詳細レコードに書き込まれる前に機密性の高い SQL コンテンツを置き換えるか隠します。この設定を尊重するコードパスには、ConnectProcessor.formatStmt (監査ログ)、StmtExecutor.addRunningQueryDetail (クエリ詳細)、および SimpleExecutor.formatSQL (内部エクゼキュータログ) が含まれます。この機能が有効になっている場合、無効な SQL は固定の非機密化メッセージに置き換えられる可能性があり、資格情報 (ユーザー/パスワード) は隠され、SQL フォーマッターはサニタイズされた表現を生成する必要があります (ダイジェスト形式の出力を有効にすることもできます)。これにより、監査/内部ログでの機密リテラルや資格情報の漏洩が減少しますが、ログとクエリ詳細に元の完全な SQL テキストが含まれなくなることになります (これは再生やデバッグに影響する可能性があります)。
- Introduced in: -

##### `internal_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 内部ログファイル (`internal_log_dir` に書き込まれます) の保持期間を指定します。値は期間文字列です。サポートされている接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。この項目は、RollingFile Delete ポリシーで使用される `<IfLastModified age="..."/>` 述語として log4j 設定に代入されます。最終変更時刻がこの期間よりも古いファイルは、ログのロールオーバー中に削除されます。この値を増やすとディスク領域をより早く解放できます。減らすと内部マテリアライズドビューまたは統計ログをより長く保持できます。
- Introduced in: v3.2.4

##### `internal_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ロギングサブシステムが内部ログ (`fe.internal.log`) を保存するために使用するディレクトリ。この設定は Log4j 設定に代入され、InternalFile アペンダーが内部/マテリアライズドビュー/統計ログを書き込む場所、および `internal.<module>` の下にあるモジュールごとのロガーがファイルを配置する場所を決定します。ディレクトリが存在し、書き込み可能であり、十分なディスク容量があることを確認してください。このディレクトリ内のファイルのログローテーションと保持は、`log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age`、および `internal_log_roll_interval` によって制御されます。`sys_log_to_console` が有効になっている場合、内部ログはこのディレクトリではなくコンソールに書き込まれることがあります。
- Introduced in: v3.2.4

##### `internal_log_json_format`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、内部統計/監査エントリはコンパクトな JSON オブジェクトとして統計監査ロガーに書き込まれます。JSON には、"executeType" (InternalType: QUERY または DML)、"queryId"、"sql"、および "time" (経過ミリ秒) のキーが含まれます。`false` に設定されている場合、同じ情報は単一のフォーマットされたテキスト行 ("statistic execute: ... | QueryId: [...] | SQL: ...") としてログに記録されます。JSON を有効にすると、機械解析とログプロセッサとの統合が向上しますが、生の SQL テキストがログに含まれるため、機密情報が公開され、ログサイズが増加する可能性があります。
- Introduced in: -

##### `internal_log_modules`

- Default: `{"base", "statistic"}`
- Type: String[]
- Unit: -
- Is mutable: No
- Description: 専用の内部ロギングを受け取るモジュール識別子のリスト。各エントリ X について、Log4j はレベル INFO と additivity="false" の `internal.<X>` という名前のロガーを作成します。これらのロガーは、内部アペンダー ( `fe.internal.log` に書き込まれます) または `sys_log_to_console` が有効になっている場合はコンソールにルーティングされます。必要に応じて短い名前またはパッケージフラグメントを使用します。正確なロガー名は `internal.` + 構成された文字列になります。内部ログファイルのローテーションと保持は、`internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval`、および `log_roll_size_mb` に従います。モジュールを追加すると、実行時メッセージが内部ロガーストリームに分離され、デバッグと監査が容易になります。
- Introduced in: v3.2.4

##### `internal_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: FE 内部ログアペンダーの時刻ベースのロール間隔を制御します。受け入れられる値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は時間別ファイルパターン (`"%d{yyyyMMddHH}"`) を生成し、`DAY` は日別ファイルパターン (`"%d{yyyyMMdd}"`) を生成します。これらは RollingFile TimeBasedTriggeringPolicy によってローテーションされた `fe.internal.log` ファイルに名前を付けるために使用されます。無効な値は、初期化の失敗 (アクティブな Log4j 設定の構築時に IOException がスローされます) を引き起こします。ロール動作は、`internal_log_dir`、`internal_roll_maxsize` (ソースにタイプミス、おそらく `log_roll_size_mb`)、`internal_log_roll_num`、および `internal_log_delete_age` などの関連設定にも依存します。
- Introduced in: v3.2.4

##### `internal_log_roll_num`

- Default: 90
- Type: Int
- Unit: -
- Is mutable: No
- Description: 内部アペンダー (`fe.internal.log`) に対して保持するローテーションされた内部 FE ログファイルの最大数。この値は Log4j DefaultRolloverStrategy の `max` 属性として使用されます。ロールオーバーが発生すると、StarRocks は最大で `internal_log_roll_num` 個のアーカイブファイルを保持し、古いファイルを削除します (`internal_log_delete_age` によって管理されます)。値を小さくするとディスク使用量が減りますが、ログ履歴が短くなります。値を大きくすると、より多くの履歴内部ログが保持されます。この項目は、`internal_log_dir`、`internal_log_roll_interval`、および `internal_roll_maxsize` (ソースにタイプミス、おそらく `log_roll_size_mb`) と連携して機能します。
- Introduced in: v3.2.4

##### `log_cleaner_audit_log_min_retention_days`

- Default: 3
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: 監査ログファイルの最小保持日数。これよりも新しい監査ログファイルは、ディスク使用量が高くても削除されません。これにより、監査ログがコンプライアンスとトラブルシューティングの目的で保持されます。
- Introduced in: -

##### `log_cleaner_check_interval_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: ディスク使用量をチェックし、ログをクリーンアップする間隔 (秒単位)。クリーナーは、各ログディレクトリのディスク使用量を定期的にチェックし、必要に応じてクリーンアップをトリガーします。デフォルトは 300 秒 (5 分) です。
- Introduced in: -

##### `log_cleaner_disk_usage_target`

- Default: 60
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: ログクリーンアップ後の目標ディスク使用量 (パーセンテージ)。ディスク使用量がこのしきい値を下回るまでログクリーンアップが続行されます。クリーナーは、目標に達するまで最も古いログファイルを 1 つずつ削除します。
- Introduced in: -

##### `log_cleaner_disk_usage_threshold`

- Default: 80
- Type: Int
- Unit: Percentage
- Is mutable: Yes
- Description: ログクリーンアップをトリガーするディスク使用量しきい値 (パーセンテージ)。ディスク使用量がこのしきい値を超えると、ログクリーンアップが開始されます。クリーナーは、設定された各ログディレクトリを独立してチェックし、このしきい値を超えるディレクトリを処理します。
- Introduced in: -

##### `log_cleaner_disk_util_based_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ディスク使用量に基づく自動ログクリーンアップを有効にします。有効にすると、ディスク使用量がしきい値を超えたときにログがクリーンアップされます。ログクリーナーは FE ノードのバックグラウンドデーモンとして実行され、ログファイルの蓄積によるディスク領域の枯渇を防ぐのに役立ちます。
- Introduced in: -

##### `log_plan_cancelled_by_crash_be`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: BE クラッシュまたは RPC 例外によりクエリがキャンセルされた場合に、クエリ実行計画のロギングを有効にするかどうか。この機能が有効になっている場合、BE クラッシュまたは `RpcException` によりクエリがキャンセルされたときに、StarRocks はクエリ実行計画 ( `TExplainLevel.COSTS` レベル) を WARN エントリとしてログに記録します。ログエントリには QueryId、SQL、および COSTS 計画が含まれ、ExecuteExceptionHandler パスでは例外スタックトレースもログに記録されます。ロギングは `enable_collect_query_detail_info` が有効になっている場合はスキップされます (その場合、計画はクエリ詳細に格納されます)。コードパスでは、クエリ詳細が null であることを検証することでチェックが実行されます。ExecuteExceptionHandler では、計画は最初のリトライ (`retryTime == 0`) のみでログに記録されることに注意してください。これを有効にすると、完全な COSTS 計画が大きくなる可能性があるため、ログのボリュームが増加する可能性があります。
- Introduced in: v3.2.0

##### `log_register_and_unregister_query_id`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE が QeProcessorImpl からのクエリ登録および登録解除メッセージ (例: `"register query id = {}"` および `"deregister query id = {}"`) をログに記録することを許可するかどうか。ログは、クエリに null 以外の ConnectContext があり、コマンドが `COM_STMT_EXECUTE` でないか、セッション変数 `isAuditExecuteStmt()` が true の場合にのみ出力されます。これらのメッセージはすべてのクエリライフサイクルイベントに対して書き込まれるため、この機能を有効にすると、ログのボリュームが大きくなり、高並行環境ではスループットのボトルネックになる可能性があります。デバッグまたは監査のために有効にし、ロギングのオーバーヘッドを減らしてパフォーマンスを向上させるために無効にします。
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
- Description: `sys_log_dir/proc_profile` 以下に生成されたプロセスプロファイリングファイル (CPU およびメモリ) の保持日数。ProcProfileCollector は、現在時刻から `proc_profile_file_retained_days` 日を差し引いて (yyyyMMdd-HHmmss 形式で) カットオフを計算し、タイムスタンプ部分がそのカットオフよりも辞書順で早いプロファイルファイル (`timePart.compareTo(timeToDelete) < 0` の場合) を削除します。ファイル削除は、`proc_profile_file_retained_size_bytes` によって制御されるサイズベースのカットオフも尊重します。プロファイルファイルは `cpu-profile-` と `mem-profile-` というプレフィックスを使用し、収集後に圧縮されます。
- Introduced in: v3.2.12

##### `proc_profile_file_retained_size_bytes`

- Default: 2L * 1024 * 1024 * 1024 (2147483648)
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: プロファイルディレクトリ下に保持される、収集された CPU およびメモリプロファイルファイル (`cpu-profile-` および `mem-profile-` というプレフィックスを持つファイル) の合計バイト数の最大値。有効なプロファイルファイルの合計が `proc_profile_file_retained_size_bytes` を超えると、コレクターは、残りの合計サイズが `proc_profile_file_retained_size_bytes` 以下になるまで、最も古いプロファイルファイルを削除します。`proc_profile_file_retained_days` よりも古いファイルもサイズに関係なく削除されます。この設定はプロファイルアーカイブのディスク使用量を制御し、`proc_profile_file_retained_days` と相互作用して削除順序と保持を決定します。
- Introduced in: v3.2.12

##### `profile_log_delete_age`

- Default: 1d
- Type: String
- Unit: -
- Is mutable: No
- Description: FE プロファイルログファイルが削除対象となるまでの保持期間を制御します。この値は Log4j の `<IfLastModified age="..."/>` ポリシー (`Log4jConfig` 経由) に注入され、`profile_log_roll_interval` や `profile_log_roll_num` などのローテーション設定と組み合わせて適用されます。サポートされる接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。
- Introduced in: v3.2.5

##### `profile_log_dir`

- Default: `Config.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE プロファイルログが書き込まれるディレクトリ。Log4jConfig はこの値を使用して、プロファイル関連のアペンダーを配置します (このディレクトリの下に `fe.profile.log` や `fe.features.log` のようなファイルを作成します)。これらのファイルのローテーションと保持は、`profile_log_roll_size_mb`、`profile_log_roll_num`、`profile_log_delete_age` によって管理されます。タイムスタンプのサフィックス形式は `profile_log_roll_interval` (DAY または HOUR をサポート) によって制御されます。デフォルトのディレクトリは `STARROCKS_HOME_DIR` の下にあるため、FE プロセスがこのディレクトリへの書き込みおよびローテーション/削除の権限を持っていることを確認してください。
- Introduced in: v3.2.5


##### `profile_log_latency_threshold_ms`

- Default: 0
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: `fe.profile.log` に書き込むクエリの最小レイテンシ（ミリ秒）。実行時間がこの値以上の場合にのみ profile を記録します。0 に設定するとすべての profile を記録します（しきい値なし）。正の値に設定すると、遅いクエリのみを記録してログ量を削減できます。
- Introduced in: -

##### `profile_log_roll_interval`

- Default: DAY
- Type: String
- Unit: -
- Is mutable: No
- Description: プロファイルログファイル名の日付部分を生成するために使用される時間粒度を制御します。有効な値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は `"%d{yyyyMMddHH}"` (時間ごとの時間バケット) のパターンを生成し、`DAY` は `"%d{yyyyMMdd}"` (日ごとの時間バケット) を生成します。この値は Log4j 設定で `profile_file_pattern` を計算する際に使用され、ロールオーバーファイル名の時間ベースのコンポーネントのみに影響します。サイズベースのロールオーバーは `profile_log_roll_size_mb` によって引き続き制御され、保持は `profile_log_roll_num` / `profile_log_delete_age` によって制御されます。無効な値は、ロギング初期化中に IOException を引き起こします (エラーメッセージ: `"profile_log_roll_interval config error: <value>"` )。高ボリュームプロファイリングの場合は `HOUR` を選択して 1 時間あたりのファイルサイズを制限するか、日次集計の場合は `DAY` を選択します。
- Introduced in: v3.2.5

##### `profile_log_roll_num`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: プロファイルロガーの Log4j DefaultRolloverStrategy が保持するローテーションされたプロファイルログファイルの最大数を指定します。この値は、ロギング XML に `${profile_log_roll_num}` として注入されます (例: `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`)。ローテーションは `profile_log_roll_size_mb` または `profile_log_roll_interval` によってトリガーされます。ローテーションが発生すると、Log4j は最大でこれらのインデックス付きファイルを保持し、古いインデックスファイルは削除対象となります。ディスク上での実際の保持は、`profile_log_delete_age` と `profile_log_dir` の場所にも影響されます。値が小さいとディスク使用量が減りますが、保持される履歴が制限されます。値が大きいと、より多くの履歴プロファイルログが保持されます。
- Introduced in: v3.2.5

##### `profile_log_roll_size_mb`

- Default: 1024
- Type: Int
- Unit: MB
- Is mutable: No
- Description: FE プロファイルログファイルのサイズベースのロールオーバーをトリガーするサイズしきい値 (メガバイト単位) を設定します。この値は、`ProfileFile` アペンダーの Log4j RollingFile SizeBasedTriggeringPolicy によって使用されます。プロファイルログが `profile_log_roll_size_mb` を超えると、ローテーションされます。ローテーションは、`profile_log_roll_interval` に達したときに時間によっても発生する可能性があります。いずれかの条件がロールオーバーをトリガーします。`profile_log_roll_num` と `profile_log_delete_age` と組み合わせることで、この項目は保持される履歴プロファイルファイルの数と古いファイルの削除時期を制御します。ローテーションされたファイルの圧縮は `enable_profile_log_compress` によって制御されます。
- Introduced in: v3.2.5

##### `qe_slow_log_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: クエリが遅いクエリかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超えると、**fe.audit.log** に遅いクエリとして記録されます。
- Introduced in: -

##### `slow_lock_log_every_ms`

- Default: 3000L
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: 同じ SlowLockLogStats インスタンスに対して別の「低速ロック」警告を発行するまでに待機する最小間隔 (ミリ秒)。LockUtils は、ロック待機が `slow_lock_threshold_ms` を超えた後にこの値をチェックし、最後のログに記録された低速ロックイベントから `slow_lock_log_every_ms` ミリ秒が経過するまで追加の警告を抑制します。長期的な競合中にログのボリュームを減らすには値を大きくし、より頻繁な診断を得るには値を小さくします。変更は、その後のチェックに対して実行時に有効になります。
- Introduced in: v3.2.0

##### `slow_lock_print_stack`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: LockManager が `logSlowLockTrace` によって出力される低速ロック警告の JSON ペイロードに、所有スレッドの完全なスタックトレースを含めることを許可するかどうか ("stack" 配列は `LogUtil.getStackTraceToJsonArray` を使用して `start=0` および `max=Short.MAX_VALUE` で設定されます)。この設定は、ロック取得が `slow_lock_threshold_ms` で設定されたしきい値を超えたときに表示されるロック所有者に関する追加のスタック情報のみを制御します。この機能を有効にすると、ロックを保持している正確なスレッドスタックを提供することでデバッグに役立ちます。無効にすると、高並行環境でスタックトレースをキャプチャしてシリアル化することによるログのボリュームと CPU/メモリのオーバーヘッドが減少します。
- Introduced in: v3.3.16, v3.4.5, v3.5.1

##### `slow_lock_threshold_ms`

- Default: 3000L
- Type: long
- Unit: Milliseconds
- Is mutable: Yes
- Description: ロック操作または保持されているロックを「遅い」と分類するために使用されるしきい値 (ミリ秒)。ロックの経過待機時間または保持時間がこの値を超えると、StarRocks は (コンテキストに応じて) 診断ログを出力し、スタックトレースまたは待機者/所有者情報を含め、LockManager ではこの遅延後にデッドロック検出を開始します。これは LockUtils (低速ロックロギング)、QueryableReentrantReadWriteLock (低速リーダーのフィルタリング)、LockManager (デッドロック検出遅延と低速ロックトレース)、LockChecker (定期的な低速ロック検出)、およびその他の呼び出し元 (例: DiskAndTabletLoadReBalancer ロギング) によって使用されます。値を下げると感度とロギング/診断のオーバーヘッドが増加します。0 または負の数に設定すると、初期の待機ベースのデッドロック検出遅延動作が無効になります。`slow_lock_log_every_ms`、`slow_lock_print_stack`、および `slow_lock_stack_trace_reserve_levels` と一緒に調整します。
- Introduced in: 3.2.0

##### `sys_log_delete_age`

- Default: 7d
- Type: String
- Unit: -
- Is mutable: No
- Description: システムログファイルの保持期間。デフォルト値 `7d` は、各システムログファイルが 7 日間保持できることを指定します。StarRocks は各システムログファイルをチェックし、7 日以上前に生成されたファイルを削除します。
- Introduced in: -

##### `sys_log_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- Type: String
- Unit: -
- Is mutable: No
- Description: システムログファイルを格納するディレクトリ。
- Introduced in: -

##### `sys_log_enable_compress`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムはローテーションされたシステムログファイル名に ".gz" 接尾辞を追加し、Log4j が gzip 圧縮されたローテーションされた FE システムログ (例: fe.log.*) を生成するようにします。この値は Log4j 設定生成時 (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) に読み取られ、RollingFile の filePattern で使用される `sys_file_postfix` プロパティを制御します。この機能を有効にすると、保持されるログのディスク使用量は減少しますが、ロールオーバー時の CPU と I/O が増加し、ログファイル名が変更されるため、ログを読み取るツールやスクリプトは .gz ファイルを処理できる必要があります。監査ログは圧縮に別の設定 (`audit_log_enable_compress`) を使用することに注意してください。
- Introduced in: v3.2.12

##### `sys_log_format`

- Default: "plaintext"
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ログに使用される Log4j レイアウトを選択します。有効な値: `"plaintext"` (デフォルト) と `"json"`。値は大文字と小文字を区別しません。`"plaintext"` は、人間が読めるタイムスタンプ、レベル、スレッド、class.method:line、および WARN/ERROR のスタックトレースを持つ PatternLayout を構成します。`"json"` は JsonTemplateLayout を構成し、ログアグリゲーター (ELK、Splunk) に適した構造化 JSON イベント (UTC タイムスタンプ、レベル、スレッド ID/名、ソースファイル/メソッド/行、メッセージ、例外スタックトレース) を出力します。JSON 出力は、`sys_log_json_max_string_length` および `sys_log_json_profile_max_string_length` の最大文字列長に準拠します。
- Introduced in: v3.2.10

##### `sys_log_json_max_string_length`

- Default: 1048576
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: JSON 形式のシステムログに使用される JsonTemplateLayout の "maxStringLength" 値を設定します。`sys_log_format` が `"json"` に設定されている場合、文字列値のフィールド (たとえば "message" や文字列化された例外スタックトレース) は、長さがこの制限を超えると切り捨てられます。この値は、生成された Log4j XML ( `Log4jConfig.generateActiveLog4jXmlConfig()` 内) に注入され、デフォルト、警告、監査、ダンプ、および大容量クエリレイアウトに適用されます。プロファイルレイアウトは別の設定 (`sys_log_json_profile_max_string_length`) を使用します。この値を小さくするとログサイズは減りますが、有用な情報が切り捨てられる可能性があります。
- Introduced in: 3.2.11

##### `sys_log_json_profile_max_string_length`

- Default: 104857600 (100 MB)
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: `sys_log_format` が "json" の場合、プロファイル (および関連機能) ログアペンダーの JsonTemplateLayout の maxStringLength を設定します。JSON 形式のプロファイルログ内の文字列フィールド値は、このバイト長に切り捨てられます。非文字列フィールドは影響を受けません。この項目は Log4jConfig の `JsonTemplateLayout maxStringLength` に適用され、プレーンテキストロギングが使用されている場合は無視されます。必要な完全なメッセージに対して十分な大きさに保ちますが、値が大きいとログサイズと I/O が増加することに注意してください。
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
  - このパラメーターが `DAY` に設定されている場合、システムログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、システムログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- Introduced in: -

##### `sys_log_roll_num`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: `sys_log_roll_interval` パラメーターで指定された各保持期間内に保持できるシステムログファイルの最大数。
- Introduced in: -

##### `sys_log_to_console`

- Default: false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、システムは Log4j を設定して、すべてのログをファイルベースのアペンダーではなくコンソール (ConsoleErr アペンダー) に送信します。この値はアクティブな Log4j XML 設定を生成する際に読み取られ (ルートロガーおよびモジュールごとのロガーアペンダーの選択に影響します)、プロセス起動時に `SYS_LOG_TO_CONSOLE` 環境変数から取得されます。実行時に変更しても効果はありません。この設定は、stdout/stderr ログ収集がログファイルの書き込みよりも優先されるコンテナ化された環境や CI 環境で一般的に使用されます。
- Introduced in: v3.2.0

##### `sys_log_verbose_modules`

- Default: Empty string
- Type: String[]
- Unit: -
- Is mutable: No
- Description: StarRocks がシステムログを生成するモジュール。このパラメーターが `org.apache.starrocks.catalog` に設定されている場合、StarRocks はカタログモジュールのみのシステムログを生成します。モジュール名をコンマ (,) とスペースで区切ります。
- Introduced in: -

##### `sys_log_warn_modules`

- Default: {}
- Type: String[]
- Unit: -
- Is mutable: No
- Description: システムが起動時に WARN レベルのロガーとして設定し、警告アペンダー (SysWF) (`fe.warn.log` ファイル) にルーティングするロガー名またはパッケージプレフィックスのリスト。エントリは生成された Log4j 設定 (org.apache.kafka、org.apache.hudi、org.apache.hadoop.io.compress などの組み込み警告モジュールとともに) に挿入され、`<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` のようなロガー要素を生成します。完全修飾パッケージおよびクラスプレフィックス (例: "com.example.lib") は、通常のログへのノイズの多い INFO/DEBUG 出力を抑制し、警告を個別にキャプチャできるようにするために推奨されます。
- Introduced in: v3.2.13

### サーバー

##### `brpc_idle_wait_max_time`

- Default: 10000
- Type: Int
- Unit: ms
- Is mutable: No
- Description: bRPC クライアントがアイドル状態で待機する最大時間。
- Introduced in: -

##### `brpc_inner_reuse_pool`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 下層の BRPC クライアントが接続/チャネルに内部共有再利用プールを使用するかどうかを制御します。StarRocks は BrpcProxy で RpcClientOptions を構築するときに `brpc_inner_reuse_pool` を読み取ります ( `rpcOptions.setInnerResuePool(...)` 経由)。有効な場合 (true)、RPC クライアントは内部プールを再利用して、呼び出しごとの接続作成を減らし、FE-to-BE / LakeService RPC の接続チャーン、メモリ、ファイルディスクリプタの使用量を削減します。無効な場合 (false)、クライアントはより多くの分離されたプールを作成する可能性があります (リソース使用量が増える代わりに並行性分離を向上させます)。この値を変更するには、プロセスを再起動して有効にする必要があります。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_min_evictable_idle_time_ms`

- Default: 120000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: アイドル状態の BRPC 接続が、接続プールで立ち退きの対象になるまでに残っている必要のある時間 (ミリ秒単位)。`BrpcProxy` が使用する RpcClientOptions に適用されます (RpcClientOptions.setMinEvictableIdleTime 経由)。この値を上げるとアイドル接続を長く保持でき (再接続のチャーンを減らす)、下げると未使用のソケットをより速く解放できます (リソース使用量を減らす)。接続の再利用、プールの成長、立ち退きの動作のバランスを取るために、`brpc_connection_pool_size` および `brpc_idle_wait_max_time` とともに調整します。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `brpc_reuse_addr`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: true の場合、StarRocks は、brpc RpcClient によって作成されたクライアントソケットにローカルアドレスの再利用を許可するソケットオプションを設定します (RpcClientOptions.setReuseAddress 経由)。これを有効にすると、バインドの失敗が減り、ソケットが閉じられた後にローカルポートの再バインドが高速化され、高レートの接続チャーンや迅速な再起動に役立ちます。false の場合、アドレス/ポートの再利用は無効になり、意図しないポート共有の可能性を減らすことができますが、一時的なバインドエラーが増加する可能性があります。このオプションは、`brpc_connection_pool_size` および `brpc_short_connection` によって設定される接続動作と相互作用します。これは、クライアントソケットを再バインドして再利用できる速度に影響するためです。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `cluster_name`

- Default: StarRocks Cluster
- Type: String
- Unit: -
- Is mutable: No
- Description: FE が属する StarRocks クラスターの名前。クラスター名は Web ページの `Title` に表示されます。
- Introduced in: -

##### `dns_cache_ttl_seconds`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: 成功した DNS ルックアップの DNS キャッシュ TTL (Time-To-Live) (秒単位)。これにより、JVM が成功した DNS ルックアップをキャッシュする期間を制御する Java セキュリティプロパティ `networkaddress.cache.ttl` が設定されます。システムが常に情報をキャッシュできるようにするにはこの項目を `-1` に設定し、キャッシュを無効にするには `0` に設定します。これは、Kubernetes デプロイメントや動的 DNS が使用されている場合など、IP アドレスが頻繁に変更される環境で特に役立ちます。
- Introduced in: v3.5.11, v4.0.4

##### `enable_http_async_handler`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: システムが HTTP リクエストを非同期で処理することを許可するかどうか。この機能が有効になっている場合、Netty ワーカー スレッドによって受信された HTTP リクエストは、HTTP サーバーのブロックを避けるために、サービスロジック処理のために別のスレッドプールに送信されます。無効になっている場合、Netty ワーカーがサービスロジックを処理します。
- Introduced in: 4.0.0

##### `enable_http_validate_headers`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Netty の HttpServerCodec が厳密な HTTP ヘッダー検証を実行するかどうかを制御します。この値は、HttpServer の HTTP パイプラインが初期化されるときに HttpServerCodec に渡されます (UseLocations を参照)。新しい Netty バージョンではより厳密なヘッダー規則が適用されるため (https://github.com/netty/netty/pull/12760)、下位互換性のためにデフォルトは false です。RFC 準拠のヘッダーチェックを強制するには true に設定します。そうすると、レガシークライアントやプロキシからの不正な形式の要求や非準拠の要求が拒否される可能性があります。変更を有効にするには HTTP サーバーの再起動が必要です。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_https`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: FE ノードで HTTP サーバーと HTTPS サーバーを同時に有効にするかどうか。
- Introduced in: v4.0

##### `frontend_address`

- Default: 0.0.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: FE ノードの IP アドレス。
- Introduced in: -

##### `http_async_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 非同期 HTTP リクエスト処理用のスレッドプールのサイズ。エイリアスは `max_http_sql_service_task_threads_num` です。
- Introduced in: 4.0.0

##### `http_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの HTTP サーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `http_max_chunk_size`

- Default: 8192
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: FE HTTP サーバーの Netty の HttpServerCodec によって処理される単一の HTTP チャンクの最大許容サイズ (バイト単位) を設定します。これは HttpServerCodec に 3 番目の引数として渡され、チャンク転送またはストリーミング要求/応答中のチャンクの長さを制限します。受信チャンクがこの値を超えると、Netty はフレームが大きすぎるエラー (TooLongFrameException など) を発生させ、要求が拒否される可能性があります。正当な大規模チャンクアップロードの場合はこれを増やし、メモリ圧力を減らしたり、DoS 攻撃の攻撃対象領域を減らしたりするために小さく保ちます。この設定は、`http_max_initial_line_length`、`http_max_header_size`、および `enable_http_validate_headers` とともに使用されます。
- Introduced in: v3.2.0

##### `http_max_header_size`

- Default: 32768
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: Netty の `HttpServerCodec` によって解析される HTTP 要求ヘッダーブロックの最大許容サイズ (バイト単位)。StarRocks はこの値を `HttpServerCodec` に渡します ( `Config.http_max_header_size` 経由)。受信要求のヘッダー (名前と値の組み合わせ) がこの制限を超えると、コーデックは要求を拒否し (デコーダー例外)、接続/要求は失敗します。クライアントが非常に大きなヘッダー (大きな Cookie または多くのカスタムヘッダー) を正当に送信する場合にのみ増やしてください。値が大きいほど、接続ごとのメモリ使用量が増加します。`http_max_initial_line_length` および `http_max_chunk_size` と合わせて調整してください。変更には FE の再起動が必要です。
- Introduced in: v3.2.0

##### `http_max_initial_line_length`

- Default: 4096
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: HttpServer で使用される Netty `HttpServerCodec` が受け入れる HTTP 初期リクエスト行 (メソッド + リクエストターゲット + HTTP バージョン) の最大許容長 (バイト単位) を設定します。この値は Netty のデコーダーに渡され、この長さよりも長い初期行を持つリクエストは拒否されます (TooLongFrameException)。非常に長いリクエスト URI をサポートする必要がある場合にのみ、これを増やしてください。値が大きいほどメモリ使用量が増加し、不正な形式の/リクエスト乱用のリスクが高まる可能性があります。`http_max_header_size` および `http_max_chunk_size` と合わせて調整してください。
- Introduced in: v3.2.0

##### `http_port`

- Default: 8030
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの HTTP サーバーがリッスンするポート。
- Introduced in: -

##### `http_web_page_display_hardware`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: true の場合、HTTP インデックスページ (/index) に oshi ライブラリ (CPU、メモリ、プロセス、ディスク、ファイルシステム、ネットワークなど) を介して入力されたハードウェア情報セクションが含まれます。oshi は、システムユーティリティを呼び出したり、間接的にシステムファイルを読み取ったりする場合があります (たとえば、`getent passwd` などのコマンドを実行する可能性があります)。これにより、機密性の高いシステムデータが表面化する可能性があります。より厳格なセキュリティが必要な場合、またはホストでこれらの間接コマンドの実行を回避したい場合は、この設定を false にして、Web UI でのハードウェア詳細の収集と表示を無効にします。
- Introduced in: v3.2.0

##### `http_worker_threads_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: No
- Description: HTTP リクエストを処理する HTTP サーバーのワーカー スレッドの数。負の値または 0 の場合、スレッド数は CPU コア数の 2 倍になります。
- Introduced in: v2.5.18, v3.0.10, v3.1.7, v3.2.2

##### `https_port`

- Default: 8443
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの HTTPS サーバーがリッスンするポート。
- Introduced in: v4.0

##### `max_mysql_service_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの MySQL サーバーがタスクを処理するために実行できる最大スレッド数。
- Introduced in: -

##### `max_task_runs_threads_num`

- Default: 512
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: タスク実行エグゼキュータースレッドプールの最大スレッド数を制御します。この値は同時タスク実行の上限であり、増やすと並行性が高まりますが、CPU、メモリ、ネットワーク使用量も増加し、減らすとタスク実行のバックログと待ち時間が長くなる可能性があります。この値は、予想される同時スケジュールジョブと利用可能なシステムリソースに応じて調整してください。
- Introduced in: v3.2.0

##### `memory_tracker_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE メモリートラッカーサブシステムを有効にします。`memory_tracker_enable` が `true` に設定されている場合、`MemoryUsageTracker` は定期的に登録されたメタデータモジュールをスキャンし、メモリ内の `MemoryUsageTracker.MEMORY_USAGE` マップを更新し、合計をログに記録し、`MetricRepo` がメモリー使用量とオブジェクト数ゲージをメトリック出力に公開するようにします。サンプリング間隔を制御するには `memory_tracker_interval_seconds` を使用します。この機能を有効にすると、メモリー消費の監視とデバッグに役立ちますが、CPU と I/O のオーバーヘッド、および追加のメトリックカーディナリティが発生します。
- Introduced in: v3.2.4

##### `memory_tracker_interval_seconds`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: FE の `MemoryUsageTracker` デーモンが FE プロセスと登録された `MemoryTrackable` モジュールのメモリ使用量をポーリングして記録する間隔 (秒単位)。`memory_tracker_enable` が `true` に設定されている場合、トラッカーはこの周期で実行され、`MEMORY_USAGE` を更新し、集計された JVM および追跡対象モジュールの使用状況をログに記録します。
- Introduced in: v3.2.4

##### `mysql_nio_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの MySQL サーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `mysql_server_version`

- Default: 8.0.33
- Type: String
- Unit: -
- Is mutable: Yes
- Description: クライアントに返される MySQL サーバーバージョン。このパラメーターを変更すると、以下の状況でバージョン情報に影響します。
  1. `select version();`
  2. ハンドシェイクパケットバージョン
  3. グローバル変数 `version` の値 (`show variables like 'version';`)
- Introduced in: -

##### `mysql_service_io_threads_num`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの MySQL サーバーが I/O イベントを処理するために実行できる最大スレッド数。
- Introduced in: -

##### `mysql_service_kill_after_disconnect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: MySQL TCP 接続が閉じられたと検出された場合 (読み取りで EOF) のサーバーによるセッションの処理方法を制御します。`true` に設定されている場合、サーバーはその接続で実行中のクエリを直ちに停止し、即座にクリーンアップを実行します。`false` の場合、サーバーは切断時に実行中のクエリを停止せず、保留中の要求タスクがない場合にのみクリーンアップを実行し、クライアントが切断された後も長時間実行中のクエリを続行できるようにします。注: TCP キープアライブを示唆する簡単なコメントにもかかわらず、このパラメーターは特に切断後の停止動作を管理し、孤立したクエリを終了させるか (信頼性の低い/負荷分散されたクライアントの背後で推奨)、完了させるかを希望するかどうかに応じて設定する必要があります。
- Introduced in: -

##### `mysql_service_nio_enable_keep_alive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: MySQL 接続で TCP Keep-Alive を有効にします。ロードバランサーの背後にある長時間アイドル状態の接続に役立ちます。
- Introduced in: -

##### `net_use_ipv6_when_priority_networks_empty`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: `priority_networks` が指定されていない場合に IPv6 アドレスを優先的に使用するかどうかを制御するブール値。`true` は、ノードをホストするサーバーに IPv4 と IPv6 アドレスの両方があり、`priority_networks` が指定されていない場合に、システムが IPv6 アドレスを優先的に使用することを許可することを示します。
- Introduced in: v3.3.0

##### `priority_networks`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: 複数の IP アドレスを持つサーバーの選択戦略を宣言します。このパラメーターで指定されたリストと一致する IP アドレスは最大 1 つである必要があることに注意してください。このパラメーターの値は、CIDR 表記でセミコロン (;) で区切られたエントリ (例: 10.10.10.0/24) で構成されるリストです。このリストのエントリに一致する IP アドレスがない場合、サーバーの使用可能な IP アドレスがランダムに選択されます。v3.3.0 から、StarRocks は IPv6 に基づくデプロイメントをサポートしています。サーバーに IPv4 と IPv6 アドレスの両方があり、このパラメーターが指定されていない場合、システムはデフォルトで IPv4 アドレスを使用します。`net_use_ipv6_when_priority_networks_empty` を `true` に設定することで、この動作を変更できます。
- Introduced in: -

##### `proc_profile_cpu_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、バックグラウンドの `ProcProfileCollector` は `AsyncProfiler` を使用して CPU プロファイルを収集し、HTML レポートを `sys_log_dir/proc_profile` に書き込みます。各収集実行では、`proc_profile_collect_time_s` で設定された期間 CPU スタックを記録し、Java スタック深度に `proc_profile_jstack_depth` を使用します。生成されたプロファイルは圧縮され、古いファイルは `proc_profile_file_retained_days` および `proc_profile_file_retained_size_bytes` に従ってパージされます。`AsyncProfiler` にはネイティブライブラリ (`libasyncProfiler.so`) が必要です。`/tmp` の noexec 問題を回避するために `one.profiler.extractPath` は `STARROCKS_HOME_DIR/bin` に設定されています。
- Introduced in: v3.2.12

##### `qe_max_connection`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードへのすべてのユーザーが確立できる最大接続数。v3.1.12 および v3.2.7 以降、デフォルト値が `1024` から `4096` に変更されました。
- Introduced in: -

##### `query_port`

- Default: 9030
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの MySQL サーバーがリッスンするポート。
- Introduced in: -

##### `rpc_port`

- Default: 9020
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの Thrift サーバーがリッスンするポート。
- Introduced in: -

##### `slow_lock_stack_trace_reserve_levels`

- Default: 15
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: StarRocks が遅いロックまたは保持されているロックのロックデバッグ情報をダンプする際に、いくつのスタックトレースフレームをキャプチャして出力するかを制御します。この値は、排他ロック所有者、現在のスレッド、および最古/共有リーダーの JSON を生成する際に `QueryableReentrantReadWriteLock` によって `LogUtil.getStackTraceToJsonArray` に渡されます。この値を増やすと、遅いロックまたはデッドロックの問題の診断に役立つより多くのコンテキストが得られますが、JSON ペイロードが大きくなり、スタックキャプチャの CPU/メモリがわずかに増加します。減らすとオーバーヘッドが減少します。注: リーダーエントリは、低速ロックのみをログに記録する場合、`slow_lock_threshold_ms` によってフィルタリングできます。
- Introduced in: v3.4.0, v3.5.0

##### `ssl_cipher_blacklist`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: IANA 名で SSL 暗号スイートをブラックリストに登録するための、正規表現をサポートするコンマ区切りリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- Introduced in: v4.0

##### `ssl_cipher_whitelist`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: IANA 名で SSL 暗号スイートをホワイトリストに登録するための、正規表現をサポートするコンマ区切りリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- Introduced in: v4.0

##### `task_runs_concurrency`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 同時に実行される TaskRun インスタンスのグローバル制限。`TaskRunScheduler` は、現在の実行数が `task_runs_concurrency` 以上の場合、新しい実行のスケジュールを停止するため、この値はスケジューラー全体で並行 TaskRun 実行を制限します。これは `MVPCTRefreshPartitioner` によって TaskRun ごとのパーティション更新粒度を計算するためにも使用されます。値を増やすと並行性とリソース使用量が増加し、減らすと並行性が減少し、実行ごとのパーティション更新が大きくなります。意図的にスケジューリングを無効にする場合を除き、0 または負の値に設定しないでください。0 (または負の値) は、`TaskRunScheduler` による新しい TaskRun のスケジューリングを事実上妨げます。
- Introduced in: v3.2.0

##### `task_runs_queue_length`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 保留中のキューに保持される保留中の TaskRun 項目の最大数を制限します。`TaskRunManager` は現在の保留中の数をチェックし、有効な保留中の TaskRun の数が `task_runs_queue_length` 以上の場合、新しい送信を拒否します。マージ/承認された TaskRun が追加される前に同じ制限が再チェックされます。メモリとスケジューリングのバックログのバランスをとるためにこの値を調整します。拒否を回避するために、大規模でバースト性の高いワークロードの場合は高く設定し、メモリを制限して保留中のバックログを減らす場合は低く設定します。
- Introduced in: v3.2.0

##### `thrift_backlog_num`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE ノードの Thrift サーバーが保持するバックログキューの長さ。
- Introduced in: -

##### `thrift_client_timeout_ms`

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: アイドル状態のクライアント接続がタイムアウトするまでの時間。
- Introduced in: -

##### `thrift_rpc_max_body_size`

- Default: -1
- Type: Int
- Unit: Bytes
- Is mutable: No
- Description: サーバーの Thrift プロトコルを構築するときに使用される Thrift RPC メッセージ本文の最大許容サイズ (バイト単位) を制御します ( `ThriftServer` の TBinaryProtocol.Factory に渡されます)。値が `-1` の場合、制限が無効になります (無制限)。正の値を設定すると、上限が適用され、これより大きいメッセージは Thrift レイヤーによって拒否され、メモリ使用量を制限し、サイズ超過の要求や DoS のリスクを軽減するのに役立ちます。正当な要求の拒否を避けるために、予想されるペイロード (大きな構造体やバッチデータ) に十分な大きさに設定してください。
- Introduced in: v3.2.0

##### `thrift_server_max_worker_threads`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE ノードの Thrift サーバーがサポートするワーカー スレッドの最大数。
- Introduced in: -

##### `thrift_server_queue_size`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: 要求が保留されているキューの長さ。Thrift サーバーで処理されているスレッドの数が `thrift_server_max_worker_threads` で指定された値を超えると、新しい要求が保留中のキューに追加されます。
- Introduced in: -

### メタデータとクラスター管理

##### `alter_max_worker_queue_size`

- Default: 4096
- Type: Int
- Unit: Tasks
- Is mutable: No
- Description: alter サブシステムで使用される内部ワーカー スレッド プール キューの容量を制御します。これは `AlterHandler` で `ThreadPoolManager.newDaemonCacheThreadPool` に `alter_max_worker_threads` とともに渡されます。保留中の alter タスクの数が `alter_max_worker_queue_size` を超えると、新しい送信は拒否され、`RejectedExecutionException` がスローされる可能性があります ( `AlterHandler.handleFinishAlterTask` を参照)。この値を調整して、メモリ使用量と、同時 alter タスクに対して許可するバックログの量のバランスを取ります。
- Introduced in: v3.2.0

##### `alter_max_worker_threads`

- Default: 4
- Type: Int
- Unit: Threads
- Is mutable: No
- Description: AlterHandler のスレッドプールの最大ワーカー スレッド数を設定します。AlterHandler はこの値を使用してエクゼキューターを構築し、alter 関連のタスクを実行および完了します (例: handleFinishAlterTask 経由で `AlterReplicaTask` を送信)。この値は alter 操作の同時実行を制限します。値を上げると並行性とリソース使用量が増加し、下げると同時 alter が制限され、ボトルネックになる可能性があります。エクゼキューターは `alter_max_worker_queue_size` とともに作成され、ハンドラーのスケジューリングは `alter_scheduler_interval_millisecond` を使用します。
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
- Description: 連続する 2 回の Hive メタデータ キャッシュ更新の間隔。
- Introduced in: v2.5.5

##### `background_refresh_metadata_time_secs_since_last_access_secs`

- Default: 3600 * 24
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Hive メタデータキャッシュ更新タスクの有効期限。アクセスされた Hive カタログの場合、指定された時間以上アクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Hive カタログの場合、StarRocks はそのキャッシュされたメタデータを更新しません。
- Introduced in: v2.5.5

##### `bdbje_cleaner_threads`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: StarRocks ジャーナルで使用される Berkeley DB Java Edition (JE) 環境のバックグラウンドクリーナースレッドの数。この値は `BDBEnvironment.initConfigs` で環境初期化中に読み取られ、`Config.bdbje_cleaner_threads` を使用して `EnvironmentConfig.CLEANER_THREADS` に適用されます。これは JE ログクリーニングとスペース再利用の並行性を制御します。値を増やすとクリーニングが高速化される可能性がありますが、追加の CPU とフォアグラウンド操作との I/O 干渉が発生する可能性があります。変更は BDB 環境が (再) 初期化された場合にのみ有効になるため、新しい値を適用するにはフロントエンドの再起動が必要です。
- Introduced in: v3.2.0

##### `bdbje_heartbeat_timeout_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: StarRocks クラスター内のリーダー、フォロワー、オブザーバー FE 間でハートビートがタイムアウトするまでの時間。
- Introduced in: -

##### `bdbje_lock_timeout_second`

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: BDB JE ベースの FE のロックがタイムアウトするまでの時間。
- Introduced in: -

##### `bdbje_replay_cost_percent`

- Default: 150
- Type: Int
- Unit: Percent
- Is mutable: No
- Description: BDB JE ログからのトランザクションのリプレイとネットワーク復元による同じデータの取得の相対コスト (パーセンテージ) を設定します。この値は基盤となる JE レプリケーションパラメーター `REPLAY_COST_PERCENT` に提供され、通常 `>100` であり、リプレイが通常ネットワーク復元よりも高価であることを示します。システムは、潜在的なリプレイのためにクリーンアップされたログファイルを保持するかどうかを決定する際に、リプレイコストにログサイズを掛けたものとネットワーク復元のコストを比較します。ネットワーク復元の方が効率的であると判断された場合、ファイルは削除されます。値が 0 の場合、このコスト比較に基づく保持は無効になります。`REP_STREAM_TIMEOUT` 内のレプリカまたはアクティブなレプリケーションに必要なログファイルは常に保持されます。
- Introduced in: v3.2.0

##### `bdbje_replica_ack_timeout_second`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: メタデータがリーダー FE からフォロワー FE に書き込まれるときに、リーダー FE が指定された数のフォロワー FE から ACK メッセージを待機できる最大時間。単位: 秒。大量のメタデータが書き込まれている場合、フォロワー FE がリーダー FE に ACK メッセージを返すまでに長い時間がかかり、ACK タイムアウトが発生する可能性があります。この状況では、メタデータ書き込みが失敗し、FE プロセスが終了します。この状況を防ぐために、このパラメーターの値を増やすことをお勧めします。
- Introduced in: -

##### `bdbje_reserved_disk_size`

- Default: 512 * 1024 * 1024 (536870912)
- Type: Long
- Unit: Bytes
- Is mutable: No
- Description: Berkeley DB JE が「保護されていない」(削除可能) ログ/データファイルとして予約するバイト数を制限します。StarRocks はこの値を BDBEnvironment の `EnvironmentConfig.RESERVED_DISK` 経由で JE に渡します。JE の組み込みデフォルトは 0 (無制限) です。StarRocks のデフォルト (512 MiB) は、JE が保護されていないファイルに過剰なディスクスペースを予約するのを防ぎつつ、古いファイルを安全にクリーンアップできるようにします。ディスクが制限されているシステムでこの値を調整します。値を減らすと JE はより多くのファイルをより早く解放でき、増やすと JE はより多くの予約スペースを保持できます。変更を有効にするにはプロセスを再起動する必要があります。
- Introduced in: v3.2.0

##### `bdbje_reset_election_group`

- Default: false
- Type: String
- Unit: -
- Is mutable: No
- Description: BDBJE レプリケーショングループをリセットするかどうか。このパラメーターが `TRUE` に設定されている場合、FE は BDBJE レプリケーショングループをリセットし (つまり、すべての選挙可能な FE ノードの情報を削除し)、リーダー FE として起動します。リセット後、この FE はクラスター内の唯一のメンバーとなり、他の FE は `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` を使用してこのクラスターに再参加できます。ほとんどのフォロワー FE のデータが破損しているためにリーダー FE を選出できない場合にのみこの設定を使用してください。`reset_election_group` は `metadata_failure_recovery` の代替として使用されます。
- Introduced in: -

##### `black_host_connect_failures_within_time`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: ブラックリストに登録された BE ノードに許可される接続失敗のしきい値。BE ノードが自動的に BE ブラックリストに追加された場合、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` で設定されたしきい値よりも少ない接続失敗である場合にのみ、BE ブラックリストから削除できます。
- Introduced in: v3.3.0

##### `black_host_history_sec`

- Default: 2 * 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: BE ブラックリストに登録された BE ノードの過去の接続失敗を保持する期間。BE ノードが自動的に BE ブラックリストに追加された場合、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` で設定されたしきい値よりも少ない接続失敗である場合にのみ、BE ブラックリストから削除できます。
- Introduced in: v3.3.0

##### `brpc_connection_pool_size`

- Default: 16
- Type: Int
- Unit: Connections
- Is mutable: No
- Description: FE の BrpcProxy がエンドポイントごとに使用する BRPC 接続プールの最大数。この値は `setMaxTotoal` および `setMaxIdleSize` を介して RpcClientOptions に適用されるため、各要求はプールから接続を借りる必要があるため、同時発信 BRPC 要求を直接制限します。高並行シナリオでは、要求キューイングを回避するためにこれを増やしてください。増やすとソケットとメモリの使用量が増加し、リモートサーバーの負荷が増加する可能性があります。調整する際には、`brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr`、および `brpc_min_evictable_idle_time_ms` などの関連設定を考慮してください。この値を変更することはホットリロード可能ではなく、再起動が必要です。
- Introduced in: v3.2.0

##### `brpc_short_connection`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: 下層の brpc RpcClient が短命の接続を使用するかどうかを制御します。有効な場合 (`true`)、RpcClientOptions.setShortConnection が設定され、要求完了後に接続が閉じられ、接続設定のオーバーヘッドが増加し、レイテンシーが増加する代わりに、長命のソケットの数が減少します。無効な場合 (`false`、デフォルト)、永続的な接続と接続プールが使用されます。このオプションを有効にすると、接続プール動作に影響するため、`brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr`、および `brpc_inner_reuse_pool` と合わせて検討する必要があります。一般的な高スループットデプロイメントでは無効のままにし、ソケットのライフタイムを制限する必要がある場合や、ネットワークポリシーによって短命の接続が要求される場合にのみ有効にします。
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
- Description: `true` の場合、CheckpointController はリーダー FE のみをチェックポイントワーカーとして選択します。`false` の場合、コントローラーは任意のフロントエンドを選択し、ヒープ使用量が少ないノードを優先します。`false` の場合、ワーカーは最近の失敗時間と `heapUsedPercent` でソートされます (リーダーは無限のヒープ使用量を持つものとして扱われ、選択されないようにします)。クラスターのスナップショットメタデータを必要とする操作の場合、コントローラーはこのフラグに関係なくリーダーの選択を強制します。`true` を有効にすると、チェックポイント作業がリーダーに集中します (単純ですが、リーダーの CPU/メモリとネットワーク負荷が増加します)。`false` のままにすると、負荷の少ない FE にチェックポイント負荷が分散されます。この設定は、ワーカーの選択と、`checkpoint_timeout_seconds` などのタイムアウトや `thrift_rpc_timeout_ms` などの RPC 設定との相互作用に影響します。
- Introduced in: v3.4.0, v3.5.0

##### `checkpoint_timeout_seconds`

- Default: 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: リーダーの CheckpointController がチェックポイントワーカーがチェックポイントを完了するのを待つ最大時間 (秒単位)。コントローラーはこの値をナノ秒に変換し、ワーカーの結果キューをポーリングします。このタイムアウト内に成功した完了が受信されない場合、チェックポイントは失敗と見なされ、createImage は失敗を返します。この値を増やすと、長時間実行されるチェックポイントに対応できますが、失敗検出と後続のイメージ伝播が遅れます。値を減らすと、より高速なフェイルオーバー/再試行が発生しますが、低速なワーカーに対して誤ったタイムアウトが発生する可能性があります。この設定は、チェックポイント作成中の `CheckpointController` での待機期間のみを制御し、ワーカーの内部チェックポイント動作は変更しません。
- Introduced in: v3.4.0, v3.5.0

##### `db_used_data_quota_update_interval_secs`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: データベース使用データクォータが更新される間隔。StarRocks は、ストレージ消費量を追跡するために、すべてのデータベースの使用データクォータを定期的に更新します。この値はクォータ強制とメトリック収集に使用されます。過剰なシステム負荷を防ぐため、許容される最小間隔は 30 秒です。30 未満の値は拒否されます。
- Introduced in: -

##### `drop_backend_after_decommission`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: BE が停止解除された後に BE を削除するかどうか。`TRUE` は、BE が停止解除された直後に削除されることを示します。`FALSE` は、BE が停止解除された後も削除されないことを示します。
- Introduced in: -

##### `edit_log_port`

- Default: 9010
- Type: Int
- Unit: -
- Is mutable: No
- Description: クラスター内のリーダー、フォロワー、オブザーバー FE 間で通信に使用されるポート。
- Introduced in: -

##### `edit_log_roll_num`

- Default: 50000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: メタデータログエントリの最大数。この数に達すると、これらのログエントリ用のログファイルが作成されます。このパラメーターは、ログファイルのサイズを制御するために使用されます。新しいログファイルは BDBJE データベースに書き込まれます。
- Introduced in: -

##### `edit_log_type`

- Default: BDB
- Type: String
- Unit: -
- Is mutable: No
- Description: 生成できる編集ログの種類。値を `BDB` に設定します。
- Introduced in: -

##### `enable_background_refresh_connector_metadata`

- Default: true in v3.0 and later and false in v2.5
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 定期的な Hive メタデータキャッシュ更新を有効にするかどうか。有効にすると、StarRocks は Hive クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Hive カタログのキャッシュされたメタデータを更新してデータ変更を認識します。`true` は Hive メタデータキャッシュ更新を有効にすることを示し、`false` は無効にすることを示します。
- Introduced in: v2.5.5

##### `enable_collect_query_detail_info`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クエリのプロファイルを収集するかどうか。このパラメーターが `TRUE` に設定されている場合、システムはクエリのプロファイルを収集します。このパラメーターが `FALSE` に設定されている場合、システムはクエリのプロファイルを収集しません。
- Introduced in: -

##### `enable_create_partial_partition_in_batch`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `false` (デフォルト) に設定されている場合、StarRocks は、バッチ作成された範囲パーティションが標準の時刻単位境界に揃うことを強制します。穴の作成を避けるために、非整列の範囲は拒否されます。この項目を `true` に設定すると、そのアラインメントチェックが無効になり、バッチで部分的な (非標準の) パーティションを作成できるようになります。これにより、ギャップや誤ったパーティション範囲が生成される可能性があります。意図的に部分的なバッチパーティションが必要であり、関連するリスクを受け入れる場合にのみ `true` に設定する必要があります。
- Introduced in: v3.2.0

##### `enable_internal_sql`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: この項目が `true` に設定されている場合、内部コンポーネント (例: SimpleExecutor) によって実行される内部 SQL ステートメントは、内部監査またはログメッセージに保持および書き込まれます ( `enable_sql_desensitize_in_log` が設定されている場合、さらに非機密化できます)。`false` に設定されている場合、内部 SQL テキストは抑制されます。フォーマットコード (SimpleExecutor.formatSQL) は "?" を返し、実際のステートメントは内部監査またはログメッセージに出力されません。この設定は、内部ステートメントの実行セマンティクスを変更しません。プライバシーまたはセキュリティのために内部 SQL のロギングと可視性のみを制御します。
- Introduced in: -

##### `enable_legacy_compatibility_for_replication`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: レプリケーションのレガシー互換性を有効にするかどうか。StarRocks は、以前のバージョンと新しいバージョンで異なる動作をする可能性があり、クロスクラスターデータ移行中に問題を引き起こすことがあります。したがって、データ移行前にターゲットクラスターでレガシー互換性を有効にし、データ移行完了後に無効にする必要があります。`true` はこのモードを有効にすることを示します。
- Introduced in: v3.1.10, v3.2.6

##### `enable_show_materialized_views_include_all_task_runs`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: SHOW MATERIALIZED VIEWS コマンドに TaskRuns が返される方法を制御します。この項目が `false` に設定されている場合、StarRocks はタスクごとに最新の TaskRun のみを返します (互換性のためのレガシー動作)。`true` (デフォルト) に設定されている場合、`TaskManager` は、同じ開始 TaskRun ID を共有する場合 (たとえば、同じジョブに属する場合) にのみ、同じタスクの追加の TaskRun を含めることができ、無関係な重複実行が表示されるのを防ぎながら、1 つのジョブに紐付けられた複数のステータスを表示できます。単一実行の出力を復元したり、デバッグと監視のために複数実行ジョブの履歴を表示したりするには、この項目を `false` に設定します。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `enable_statistics_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 統計クエリのプロファイルを生成するかどうか。この項目を `true` に設定すると、StarRocks がシステム統計に関するクエリのクエリプロファイルを生成できるようになります。
- Introduced in: v3.1.5

##### `enable_table_name_case_insensitive`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: カタログ名、データベース名、テーブル名、ビュー名、マテリアライズドビュー名の大文字小文字を区別しない処理を有効にするかどうか。現在、テーブル名は大文字小文字を区別します。
  - この機能を有効にすると、関連するすべての名前は小文字で格納され、これらの名前を含むすべての SQL コマンドは自動的に小文字に変換されます。
  - この機能は、クラスター作成時にのみ有効にできます。**クラスター起動後、この設定の値をいかなる方法でも変更することはできません**。変更しようとするとエラーが発生します。FE は、この設定アイテムの値がクラスターが最初に起動されたときと一致しないことを検出すると、起動に失敗します。
  - 現在、この機能は JDBC カタログおよびテーブル名をサポートしていません。JDBC または ODBC データソースで大文字小文字を区別しない処理を実行する場合は、この機能を有効にしないでください。
- Introduced in: v4.0

##### `enable_task_history_archive`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、完了したタスク実行レコードは永続的なタスク実行履歴テーブルにアーカイブされ、編集ログに記録されるため、ルックアップ (例: `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`) にアーカイブされた結果が含まれます。アーカイブは FE リーダーによって実行され、単体テスト中 (`FeConstants.runningUnitTest`) はスキップされます。有効にすると、インメモリの有効期限と強制 GC パスはバイパスされ (コードは `removeExpiredRuns` と `forceGC` から早期にリターンします)、保持/削除は `task_runs_ttl_second` と `task_runs_max_history_number` ではなく永続アーカイブによって処理されます。無効にすると、履歴はメモリ内に残り、これらの設定によってプルーニングされます。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `enable_task_run_fe_evaluation`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、FE は `TaskRunsSystemTable.supportFeEvaluation` でシステムテーブル `task_runs` のローカル評価を実行します。FE 側の評価は、列と定数を比較する結合等価述語にのみ許可され、列 `QUERY_ID` と `TASK_NAME` に制限されます。これを有効にすると、対象を絞ったルックアップのパフォーマンスが向上し、より広範なスキャンや追加のリモート処理が回避されます。無効にすると、プランナーは `task_runs` の FE 評価をスキップするため、述語のプルーニングが減少し、これらのフィルターのクエリレイテンシーに影響する可能性があります。
- Introduced in: v3.3.13, v3.4.3, v3.5.0

##### `heartbeat_mgr_blocking_queue_size`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: No
- Description: Heartbeat Manager によって実行されるハートビートタスクを格納するブロッキングキューのサイズ。
- Introduced in: -

##### `heartbeat_mgr_threads_num`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: Heartbeat Manager がハートビートタスクを実行するために実行できるスレッド数。
- Introduced in: -

##### `ignore_materialized_view_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE がマテリアライズドビューエラーによって引き起こされたメタデータ例外を無視するかどうか。マテリアライズドビューエラーによって引き起こされたメタデータ例外のために FE の起動に失敗した場合、このパラメーターを `true` に設定して FE が例外を無視できるようにすることができます。
- Introduced in: v2.5.10

##### `ignore_meta_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非リーダー FE がリーダー FE からのメタデータギャップを無視するかどうか。値が TRUE の場合、非リーダー FE はリーダー FE からのメタデータギャップを無視し、データ読み取りサービスを継続して提供します。このパラメーターは、リーダー FE を長期間停止した場合でも継続的なデータ読み取りサービスを保証します。値が FALSE の場合、非リーダー FE はリーダー FE からのメタデータギャップを無視せず、データ読み取りサービスの提供を停止します。
- Introduced in: -

##### `ignore_task_run_history_replay_error`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: StarRocks が `information_schema.task_runs` の TaskRun 履歴行を逆シリアル化する際、破損または無効な JSON 行は通常、逆シリアル化が警告をログに記録し、RuntimeException をスローします。この項目が `true` に設定されている場合、システムは逆シリアル化エラーをキャッチし、不正な形式のレコードをスキップし、クエリを失敗させるのではなく、残りの行の処理を続行します。これにより、`information_schema.task_runs` クエリは `_statistics_.task_run_history` テーブル内の不正なエントリに対して寛容になります。ただし、これを有効にすると、破損した履歴レコードが明示的なエラーを表面化する代わりにサイレントにドロップされることになります (潜在的なデータ損失)。
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### `lock_checker_interval_second`

- Default: 30
- Type: long
- Unit: Seconds
- Is mutable: Yes
- Description: LockChecker フロントエンドデーモン ("deadlock-checker" という名前) の実行間の間隔 (秒単位)。デーモンはデッドロック検出と低速ロックのスキャンを実行します。設定値はミリ秒でタイマーを設定するために 1000 倍されます。この値を減らすと検出レイテンシーが短くなりますが、スケジューリングと CPU オーバーヘッドが増加します。増やすとオーバーヘッドが減少しますが、検出と低速ロックレポートが遅れます。変更は、デーモンが実行ごとに間隔をリセットするため、実行時に有効になります。この設定は、`lock_checker_enable_deadlock_check` (デッドロックチェックを有効にする) と `slow_lock_threshold_ms` (低速ロックを構成するものを定義する) と相互作用します。
- Introduced in: v3.2.0

##### `master_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: リーダー FE がログをディスクにフラッシュするポリシー。このパラメーターは、現在の FE がリーダー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされると、ログエントリの生成とフラッシュは同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。

  フォロワー FE を 1 つだけデプロイしている場合、このパラメーターを `SYNC` に設定することをお勧めします。フォロワー FE を 3 つ以上デプロイしている場合、このパラメーターと `replica_sync_policy` の両方を `WRITE_NO_SYNC` に設定することをお勧めします。

- Introduced in: -

##### `max_bdbje_clock_delta_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: StarRocks クラスター内のリーダー FE とフォロワーまたはオブザーバー FE 間で許可される最大クロックオフセット。
- Introduced in: -

##### `meta_delay_toleration_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: フォロワー FE およびオブザーバー FE のメタデータがリーダー FE のメタデータよりも遅延できる最大期間。単位: 秒。この期間を超えると、非リーダー FE はサービスの提供を停止します。
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
- Description: 未知のログ ID を無視するかどうか。FE がロールバックされた場合、以前のバージョンの FE は一部のログ ID を認識できない可能性があります。値が `TRUE` の場合、FE は未知のログ ID を無視します。値が `FALSE` の場合、FE は終了します。
- Introduced in: -

##### `profile_info_format`

- Default: default
- Type: String
- Unit: -
- Is mutable: Yes
- Description: システムが出力するプロファイルの形式。有効な値: `default` と `json`。`default` に設定すると、プロファイルはデフォルトの形式になります。`json` に設定すると、システムはプロファイルを JSON 形式で出力します。
- Introduced in: v2.5

##### `replica_ack_policy`

- Default: `SIMPLE_MAJORITY`
- Type: String
- Unit: -
- Is mutable: No
- Description: ログエントリが有効であると見なされるポリシー。デフォルト値 `SIMPLE_MAJORITY` は、ログエントリがフォロワー FE の過半数が ACK メッセージを返した場合に有効であると見なされることを指定します。
- Introduced in: -

##### `replica_sync_policy`

- Default: SYNC
- Type: String
- Unit: -
- Is mutable: No
- Description: フォロワー FE がログをディスクにフラッシュするポリシー。このパラメーターは、現在の FE がフォロワー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされると、ログエントリの生成とフラッシュは同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。
- Introduced in: -

##### `start_with_incomplete_meta`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: No
- Description: true の場合、FE はイメージデータが存在するが Berkeley DB JE (BDB) ログファイルが欠落または破損している場合に起動を許可します。`MetaHelper.checkMetaDir()` はこのフラグを使用して、対応する BDB ログのないイメージからの起動を防ぐ安全チェックをバイパスします。この方法で起動すると、古いまたは矛盾したメタデータが生成される可能性があり、緊急復旧にのみ使用する必要があります。`RestoreClusterSnapshotMgr` はクラスターのスナップショットを復元する際に一時的にこのフラグを true に設定し、その後ロールバックします。このコンポーネントは復元中に `bdbje_reset_election_group` も切り替えます。通常の操作では有効にしないでください。破損した BDB データから復旧する場合、またはイメージベースのスナップショットを明示的に復元する場合にのみ有効にしてください。
- Introduced in: v3.2.0

##### `table_keeper_interval_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: TableKeeper デーモンの実行間の間隔 (秒単位)。TableKeeperDaemon はこの値 (1000 倍) を使用して内部タイマーを設定し、履歴テーブルが存在すること、正しいテーブルプロパティ (レプリケーション番号) があること、パーティション TTL を更新することを保証するキーパータスクを定期的に実行します。デーモンはリーダーノードでのみ作業を実行し、`table_keeper_interval_second` が変更されると `setInterval` を介して実行時間隔を更新します。スケジューリング頻度と負荷を減らすには値を増やし、欠落または古い履歴テーブルへの反応を速くするには値を減らします。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `task_runs_ttl_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タスク実行履歴の Time-To-Live (TTL) を制御します。この値を小さくすると履歴の保持期間が短くなり、メモリ/ディスク使用量が減ります。値を大きくすると履歴が長く保持されますが、リソース使用量が増加します。予測可能な保持とストレージ動作のために、`task_runs_max_history_number` と `enable_task_history_archive` と一緒に調整してください。
- Introduced in: v3.2.0

##### `task_ttl_second`

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タスクの Time-to-live (TTL)。手動タスク (スケジュールが設定されていない場合)、TaskBuilder はこの値を使用してタスクの `expireTime` を計算します (`expireTime = now + task_ttl_second * 1000L`)。TaskRun も、実行のタイムアウトを計算する際にこの値を上限として使用します。実質的な実行タイムアウトは `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)` です。この値を調整すると、手動で作成されたタスクが有効な期間が変更され、タスク実行の最大許容実行時間を間接的に制限できます。
- Introduced in: v3.2.0

##### `thrift_rpc_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Thrift RPC 呼び出しが行う合計試行回数を制御します。この値は `ThriftRPCRequestExecutor` (および `NodeMgr` や `VariableMgr` などの呼び出し元) によって再試行のループカウントとして使用されます。つまり、値 3 は初期試行を含めて最大 3 回の試行を許可します。`TTransportException` の場合、エグゼキューターは接続を再開してこの回数まで再試行します。原因が `SocketTimeoutException` の場合や再開が失敗した場合は再試行しません。各試行は `thrift_rpc_timeout_ms` で設定された試行ごとのタイムアウトの対象となります。この値を増やすと、一時的な接続障害に対する回復力は向上しますが、全体の RPC レイテンシーとリソース使用量が増加する可能性があります。
- Introduced in: v3.2.0

##### `thrift_rpc_strict_mode`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Thrift サーバーで使用される TBinaryProtocol の「厳密読み取り」モードを制御します。この値は Thrift サーバーのスタックにある org.apache.thrift.protocol.TBinaryProtocol.Factory に最初の引数として渡され、受信 Thrift メッセージの解析と検証方法に影響します。`true` (デフォルト) の場合、サーバーは厳密な Thrift エンコーディング/バージョンチェックを強制し、設定された `thrift_rpc_max_body_size` 制限を尊重します。`false` の場合、サーバーは非厳密な (レガシー/寛容な) メッセージ形式を受け入れます。これにより、古いクライアントとの互換性は向上する可能性がありますが、一部のプロトコル検証がバイパスされる可能性があります。これは変更不可能であり、相互運用性と解析の安全性に影響するため、稼働中のクラスターでこれを変更する場合は注意してください。
- Introduced in: v3.2.0

##### `thrift_rpc_timeout_ms`

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Thrift RPC 呼び出しのデフォルトのネットワーク/ソケットタイムアウトとして使用されるタイムアウト (ミリ秒単位)。`ThriftConnectionPool` (フロントエンドおよびバックエンドプールで使用) で Thrift クライアントを作成するときに TSocket に渡され、また `ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr`、`CheckpointWorker` などの場所で RPC 呼び出しのタイムアウトを計算するときに操作の実行タイムアウトに追加されます (例: ExecTimeout*1000 + `thrift_rpc_timeout_ms`)。この値を増やすと、RPC 呼び出しは長いネットワークまたはリモート処理の遅延を許容できます。減らすと、低速なネットワークでのフェイルオーバーが高速化されます。この値を変更すると、Thrift RPC を実行する FE コードパス全体で接続作成と要求の期限に影響します。
- Introduced in: v3.2.0

##### `txn_latency_metric_report_groups`

- Default: An empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: レポートするトランザクションレイテンシーメトリックグループのコンマ区切りリスト。ロードタイプは監視のために論理グループに分類されます。グループが有効になっている場合、その名前はトランザクションメトリックに「type」ラベルとして追加されます。有効な値: `stream_load`、`routine_load`、`broker_load`、`insert`、および `compaction` (共有データクラスターでのみ利用可能)。例: `"stream_load,routine_load"`。
- Introduced in: v4.0

##### `txn_rollback_limit`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: ロールバックできるトランザクションの最大数。
- Introduced in: -

### ユーザー、ロール、権限

##### `enable_task_info_mask_credential`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: true の場合、StarRocks は `information_schema.tasks` および `information_schema.task_runs` で返される前に、タスク SQL 定義から資格情報を編集します。これは、DEFINITION 列に SqlCredentialRedactor.redact を適用することで行われます。`information_schema.task_runs` では、定義がタスク実行ステータスから来るか、空の場合にタスク定義ルックアップから来るかに関係なく、同じ編集が適用されます。false の場合、生のタスク定義が返されます (資格情報が公開される可能性があります)。マスキングは CPU/文字列処理作業であり、タスクまたは `task_runs` の数が大きい場合は時間がかかる場合があります。非編集定義が必要であり、セキュリティリスクを受け入れる場合にのみ無効にしてください。
- Introduced in: v3.5.6

##### `privilege_max_role_depth`

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description: ロールの最大ロール深度 (継承レベル)。
- Introduced in: v3.0.0

##### `privilege_max_total_roles_per_user`

- Default: 64
- Type: Int
- Unit:
- Is mutable: Yes
- Description: ユーザーが持つことができるロールの最大数。
- Introduced in: v3.0.0

### クエリエンジン

##### `brpc_send_plan_fragment_timeout_ms`

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: プランフラグメントを送信する前に BRPC TalkTimeoutController に適用されるタイムアウト (ミリ秒単位)。`BackendServiceClient.sendPlanFragmentAsync` は、バックエンド `execPlanFragmentAsync` を呼び出す前にこの値を設定します。これは、BRPC がアイドル接続を接続プールから借りる際や送信を実行する際に待機する期間を管理します。超過した場合、RPC は失敗し、メソッドの再試行ロジックをトリガーする可能性があります。競合時に迅速に失敗させるにはこれを低く設定し、一時的なプール枯渇や低速ネットワークを許容するには高く設定します。注意: 非常に大きな値は、失敗検出を遅延させ、要求スレッドをブロックする可能性があります。
- Introduced in: v3.3.11, v3.4.1, v3.5.0

##### `connector_table_query_trigger_analyze_large_table_interval`

- Default: 12 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: 大規模テーブルのクエリトリガー ANALYZE タスクの間隔。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_max_pending_task_num`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE で保留状態にあるクエリトリガー ANALYZE タスクの最大数。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_max_running_task_num`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE で実行状態にあるクエリトリガー ANALYZE タスクの最大数。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_interval`

- Default: 2 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: 小規模テーブルのクエリトリガー ANALYZE タスクの間隔。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_analyze_small_table_rows`

- Default: 10000000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: クエリトリガー ANALYZE タスクのテーブルが小規模テーブルであるかどうかを判断するためのしきい値。
- Introduced in: v3.4.0

##### `connector_table_query_trigger_task_schedule_interval`

- Default: 30
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: スケジューラースレッドがクエリトリガーバックグラウンドタスクをスケジュールする間隔。この項目は v3.4.0 で導入された `connector_table_query_trigger_analyze_schedule_interval` を置き換えるものです。ここで、バックグラウンドタスクとは v3.4 の `ANALYZE` タスクと、v3.4 以降の低カーディナリティ列の辞書収集タスクを指します。
- Introduced in: v3.4.2

##### `create_table_max_serial_replicas`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: シリアルに作成するレプリカの最大数。実際のレプリカ数がこの値を超えると、レプリカは並行して作成されます。テーブル作成に時間がかかりすぎる場合は、この値を減らしてみてください。
- Introduced in: -

##### `default_mv_partition_refresh_number`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新が複数のパーティションを伴う場合、このパラメーターは、デフォルトで 1 つのバッチで更新されるパーティションの数を制御します。
バージョン 3.3.0 以降、システムはデフォルトで一度に 1 つのパーティションを更新して、潜在的なメモリ不足 (OOM) の問題を回避します。以前のバージョンでは、デフォルトですべてのパーティションが一度に更新され、メモリ枯渇やタスク障害につながる可能性がありました。ただし、マテリアライズドビューの更新が多数のパーティションを伴う場合、一度に 1 つのパーティションのみを更新すると、スケジューリングのオーバーヘッドが過剰になり、全体の更新時間が長くなり、大量の更新レコードが生成される可能性があることに注意してください。そのような場合は、更新効率を向上させ、スケジューリングコストを削減するために、このパラメーターを適切に調整することをお勧めします。
- Introduced in: v3.3.0

##### `default_mv_refresh_immediate`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非同期マテリアライズドビューの作成後すぐに更新するかどうか。この項目が `true` に設定されている場合、新しく作成されたマテリアライズドビューはすぐに更新されます。
- Introduced in: v3.2.3

##### `dynamic_partition_check_interval_seconds`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: 新しいデータがチェックされる間隔。新しいデータが検出された場合、StarRocks は自動的にデータ用のパーティションを作成します。
- Introduced in: -

##### `dynamic_partition_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 動的パーティショニング機能を有効にするかどうか。この機能が有効になっている場合、StarRocks は新しいデータ用のパーティションを動的に作成し、期限切れのパーティションを自動的に削除してデータの鮮度を保証します。
- Introduced in: -

##### `enable_active_materialized_view_schema_strict_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非アクティブなマテリアライズドビューをアクティブ化するときに、データ型の長さの一貫性を厳密にチェックするかどうか。この項目が `false` に設定されている場合、基底テーブルでデータ型の長さが変更されても、マテリアライズドビューのアクティブ化は影響を受けません。
- Introduced in: v3.3.4

##### `enable_auto_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ARRAY 型の NDV 情報の自動収集を有効にするかどうか。
- Introduced in: v4.0

##### `enable_backup_materialized_view`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 特定のデータベースをバックアップまたは復元する際に、非同期マテリアライズドビューの BACKUP と RESTORE を有効にするかどうか。この項目が `false` に設定されている場合、StarRocks は非同期マテリアライズドビューのバックアップをスキップします。
- Introduced in: v3.2.0

##### `enable_collect_full_statistic`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 自動完全統計収集を有効にするかどうか。この機能はデフォルトで有効になっています。
- Introduced in: -

##### `enable_colocate_mv_index`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 同期マテリアライズドビューを作成するときに、同期マテリアライズドビューインデックスを基底テーブルとコロケートすることをサポートするかどうか。この項目が `true` に設定されている場合、タブレットシンクは同期マテリアライズドビューの書き込みパフォーマンスを高速化します。
- Introduced in: v3.2.0

##### `enable_decimal_v3`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: DECIMAL V3 データ型をサポートするかどうか。
- Introduced in: -

##### `enable_experimental_mv`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非同期マテリアライズドビュー機能を有効にするかどうか。TRUE はこの機能が有効であることを示します。v2.5.2 以降、この機能はデフォルトで有効になっています。v2.5.2 以前のバージョンでは、この機能はデフォルトで無効になっています。
- Introduced in: v2.4

##### `enable_local_replica_selection`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クエリのためにローカルレプリカを選択するかどうか。ローカルレプリカはネットワーク転送コストを削減します。このパラメーターが TRUE に設定されている場合、CBO は現在の FE と同じ IP アドレスを持つ BE 上のタブレットレプリカを優先的に選択します。このパラメーターが `FALSE` に設定されている場合、ローカルレプリカと非ローカルレプリカの両方を選択できます。
- Introduced in: -

##### `enable_manual_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ARRAY 型の NDV 情報の手動収集を有効にするかどうか。
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
- Description: 基底テーブルが外部 (クラウドネイティブではない) テーブルである場合に、マテリアライズドビューの更新のための内部最適化を有効にするには、この項目を `true` に設定します。有効にすると、マテリアライズドビューの更新プロセッサは候補パーティションを計算し、すべてのパーティションではなく影響を受ける基底テーブルパーティションのみを更新し、I/O と更新コストを削減します。外部テーブルの完全パーティション更新を強制するには `false` に設定します。
- Introduced in: v3.2.9

##### `enable_materialized_view_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 非同期マテリアライズドビューの監視メトリックをデフォルトで収集するかどうか。
- Introduced in: v3.1.11, v3.2.5

##### `enable_materialized_view_spill`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビュー更新タスクの途中結果スピルを有効にするかどうか。
- Introduced in: v3.1.1

##### `enable_materialized_view_text_based_rewrite`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: デフォルトでテキストベースのクエリ書き換えを有効にするかどうか。この項目が `true` に設定されている場合、システムは非同期マテリアライズドビューの作成中に抽象構文ツリーを構築します。
- Introduced in: v3.2.5

##### `enable_mv_automatic_active_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: スキーマ変更または基底テーブル (ビュー) の削除と再作成によって非アクティブになった非同期マテリアライズドビューを、システムが自動的にチェックして再アクティブ化する機能を有効にするかどうか。この機能は、ユーザーが手動で非アクティブに設定したマテリアライズドビューを再アクティブ化しないことに注意してください。
- Introduced in: v3.1.6

##### `enable_mv_automatic_repairing_for_broken_base_tables`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、StarRocks は、基底外部テーブルが削除され再作成されたり、テーブル識別子が変更されたりした場合に、マテリアライズドビューの基底テーブルメタデータを自動的に修復しようとします。修復フローは、マテリアライズドビューの基底テーブル情報を更新し、外部テーブルパーティションのパーティションレベルの修復情報を収集し、`autoRefreshPartitionsLimit` を尊重しながら非同期自動更新マテリアライズドビューのパーティション更新決定を駆動することができます。現在、自動修復は Hive 外部テーブルをサポートしています。サポートされていないテーブルタイプでは、マテリアライズドビューが非アクティブに設定され、修復例外が発生します。パーティション情報収集は非ブロッキングであり、失敗はログに記録されます。
- Introduced in: v3.3.19, v3.4.8, v3.5.6

##### `enable_predicate_columns_collection`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 述語列収集を有効にするかどうか。無効にすると、クエリオプティマイゼーション中に述語列は記録されません。
- Introduced in: -

##### `enable_query_queue_v2`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: true の場合、FE のスロットベースクエリスケジューラを Query Queue V2 に切り替えます。このフラグはスロットマネージャーとトラッカー (例: `BaseSlotManager.isEnableQueryQueueV2` と `SlotTracker#createSlotSelectionStrategy`) によって読み取られ、レガシー戦略ではなく `SlotSelectionStrategyV2` を選択します。`query_queue_v2_xxx` 構成オプションと `QueryQueueOptions` は、このフラグが有効な場合にのみ有効になります。v4.1 以降、デフォルト値は `false` から `true` に変更されました。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `enable_sql_blacklist`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: SQL クエリのブラックリストチェックを有効にするかどうか。この機能が有効になっている場合、ブラックリスト内のクエリは実行できません。
- Introduced in: -

##### `enable_statistic_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: CBO の統計情報を収集するかどうか。この機能はデフォルトで有効になっています。
- Introduced in: -

##### `enable_statistic_collect_on_first_load`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: データロード操作によってトリガーされる自動統計収集とメンテナンスを制御します。これには以下が含まれます。
  - データがパーティションに最初にロードされたとき (パーティションバージョンが 2 のとき) の統計収集。
  - マルチパーティションテーブルの空のパーティションにデータがロードされたときの統計収集。
  - INSERT OVERWRITE 操作の統計コピーと更新。

  **統計収集タイプの決定ポリシー:**
  
  - INSERT OVERWRITE の場合: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。既存の統計のみがコピーされます。
    - それ以外の場合、`targetRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  - 最初のロードの場合: `deltaRatio = loadRows / (totalRows + 1)`
    - `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (デフォルト: 0.1) の場合、統計収集は実行されません。
    - それ以外の場合、`loadRows > statistic_sample_collect_rows` (デフォルト: 200000) の場合、SAMPLE 統計収集が使用されます。
    - それ以外の場合、FULL 統計収集が使用されます。
  
  **同期動作:**
  
  - DML ステートメント (INSERT INTO/INSERT OVERWRITE) の場合: テーブルロックを使用した同期モード。ロード操作は統計収集が完了するまで待機します (最大 `semi_sync_collect_statistic_await_seconds` まで)。
  - ストリームロードとブローカーロードの場合: ロックなしの非同期モード。統計収集はバックグラウンドで実行され、ロード操作をブロックしません。
  
  :::note
  この設定を無効にすると、ロードトリガー統計操作がすべて防止されます。これには INSERT OVERWRITE の統計メンテナンスも含まれ、テーブルに統計が不足する可能性があります。新しいテーブルが頻繁に作成され、データが頻繁にロードされる場合、この機能を有効にするとメモリと CPU のオーバーヘッドが増加します。
  :::

- Introduced in: v3.1

##### `enable_statistic_collect_on_update`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: UPDATE ステートメントが自動統計収集をトリガーできるかどうかを制御します。有効にすると、テーブルデータを変更する UPDATE 操作は、`enable_statistic_collect_on_first_load` によって制御される同じインジェストベースの統計フレームワークを通じて統計収集をスケジュールする可能性があります。この設定を無効にすると、UPDATE ステートメントの統計収集はスキップされますが、ロードトリガー統計収集の動作は変更されません。
- Introduced in: v3.5.11, v4.0.4

##### `enable_udf`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: UDF を有効にするかどうか。
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
- Description: ヒストグラムで収集する最大行数。
- Introduced in: -

##### `histogram_mcv_size`

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ヒストグラムの最頻値 (MCV) の数。
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
- Unit: Milliseconds
- Is mutable: Yes
- Description: HTTP リクエストの応答時間がこのパラメーターで指定された値を超えると、そのリクエストを追跡するためのログが生成されます。
- Introduced in: v2.5.15, v3.1.5

##### `lock_checker_enable_deadlock_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、LockChecker スレッドは ThreadMXBean.findDeadlockedThreads() を使用して JVM レベルのデッドロック検出を実行し、問題のあるスレッドのスタックトレースをログに記録します。このチェックは LockChecker デーモン内で実行され (その頻度は `lock_checker_interval_second` で制御されます)、詳細なスタック情報をログに書き込みます。これは CPU および I/O を集中的に行う可能性があります。このオプションは、ライブまたは再現可能なデッドロック問題のトラブルシューティングにのみ有効にしてください。通常の操作で有効のままにすると、オーバーヘッドとログのボリュームが増加する可能性があります。
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
- Unit: Seconds
- Is mutable: Yes
- Description: 非同期マテリアライズドビューのスケジュールで許可される最小更新間隔 (秒単位)。マテリアライズドビューが時間ベースの間隔で作成された場合、その間隔は秒に変換され、この値以上でなければなりません。そうでない場合、CREATE/ALTER 操作は DDL エラーで失敗します。この値が 0 より大きい場合、チェックが強制されます。TaskManager の過剰なスケジューリングと、頻繁すぎる更新による FE のメモリ/CPU 使用率の高さを防ぐため、制限を無効にするには 0 または負の値に設定します。この項目は `EVENT_TRIGGERED` の更新には適用されません。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `materialized_view_refresh_ascending`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、マテリアライズドビューのパーティション更新は昇順のパーティションキー順 (最も古いものから最も新しいものへ) でパーティションを反復処理します。`false` (デフォルト) に設定されている場合、システムは降順 (最も新しいものから最も古いものへ) で反復処理します。StarRocks は、パーティション更新制限が適用される場合に処理するパーティションを選択し、後続の TaskRun 実行の次の開始/終了パーティション境界を計算するために、リストパーティションおよび範囲パーティションのマテリアライズドビュー更新ロジックの両方でこの項目を使用します。この項目を変更すると、最初に更新されるパーティションと、次のパーティション範囲がどのように導出されるかが変わります。範囲パーティションのマテリアライズドビューの場合、スケジューラは新しい開始/終了を検証し、変更によって繰り返される境界 (デッドループ) が作成される場合はエラーを発生させるため、この項目は注意して設定してください。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `max_allowed_in_element_num_of_delete`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: DELETE ステートメントの IN 述語で許可される要素の最大数。
- Introduced in: -

##### `max_create_table_timeout_second`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: テーブル作成の最大タイムアウト期間。
- Introduced in: -

##### `max_distribution_pruner_recursion_depth`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: パーティションプルーナーによって許可される最大再帰深度。再帰深度を増やすと、より多くの要素をプルーニングできますが、CPU 消費量も増加します。
- Introduced in: -

##### `max_partitions_in_one_batch`

- Default: 4096
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: 複数のパーティションを一括作成する際に作成できる最大パーティション数。
- Introduced in: -

##### `max_planner_scalar_rewrite_num`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: オプティマイザがスカラー演算子を書き換えできる最大回数。
- Introduced in: -

##### `max_query_queue_history_slots_number`

- Default: 0
- Type: Int
- Unit: Slots
- Is mutable: Yes
- Description: 監視および可観測性のために、クエリキューごとに保持される最近解放された (履歴) 割り当てスロットの数を制御します。`max_query_queue_history_slots_number` が `> 0` の値に設定されている場合、BaseSlotTracker は、その数の最も最近解放された LogicalSlot エントリをメモリ内キューに保持し、制限を超えると最も古いエントリを削除します。これを有効にすると、getSlots() にこれらの履歴エントリ (最新のものが最初) が含まれるようになり、BaseSlotTracker はより豊富な ExtraMessage データのために ConnectContext にスロットを登録しようとすることができ、LogicalSlot.ConnectContextListener はクエリ完了メタデータを履歴スロットにアタッチできます。`max_query_queue_history_slots_number` が `<= 0` の場合、履歴メカニズムは無効になります (余分なメモリは使用されません)。可観測性とメモリオーバーヘッドのバランスを取るために、適切な値を使用してください。
- Introduced in: v3.5.0

##### `max_query_retry_time`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE でのクエリ再試行の最大数。
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
- Description：ScalarOperator のフラットな子の最大数。オプティマイザが過剰なメモリを使用するのを防ぐために、この制限を設定できます。
- Introduced in: -

##### `max_scalar_operator_optimize_depth`

- Default：256
- Type：Int
- Unit：-
- Is mutable: Yes
- Description: ScalarOperator 最適化が適用できる最大深度。
- Introduced in: -

##### `mv_active_checker_interval_seconds`

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: バックグラウンドの `active_checker` スレッドが有効な場合、システムは定期的に、スキーマ変更または基底テーブル (ビュー) の再構築によって非アクティブになったマテリアライズドビューを検出して自動的に再アクティブ化します。このパラメーターは、チェッカー スレッドのスケジューリング間隔を秒単位で制御します。デフォルト値はシステム定義です。
- Introduced in: v3.1.6

##### `mv_rewrite_consider_data_layout_mode`

- Default: `enable`
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 最適なマテリアライズドビューを選択する際に、マテリアライズドビューの書き換えで基底テーブルのデータレイアウトを考慮するかどうかを制御します。有効な値:
  - `disable`: 候補マテリアライズドビューを選択する際に、データレイアウト基準を使用しない。
  - `enable`: クエリがレイアウトを意識していると認識された場合にのみ、データレイアウト基準を使用する。
  - `force`: 最適なマテリアライズドビューを選択する際に、常にデータレイアウト基準を適用する。
  この項目を変更すると、`BestMvSelector` の動作に影響し、物理的なレイアウトが計画の正確性やパフォーマンスに影響するかどうかに応じて、書き換えの適用範囲を改善または拡大することができます。
- Introduced in: -

##### `publish_version_interval_ms`

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: リリース検証タスクが発行される時間間隔。
- Introduced in: -

##### `query_queue_slots_estimator_strategy`

- Default: MAX
- Type: String
- Unit: -
- Is mutable: Yes
- Description: `enable_query_queue_v2` が true の場合に、キューベースクエリに使用されるスロット推定戦略を選択します。有効な値は、MBE (メモリベース)、PBE (並行性ベース)、MAX (MBE と PBE の最大値を取る)、MIN (MBE と PBE の最小値を取る) です。MBE は、予測されたメモリまたは計画コストをスロットあたりのメモリターゲットで割ってスロットを推定し、`totalSlots` で上限が設定されます。PBE は、フラグメントの並行性 (スキャン範囲のカウントまたはカーディナリティ / スロットあたりの行数) と CPU コストベースの計算 (スロットあたりの CPU コストを使用) からスロットを導出し、結果を [numSlots/2, numSlots] の範囲内に制限します。MAX と MIN は、MBE と PBE の最大値または最小値を取ることによってそれらを結合します。設定された値が無効な場合、デフォルト (`MAX`) が使用されます。
- Introduced in: v3.5.0

##### `query_queue_v2_concurrency_level`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: システムの総クエリスロットを計算する際に使用される論理的な並行性「レイヤー」の数を制御します。Shared-nothing モードでは、総スロット = `query_queue_v2_concurrency_level` * BE の数 * BE ごとのコア数 (BackendResourceStat から派生)。マルチウェアハウスモードでは、実効並行性は max(1, `query_queue_v2_concurrency_level` / 4) にスケーリングされます。設定値が非正の場合、`4` として扱われます。この値を変更すると、totalSlots (したがって同時クエリ容量) が増減し、スロットごとのリソースに影響します。memBytesPerSlot はワーカーごとのメモリを (ワーカーごとのコア数 * 並行性) で割って導出され、CPU アカウンティングは `query_queue_v2_cpu_costs_per_slot` を使用します。クラスターサイズに比例して設定してください。非常に大きな値はスロットごとのメモリを減らし、リソースの断片化を引き起こす可能性があります。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_cpu_costs_per_slot`

- Default: 1000000000
- Type: Long
- Unit: planner CPU cost units
- Is mutable: Yes
- Description: プランナー CPU コストからクエリが必要とするスロット数を推定するために使用されるスロットごとの CPU コストしきい値。スケジューラーは、スロットを整数 (`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`) として計算し、結果を [1, totalSlots] の範囲にクランプします (totalSlots はクエリキュー V2 `V2` パラメーターから派生)。V2 コードは非正の設定を 1 に正規化するため (Math.max(1, value))、非正の値は事実上 `1` になります。この値を増やすと、クエリごとに割り当てられるスロットが減少し (より少ない、より大きなスロットのクエリを優先)、減らすとクエリごとのスロットが増加します。並行性対リソースの粒度を制御するために、`query_queue_v2_num_rows_per_slot` および並行性設定と合わせて調整してください。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_num_rows_per_slot`

- Default: 4096
- Type: Int
- Unit: Rows
- Is mutable: Yes
- Description: クエリごとのスロット数を推定する際に、単一のスケジューリングスロットに割り当てられるターゲットのソース行レコード数。StarRocks は、`estimated_slots` = (ソースノードのカーディナリティ) / `query_queue_v2_num_rows_per_slot` を計算し、その結果を [1, totalSlots] の範囲にクランプし、計算された値が非正の場合は最低 1 を強制します。totalSlots は利用可能なリソース (おおよそ DOP * `query_queue_v2_concurrency_level` * ワーカー/BE の数) から導出され、したがってクラスター/コア数に依存します。この値を増やすと、スロット数が減少し (各スロットがより多くの行を処理)、スケジューリングオーバーヘッドが減少します。減らすと、並行性が増加し (より多くの、より小さいスロット)、リソース制限まで増加します。
- Introduced in: v3.3.4, v3.4.0, v3.5.0

##### `query_queue_v2_schedule_strategy`

- Default: SWRR
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Query Queue V2 が保留中のクエリを並べ替えるために使用するスケジューリングポリシーを選択します。サポートされている値 (大文字と小文字を区別しない) は `SWRR` (Smooth Weighted Round Robin) - デフォルトで、公平な重み付き共有が必要な混合/ハイブリッドワークロードに適しています - と `SJF` (Short Job First + Aging) - 短いジョブを優先し、エージングを使用してスタベーションを回避します。値は大文字と小文字を区別しない enum ルックアップで解析されます。認識されない値はエラーとしてログに記録され、デフォルトポリシーが使用されます。この設定は Query Queue V2 が有効な場合にのみ動作に影響し、`query_queue_v2_concurrency_level` などの V2 サイジング設定と相互作用します。
- Introduced in: v3.3.12, v3.4.2, v3.5.0

##### `semi_sync_collect_statistic_await_seconds`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: DML 操作 (INSERT INTO および INSERT OVERWRITE ステートメント) 中の半同期統計収集の最大待機時間。ストリームロードおよびブローカーロードは非同期モードを使用するため、この設定の影響を受けません。統計収集時間がこの値を超えると、ロード操作は収集完了を待たずに続行します。この設定は `enable_statistic_collect_on_first_load` と連携して機能します。
- Introduced in: v3.1

##### `slow_query_analyze_threshold`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: クエリフィードバックの分析をトリガーするクエリの実行時間しきい値。
- Introduced in: v3.4.0

##### `statistic_analyze_status_keep_second`

- Default: 3 * 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: 収集タスクの履歴を保持する期間。デフォルト値は 3 日間です。
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
- Description: 自動収集の統計が健全であるかどうかを判断するためのしきい値。統計の健全性がこのしきい値よりも低い場合、自動収集がトリガーされます。
- Introduced in: -

##### `statistic_auto_collect_small_table_rows`

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: 自動収集中に、外部データソース (Hive、Iceberg、Hudi) のテーブルが小さいテーブルであるかどうかを判断するためのしきい値。テーブルの行数がこの値より少ない場合、そのテーブルは小さいテーブルと見なされます。
- Introduced in: v3.2

##### `statistic_cache_columns`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: No
- Description: 統計テーブルにキャッシュできる行数。
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
- Unit: Seconds
- Is mutable: Yes
- Description: 自動収集中にデータ更新をチェックする間隔。
- Introduced in: -

##### `statistic_max_full_collect_data_size`

- Default: 100 * 1024 * 1024 * 1024
- Type: Long
- Unit: bytes
- Is mutable: Yes
- Description: 統計の自動収集のデータサイズしきい値。合計サイズがこの値を超えると、完全収集ではなくサンプリング収集が実行されます。
- Introduced in: -

##### `statistic_sample_collect_rows`

- Default: 200000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: ロードトリガー統計操作中に SAMPLE 統計収集と FULL 統計収集のどちらかを決定するための行数しきい値。ロードまたは変更された行数がこのしきい値 (デフォルト 200,000) を超える場合、SAMPLE 統計収集が使用されます。そうでない場合、FULL 統計収集が使用されます。この設定は、`enable_statistic_collect_on_first_load` および `statistic_sample_collect_ratio_threshold_of_first_load` と連携して機能します。
- Introduced in: -

##### `statistic_update_interval_sec`

- Default: 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: 統計情報のキャッシュが更新される間隔。
- Introduced in: -

##### `task_check_interval_second`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タスクバックグラウンドジョブの実行間隔。GlobalStateMgr はこの値を使用して `doTaskBackgroundJob()` を呼び出す TaskCleaner FrontendDaemon をスケジュールします。この値はデーモン間隔をミリ秒単位で設定するために 1000 倍されます。値を減らすとバックグラウンドメンテナンス (タスククリーンアップ、チェック) の実行頻度が高まり、反応が速くなりますが、CPU/IO オーバーヘッドが増加します。値を増やすとオーバーヘッドが減少しますが、クリーンアップと古いタスクの検出が遅れます。この値を調整して、メンテナンスの応答性とリソース使用量のバランスを取ります。
- Introduced in: v3.2.0

##### `task_min_schedule_interval_s`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: SQL レイヤーによってチェックされるタスクスケジュールの最小許容スケジュール間隔 (秒単位)。タスクが送信されると、TaskAnalyzer はスケジュール期間を秒に変換し、期間が `task_min_schedule_interval_s` より小さい場合、`ERR_INVALID_PARAMETER` で送信を拒否します。これにより、頻繁すぎる実行タスクの作成が防止され、スケジューラーが高頻度タスクから保護されます。スケジュールに明示的な開始時間がない場合、TaskAnalyzer は開始時間を現在のエポック秒に設定します。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `task_runs_timeout_second`

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: TaskRun のデフォルトの実行タイムアウト (秒単位)。この項目は TaskRun 実行の基準タイムアウトとして使用されます。タスク実行のプロパティに、正の整数値を持つセッション変数 `query_timeout` または `insert_timeout` が含まれている場合、実行時はそのセッションタイムアウトと `task_runs_timeout_second` のうち大きい方の値を使用します。その後、実質的なタイムアウトは、設定された `task_runs_ttl_second` および `task_ttl_second` を超えないように制限されます。タスク実行の最大実行時間を制限するには、この項目を設定します。非常に大きな値は、タスク/タスク実行の TTL 設定によって切り捨てられる可能性があります。
- Introduced in: -

### ロードとアンロード

##### `broker_load_default_timeout_second`

- Default: 14400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Broker Load ジョブのタイムアウト期間。
- Introduced in: -

##### `desired_max_waiting_jobs`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE での保留中のジョブの最大数。この数は、テーブル作成、ロード、スキーマ変更ジョブなど、すべてのジョブを指します。FE での保留中のジョブの数がこの値に達すると、FE は新しいロード要求を拒否します。このパラメーターは、非同期ロードにのみ有効です。v2.5 以降、デフォルト値は 100 から 1024 に変更されました。
- Introduced in: -

##### `disable_load_job`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クラスターでエラーが発生した場合にロードを無効にするかどうか。これにより、クラスターエラーによる損失を防ぎます。デフォルト値は `FALSE` で、ロードが無効になっていないことを示します。`TRUE` はロードが無効になっており、クラスターが読み取り専用状態であることを示します。
- Introduced in: -

##### `empty_load_as_error`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: データがロードされない場合にエラーメッセージ「すべてのパーティションにロードデータがありません」を返すかどうか。有効な値:
  - `true`: データがロードされない場合、システムは失敗メッセージを表示し、エラー「すべてのパーティションにロードデータがありません」を返します。
  - `false`: データがロードされない場合、システムは成功メッセージを表示し、エラーではなく OK を返します。
- Introduced in: -

##### `enable_file_bundling`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クラウドネイティブテーブルのファイルバンドル最適化を有効にするかどうか。この機能が有効 ( `true` に設定) の場合、システムはロード、コンパクション、または公開操作によって生成されたデータファイルを自動的にバンドルし、それによって外部ストレージシステムへの高頻度アクセスによって発生する API コストを削減します。この動作は、CREATE TABLE プロパティ `file_bundling` を使用してテーブルレベルで制御することもできます。詳細な手順については、[CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md) を参照してください。
- Introduced in: v4.0

##### `enable_routine_load_lag_metrics`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ルーチンロード Kafka パーティションオフセットラグメトリックを収集するかどうか。この項目を `true` に設定すると、Kafka API が呼び出されてパーティションの最新オフセットが取得されることに注意してください。
- Introduced in: -

##### `enable_sync_publish`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ロードトランザクションの公開フェーズで適用タスクを同期的に実行するかどうか。このパラメーターは主キーテーブルにのみ適用されます。有効な値:
  - `TRUE` (デフォルト): 適用タスクはロードトランザクションの公開フェーズで同期的に実行されます。これは、適用タスクが完了し、ロードされたデータが実際にクエリ可能になった後にのみ、ロードトランザクションが成功として報告されることを意味します。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりする場合、このパラメーターを `true` に設定するとクエリパフォーマンスと安定性が向上しますが、ロードレイテンシーが増加する可能性があります。
  - `FALSE`: 適用タスクはロードトランザクションの公開フェーズで非同期的に実行されます。これは、適用タスクが送信された後にロードトランザクションが成功として報告されますが、ロードされたデータはすぐにクエリできないことを意味します。この場合、同時クエリは適用タスクが完了するかタイムアウトするまで待機してから続行する必要があります。タスクが一度に大量のデータをロードしたり、頻繁にデータをロードしたりしたりする場合、このパラメーターを `false` に設定すると、クエリパフォーマンスと安定性に影響する可能性があります。
- Introduced in: v3.2.0

##### `export_checker_interval_second`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: ロードジョブがスケジュールされる時間間隔。
- Introduced in: -

##### `export_max_bytes_per_be_per_task`

- Default: 268435456
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: 単一の BE から単一のデータアンロードタスクによってエクスポートできる最大データ量。
- Introduced in: -

##### `export_running_job_num_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 並行して実行できるデータエクスポートタスクの最大数。
- Introduced in: -

##### `export_task_default_timeout_second`

- Default: 2 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: データエクスポートタスクのタイムアウト期間。
- Introduced in: -

##### `export_task_pool_size`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: アンロードタスクのスレッドプールのサイズ。
- Introduced in: -

##### `external_table_commit_timeout_ms`

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: StarRocks 外部テーブルへの書き込みトランザクションをコミット (公開) するためのタイムアウト期間。デフォルト値 `10000` は 10 秒のタイムアウト期間を示します。
- Introduced in: -

##### `finish_transaction_default_lock_timeout_ms`

- Default: 1000
- Type: Int
- Unit: MilliSeconds
- Is mutable: Yes
- Description: トランザクション完了中の db およびテーブルロック取得のデフォルトタイムアウト。
- Introduced in: v4.0.0, v3.5.8

##### `history_job_keep_max_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: スキーマ変更ジョブなど、履歴ジョブが保持できる最大期間。
- Introduced in: -

##### `insert_load_default_timeout_second`

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: データをロードするために使用される INSERT INTO ステートメントのタイムアウト期間。
- Introduced in: -

##### `label_clean_interval_second`

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: ラベルがクリーンアップされる時間間隔。単位: 秒。履歴ラベルがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- Introduced in: -

##### `label_keep_max_num`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 一定期間内に保持できるロードジョブの最大数。この数を超えると、履歴ジョブの情報は削除されます。
- Introduced in: -

##### `label_keep_max_second`

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 完了し、FINISHED または CANCELLED 状態にあるロードジョブのラベルを保持する最大期間 (秒単位)。デフォルト値は 3 日間です。この期間が経過すると、ラベルは削除されます。このパラメーターはすべてのタイプのロードジョブに適用されます。値が大きすぎると大量のメモリを消費します。
- Introduced in: -

##### `load_checker_interval_second`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: ロードジョブがローリングベースで処理される時間間隔。
- Introduced in: -

##### `load_parallel_instance_num`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: ブローカーおよびストリームロードの単一ホストで作成される並行ロードフラグメントインスタンスの数を制御します。LoadPlanner は、セッションがアダプティブシンク DOP を有効にしない限り、この値をホストごとの並行度として使用します。セッション変数 `enable_adaptive_sink_dop` が true の場合、セッションの `sink_degree_of_parallelism` がこの設定を上書きします。シャッフルが必要な場合、この値はフラグメント並行実行 (スキャンフラグメントおよびシンクフラグメント並行実行インスタンス) に適用されます。シャッフルが不要な場合、シンクパイプライン DOP として使用されます。注: ローカルファイルからのロードは、ローカルディスク競合を避けるため、単一インスタンスに強制されます (パイプライン DOP = 1、並行実行 = 1)。この数を増やすと、ホストごとの並行性とスループットが向上しますが、CPU、メモリ、I/O 競合が増加する可能性があります。
- Introduced in: v3.2.0

##### `load_straggler_wait_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: BE レプリカが許容できる最大ロード遅延。この値を超えると、他のレプリカからデータをクローンするクローニングが実行されます。
- Introduced in: -

##### `loads_history_retained_days`

- Default: 30
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: 内部 `_statistics_.loads_history` テーブルにロード履歴を保持する日数。この値はテーブル作成時にテーブルプロパティ `partition_live_number` を設定するために使用され、`TableKeeper` (最小値 1 にクランプ) に渡されて、保持する日次パーティションの数を決定します。この値を増減すると、完了したロードジョブが日次パーティションに保持される期間が調整されます。新しいテーブルの作成とキーパーのプルーニング動作に影響しますが、過去のパーティションを自動的に再作成することはありません。`LoadsHistorySyncer` は、ロード履歴のライフサイクルを管理する際にこの保持に依存します。その同期頻度は `loads_history_sync_interval_second` によって制御されます。
- Introduced in: v3.3.6, v3.4.0, v3.5.0

##### `loads_history_sync_interval_second`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: LoadsHistorySyncer が `information_schema.loads` から内部 `_statistics_.loads_history` テーブルに完了したロードジョブを定期的に同期する間隔 (秒単位)。この値は、FrontendDaemon の間隔を設定するためにコンストラクターで 1000 倍されます。シンカーは最初の実行をスキップし (テーブル作成を許可するため)、1 分以上前に完了したロードのみをインポートします。値が小さいと DML とエクゼキュータの負荷が増加し、値が大きいと履歴ロードレコードの可用性が遅延します。ターゲットテーブルの保持/パーティショニング動作については `loads_history_retained_days` を参照してください。
- Introduced in: v3.3.6, v3.4.0, v3.5.0

##### `max_broker_load_job_concurrency`

- Default: 5
- Alias: `async_load_task_pool_size`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: StarRocks クラスター内で許可される同時 Broker Load ジョブの最大数。このパラメーターは Broker Load にのみ有効です。このパラメーターの値は、`max_running_txn_num_per_db` の値よりも小さくなければなりません。v2.5 以降、デフォルト値は `10` から `5` に変更されました。
- Introduced in: -

##### `max_load_timeout_second`

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: ロードジョブに許可される最大タイムアウト期間。この制限を超えると、ロードジョブは失敗します。この制限はすべての種類のロードジョブに適用されます。
- Introduced in: -

##### `max_routine_load_batch_size`

- Default: 4294967296
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: ルーチンロードタスクでロードできる最大データ量。
- Introduced in: -

##### `max_routine_load_task_concurrent_num`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 各ルーチンロードジョブの同時実行タスクの最大数。
- Introduced in: -

##### `max_routine_load_task_num_per_be`

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 各 BE での同時ルーチンロードタスクの最大数。v3.1.0 以降、このパラメーターのデフォルト値は 5 から 16 に増加され、BE 静的パラメーター `routine_load_thread_pool_size` (非推奨) の値以下である必要はなくなりました。
- Introduced in: -

##### `max_running_txn_num_per_db`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: StarRocks クラスター内の各データベースで実行できるロードトランザクションの最大数。デフォルト値は `1000` です。v3.1 以降、デフォルト値は `100` から `1000` に変更されました。データベースで実行されているロードトランザクションの実際の数がこのパラメーターの値を超えると、新しいロード要求は処理されません。同期ロードジョブの新しい要求は拒否され、非同期ロードジョブの新しい要求はキューに配置されます。この値を増やすことはシステム負荷を増加させるため、お勧めしません。
- Introduced in: -

##### `max_stream_load_timeout_second`

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Stream Load ジョブに許可される最大タイムアウト期間。
- Introduced in: -

##### `max_tolerable_backend_down_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 許可される BE ノードの最大障害数。この数を超えると、ルーチンロードジョブは自動的に回復できません。
- Introduced in: -

##### `min_bytes_per_broker_scanner`

- Default: 67108864
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Broker Load インスタンスによって処理できる最小データ量。
- Introduced in: -

##### `min_load_timeout_second`

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: ロードジョブに許可される最小タイムアウト期間。この制限はすべての種類のロードジョブに適用されます。
- Introduced in: -

##### `min_routine_load_lag_for_metrics`

- Default: 10000
- Type: INT
- Unit: -
- Is mutable: Yes
- Description: 監視メトリックに表示されるルーチンロードジョブの最小オフセットラグ。オフセットラグがこの値よりも大きいルーチンロードジョブがメトリックに表示されます。
- Introduced in: -

##### `period_of_auto_resume_min`

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: ルーチンロードジョブが自動的に回復される間隔。
- Introduced in: -

##### `prepared_transaction_default_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 準備されたトランザクションのデフォルトのタイムアウト期間。
- Introduced in: -

##### `routine_load_task_consume_second`

- Default: 15
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: クラスター内の各ルーチンロードタスクがデータを消費する最大時間。v3.1.0 以降、ルーチンロードジョブは [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメーター `task_consume_second` をサポートしています。このパラメーターはルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- Introduced in: -

##### `routine_load_task_timeout_second`

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: クラスター内の各ルーチンロードタスクのタイムアウト期間。v3.1.0 以降、ルーチンロードジョブは [`job_properties`](../../sql-reference/sql-statements/loading_unloading/routine_load/CREATE_ROUTINE_LOAD.md#job_properties) に新しいパラメーター `task_timeout_second` をサポートしています。このパラメーターはルーチンロードジョブ内の個々のロードタスクに適用され、より柔軟です。
- Introduced in: -

##### `routine_load_unstable_threshold_second`

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: ルーチンロードジョブ内のいずれかのタスクが遅延した場合に、ルーチンロードジョブが UNSTABLE 状態に設定されます。具体的には、消費されているメッセージのタイムスタンプと現在時刻の差がこのしきい値を超え、データソースに未消費のメッセージが存在する場合です。
- Introduced in: -

##### `spark_dpp_version`

- Default: 1.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: 使用される Spark Dynamic Partition Pruning (DPP) のバージョン。
- Introduced in: -

##### `spark_home_default_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- Type: String
- Unit: -
- Is mutable: No
- Description: Spark クライアントのルートディレクトリ。
- Introduced in: -

##### `spark_launcher_log_dir`

- Default: `sys_log_dir` + "/spark_launcher_log"
- Type: String
- Unit: -
- Is mutable: No
- Description: Spark ログファイルを格納するディレクトリ。
- Introduced in: -

##### `spark_load_default_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 各 Spark Load ジョブのタイムアウト期間。
- Introduced in: -

##### `spark_load_submit_timeout_second`

- Default: 300
- Type: long
- Unit: Seconds
- Is mutable: No
- Description: Spark アプリケーションの送信後、YARN 応答を待機する最大時間 (秒単位)。`SparkLauncherMonitor.LogMonitor` はこの値をミリ秒に変換し、ジョブが UNKNOWN/CONNECTED/SUBMITTED 状態のままこのタイムアウトを超えると、監視を停止し、Spark ランチャープロセスを強制終了します。`SparkLoadJob` はこの設定をデフォルトとして読み取り、`LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` プロパティを使用してロードごとのオーバーライドを許可します。YARN キューイングの遅延に対応するのに十分な大きさに設定してください。低く設定しすぎると、正当にキューに入れられたジョブが中止される可能性があり、高く設定しすぎると、障害処理とリソースクリーンアップが遅れる可能性があります。
- Introduced in: v3.2.0

##### `spark_resource_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Spark 依存関係パッケージのルートディレクトリ。
- Introduced in: -

##### `stream_load_default_timeout_second`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 各 Stream Load ジョブのデフォルトのタイムアウト期間。
- Introduced in: -

##### `stream_load_max_txn_num_per_be`

- Default: -1
- Type: Int
- Unit: Transactions
- Is mutable: Yes
- Description: 単一の BE (バックエンド) ホストから受け入れられる同時ストリームロードトランザクションの数を制限します。非負の整数に設定されている場合、FrontendServiceImpl は BE (クライアント IP 別) の現在のトランザクション数をチェックし、数がこの制限を `>=` 超える場合、新しいストリームロード開始要求を拒否します。値が `< 0` の場合、制限は無効になります (無制限)。このチェックはストリームロード開始時に発生し、超過すると `streamload txn num per be exceeds limit` エラーが発生する可能性があります。関連する実行時動作では、`stream_load_default_timeout_second` を使用して要求タイムアウトのフォールバックが行われます。
- Introduced in: v3.3.0, v3.4.0, v3.5.0

##### `stream_load_task_keep_max_num`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: StreamLoadMgr がメモリに保持する Stream Load タスクの最大数 (すべてのデータベースにわたるグローバル)。追跡されたタスク (`idToStreamLoadTask`) の数がこのしきい値を超えると、StreamLoadMgr はまず `cleanSyncStreamLoadTasks()` を呼び出して完了した同期ストリームロードタスクを削除します。サイズがまだこのしきい値の半分より大きい場合、`cleanOldStreamLoadTasks(true)` を呼び出して古いまたは完了したタスクを強制的に削除します。メモリにさらにタスク履歴を保持するにはこの値を増やし、メモリ使用量を減らしてクリーンアップをより積極的するには減らします。この値はインメモリ保持のみを制御し、永続化/再生されたタスクには影響しません。
- Introduced in: v3.2.0

##### `stream_load_task_keep_max_second`

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 完了またはキャンセルされた Stream Load タスクの保持ウィンドウ。タスクが最終状態に達し、その終了タイムスタンプがこのしきい値 (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`) より古い場合、`StreamLoadMgr.cleanOldStreamLoadTasks` による削除の対象となり、永続化された状態のロード時に破棄されます。`StreamLoadTask` と `StreamLoadMultiStmtTask` の両方に適用されます。合計タスク数が `stream_load_task_keep_max_num` を超える場合、クリーンアップが早期にトリガーされる可能性があります (同期タスクは `cleanSyncStreamLoadTasks` によって優先されます)。履歴/デバッグ可能性とメモリ使用量のバランスを取るためにこれを設定します。
- Introduced in: v3.2.0

##### `transaction_clean_interval_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: 完了したトランザクションがクリーンアップされる時間間隔。単位: 秒。完了したトランザクションがタイムリーにクリーンアップされるように、短い時間間隔を指定することをお勧めします。
- Introduced in: -

##### `transaction_stream_load_coordinator_cache_capacity`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: トランザクションラベルからコーディネーターノードへのマッピングを格納するキャッシュの容量。
- Introduced in: -

##### `transaction_stream_load_coordinator_cache_expire_seconds`

- Default: 900
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: キャッシュ内のコーディネーターマッピングを削除するまでの時間 (TTL)。
- Introduced in: -

##### `yarn_client_path`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- Type: String
- Unit: -
- Is mutable: No
- Description: Yarn クライアントパッケージのルートディレクトリ。
- Introduced in: -

##### `yarn_config_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- Type: String
- Unit: -
- Is mutable: No
- Description: Yarn 設定ファイルを格納するディレクトリ。
- Introduced in: -

### 統計レポート

##### `enable_collect_warehouse_metrics`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、システムはウェアハウスごとのメトリックを収集してエクスポートします。これを有効にすると、ウェアハウスレベルのメトリック (スロット/使用量/可用性) がメトリック出力に追加され、メトリックのカーディナリティと収集オーバーヘッドが増加します。ウェアハウス固有のメトリックを省略し、CPU/ネットワークと監視ストレージコストを削減するには無効にします。
- Introduced in: v3.5.0

##### `enable_http_detail_metrics`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: true の場合、HTTP サーバーは詳細な HTTP ワーカーメトリック (特に `HTTP_WORKER_PENDING_TASKS_NUM` ゲージ) を計算して公開します。これを有効にすると、サーバーは Netty ワーカーエクゼキューターを反復処理し、各 `NioEventLoop` で `pendingTasks()` を呼び出して保留中のタスクカウントを合計します。無効の場合、ゲージはコストを回避するために 0 を返します。この追加の収集は CPU およびレイテンシーに敏感である可能性があるため、デバッグまたは詳細な調査のためにのみ有効にしてください。
- Introduced in: v3.2.3

##### `proc_profile_collect_time_s`

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 単一プロセスプロファイル収集の期間 (秒単位)。`proc_profile_cpu_enable` または `proc_profile_mem_enable` が `true` に設定されている場合、AsyncProfiler が起動し、コレクタースレッドはこの期間だけスリープし、その後プロファイラーが停止してプロファイルが書き込まれます。値が大きいほどサンプルカバレッジとファイルサイズは増加しますが、プロファイラーの実行時間が長くなり、その後の収集が遅れます。値が小さいほどオーバーヘッドは減少しますが、不十分なサンプルが生成される可能性があります。`proc_profile_file_retained_days` や `proc_profile_file_retained_size_bytes` などの保持設定とこの値が一致していることを確認してください。
- Introduced in: v3.2.12

### ストレージ

##### `alter_table_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: スキーマ変更操作 (ALTER TABLE) のタイムアウト期間。
- Introduced in: -

##### `capacity_used_percent_high_water`

- Default: 0.75
- Type: double
- Unit: Fraction (0.0–1.0)
- Is mutable: Yes
- Description: バックエンド負荷スコアを計算する際に使用されるディスク容量使用率 (総容量に対する割合) の高水位しきい値。`BackendLoadStatistic.calcSore` は `capacity_used_percent_high_water` を使用して `LoadScore.capacityCoefficient` を設定します。バックエンドの使用率が 0.5 未満の場合、係数は 0.5 になります。使用率が `capacity_used_percent_high_water` を超える場合、係数は 1.0 になります。それ以外の場合、係数は使用率に応じて線形に推移します (2 * usedPercent - 0.5)。係数が 1.0 の場合、負荷スコアは完全に容量比率によって決定されます。値が低いほどレプリカ数の重みが増加します。この値を調整すると、バランサーがディスク使用率の高いバックエンドにペナルティを課す積極性が変わります。
- Introduced in: v3.2.0

##### `catalog_trash_expire_second`

- Default: 86400
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: データベース、テーブル、またはパーティションが削除された後にメタデータが保持される最長期間。この期間が経過すると、データは削除され、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) コマンドで回復することはできません。
- Introduced in: -

##### `check_consistency_default_timeout_second`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: レプリカの一貫性チェックのタイムアウト期間。タブレットのサイズに基づいてこのパラメーターを設定できます。
- Introduced in: -

##### `consistency_check_cooldown_time_second`

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 同じタブレットの一貫性チェック間で必要な最小間隔 (秒単位) を制御します。タブレット選択中、タブレットは `tablet.getLastCheckTime()` が `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` 未満の場合にのみ適格と見なされます。デフォルト値 (24 * 3600) は、バックエンドのディスク I/O を減らすために、タブレットごとに約 1 日に 1 回のチェックを強制します。この値を減らすとチェック頻度とリソース使用量が増加し、増やすと不整合の検出が遅れる代わりに I/O が減少します。この値は、インデックスのタブレットリストからクールダウンしたタブレットをフィルタリングする際にグローバルに適用されます。
- Introduced in: v3.5.5

##### `consistency_check_end_time`

- Default: "4"
- Type: String
- Unit: Hour of day (0-23)
- Is mutable: No
- Description: ConsistencyChecker の作業ウィンドウの終了時間 (時、0-23) を指定します。値はシステムタイムゾーンの SimpleDateFormat("HH") で解析され、0-23 (1 桁または 2 桁) として受け入れられます。StarRocks はこれを `consistency_check_start_time` とともに使用して、一貫性チェックジョブをスケジュールして追加する時期を決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます (たとえば、デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4")。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると FE の起動がエラーをログに記録して終了するため、有効な時刻文字列を指定してください。
- Introduced in: v3.2.0

##### `consistency_check_start_time`

- Default: "23"
- Type: String
- Unit: Hour of day (00-23)
- Is mutable: No
- Description: ConsistencyChecker の作業ウィンドウの開始時間 (時、00-23) を指定します。値はシステムタイムゾーンの SimpleDateFormat("HH") で解析され、0-23 (1 桁または 2 桁) として受け入れられます。StarRocks はこれを `consistency_check_end_time` とともに使用して、一貫性チェックジョブをスケジュールして追加する時期を決定します。`consistency_check_start_time` が `consistency_check_end_time` より大きい場合、ウィンドウは深夜をまたぎます (たとえば、デフォルトは `consistency_check_start_time` = "23" から `consistency_check_end_time` = "4")。`consistency_check_start_time` が `consistency_check_end_time` と等しい場合、チェッカーは実行されません。解析に失敗すると FE の起動がエラーをログに記録して終了するため、有効な時刻文字列を指定してください。
- Introduced in: v3.2.0

##### `consistency_tablet_meta_check_interval_ms`

- Default: 2 * 3600 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: ConsistencyChecker が `TabletInvertedIndex` と `LocalMetastore` の間の完全なタブレットメタの一貫性スキャンを実行するために使用する間隔 (ミリ秒)。`runAfterCatalogReady` のデーモンは、`現在の時間 - lastTabletMetaCheckTime` がこの値を超えたときに checkTabletMetaConsistency をトリガーします。無効なタブレットが最初に検出された場合、その `toBeCleanedTime` は `now + (consistency_tablet_meta_check_interval_ms / 2)` に設定されるため、実際の削除は後続のスキャンまで遅延されます。この値を増やすとスキャン頻度と負荷が減少し (クリーンアップが遅くなる)、減らすと古いタブレットの検出と削除が高速化されます (オーバーヘッドが増加する)。
- Introduced in: v3.2.0

##### `default_replication_num`

- Default: 3
- Type: Short
- Unit: -
- Is mutable: Yes
- Description: StarRocks でテーブルを作成する際に、各データパーティションのレプリカのデフォルト数を設定します。この設定は、CREATE TABLE DDL で `replication_num=x` を指定することで、テーブル作成時に上書きできます。
- Introduced in: -

##### `enable_auto_tablet_distribution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: バケット数を自動的に設定するかどうか。
  - このパラメーターが `TRUE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を指定する必要はありません。StarRocks は自動的にバケット数を決定します。
  - このパラメーターが `FALSE` に設定されている場合、テーブルを作成したりパーティションを追加したりする際にバケット数を手動で指定する必要があります。テーブルに新しいパーティションを追加する際にバケット数を指定しない場合、新しいパーティションはテーブル作成時に設定されたバケット数を継承します。ただし、新しいパーティションにバケット数を手動で指定することもできます。
- Introduced in: v2.5.7

##### `enable_experimental_rowstore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: [ハイブリッド行指向・列指向ストレージ](../../table_design/hybrid_table.md) 機能を有効にするかどうか。
- Introduced in: v3.2.3

##### `enable_fast_schema_evolution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: StarRocks クラスター内のすべてのテーブルで高速スキーマ進化を有効にするかどうか。有効な値は `TRUE` と `FALSE` (デフォルト) です。高速スキーマ進化を有効にすると、スキーマ変更の速度が向上し、列の追加または削除時のリソース使用量が削減されます。
- Introduced in: v3.2.0

> **NOTE**
>
> - StarRocks Shared-data クラスターは v3.3.0 以降でこのパラメーターをサポートしています。
> - 特定のテーブルの高速スキーマ進化を設定する必要がある場合 (特定のテーブルの高速スキーマ進化を無効にするなど) は、テーブル作成時にテーブルプロパティ [`fast_schema_evolution`](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) を設定できます。

##### `enable_online_optimize_table`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 最適化ジョブ作成時に StarRocks が非ブロッキングのオンライン最適化パスを使用するかどうかを制御します。`enable_online_optimize_table` が true で、ターゲットテーブルが互換性チェックを満たす場合 (パーティション/キー/ソート指定なし、分散が `RandomDistributionDesc` でない、ストレージタイプが `COLUMN_WITH_ROW` でない、レプリケートストレージが有効、テーブルがクラウドネイティブテーブルまたはマテリアライズドビューでない)、プランナーは書き込みをブロックせずに最適化を実行するために `OnlineOptimizeJobV2` を作成します。false の場合、または互換性条件が失敗した場合、StarRocks は `OptimizeJobV2` にフォールバックします。これは、最適化中に書き込み操作をブロックする可能性があります。
- Introduced in: v3.3.3, v3.4.0, v3.5.0

##### `enable_strict_storage_medium_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ユーザーがテーブルを作成する際に、FE が BE のストレージメディアを厳密にチェックするかどうか。このパラメーターが `TRUE` に設定されている場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックし、BE のストレージメディアが CREATE TABLE ステートメントで指定された `storage_medium` パラメーターと異なる場合、エラーを返します。例えば、CREATE TABLE ステートメントで指定されたストレージメディアが SSD であるのに、BE の実際のストレージメディアが HDD である場合、テーブル作成は失敗します。このパラメーターが `FALSE` の場合、FE はユーザーがテーブルを作成する際に BE のストレージメディアをチェックしません。
- Introduced in: -

##### `max_bucket_number_per_partition`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: パーティション内に作成できるバケットの最大数。
- Introduced in: v3.3.2

##### `max_column_number_per_table`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: テーブル内に作成できる列の最大数。
- Introduced in: v3.3.2

##### `max_dynamic_partition_num`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 動的パーティションテーブルの分析または作成時に一度に作成できる最大パーティション数を制限します。動的パーティションプロパティ検証中、`systemtask_runs_max_history_number` は予想されるパーティション (終了オフセット + 履歴パーティション番号) を計算し、合計が `max_dynamic_partition_num` を超える場合、DDL エラーをスローします。正当に大きなパーティション範囲を予想する場合にのみこの値を増やしてください。増やすとより多くのパーティションを作成できますが、メタデータサイズ、スケジューリング作業、および運用上の複雑さが増加する可能性があります。
- Introduced in: v3.2.0

##### `max_partition_number_per_table`

- Default: 100000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: テーブル内に作成できるパーティションの最大数。
- Introduced in: v3.3.2

##### `max_task_consecutive_fail_count`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: タスクが自動的に停止されるまでに発生する可能性のある連続失敗の最大数。`TaskSource.MV.equals(task.getSource())` が `true` で `max_task_consecutive_fail_count` が 0 より大きい場合、タスクの連続失敗カウンターが `max_task_consecutive_fail_count` に達するか超えると、タスクは TaskManager を介して停止され、マテリアライズドビュータスクの場合はマテリアライズドビューが非アクティブ化されます。停止と再アクティブ化の方法を示す例外がスローされます (例: `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`)。自動停止を無効にするには、この項目を 0 または負の値に設定します。
- Introduced in: -

##### `partition_recycle_retention_period_secs`

- Default: 1800
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: INSERT OVERWRITE またはマテリアライズドビュー更新操作によって削除されたパーティションのメタデータ保持時間。このようなメタデータは、[RECOVER](../../sql-reference/sql-statements/backup_restore/RECOVER.md) の実行によって回復できないことに注意してください。
- Introduced in: v3.5.9

##### `recover_with_empty_tablet`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 失われたまたは破損したタブレットレプリカを空のレプリカに置き換えるかどうか。タブレットレプリカが失われたり破損したりした場合、このタブレットまたは他の健全なタブレットのデータクエリは失敗する可能性があります。失われたまたは破損したタブレットレプリカを空のタブレットに置き換えることで、クエリは引き続き実行できます。ただし、データが失われているため、結果が正しくない可能性があります。デフォルト値は `FALSE` で、失われたまたは破損したタブレットレプリカは空のレプリカに置き換えられず、クエリは失敗します。
- Introduced in: -

##### `storage_usage_hard_limit_percent`

- Default: 95
- Alias: `storage_flood_stage_usage_percent`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: BE ディレクトリのストレージ使用率のハードリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_usage_hard_limit_reserve_bytes` 未満の場合、ロードおよび復元ジョブは拒否されます。この設定を有効にするには、BE 設定項目 `storage_flood_stage_usage_percent` と合わせてこの項目を設定する必要があります。
- Introduced in: -

##### `storage_usage_hard_limit_reserve_bytes`

- Default: 100 * 1024 * 1024 * 1024
- Alias: `storage_flood_stage_left_capacity_bytes`
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: BE ディレクトリの残りストレージスペースのハードリミット。BE ストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が `storage_usage_hard_limit_percent` を超える場合、ロードおよび復元ジョブは拒否されます。この設定を有効にするには、BE 設定項目 `storage_flood_stage_left_capacity_bytes` と合わせてこの項目を設定する必要があります。
- Introduced in: -

##### `storage_usage_soft_limit_percent`

- Default: 90
- Alias: `storage_high_watermark_usage_percent`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: BE ディレクトリのストレージ使用率のソフトリミット。BE ストレージディレクトリのストレージ使用率 (パーセンテージ) がこの値を超え、残りのストレージスペースが `storage_usage_soft_limit_reserve_bytes` 未満の場合、タブレットは このディレクトリにクローンできません。
- Introduced in: -

##### `storage_usage_soft_limit_reserve_bytes`

- Default: 200 * 1024 * 1024 * 1024
- Alias: `storage_min_left_capacity_bytes`
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: BE ディレクトリの残りストレージスペースのソフトリミット。BE ストレージディレクトリの残りストレージスペースがこの値未満で、ストレージ使用率 (パーセンテージ) が `storage_usage_soft_limit_percent` を超える場合、タブレットはこのディレクトリにクローンできません。
- Introduced in: -

##### `tablet_checker_lock_time_per_cycle_ms`

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: テーブルロックを解放して再取得するまでの、タブレットチェッカーがサイクルごとにロックを保持する最大時間。100 未満の値は 100 として扱われます。
- Introduced in: v3.5.9, v4.0.2

##### `tablet_create_timeout_second`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タブレット作成のタイムアウト期間。v3.1 以降、デフォルト値は 1 から 10 に変更されました。
- Introduced in: -

##### `tablet_delete_timeout_second`

- Default: 2
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: タブレット削除のタイムアウト期間。
- Introduced in: -

##### `tablet_sched_balance_load_disk_safe_threshold`

- Default: 0.5
- Alias: `balance_load_disk_safe_threshold`
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: BE のディスク使用量がバランスしているかどうかを判断するためのパーセンテージしきい値。すべての BE のディスク使用量がこの値よりも低い場合、バランスしていると見なされます。ディスク使用量がこの値よりも大きく、最高と最低の BE ディスク使用量の差が 10% を超える場合、ディスク使用量はバランスしていないと見なされ、タブレットの再バランシングがトリガーされます。
- Introduced in: -

##### `tablet_sched_balance_load_score_threshold`

- Default: 0.1
- Alias: `balance_load_score_threshold`
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: BE の負荷がバランスしているかどうかを判断するためのパーセンテージしきい値。BE の負荷がすべての BE の平均負荷よりも低く、その差がこの値よりも大きい場合、その BE は低負荷状態にあります。逆に、BE の負荷が平均負荷よりも高く、その差がこの値よりも大きい場合、その BE は高負荷状態にあります。
- Introduced in: -

##### `tablet_sched_be_down_tolerate_time_s`

- Default: 900
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: スケジューラが BE ノードを非アクティブのままにする最大期間。この時間しきい値に達すると、その BE ノード上のタブレットは他のアクティブな BE ノードに移行されます。
- Introduced in: v2.5.7

##### `tablet_sched_disable_balance`

- Default: false
- Alias: `disable_balance`
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: タブレットバランシングを無効にするかどうか。`TRUE` はタブレットバランシングが無効になっていることを示します。`FALSE` はタブレットバランシングが有効になっていることを示します。
- Introduced in: -

##### `tablet_sched_disable_colocate_balance`

- Default: false
- Alias: `disable_colocate_balance`
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: コロケーションテーブルのレプリカバランシングを無効にするかどうか。`TRUE` はレプリカバランシングが無効になっていることを示します。`FALSE` はレプリカバランシングが有効になっていることを示します。
- Introduced in: -

##### `tablet_sched_max_balancing_tablets`

- Default: 500
- Alias: `max_balancing_tablets`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 同時にバランスできるタブレットの最大数。この値を超えると、タブレットの再バランシングはスキップされます。
- Introduced in: -

##### `tablet_sched_max_clone_task_timeout_sec`

- Default: 2 * 60 * 60
- Alias: `max_clone_task_timeout_sec`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: タブレットのクローン作成の最大タイムアウト期間。
- Introduced in: -

##### `tablet_sched_max_not_being_scheduled_interval_ms`

- Default: 15 * 60 * 1000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: タブレットクローンタスクがスケジュールされている場合、このパラメーターで指定された時間の間タブレットがスケジュールされていない場合、StarRocks はそのタブレットに高い優先度を与え、できるだけ早くスケジュールします。
- Introduced in: -

##### `tablet_sched_max_scheduling_tablets`

- Default: 10000
- Alias: `max_scheduling_tablets`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 同時にスケジュールできるタブレットの最大数。この値を超えると、タブレットのバランシングと修復チェックはスキップされます。
- Introduced in: -

##### `tablet_sched_min_clone_task_timeout_sec`

- Default: 3 * 60
- Alias: `min_clone_task_timeout_sec`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: タブレットのクローン作成の最小タイムアウト期間。
- Introduced in: -

##### `tablet_sched_num_based_balance_threshold_ratio`

- Default: 0.5
- Alias: -
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: 数ベースのバランシングはディスクサイズバランスを損なう可能性がありますが、ディスク間の最大ギャップは `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold` を超えることはできません。クラスター内に A から B へ、そして B から A へと常にバランシングしているタブレットがある場合、この値を減らしてください。タブレットの分散をよりバランスさせたい場合、この値を増やしてください。
- Introduced in: - 3.1

##### `tablet_sched_repair_delay_factor_second`

- Default: 60
- Alias: `tablet_repair_delay_factor_second`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: レプリカが修復される間隔 (秒単位)。
- Introduced in: -

##### `tablet_sched_slot_num_per_path`

- Default: 8
- Alias: `schedule_slot_num_per_path`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: BE ストレージディレクトリで同時に実行できるタブレット関連タスクの最大数。v2.5 以降、このパラメーターのデフォルト値は `4` から `8` に変更されました。
- Introduced in: -

##### `tablet_sched_storage_cooldown_second`

- Default: -1
- Alias: `storage_cooldown_second`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: テーブル作成時からの自動冷却の遅延時間。デフォルト値 `-1` は自動冷却が無効になっていることを指定します。自動冷却を有効にするには、このパラメーターを `-1` より大きい値に設定します。
- Introduced in: -

##### `tablet_stat_update_interval_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: FE が各 BE からタブレット統計を取得する時間間隔。
- Introduced in: -

### 共有データ

##### `aws_s3_access_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: S3 バケットにアクセスするために使用するアクセスキー ID。
- Introduced in: v3.0

##### `aws_s3_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: S3 バケットにアクセスするために使用するエンドポイント。例: `https://s3.us-west-2.amazonaws.com`。
- Introduced in: v3.0

##### `aws_s3_external_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: S3 バケットへのクロスアカウントアクセスに使用される AWS アカウントの外部 ID。
- Introduced in: v3.0

##### `aws_s3_iam_role_arn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: データファイルが格納されている S3 バケットに対する権限を持つ IAM ロールの ARN。
- Introduced in: v3.0

##### `aws_s3_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: データを格納するために使用される S3 パス。S3 バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- Introduced in: v3.0

##### `aws_s3_region`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: S3 バケットが存在するリージョン。例: `us-west-2`。
- Introduced in: v3.0

##### `aws_s3_secret_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: S3 バケットにアクセスするために使用するシークレットアクセスキー。
- Introduced in: v3.0

##### `aws_s3_use_aws_sdk_default_behavior`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: AWS SDK のデフォルトの認証資格情報を使用するかどうか。有効な値: true および false (デフォルト)。
- Introduced in: v3.0

##### `aws_s3_use_instance_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: S3 にアクセスするための認証方法としてインスタンスプロファイルと引き受けロールを使用するかどうか。有効な値: true および false (デフォルト)。
  - IAM ユーザーベースの認証情報 (アクセスキーとシークレットキー) を使用して S3 にアクセスする場合、この項目を `false` に指定し、`aws_s3_access_key` と `aws_s3_secret_key` を指定する必要があります。
  - インスタンスプロファイルを使用して S3 にアクセスする場合、この項目を `true` に指定する必要があります。
  - 引き受けロールを使用して S3 にアクセスする場合、この項目を `true` に指定し、`aws_s3_iam_role_arn` を指定する必要があります。
  - 外部 AWS アカウントを使用する場合、`aws_s3_external_id` も指定する必要があります。
- Introduced in: v3.0

##### `azure_adls2_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 アカウントのエンドポイント。例: `https://test.dfs.core.windows.net`。
- Introduced in: v3.4.1

##### `azure_adls2_oauth2_client_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 の要求を承認するために使用されるマネージド ID のクライアント ID。
- Introduced in: v3.4.4

##### `azure_adls2_oauth2_tenant_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 の要求を承認するために使用されるマネージド ID のテナント ID。
- Introduced in: v3.4.4

##### `azure_adls2_oauth2_use_managed_identity`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 の要求を承認するためにマネージド ID を使用するかどうか。
- Introduced in: v3.4.4

##### `azure_adls2_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: データを格納するために使用される Azure Data Lake Storage Gen2 パス。ファイルシステム名とディレクトリ名で構成されます。例: `testfilesystem/starrocks`。
- Introduced in: v3.4.1

##### `azure_adls2_sas_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 の要求を承認するために使用される共有アクセスシグネチャ (SAS)。
- Introduced in: v3.4.1

##### `azure_adls2_shared_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Data Lake Storage Gen2 の要求を承認するために使用される共有キー。
- Introduced in: v3.4.1

##### `azure_blob_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Blob Storage アカウントのエンドポイント。例: `https://test.blob.core.windows.net`。
- Introduced in: v3.1

##### `azure_blob_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: データを格納するために使用される Azure Blob Storage パス。ストレージアカウント内のコンテナーの名前とコンテナー内のサブパス (存在する場合) で構成されます。例: `testcontainer/subpath`。
- Introduced in: v3.1

##### `azure_blob_sas_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Blob Storage の要求を承認するために使用される共有アクセスシグネチャ (SAS)。
- Introduced in: v3.1

##### `azure_blob_shared_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Azure Blob Storage の要求を承認するために使用される共有キー。
- Introduced in: v3.1

##### `azure_use_native_sdk`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Azure Blob Storage にアクセスするためにネイティブ SDK を使用するかどうか。これにより、マネージド ID およびサービスプリンシパルでの認証が可能になります。この項目が `false` に設定されている場合、共有キーと SAS トークンでの認証のみが許可されます。
- Introduced in: v3.4.4

##### `cloud_native_hdfs_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: HDFS ストレージの URL。例: `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- Introduced in: -

##### `cloud_native_meta_port`

- Default: 6090
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE クラウドネイティブメタデータサーバー RPC リッスンポート。
- Introduced in: -

##### `cloud_native_storage_type`

- Default: S3
- Type: String
- Unit: -
- Is mutable: No
- Description: 使用するオブジェクトストレージのタイプ。共有データモードでは、StarRocks は HDFS、Azure Blob (v3.1.1 以降でサポート)、Azure Data Lake Storage Gen2 (v3.4.1 以降でサポート)、Google Storage (ネイティブ SDK、v3.5.1 以降でサポート)、および S3 プロトコルと互換性のあるオブジェクトストレージシステム (AWS S3、MinIO など) にデータを格納することをサポートしています。有効な値: `S3` (デフォルト)、`HDFS`、`AZBLOB`、`ADLS2`、および `GS`。このパラメーターを `S3` に指定した場合、`aws_s3` で始まるパラメーターを追加する必要があります。`AZBLOB` に指定した場合、`azure_blob` で始まるパラメーターを追加する必要があります。`ADLS2` に指定した場合、`azure_adls2` で始まるパラメーターを追加する必要があります。`GS` に指定した場合、`gcp_gcs` で始まるパラメーターを追加する必要があります。`HDFS` に指定した場合、`cloud_native_hdfs_url` のみを指定する必要があります。
- Introduced in: -

##### `enable_load_volume_from_conf`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: StarRocks が FE 設定ファイルで指定されたオブジェクトストレージ関連プロパティを使用して、組み込みストレージボリュームを作成することを許可するかどうか。デフォルト値は v3.4.1 以降 `true` から `false` に変更されました。
- Introduced in: v3.1.0

##### `gcp_gcs_impersonation_service_account`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: Google Storage にアクセスするために、偽装ベースの認証を使用する場合に偽装するサービスアカウント。
- Introduced in: v3.5.1

##### `gcp_gcs_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: データを格納するために使用される Google Cloud パス。Google Cloud バケットの名前とその下のサブパス (存在する場合) で構成されます。例: `testbucket/subpath`。
- Introduced in: v3.5.1

##### `gcp_gcs_service_account_email`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: サービスアカウント作成時に生成された JSON ファイル内のメールアドレス。例: `user@hello.iam.gserviceaccount.com`。
- Introduced in: v3.5.1

##### `gcp_gcs_service_account_private_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: サービスアカウント作成時に生成された JSON ファイル内の秘密鍵。例: `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- Introduced in: v3.5.1

##### `gcp_gcs_service_account_private_key_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: サービスアカウント作成時に生成された JSON ファイル内の秘密鍵 ID。
- Introduced in: v3.5.1

##### `gcp_gcs_use_compute_engine_service_account`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Compute Engine にバインドされているサービスアカウントを使用するかどうか。
- Introduced in: v3.5.1

##### `hdfs_file_system_expire_seconds`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: HdfsFsManager が管理する未使用のキャッシュ HDFS/ObjectStore FileSystem の Time-to-live (秒単位)。FileSystemExpirationChecker (60 秒ごとに実行) はこの値を使用して各 HdfsFs.isExpired(...) を呼び出します。期限切れになるとマネージャーは基盤となる FileSystem を閉じ、キャッシュから削除します。アクセサーメソッド (例: `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`) は最終アクセス時刻を更新するため、有効期限は非アクティブに基づいています。値が小さいとアイドルリソースの保持は減りますが、再オープンオーバーヘッドが増加します。値が大きいとハンドルが長く保持され、より多くのリソースを消費する可能性があります。
- Introduced in: v3.2.0

##### `lake_autovacuum_grace_period_minutes`

- Default: 30
- Type: Long
- Unit: Minutes
- Is mutable: Yes
- Description: 共有データクラスターで履歴データバージョンを保持する時間範囲。この時間範囲内の履歴データバージョンは、コンパクション後に AutoVacuum によって自動的にクリーンアップされません。実行中のクエリによってアクセスされるデータがクエリ完了前に削除されることを避けるため、この値を最大クエリ時間よりも大きく設定する必要があります。v3.3.0、v3.2.5、および v3.1.10 以降、デフォルト値は `5` から `30` に変更されました。
- Introduced in: v3.1.0

##### `lake_autovacuum_parallel_partitions`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: 共有データクラスターで同時に AutoVacuum を実行できるパーティションの最大数。AutoVacuum はコンパクション後のガベージコレクションです。
- Introduced in: v3.1.0

##### `lake_autovacuum_partition_naptime_seconds`

- Default: 180
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: 共有データクラスターで同じパーティションに対する AutoVacuum 操作間の最小間隔。
- Introduced in: v3.1.0

##### `lake_autovacuum_stale_partition_threshold`

- Default: 12
- Type: Long
- Unit: Hours
- Is mutable: Yes
- Description: この時間範囲内にパーティションが更新されていない場合 (ロード、DELETE、またはコンパクション)、システムはこのパーティションに対して AutoVacuum を実行しません。
- Introduced in: v3.1.0

##### `lake_compaction_allow_partial_success`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、サブタスクの 1 つが成功した場合、システムは共有データクラスターでのコンパクション操作を成功と見なします。
- Introduced in: v3.5.2

##### `lake_compaction_disable_ids`

- Default: ""
- Type: String
- Unit: -
- Is mutable: Yes
- Description: 共有データモードでコンパクションが無効になっているテーブルまたはパーティションのリスト。形式はセミコロンで区切られた `tableId1;partitionId2` です。例: `12345;98765`。
- Introduced in: v3.4.4

##### `lake_compaction_history_size`

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターのリーダー FE ノードのメモリに保持される最近の成功したコンパクションタスクレコードの数。`SHOW PROC '/compactions'` コマンドを使用して、最近の成功したコンパクションタスクレコードを表示できます。コンパクション履歴は FE プロセスメモリに格納され、FE プロセスが再起動されると失われることに注意してください。
- Introduced in: v3.1.0

##### `lake_compaction_max_tasks`

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターで許可される同時コンパクションタスクの最大数。この項目を `-1` に設定すると、同時タスク数が適応的に計算されることを示します。この値を `0` に設定すると、コンパクションが無効になります。
- Introduced in: v3.1.0

##### `lake_compaction_score_selector_min_score`

- Default: 10.0
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターでコンパクション操作をトリガーするコンパクションスコアのしきい値。パーティションのコンパクションスコアがこの値以上の場合、システムはそのパーティションでコンパクションを実行します。
- Introduced in: v3.1.0

##### `lake_compaction_score_upper_bound`

- Default: 2000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターのパーティションのコンパクションスコアの上限。`0` は上限がないことを示します。この項目は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効になります。パーティションのコンパクションスコアがこの上限に達するか超えると、受信ロードタスクは拒否されます。v3.3.6 以降、デフォルト値は `0` から `2000` に変更されました。
- Introduced in: v3.2.0

##### `lake_enable_balance_tablets_between_workers`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターでクラウドネイティブテーブルのタブレット移行中に、Compute ノード間でタブレットの数をバランスさせるかどうか。`true` は Compute ノード間でタブレットをバランスさせることを示し、`false` はこの機能を無効にすることを示します。
- Introduced in: v3.3.4

##### `lake_enable_ingest_slowdown`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターでデータ取り込みの減速を有効にするかどうか。データ取り込みの減速が有効になっている場合、パーティションのコンパクションスコアが `lake_ingest_slowdown_threshold` を超えると、そのパーティションでのロードタスクがスロットルダウンされます。この設定は `run_mode` が `shared_data` に設定されている場合にのみ有効になります。v3.3.6 以降、デフォルト値は `false` から `true` に変更されました。
- Introduced in: v3.2.0

##### `lake_ingest_slowdown_threshold`

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターでデータ取り込みの減速をトリガーするコンパクションスコアのしきい値。この設定は `lake_enable_ingest_slowdown` が `true` に設定されている場合にのみ有効になります。
- Introduced in: v3.2.0

##### `lake_publish_version_max_threads`

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターでのバージョン公開タスクの最大スレッド数。
- Introduced in: v3.2.0

##### `meta_sync_force_delete_shard_meta`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 共有データクラスターのメタデータを直接削除し、リモートストレージファイルのクリーンアップをバイパスすることを許可するかどうか。クリーンアップするシャードが過剰に多く、FE JVM のメモリ圧力が極端に高くなる場合にのみ、この項目を `true` に設定することをお勧めします。この機能を有効にすると、シャードまたはタブレットに属するデータファイルが自動的にクリーンアップされないことに注意してください。
- Introduced in: v3.2.10, v3.3.3

##### `run_mode`

- Default: `shared_nothing`
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks クラスターの実行モード。有効な値: `shared_data` および `shared_nothing` (デフォルト)。
  - `shared_data` は StarRocks を共有データモードで実行することを示します。
  - `shared_nothing` は StarRocks を共有なしモードで実行することを示します。

  > **CAUTION**
  >
  > - StarRocks クラスターで `shared_data` と `shared_nothing` モードを同時に採用することはできません。混合デプロイメントはサポートされていません。
  > - クラスターのデプロイ後に `run_mode` を変更しないでください。そうしないと、クラスターの再起動に失敗します。共有なしクラスターから共有データクラスターへの変換、またはその逆の変換はサポートされていません。

- Introduced in: -

##### `shard_group_clean_threshold_sec`

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: FE が共有データクラスターで未使用のタブレットおよびシャードグループをクリーンアップするまでの時間。このしきい値内に作成されたタブレットおよびシャードグループはクリーンアップされません。
- Introduced in: -

##### `star_mgr_meta_sync_interval_sec`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: FE が共有データクラスターで StarMgr と定期的なメタデータ同期を実行する間隔。
- Introduced in: -

##### `starmgr_grpc_server_max_worker_threads`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE の starmgr モジュールの grpc サーバーが使用するワーカー スレッドの最大数。
- Introduced in: v4.0.0, v3.5.8

##### `starmgr_grpc_timeout_seconds`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -

### データレイク

##### `files_enable_insert_push_down_schema`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、アナライザは INSERT ... FROM files() 操作のためにターゲットテーブルスキーマを `files()` テーブル関数にプッシュしようとします。これは、ソースが FileTableFunctionRelation であり、ターゲットがネイティブテーブルであり、SELECT リストにそれに対応するスロット参照列 (または *) が含まれている場合にのみ適用されます。アナライザは選択された列をターゲット列に一致させ (カウントが一致する必要があります)、ターゲットテーブルを一時的にロックし、ファイル列の型を非複合型 (Parquet JSON -> `array<varchar>` のような複合型はスキップされます) のディープコピーされたターゲット列型で置き換えます。元のファイルテーブルからの列名は保持されます。これにより、取り込み中のファイルベースの型推論による型ミスマッチと緩さが軽減されます。
- Introduced in: v3.4.0, v3.5.0

##### `hdfs_read_buffer_size_kb`

- Default: 8192
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: HDFS 読み取りバッファのサイズ (キロバイト単位)。StarRocks はこの値をバイト (`<< 10`) に変換し、`HdfsFsManager` で HDFS 読み取りバッファを初期化したり、ブローカーアクセスが使用されていない場合に BE タスク (例: `TBrokerScanRangeParams`、`TDownloadReq`) に送信される thrift フィールド `hdfs_read_buffer_size_kb` を設定したりするために使用します。`hdfs_read_buffer_size_kb` を増やすと、シーケンシャル読み取りスループットが向上し、システムコールオーバーヘッドが減少しますが、ストリームごとのメモリ使用量が増加します。減らすとメモリフットプリントが減少しますが、I/O 効率が低下する可能性があります。調整時にはワークロード (多くの小さなストリームと少数の大きなシーケンシャル読み取り) を考慮してください。
- Introduced in: v3.2.0

##### `hdfs_write_buffer_size_kb`

- Default: 1024
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: ブローカーを使用しない HDFS またはオブジェクトストレージへの直接書き込みに使用される HDFS 書き込みバッファサイズ (KB 単位) を設定します。FE はこの値をバイト (`<< 10`) に変換し、`HdfsFsManager` でローカル書き込みバッファを初期化します。また、Thrift リクエスト (例: TUploadReq、TExportSink、シンクオプション) に伝播され、バックエンド/エージェントが同じバッファサイズを使用するようにします。この値を増やすと、大きなシーケンシャル書き込みのスループットが向上しますが、ライターごとのメモリが増加します。減らすと、ストリームごとのメモリ使用量が減少し、小さな書き込みのレイテンシーが低下する可能性があります。`hdfs_read_buffer_size_kb` と並行して調整し、利用可能なメモリと同時ライターを考慮してください。
- Introduced in: v3.2.0

##### `lake_batch_publish_max_version_num`

- Default: 10
- Type: Int
- Unit: Count
- Is mutable: Yes
- Description: レイク (クラウドネイティブ) テーブルの公開バッチを構築する際に、連続するトランザクションバージョンをいくつのグループにまとめるかの上限を設定します。この値はトランザクショングラフバッチ処理ルーチン (getReadyToPublishTxnListBatch を参照) に渡され、`lake_batch_publish_min_version_num` と連携して TransactionStateBatch の候補範囲サイズを決定します。値が大きいほど、より多くのコミットをバッチ処理することで公開スループットが向上しますが、アトミック公開の範囲が広がり (可視性レイテンシーが長く、ロールバック対象が大きくなる)、バージョンが連続していない場合、実行時に制限される可能性があります。ワークロードと可視性/レイテンシー要件に応じて調整してください。
- Introduced in: v3.2.0

##### `lake_batch_publish_min_version_num`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: レイクテーブルの公開バッチを形成するために必要な連続するトランザクションバージョンの最小数を設定します。DatabaseTransactionMgr.getReadyToPublishTxnListBatch は、依存トランザクションを選択するためにこの値を `lake_batch_publish_max_version_num` とともに transactionGraph.getTxnsWithTxnDependencyBatch に渡します。値が `1` の場合、単一トランザクションの公開が許可されます (バッチ処理なし)。値が `>1` の場合、少なくともその数の連続したバージョンを持つ単一テーブル、非レプリケーショントランザクションが利用可能である必要があります。バージョンが連続していない場合、レプリケーショントランザクションが出現した場合、またはスキーマ変更がバージョンを消費した場合、バッチ処理は中止されます。この値を増やすと、コミットをグループ化することで公開スループットが向上する可能性がありますが、十分な連続トランザクションを待機している間に公開が遅延する可能性があります。
- Introduced in: v3.2.0

##### `lake_enable_batch_publish_version`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、PublishVersionDaemon は同じ Lake (共有データ) テーブル/パーティションの準備完了トランザクションをバッチ処理し、トランザクションごとの公開を発行するのではなく、まとめてバージョンを公開します。RunMode shared-data では、デーモンは getReadyPublishTransactionsBatch() を呼び出し、publishVersionForLakeTableBatch(...) を使用してグループ化された公開操作を実行します (RPC を削減し、スループットを向上させます)。無効の場合、デーモンは publishVersionForLakeTable(...) を介してトランザクションごとの公開にフォールバックします。実装は、スイッチが切り替えられたときに重複公開を避けるために内部セットを使用して進行中の作業を調整し、`lake_publish_version_max_threads` を介したスレッドプールサイズ設定の影響を受けます。
- Introduced in: v3.2.0

##### `lake_enable_tablet_creation_optimization`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: 有効にすると、StarRocks は共有データモードのクラウドネイティブテーブルおよびマテリアライズドビューのタブレット作成を最適化し、タブレットごとに個別のメタデータではなく、物理パーティション下のすべてのタブレットに単一の共有タブレットメタデータを作成します。これにより、テーブル作成、ロールアップ、スキーマ変更ジョブ中に作成されるタブレット作成タスクとメタデータ/ファイルの数が削減されます。この最適化はクラウドネイティブテーブル/マテリアライズドビューにのみ適用され、`file_bundling` と組み合わせて使用されます (後者は同じ最適化ロジックを再利用します)。注: スキーマ変更およびロールアップジョブは、同じ名前のファイルが上書きされるのを避けるため、`file_bundling` を使用するテーブルでは明示的に最適化を無効にします。注意して有効にしてください。作成されるタブレットメタデータの粒度が変更され、レプリカ作成とファイル命名の動作に影響する可能性があります。
- Introduced in: v3.3.1, v3.4.0, v3.5.0

##### `lake_use_combined_txn_log`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: この項目が `true` に設定されている場合、システムは Lake テーブルが関連トランザクションの結合トランザクションログパスを使用することを許可します。共有データクラスターでのみ利用可能です。
- Introduced in: v3.3.7, v3.4.0, v3.5.0

##### `enable_iceberg_commit_queue`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Iceberg テーブルのコミットキューを有効にして、同時コミット競合を回避するかどうか。Iceberg は、メタデータコミットに楽観的並行性制御 (OCC) を使用します。複数のスレッドが同じテーブルに同時にコミットすると、「Cannot commit: Base metadata location is not same as the current table metadata location」のようなエラーで競合が発生する可能性があります。有効にすると、各 Iceberg テーブルにはコミット操作用の単一スレッドエクゼキューターがあり、同じテーブルへのコミットがシリアル化され、OCC 競合が防止されます。異なるテーブルは同時にコミットでき、全体のスループットを維持します。これは信頼性を向上させるためのシステムレベルの最適化であり、デフォルトで有効にする必要があります。無効にすると、楽観的ロック競合により同時コミットが失敗する可能性があります。
- Introduced in: v4.1.0

##### `iceberg_commit_queue_timeout_seconds`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Iceberg コミット操作が完了するまでの待機タイムアウト (秒単位)。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、各コミット操作はこのタイムアウト内で完了する必要があります。コミットがこのタイムアウトよりも長くかかる場合、キャンセルされ、エラーが発生します。コミット時間に影響する要因には、コミットされるデータファイルの数、テーブルのメタデータサイズ、基盤となるストレージ (S3、HDFS など) のパフォーマンスが含まれます。
- Introduced in: v4.1.0

##### `iceberg_commit_queue_max_size`

- Default: 1000
- Type: Int
- Unit: Count
- Is mutable: No
- Description: Iceberg テーブルごとの保留中のコミット操作の最大数。コミットキュー (`enable_iceberg_commit_queue=true`) を使用する場合、これは単一テーブルのキューに入れられるコミット操作の数を制限します。制限に達すると、追加のコミット操作は呼び出し元のスレッドで実行されます (容量が利用可能になるまでブロックします)。この設定は FE 起動時に読み取られ、新しく作成されたテーブルエクゼキューターに適用されます。有効にするには FE の再起動が必要です。同じテーブルへの同時コミットが多いと予想される場合は、この値を増やしてください。この値が低すぎると、高並行時に呼び出し元スレッドでコミットがブロックされる可能性があります。
- Introduced in: v4.1.0

### その他

##### `agent_task_resend_wait_time_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: FE がエージェントタスクを再送信するまでに待機する必要がある期間。エージェントタスクは、タスク作成時間と現在時刻の間のギャップがこのパラメーターの値を超えた場合にのみ再送信できます。このパラメーターは、エージェントタスクの繰り返し送信を防ぐために使用されます。
- Introduced in: -

##### `allow_system_reserved_names`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: ユーザーが `__op` と `__row` で始まる名前の列を作成することを許可するかどうか。この機能を有効にするには、このパラメーターを `TRUE` に設定します。これらの名前形式は StarRocks の特殊な目的のために予約されており、そのような列を作成すると未定義の動作になる可能性があるため、この機能はデフォルトで無効になっています。
- Introduced in: v3.2.0

##### `auth_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: FE が属する StarRocks クラスター内で ID 認証に使用されるトークン。このパラメーターが指定されていない場合、StarRocks はクラスターのリーダー FE が最初に起動されたときに、クラスターのランダムなトークンを生成します。
- Introduced in: -

##### `authentication_ldap_simple_bind_base_dn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: LDAP サーバーがユーザーの認証情報の検索を開始する基底 DN。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_dn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: ユーザーの認証情報を検索するために使用される管理者 DN。
- Introduced in: -

##### `authentication_ldap_simple_bind_root_pwd`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: ユーザーの認証情報を検索するために使用される管理者のパスワード。
- Introduced in: -

##### `authentication_ldap_simple_server_host`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: LDAP サーバーが実行されているホスト。
- Introduced in: -

##### `authentication_ldap_simple_server_port`

- Default: 389
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: LDAP サーバーのポート。
- Introduced in: -

##### `authentication_ldap_simple_user_search_attr`

- Default: uid
- Type: String
- Unit: -
- Is mutable: Yes
- Description: LDAP オブジェクトでユーザーを識別する属性の名前。
- Introduced in: -

##### `backup_job_default_timeout_ms`

- Default: 86400 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: バックアップジョブのタイムアウト期間。この値を超えると、バックアップジョブは失敗します。
- Introduced in: -

##### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: `SHOW PROC /BACKENDS/{id}` コマンドで各ディスクのタブレット数を収集することを有効にするかどうか。
- Introduced in: v4.0.1, v3.5.8

##### `enable_colocate_restore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: コロケーションテーブルのバックアップと復元を有効にするかどうか。`true` はコロケーションテーブルのバックアップと復元を有効にすることを示し、`false` は無効にすることを示します。
- Introduced in: v3.2.10, v3.3.3

##### `enable_materialized_view_concurrent_prepare`

- Default: true
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: パフォーマンスを向上させるために、マテリアライズドビューを並行して準備するかどうか。
- Introduced in: v3.4.4

##### `enable_metric_calculator`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: メトリックを定期的に収集する機能を有効にするかどうかを指定します。有効な値: `TRUE` および `FALSE`。`TRUE` はこの機能を有効にすることを指定し、`FALSE` はこの機能を無効にすることを指定します。
- Introduced in: -

##### `enable_table_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE でテーブルレベルのメトリックをエクスポートするかどうか。無効にすると、FE はテーブルメトリック (テーブルスキャン/ロードカウンターやテーブルサイズメトリックなど) のエクスポートをスキップしますが、カウンターはメモリに記録されます。
- Introduced in: -

##### `enable_mv_post_image_reload_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: FE がイメージをロードした後、リロードフラグチェックを実行するかどうか。ベースマテリアライズドビューに対してチェックが実行された場合、それに関連する他のマテリアライズドビューに対しては不要です。
- Introduced in: v3.5.0

##### `enable_mv_query_context_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: クエリ書き換えパフォーマンスを向上させるために、クエリレベルのマテリアライズドビュー書き換えキャッシュを有効にするかどうか。
- Introduced in: v3.3

##### `enable_mv_refresh_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: すべてのマテリアライズドビューに対して、デフォルトでマテリアライズドビューの更新時にプロファイルを有効にするかどうか。
- Introduced in: v3.3.0

##### `enable_mv_refresh_extra_prefix_logging`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: デバッグを容易にするために、ログにマテリアライズドビュー名をプレフィックスとして含めることを有効にするかどうか。
- Introduced in: v3.4.0

##### `enable_mv_refresh_query_rewrite`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新中にクエリ書き換えを有効にして、クエリパフォーマンスを向上させるために基底テーブルではなく書き換えられた MV を直接使用できるようにするかどうか。
- Introduced in: v3.3

##### `enable_trace_historical_node`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: システムが履歴ノードをトレースすることを許可するかどうか。この項目を `true` に設定することで、キャッシュ共有機能を有効にし、エラスティック スケーリング中にシステムが適切なキャッシュノードを選択できるようにすることができます。
- Introduced in: v3.5.1

##### `es_state_sync_interval_second`

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: FE が Elasticsearch インデックスを取得し、StarRocks 外部テーブルのメタデータを同期する時間間隔。
- Introduced in: -

##### `hive_meta_cache_refresh_interval_s`

- Default: 3600 * 2
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: Hive 外部テーブルのキャッシュされたメタデータが更新される時間間隔。
- Introduced in: -

##### `hive_meta_store_timeout_s`

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: Hive メタストアへの接続がタイムアウトするまでの時間。
- Introduced in: -

##### `jdbc_connection_idle_timeout_ms`

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: JDBC カタログへのアクセス接続がタイムアウトするまでの最大時間。タイムアウトした接続はアイドル状態と見なされます。
- Introduced in: -

##### `jdbc_connection_timeout_ms`

- Default: 10000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: HikariCP コネクションプールが接続を取得するまでのタイムアウト (ミリ秒単位)。この時間内にプールから接続を取得できない場合、操作は失敗します。
- Introduced in: v3.5.13

##### `jdbc_query_timeout_ms`

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: JDBC ステートメントのクエリ実行のタイムアウト (ミリ秒単位)。このタイムアウトは、JDBC カタログを通じて実行されるすべての SQL クエリ (パーティションメタデータクエリなど) に適用されます。この値は、JDBC ドライバーに渡されるときに秒に変換されます。
- Introduced in: v3.5.13

##### `jdbc_network_timeout_ms`

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: JDBC ネットワーク操作 (ソケット読み取り) のタイムアウト (ミリ秒単位)。このタイムアウトは、外部データベースが応答しない場合に無期限のブロッキングを防ぐために、データベースメタデータ呼び出し (getSchemas()、getTables()、getColumns() など) に適用されます。
- Introduced in: v3.5.13

##### `jdbc_connection_pool_size`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: JDBC カタログにアクセスするための JDBC 接続プールの最大容量。
- Introduced in: -

##### `jdbc_meta_default_cache_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: JDBC カタログのメタデータキャッシュが有効になっているかどうかのデフォルト値。true に設定すると、新しく作成された JDBC カタログはデフォルトでメタデータキャッシュが有効になります。
- Introduced in: -

##### `jdbc_meta_default_cache_expire_sec`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: JDBC カタログのメタデータキャッシュのデフォルトの有効期限。`jdbc_meta_default_cache_enable` が true に設定されている場合、新しく作成された JDBC カタログはデフォルトでメタデータキャッシュの有効期限を設定します。
- Introduced in: -

##### `jdbc_minimum_idle_connections`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: JDBC カタログにアクセスするための JDBC 接続プールのアイドル接続の最小数。
- Introduced in: -

##### `jwt_jwks_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JSON Web Key Set (JWKS) サービスの URL、または `fe/conf` ディレクトリ下の公開鍵ローカルファイルへのパス。
- Introduced in: v3.5.0

##### `jwt_principal_field`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は StarRocks へのログインに使用するユーザー名と同一である必要があります。
- Introduced in: v3.5.0

##### `jwt_required_audience`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT のオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT のオーディエンスと一致する場合のみです。
- Introduced in: v3.5.0

##### `jwt_required_issuer`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT の発行者 (`iss`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT の発行者と一致する場合のみです。
- Introduced in: v3.5.0

##### locale

- Default: `zh_CN.UTF-8`
- Type: String
- Unit: -
- Is mutable: No
- Description: FE が使用する文字セット。
- Introduced in: -

##### `max_agent_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: エージェントタスクスレッドプールで許可されるスレッドの最大数。
- Introduced in: -

##### `max_download_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 各 RESTORE 操作において、StarRocks が BE ノードに割り当てるダウンロードタスクの最大数。この項目が 0 以下に設定されている場合、タスク数に制限は課されません。
- Introduced in: v3.1.0

##### `max_mv_check_base_table_change_retry_times`

- Default: 10
- Type: -
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新時に、基底テーブルの変更を検出するための最大再試行回数。
- Introduced in: v3.3.0

##### `max_mv_refresh_failure_retry_times`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新が失敗した場合の最大再試行回数。
- Introduced in: v3.3.0

##### `max_mv_refresh_try_lock_failure_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビューの更新が失敗した場合の、ロック試行の最大再試行回数。
- Introduced in: v3.3.0

##### `max_small_file_number`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: FE ディレクトリに格納できる小型ファイルの最大数。
- Introduced in: -

##### `max_small_file_size_bytes`

- Default: 1024 * 1024
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: 小型ファイルの最大サイズ。
- Introduced in: -

##### `max_upload_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 各 BACKUP 操作において、StarRocks が BE ノードに割り当てるアップロードタスクの最大数。この項目が 0 以下に設定されている場合、タスク数に制限は課されません。
- Introduced in: v3.1.0

##### `mv_create_partition_batch_interval_ms`

- Default: 1000
- Type: Int
- Unit: ms
- Is mutable: Yes
- Description: マテリアライズドビューの更新中、複数のパーティションを一括作成する必要がある場合、システムはそれらをそれぞれ 64 パーティションのバッチに分割します。頻繁なパーティション作成による障害のリスクを軽減するため、作成頻度を制御するために各バッチ間にデフォルトの間隔 (ミリ秒単位) が設定されます。
- Introduced in: v3.3

##### `mv_plan_cache_max_size`

- Default: 1000
- Type: Long
- Unit:
- Is mutable: Yes
- Description: マテリアライズドビュー計画キャッシュの最大サイズ (マテリアライズドビューの書き換えに使用されます)。透過的クエリ書き換えに多くのマテリアライズドビューが使用されている場合、この値を増やすことができます。
- Introduced in: v3.2

##### `mv_plan_cache_thread_pool_size`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビュー計画キャッシュのデフォルトスレッドプールサイズ (マテリアライズドビュー書き換えに使用されます)。
- Introduced in: v3.2

##### `mv_refresh_default_planner_optimize_timeout`

- Default: 30000
- Type: -
- Unit: -
- Is mutable: Yes
- Description: マテリアライズドビュー更新時のオプティマイザの計画フェーズのデフォルトタイムアウト。
- Introduced in: v3.3.0

##### `mv_refresh_fail_on_filter_data`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: 更新時にフィルタリングされたデータがある場合、マテリアライズドビューの更新は失敗します (デフォルトは true)。そうでない場合、フィルタリングされたデータを無視して成功を返します。
- Introduced in: -

##### `mv_refresh_try_lock_timeout_ms`

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: マテリアライズドビューの更新が、基底テーブル/マテリアライズドビューの DB ロックを試行するデフォルトの試行ロックタイムアウト。
- Introduced in: v3.3.0

##### `oauth2_auth_server_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: 認証 URL。OAuth 2.0 認証プロセスを開始するためにユーザーのブラウザがリダイレクトされる URL。
- Introduced in: v3.5.0

##### `oauth2_client_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks クライアントの公開識別子。
- Introduced in: v3.5.0

##### `oauth2_client_secret`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks クライアントを認証サーバーで認証するために使用されるシークレット。
- Introduced in: v3.5.0

##### `oauth2_jwks_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JSON Web Key Set (JWKS) サービスの URL、または `conf` ディレクトリ下のローカルファイルへのパス。
- Introduced in: v3.5.0

##### `oauth2_principal_field`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT のサブジェクト (`sub`) を示すフィールドを識別するために使用される文字列。デフォルト値は `sub` です。このフィールドの値は StarRocks へのログインに使用するユーザー名と同一である必要があります。
- Introduced in: v3.5.0

##### `oauth2_redirect_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: OAuth 2.0 認証が成功した後にユーザーのブラウザがリダイレクトされる URL。認証コードはこの URL に送信されます。ほとんどの場合、`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2` として設定する必要があります。
- Introduced in: v3.5.0

##### `oauth2_required_audience`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT のオーディエンス (`aud`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT のオーディエンスと一致する場合のみです。
- Introduced in: v3.5.0

##### `oauth2_required_issuer`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: JWT の発行者 (`iss`) を識別するために使用される文字列のリスト。JWT が有効であると見なされるのは、リスト内の値のいずれかが JWT の発行者と一致する場合のみです。
- Introduced in: v3.5.0

##### `oauth2_token_server_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: StarRocks がアクセストークンを取得する認証サーバーのエンドポイントの URL。
- Introduced in: v3.5.0

##### `plugin_dir`

- Default: `System.getenv("STARROCKS_HOME")` + "/plugins"
- Type: String
- Unit: -
- Is mutable: No
- Description: プラグインインストールパッケージを格納するディレクトリ。
- Introduced in: -

##### `plugin_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: プラグインを FE にインストールできるかどうか。プラグインはリーダー FE にのみインストールまたはアンインストールできます。
- Introduced in: -

##### `proc_profile_jstack_depth`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: システムが CPU およびメモリプロファイルを収集する際の最大 Java スタック深度。この値は、サンプリングされた各スタックについていくつの Java スタックフレームがキャプチャされるかを制御します。値が大きいほどトレースの詳細と出力サイズが増加し、プロファイリングのオーバーヘッドが追加される可能性があります。値が小さいほど詳細は減少します。この設定は CPU およびメモリプロファイリングの両方でプロファイラーが起動されるときに使用されるため、診断のニーズとパフォーマンスへの影響のバランスをとるように調整してください。
- Introduced in: -

##### `proc_profile_mem_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: プロセスメモリ割り当てプロファイルの収集を有効にするかどうか。この項目が `true` に設定されている場合、システムは `sys_log_dir/proc_profile` に `mem-profile-<timestamp>.html` という名前の HTML プロファイルを生成し、`proc_profile_collect_time_s` 秒間サンプリングしながらスリープし、Java スタック深度に `proc_profile_jstack_depth` を使用します。生成されたファイルは `proc_profile_file_retained_days` および `proc_profile_file_retained_size_bytes` に従って圧縮され、パージされます。ネイティブ抽出パスは `/tmp` の noexec 問題を回避するために `STARROCKS_HOME_DIR` を使用します。この項目はメモリ割り当てホットスポットのトラブルシューティングを目的としています。これを有効にすると CPU、I/O、ディスク使用量が増加し、大きなファイルが生成される可能性があります。
- Introduced in: v3.2.12

##### `query_detail_explain_level`

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: true
- Description: EXPLAIN ステートメントによって返されるクエリ計画の詳細レベル。有効な値: COSTS, NORMAL, VERBOSE。
- Introduced in: v3.2.12, v3.3.5

##### `replication_interval_ms`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: レプリケーションタスクがスケジュールされる最小時間間隔。
- Introduced in: v3.3.5

##### `replication_max_parallel_data_size_mb`

- Default: 1048576
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: 同時同期に許可されるデータの最大サイズ。
- Introduced in: v3.3.5

##### `replication_max_parallel_replica_count`

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 同時同期に許可されるタブレットレプリカの最大数。
- Introduced in: v3.3.5

##### `replication_max_parallel_table_count`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 許可される同時データ同期タスクの最大数。StarRocks はテーブルごとに 1 つの同期タスクを作成します。
- Introduced in: v3.3.5

##### `replication_transaction_timeout_sec`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: 同期タスクのタイムアウト期間。
- Introduced in: v3.3.5

##### `skip_whole_phase_lock_mv_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: 関連するマテリアライズドビューを持つテーブルに「非ロック」最適化を StarRocks がいつ適用するかを制御します。この項目
