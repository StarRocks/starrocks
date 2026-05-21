---
displayed_sidebar: docs
sidebar_label: "ログ、サーバー、およびメタデータ"
---

# FE 設定 - ログ、サーバー、およびメタデータ

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

<FEConfigMethod />

## FE 設定項目の表示

FE の起動後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行して、パラメーター設定を確認できます。特定のパラメーターの設定をクエリするには、次のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスター管理関連コマンドを実行するには、管理者権限が必要です。
:::

## FE パラメーターの設定

### FE 動的パラメーターの設定

[`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して、FE 動的パラメーターの設定を構成または変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメーターの設定

<StaticFEConfigNote />

---

このトピックでは、以下の種類のFE構成について紹介します：
- [ログ](#ログ)
- [サーバー](#サーバー)
- [メタデータとクラスター管理](#メタデータとクラスター管理)

## ログ

### `audit_log_delete_age`

- デフォルト：30d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：監査ログファイルの保持期間。デフォルト値 `30d` は、各監査ログファイルが 30 日間保持できることを指定します。StarRocks は各監査ログファイルをチェックし、30 日以上前に生成されたファイルを削除します。
- 導入時期：-

### `audit_log_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：監査ログファイルを格納するディレクトリ。
- 導入時期：-

### `audit_log_enable_compress`

- デフォルト：false
- タイプ：Boolean
- 単位：N/A
- 変更可能：No
- 説明：true の場合、生成された Log4j2 設定は、ローテーションされた監査ログファイル名 (fe.audit.log.*) に ".gz" 接尾辞を追加し、Log4j2 がロールオーバー時に圧縮された (.gz) アーカイブ監査ログファイルを生成するようにします。この設定は、FE 起動時に Log4jConfig.initLogging で読み込まれ、監査ログの RollingFile アペンダーに適用されます。アクティブな監査ログではなく、ローテーション/アーカイブされたファイルにのみ影響します。値は起動時に初期化されるため、変更を有効にするには FE の再起動が必要です。監査ログのローテーション設定 (`audit_log_dir`、`audit_log_roll_interval`、`audit_roll_maxsize`、`audit_log_roll_num`) とともに使用します。
- 導入時期：3.2.12

### `audit_log_json_format`

- デフォルト：false
- タイプ：Boolean
- 単位：N/A
- 変更可能：Yes
- 説明：true の場合、FE 監査イベントは、デフォルトのパイプ区切り "key=value" 文字列ではなく、構造化された JSON (Jackson ObjectMapper が注釈付き AuditEvent フィールドの Map をシリアル化) として出力されます。この設定は、AuditLogBuilder が処理するすべての組み込み監査シンクに影響します。接続監査、クエリ監査、大容量クエリ監査 (イベントが条件を満たす場合、大容量クエリしきい値フィールドが JSON に追加されます)、および低速監査出力です。大容量クエリしきい値および "features" フィールドに注釈が付けられたフィールドは特別に扱われます (通常の監査エントリから除外され、該当する場合、大容量クエリまたは機能ログに含まれます)。これを有効にすると、ログコレクターまたは SIEM のログが機械で解析可能になります。ログ形式が変更されるため、従来のパイプ区切り形式を期待する既存のパーサーを更新する必要がある場合があります。
- 導入時期：3.2.7

### `audit_log_modules`

- デフォルト：`slow_query`, query
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：StarRocks が監査ログエントリを生成するモジュール。デフォルトでは、StarRocks は `slow_query` モジュールと `query` モジュールの監査ログを生成します。`connection` モジュールは v3.0 以降でサポートされています。モジュール名をコンマ (,) とスペースで区切ります。
- 導入時期：-

### `audit_log_roll_interval`

- デフォルト：DAY
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks が監査ログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメーターが `DAY` に設定されている場合、監査ログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、監査ログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入時期：-

### `audit_log_roll_num`

- デフォルト：90
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：`audit_log_roll_interval` パラメーターで指定された各保持期間内に保持できる監査ログファイルの最大数。
- 導入時期：-

### `bdbje_log_level`

- デフォルト：INFO
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks で Berkeley DB Java Edition (BDB JE) が使用するロギングレベルを制御します。BDB 環境の初期化中に BDBEnvironment.initConfigs() は、この値を `com.sleepycat.je` パッケージの Java ロガーと BDB JE 環境ファイルロギングレベル (`EnvironmentConfig.FILE_LOGGING_LEVEL`) に適用します。SEVERE、WARNING、INFO、CONFIG、FINE、FINER、FINEST、ALL、OFF などの標準的な java.util.logging.Level 名を受け入れます。ALL に設定すると、すべてのログメッセージが有効になります。詳細度を上げると、ログのボリュームが増加し、ディスク I/O とパフォーマンスに影響を与える可能性があります。この値は BDB 環境が初期化されるときに読み込まれるため、環境の (再) 初期化後にのみ有効になります。
- 導入時期：v3.2.0

### `big_query_log_delete_age`

- デフォルト：7d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE の大容量クエリログファイル (`fe.big_query.log.*`) が自動削除されるまでの保持期間を制御します。この値は、Log4j の削除ポリシーに IfLastModified age として渡されます。最終更新時刻がこの値よりも古いローテーションされた大容量クエリログは削除されます。`d` (日)、`h` (時間)、`m` (分)、`s` (秒) などの接尾辞をサポートしています。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。この項目は `big_query_log_roll_interval` および `big_query_log_roll_num` と連携して、どのファイルを保持またはパージするかを決定します。
- 導入時期：v3.2.0

### `big_query_log_dir`

- デフォルト：`Config.STARROCKS_HOME_DIR + "/log"`
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE が大容量クエリダンプログ (`fe.big_query.log.*`) を書き込むディレクトリ。Log4j 設定はこのパスを使用して、`fe.big_query.log` とそのローテーションされたファイル用の RollingFile アペンダーを作成します。ローテーションと保持は、`big_query_log_roll_interval` (時刻ベースのサフィックス)、`log_roll_size_mb` (サイズトリガー)、`big_query_log_roll_num` (最大ファイル数)、および `big_query_log_delete_age` (年齢ベースの削除) によって管理されます。大容量クエリレコードは、`big_query_log_cpu_second_threshold`、`big_query_log_scan_rows_threshold`、または `big_query_log_scan_bytes_threshold` などのユーザー定義のしきい値を超えるクエリに対してログに記録されます。このファイルにログを記録するモジュールを制御するには、`big_query_log_modules` を使用します。
- 導入時期：v3.2.0

### `big_query_log_modules`

- デフォルト：`{"query"}`
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：モジュールごとの大容量クエリロギングを有効にするモジュール名サフィックスのリスト。一般的な値は論理コンポーネント名です。たとえば、デフォルトの `query` は `big_query.query` を生成します。
- 導入時期：v3.2.0

### `big_query_log_roll_interval`

- デフォルト：`"DAY"`
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：`big_query` ログアペンダーのローリングファイル名の日付コンポーネントを構築するために使用される時間間隔を指定します。有効な値 (大文字と小文字を区別しない) は `DAY` (デフォルト) と `HOUR` です。`DAY` は日次パターン (`"%d{yyyyMMdd}"`) を生成し、`HOUR` は時間別パターン (`"%d{yyyyMMddHH}"`) を生成します。この値は、サイズベースのロールオーバー (`big_query_roll_maxsize`) およびインデックスベースのロールオーバー (`big_query_log_roll_num`) と組み合わせて、RollingFile の filePattern を形成します。無効な値は、ログ設定の生成が失敗し (IOException)、ログの初期化または再構成を妨げる可能性があります。`big_query_log_dir`、`big_query_roll_maxsize`、`big_query_log_roll_num`、および `big_query_log_delete_age` とともに使用します。
- 導入時期：v3.2.0

### `big_query_log_roll_num`

- デフォルト：10
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：`big_query_log_roll_interval` ごとに保持するローテーションされた FE 大容量クエリログファイルの最大数。この値は、`fe.big_query.log` の RollingFile アペンダーの DefaultRolloverStrategy `max` 属性にバインドされます。ログが (時間または `log_roll_size_mb` によって) ロールオーバーすると、StarRocks は `big_query_log_roll_num` 個のインデックス付きファイル (filePattern は時刻サフィックスとインデックスを使用) を保持します。この数よりも古いファイルはロールオーバーによって削除される可能性があり、`big_query_log_delete_age` は最終更新時刻によってさらにファイルを削除できます。
- 導入時期：v3.2.0

### `dump_log_delete_age`

- デフォルト：7d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：ダンプログファイルの保持期間。デフォルト値 `7d` は、各ダンプログファイルが 7 日間保持できることを指定します。StarRocks は各ダンプログファイルをチェックし、7 日以上前に生成されたファイルを削除します。
- 導入時期：-

### `dump_log_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：ダンプログファイルを格納するディレクトリ。
- 導入時期：-

### `dump_log_modules`

- デフォルト：query
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：StarRocks がダンプログエントリを生成するモジュール。デフォルトでは、StarRocks はクエリモジュールのダンプログを生成します。モジュール名をコンマ (,) とスペースで区切ります。
- 導入時期：-

### `dump_log_roll_interval`

- デフォルト：DAY
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks がダンプログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメーターが `DAY` に設定されている場合、ダンプログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、ダンプログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入時期：-

### `dump_log_roll_num`

- デフォルト：10
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：`dump_log_roll_interval` パラメーターで指定された各保持期間内に保持できるダンプログファイルの最大数。
- 導入時期：-

### `edit_log_write_slow_log_threshold_ms`

- デフォルト：2000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：JournalWriter が低速な編集ログバッチ書き込みを検出してログに記録するために使用するしきい値 (ミリ秒)。バッチコミット後、バッチ期間がこの値を超えると、JournalWriter はバッチサイズ、期間、現在のジャーナルキューサイズを伴う WARN を出力します (約 2 秒に 1 回にレート制限)。この設定は、FE リーダーでの潜在的な I/O またはレプリケーションの遅延に対するロギング/アラートのみを制御します。コミットまたはロールの動作は変更しません (`edit_log_roll_num` およびコミット関連の設定を参照)。このしきい値に関係なく、メトリック更新は引き続き発生します。
- 導入時期：v3.2.3

### `enable_audit_sql`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：この項目が `true` に設定されている場合、FE 監査サブシステムは、ConnectProcessor によって処理されたステートメントの SQL テキストを FE 監査ログ (`fe.audit.log`) に記録します。格納されたステートメントは、他の制御に従います。暗号化されたステートメントは編集され (`AuditEncryptionChecker`)、`enable_sql_desensitize_in_log` が設定されている場合、機密性の高い資格情報は編集または非機密化される可能性があり、ダイジェストレコーディングは `enable_sql_digest` によって制御されます。`false` に設定されている場合、ConnectProcessor は監査イベントのステートメントテキストを "?" に置き換えます。他の監査フィールド (ユーザー、ホスト、期間、ステータス、`qe_slow_log_ms` を介した低速クエリ検出、およびメトリック) は引き続き記録されます。SQL 監査を有効にすると、フォレンジックとトラブルシューティングの可視性が向上しますが、機密性の高い SQL コンテンツが公開され、ログのボリュームと I/O が増加する可能性があります。無効にすると、監査ログでの完全なステートメントの可視性を失う代わりにプライバシーが向上します。
- 導入時期：-

### `enable_profile_log`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：プロファイルロギングを有効にするかどうか。この機能が有効になっている場合、FE はクエリごとのプロファイルログ (ProfileManager によって生成されたシリアル化された `queryDetail` JSON) をプロファイルログシンクに書き込みます。このロギングは `enable_collect_query_detail_info` も有効になっている場合にのみ実行されます。`enable_profile_log_compress` が有効になっている場合、JSON はロギング前に gzipped されることがあります。プロファイルログファイルは `profile_log_dir`、`profile_log_roll_num`、`profile_log_roll_interval` によって管理され、`profile_log_delete_age` ( `7d`、`10h`、`60m`、`120s` などの形式をサポート) に従ってローテーション/削除されます。この機能を無効にすると、プロファイルログの書き込みが停止します (ディスク I/O、圧縮 CPU、ストレージ使用量の削減)。
- 導入時期：v3.2.5

### `enable_qe_slow_log`

- デフォルト：true
- タイプ：Boolean
- 単位：N/A
- 変更可能：Yes
- 説明：有効にすると、FE 組み込み監査プラグイン (AuditLogBuilder) は、測定された実行時間 ("Time" フィールド) が `qe_slow_log_ms` で設定されたしきい値を超えるクエリイベントを低速クエリ監査ログ (AuditLog.getSlowAudit) に書き込みます。無効にすると、これらの低速クエリエントリは抑制されます (通常のクエリおよび接続監査ログは影響を受けません)。低速監査エントリは、グローバルな `audit_log_json_format` 設定 (JSON とプレーン文字列) に従います。このフラグを使用して、通常の監査ロギングとは独立して低速クエリ監査ボリュームの生成を制御します。無効にすると、`qe_slow_log_ms` が低い場合やワークロードが多くの長時間実行クエリを生成する場合にログ I/O を削減できます。
- 導入時期：3.2.11

### `enable_sql_desensitize_in_log`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：この項目が `true` に設定されている場合、システムはログ、クエリ詳細レコード、およびクエリプロファイルに書き込まれる前に機密性の高い SQL コンテンツを置き換えるか隠します。この設定を尊重するコードパスには、ConnectProcessor.formatStmt (監査ログ)、StmtExecutor.addRunningQueryDetail (クエリ詳細)、SimpleExecutor.formatSQL (内部エクゼキュータログ)、および StmtExecutor.buildTopLevelProfile / processProfileAsync (プロファイルの `Summary` セクションに格納される `Sql Statement` および `ExplainPlan` info-string) が含まれます。この機能が有効になっている場合、無効な SQL は固定の非機密化メッセージに置き換えられる可能性があり、資格情報 (ユーザー/パスワード) は隠され、SQL フォーマッターはサニタイズされた表現を生成する必要があります (ダイジェスト形式の出力を有効にすることもできます)。セッション変数 `enable_explain_in_profile` によって追加される `ExplainPlan` フィールドについても、本設定により埋め込まれる `EXPLAIN COSTS` テキストのリテラルが強制的にダイジェスト化されるため、プロファイル内で `Sql Statement` が非機密化されているにもかかわらず `ExplainPlan` が元のリテラルを露出してしまうことを防ぎます。これにより、監査/内部ログおよびプロファイルでの機密リテラルや資格情報の漏洩が減少しますが、ログ、クエリ詳細、およびプロファイルに元の完全な SQL テキストが含まれなくなることになります (これは再生やデバッグに影響する可能性があります)。
- 導入時期：-

### `internal_log_delete_age`

- デフォルト：7d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE 内部ログファイル (`internal_log_dir` に書き込まれます) の保持期間を指定します。値は期間文字列です。サポートされている接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。この項目は、RollingFile Delete ポリシーで使用される `<IfLastModified age="..."/>` 述語として log4j 設定に代入されます。最終変更時刻がこの期間よりも古いファイルは、ログのロールオーバー中に削除されます。この値を増やすとディスク領域をより早く解放できます。減らすと内部マテリアライズドビューまたは統計ログをより長く保持できます。
- 導入時期：v3.2.4

### `internal_log_dir`

- デフォルト：`Config.STARROCKS_HOME_DIR` + "/log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE ロギングサブシステムが内部ログ (`fe.internal.log`) を保存するために使用するディレクトリ。この設定は Log4j 設定に代入され、InternalFile アペンダーが内部/マテリアライズドビュー/統計ログを書き込む場所、および `internal.<module>` の下にあるモジュールごとのロガーがファイルを配置する場所を決定します。ディレクトリが存在し、書き込み可能であり、十分なディスク容量があることを確認してください。このディレクトリ内のファイルのログローテーションと保持は、`log_roll_size_mb`、`internal_log_roll_num`、`internal_log_delete_age`、および `internal_log_roll_interval` によって制御されます。`sys_log_to_console` が有効になっている場合、内部ログはこのディレクトリではなくコンソールに書き込まれることがあります。
- 導入時期：v3.2.4

### `internal_log_json_format`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、内部統計/監査エントリはコンパクトな JSON オブジェクトとして統計監査ロガーに書き込まれます。JSON には、"executeType" (InternalType: QUERY または DML)、"queryId"、"sql"、および "time" (経過ミリ秒) のキーが含まれます。`false` に設定されている場合、同じ情報は単一のフォーマットされたテキスト行 ("statistic execute: ... | QueryId: [...] | SQL: ...") としてログに記録されます。JSON を有効にすると、機械解析とログプロセッサとの統合が向上しますが、生の SQL テキストがログに含まれるため、機密情報が公開され、ログサイズが増加する可能性があります。
- 導入時期：-

### `internal_log_modules`

- デフォルト：`{"base", "statistic"}`
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：専用の内部ロギングを受け取るモジュール識別子のリスト。各エントリ X について、Log4j はレベル INFO と additivity="false" の `internal.<X>` という名前のロガーを作成します。これらのロガーは、内部アペンダー ( `fe.internal.log` に書き込まれます) または `sys_log_to_console` が有効になっている場合はコンソールにルーティングされます。必要に応じて短い名前またはパッケージフラグメントを使用します。正確なロガー名は `internal.` + 構成された文字列になります。内部ログファイルのローテーションと保持は、`internal_log_dir`、`internal_log_roll_num`、`internal_log_delete_age`、`internal_log_roll_interval`、および `log_roll_size_mb` に従います。モジュールを追加すると、実行時メッセージが内部ロガーストリームに分離され、デバッグと監査が容易になります。
- 導入時期：v3.2.4

### `internal_log_roll_interval`

- デフォルト：DAY
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE 内部ログアペンダーの時刻ベースのロール間隔を制御します。受け入れられる値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は時間別ファイルパターン (`"%d{yyyyMMddHH}"`) を生成し、`DAY` は日別ファイルパターン (`"%d{yyyyMMdd}"`) を生成します。これらは RollingFile TimeBasedTriggeringPolicy によってローテーションされた `fe.internal.log` ファイルに名前を付けるために使用されます。無効な値は、初期化の失敗 (アクティブな Log4j 設定の構築時に IOException がスローされます) を引き起こします。ロール動作は、`internal_log_dir`、`internal_roll_maxsize` (ソースにタイプミス、おそらく `log_roll_size_mb`)、`internal_log_roll_num`、および `internal_log_delete_age` などの関連設定にも依存します。
- 導入時期：v3.2.4

### `internal_log_roll_num`

- デフォルト：90
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：内部アペンダー (`fe.internal.log`) に対して保持するローテーションされた内部 FE ログファイルの最大数。この値は Log4j DefaultRolloverStrategy の `max` 属性として使用されます。ロールオーバーが発生すると、StarRocks は最大で `internal_log_roll_num` 個のアーカイブファイルを保持し、古いファイルを削除します (`internal_log_delete_age` によって管理されます)。値を小さくするとディスク使用量が減りますが、ログ履歴が短くなります。値を大きくすると、より多くの履歴内部ログが保持されます。この項目は、`internal_log_dir`、`internal_log_roll_interval`、および `internal_roll_maxsize` (ソースにタイプミス、おそらく `log_roll_size_mb`) と連携して機能します。
- 導入時期：v3.2.4

### `log_cleaner_audit_log_min_retention_days`

- デフォルト：3
- タイプ：Int
- 単位：Days
- 変更可能：Yes
- 説明：監査ログファイルの最小保持日数。これよりも新しい監査ログファイルは、ディスク使用量が高くても削除されません。これにより、監査ログがコンプライアンスとトラブルシューティングの目的で保持されます。
- 導入時期：-

### `log_cleaner_check_interval_second`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：ディスク使用量をチェックし、ログをクリーンアップする間隔 (秒単位)。クリーナーは、各ログディレクトリのディスク使用量を定期的にチェックし、必要に応じてクリーンアップをトリガーします。デフォルトは 300 秒 (5 分) です。
- 導入時期：-

### `log_cleaner_disk_usage_target`

- デフォルト：60
- タイプ：Int
- 単位：Percentage
- 変更可能：Yes
- 説明：ログクリーンアップ後の目標ディスク使用量 (パーセンテージ)。ディスク使用量がこのしきい値を下回るまでログクリーンアップが続行されます。クリーナーは、目標に達するまで最も古いログファイルを 1 つずつ削除します。
- 導入時期：-

### `log_cleaner_disk_usage_threshold`

- デフォルト：80
- タイプ：Int
- 単位：Percentage
- 変更可能：Yes
- 説明：ログクリーンアップをトリガーするディスク使用量しきい値 (パーセンテージ)。ディスク使用量がこのしきい値を超えると、ログクリーンアップが開始されます。クリーナーは、設定された各ログディレクトリを独立してチェックし、このしきい値を超えるディレクトリを処理します。
- 導入時期：-

### `log_cleaner_disk_util_based_enable`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：ディスク使用量に基づく自動ログクリーンアップを有効にします。有効にすると、ディスク使用量がしきい値を超えたときにログがクリーンアップされます。ログクリーナーは FE ノードのバックグラウンドデーモンとして実行され、ログファイルの蓄積によるディスク領域の枯渇を防ぐのに役立ちます。
- 導入時期：-

### `log_plan_cancelled_by_crash_be`

- デフォルト：true
- タイプ：boolean
- 単位：-
- 変更可能：Yes
- 説明：BE クラッシュまたは RPC 例外によりクエリがキャンセルされた場合に、クエリ実行計画のロギングを有効にするかどうか。この機能が有効になっている場合、BE クラッシュまたは `RpcException` によりクエリがキャンセルされたときに、StarRocks はクエリ実行計画 ( `TExplainLevel.COSTS` レベル) を WARN エントリとしてログに記録します。ログエントリには QueryId、SQL、および COSTS 計画が含まれ、ExecuteExceptionHandler パスでは例外スタックトレースもログに記録されます。ロギングは `enable_collect_query_detail_info` が有効になっている場合はスキップされます (その場合、計画はクエリ詳細に格納されます)。コードパスでは、クエリ詳細が null であることを検証することでチェックが実行されます。ExecuteExceptionHandler では、計画は最初のリトライ (`retryTime == 0`) のみでログに記録されることに注意してください。これを有効にすると、完全な COSTS 計画が大きくなる可能性があるため、ログのボリュームが増加する可能性があります。
- 導入時期：v3.2.0

### `log_register_and_unregister_query_id`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：FE が QeProcessorImpl からのクエリ登録および登録解除メッセージ (例: `"register query id = {}"` および `"deregister query id = {}"`) をログに記録することを許可するかどうか。ログは、クエリに null 以外の ConnectContext があり、コマンドが `COM_STMT_EXECUTE` でないか、セッション変数 `isAuditExecuteStmt()` が true の場合にのみ出力されます。これらのメッセージはすべてのクエリライフサイクルイベントに対して書き込まれるため、この機能を有効にすると、ログのボリュームが大きくなり、高並行環境ではスループットのボトルネックになる可能性があります。デバッグまたは監査のために有効にし、ロギングのオーバーヘッドを減らしてパフォーマンスを向上させるために無効にします。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `log_roll_size_mb`

- デフォルト：1024
- タイプ：Int
- 単位：MB
- 変更可能：No
- 説明：システムログファイルまたは監査ログファイルの最大サイズ。
- 導入時期：-

### `proc_profile_file_retained_days`

- デフォルト：1
- タイプ：Int
- 単位：Days
- 変更可能：Yes
- 説明：`sys_log_dir/proc_profile` 以下に生成されたプロセスプロファイリングファイル (CPU およびメモリ) の保持日数。ProcProfileCollector は、現在時刻から `proc_profile_file_retained_days` 日を差し引いて (yyyyMMdd-HHmmss 形式で) カットオフを計算し、タイムスタンプ部分がそのカットオフよりも辞書順で早いプロファイルファイル (`timePart.compareTo(timeToDelete) < 0` の場合) を削除します。ファイル削除は、`proc_profile_file_retained_size_bytes` によって制御されるサイズベースのカットオフも尊重します。プロファイルファイルは `cpu-profile-` と `mem-profile-` というプレフィックスを使用し、収集後に圧縮されます。
- 導入時期：v3.2.12

### `proc_profile_file_retained_size_bytes`

- デフォルト：2L * 1024 * 1024 * 1024 (2147483648)
- タイプ：Long
- 単位：Bytes
- 変更可能：Yes
- 説明：プロファイルディレクトリ下に保持される、収集された CPU およびメモリプロファイルファイル (`cpu-profile-` および `mem-profile-` というプレフィックスを持つファイル) の合計バイト数の最大値。有効なプロファイルファイルの合計が `proc_profile_file_retained_size_bytes` を超えると、コレクターは、残りの合計サイズが `proc_profile_file_retained_size_bytes` 以下になるまで、最も古いプロファイルファイルを削除します。`proc_profile_file_retained_days` よりも古いファイルもサイズに関係なく削除されます。この設定はプロファイルアーカイブのディスク使用量を制御し、`proc_profile_file_retained_days` と相互作用して削除順序と保持を決定します。
- 導入時期：v3.2.12

### `profile_log_delete_age`

- デフォルト：1d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE プロファイルログファイルが削除対象となるまでの保持期間を制御します。この値は Log4j の `<IfLastModified age="..."/>` ポリシー (`Log4jConfig` 経由) に注入され、`profile_log_roll_interval` や `profile_log_roll_num` などのローテーション設定と組み合わせて適用されます。サポートされる接尾辞: `d` (日)、`h` (時間)、`m` (分)、`s` (秒)。例: `7d` (7 日間)、`10h` (10 時間)、`60m` (60 分)、`120s` (120 秒)。
- 導入時期：v3.2.5

### `profile_log_dir`

- デフォルト：`Config.STARROCKS_HOME_DIR` + "/log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE プロファイルログが書き込まれるディレクトリ。Log4jConfig はこの値を使用して、プロファイル関連のアペンダーを配置します (このディレクトリの下に `fe.profile.log` や `fe.features.log` のようなファイルを作成します)。これらのファイルのローテーションと保持は、`profile_log_roll_size_mb`、`profile_log_roll_num`、`profile_log_delete_age` によって管理されます。タイムスタンプのサフィックス形式は `profile_log_roll_interval` (DAY または HOUR をサポート) によって制御されます。デフォルトのディレクトリは `STARROCKS_HOME_DIR` の下にあるため、FE プロセスがこのディレクトリへの書き込みおよびローテーション/削除の権限を持っていることを確認してください。
- 導入時期：v3.2.5


### `profile_log_latency_threshold_ms`

- デフォルト：0
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：`fe.profile.log` に書き込むクエリの最小レイテンシ（ミリ秒）。実行時間がこの値以上の場合にのみ profile を記録します。0 に設定するとすべての profile を記録します（しきい値なし）。正の値に設定すると、遅いクエリのみを記録してログ量を削減できます。
- 導入時期：-

### `profile_log_roll_interval`

- デフォルト：DAY
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：プロファイルログファイル名の日付部分を生成するために使用される時間粒度を制御します。有効な値 (大文字と小文字を区別しない) は `HOUR` と `DAY` です。`HOUR` は `"%d{yyyyMMddHH}"` (時間ごとの時間バケット) のパターンを生成し、`DAY` は `"%d{yyyyMMdd}"` (日ごとの時間バケット) を生成します。この値は Log4j 設定で `profile_file_pattern` を計算する際に使用され、ロールオーバーファイル名の時間ベースのコンポーネントのみに影響します。サイズベースのロールオーバーは `profile_log_roll_size_mb` によって引き続き制御され、保持は `profile_log_roll_num` / `profile_log_delete_age` によって制御されます。無効な値は、ロギング初期化中に IOException を引き起こします (エラーメッセージ: `"profile_log_roll_interval config error: <value>"` )。高ボリュームプロファイリングの場合は `HOUR` を選択して 1 時間あたりのファイルサイズを制限するか、日次集計の場合は `DAY` を選択します。
- 導入時期：v3.2.5

### `profile_log_roll_num`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：プロファイルロガーの Log4j DefaultRolloverStrategy が保持するローテーションされたプロファイルログファイルの最大数を指定します。この値は、ロギング XML に `${profile_log_roll_num}` として注入されます (例: `<DefaultRolloverStrategy max="${profile_log_roll_num}" fileIndex="min">`)。ローテーションは `profile_log_roll_size_mb` または `profile_log_roll_interval` によってトリガーされます。ローテーションが発生すると、Log4j は最大でこれらのインデックス付きファイルを保持し、古いインデックスファイルは削除対象となります。ディスク上での実際の保持は、`profile_log_delete_age` と `profile_log_dir` の場所にも影響されます。値が小さいとディスク使用量が減りますが、保持される履歴が制限されます。値が大きいと、より多くの履歴プロファイルログが保持されます。
- 導入時期：v3.2.5

### `profile_log_roll_size_mb`

- デフォルト：1024
- タイプ：Int
- 単位：MB
- 変更可能：No
- 説明：FE プロファイルログファイルのサイズベースのロールオーバーをトリガーするサイズしきい値 (メガバイト単位) を設定します。この値は、`ProfileFile` アペンダーの Log4j RollingFile SizeBasedTriggeringPolicy によって使用されます。プロファイルログが `profile_log_roll_size_mb` を超えると、ローテーションされます。ローテーションは、`profile_log_roll_interval` に達したときに時間によっても発生する可能性があります。いずれかの条件がロールオーバーをトリガーします。`profile_log_roll_num` と `profile_log_delete_age` と組み合わせることで、この項目は保持される履歴プロファイルファイルの数と古いファイルの削除時期を制御します。ローテーションされたファイルの圧縮は `enable_profile_log_compress` によって制御されます。
- 導入時期：v3.2.5

### `qe_slow_log_ms`

- デフォルト：5000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：クエリが遅いクエリかどうかを判断するために使用されるしきい値。クエリの応答時間がこのしきい値を超えると、**fe.audit.log** に遅いクエリとして記録されます。
- 導入時期：-

### `slow_lock_log_every_ms`

- デフォルト：3000L
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：同じ SlowLockLogStats インスタンスに対して別の「低速ロック」警告を発行するまでに待機する最小間隔 (ミリ秒)。LockUtils は、ロック待機が `slow_lock_threshold_ms` を超えた後にこの値をチェックし、最後のログに記録された低速ロックイベントから `slow_lock_log_every_ms` ミリ秒が経過するまで追加の警告を抑制します。長期的な競合中にログのボリュームを減らすには値を大きくし、より頻繁な診断を得るには値を小さくします。変更は、その後のチェックに対して実行時に有効になります。
- 導入時期：v3.2.0

### `slow_lock_print_stack`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：LockManager が `logSlowLockTrace` によって出力される低速ロック警告の JSON ペイロードに、所有スレッドの完全なスタックトレースを含めることを許可するかどうか ("stack" 配列は `LogUtil.getStackTraceToJsonArray` を使用して `start=0` および `max=Short.MAX_VALUE` で設定されます)。この設定は、ロック取得が `slow_lock_threshold_ms` で設定されたしきい値を超えたときに表示されるロック所有者に関する追加のスタック情報のみを制御します。この機能を有効にすると、ロックを保持している正確なスレッドスタックを提供することでデバッグに役立ちます。無効にすると、高並行環境でスタックトレースをキャプチャしてシリアル化することによるログのボリュームと CPU/メモリのオーバーヘッドが減少します。
- 導入時期：v3.3.16, v3.4.5, v3.5.1

### `slow_lock_threshold_ms`

- デフォルト：3000L
- タイプ：long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：ロック操作または保持されているロックを「遅い」と分類するために使用されるしきい値 (ミリ秒)。ロックの経過待機時間または保持時間がこの値を超えると、StarRocks は (コンテキストに応じて) 診断ログを出力し、スタックトレースまたは待機者/所有者情報を含め、LockManager ではこの遅延後にデッドロック検出を開始します。これは LockUtils (低速ロックロギング)、QueryableReentrantReadWriteLock (低速リーダーのフィルタリング)、LockManager (デッドロック検出遅延と低速ロックトレース)、LockChecker (定期的な低速ロック検出)、およびその他の呼び出し元 (例: DiskAndTabletLoadReBalancer ロギング) によって使用されます。値を下げると感度とロギング/診断のオーバーヘッドが増加します。0 または負の数に設定すると、初期の待機ベースのデッドロック検出遅延動作が無効になります。`slow_lock_log_every_ms`、`slow_lock_print_stack`、および `slow_lock_stack_trace_reserve_levels` と一緒に調整します。
- 導入時期：3.2.0

### `sys_log_delete_age`

- デフォルト：7d
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：システムログファイルの保持期間。デフォルト値 `7d` は、各システムログファイルが 7 日間保持できることを指定します。StarRocks は各システムログファイルをチェックし、7 日以上前に生成されたファイルを削除します。
- 導入時期：-

### `sys_log_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/log"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：システムログファイルを格納するディレクトリ。
- 導入時期：-

### `sys_log_enable_compress`

- デフォルト：false
- タイプ：boolean
- 単位：-
- 変更可能：No
- 説明：この項目が `true` に設定されている場合、システムはローテーションされたシステムログファイル名に ".gz" 接尾辞を追加し、Log4j が gzip 圧縮されたローテーションされた FE システムログ (例: fe.log.*) を生成するようにします。この値は Log4j 設定生成時 (Log4jConfig.initLogging / generateActiveLog4jXmlConfig) に読み取られ、RollingFile の filePattern で使用される `sys_file_postfix` プロパティを制御します。この機能を有効にすると、保持されるログのディスク使用量は減少しますが、ロールオーバー時の CPU と I/O が増加し、ログファイル名が変更されるため、ログを読み取るツールやスクリプトは .gz ファイルを処理できる必要があります。監査ログは圧縮に別の設定 (`audit_log_enable_compress`) を使用することに注意してください。
- 導入時期：v3.2.12

### `sys_log_format`

- デフォルト："plaintext"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE ログに使用される Log4j レイアウトを選択します。有効な値: `"plaintext"` (デフォルト) と `"json"`。値は大文字と小文字を区別しません。`"plaintext"` は、人間が読めるタイムスタンプ、レベル、スレッド、class.method:line、および WARN/ERROR のスタックトレースを持つ PatternLayout を構成します。`"json"` は JsonTemplateLayout を構成し、ログアグリゲーター (ELK、Splunk) に適した構造化 JSON イベント (UTC タイムスタンプ、レベル、スレッド ID/名、ソースファイル/メソッド/行、メッセージ、例外スタックトレース) を出力します。JSON 出力は、`sys_log_json_max_string_length` および `sys_log_json_profile_max_string_length` の最大文字列長に準拠します。
- 導入時期：v3.2.10

### `sys_log_json_max_string_length`

- デフォルト：1048576
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：JSON 形式のシステムログに使用される JsonTemplateLayout の "maxStringLength" 値を設定します。`sys_log_format` が `"json"` に設定されている場合、文字列値のフィールド (たとえば "message" や文字列化された例外スタックトレース) は、長さがこの制限を超えると切り捨てられます。この値は、生成された Log4j XML ( `Log4jConfig.generateActiveLog4jXmlConfig()` 内) に注入され、デフォルト、警告、監査、ダンプ、および大容量クエリレイアウトに適用されます。プロファイルレイアウトは別の設定 (`sys_log_json_profile_max_string_length`) を使用します。この値を小さくするとログサイズは減りますが、有用な情報が切り捨てられる可能性があります。
- 導入時期：3.2.11

### `sys_log_json_profile_max_string_length`

- デフォルト：104857600 (100 MB)
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：`sys_log_format` が "json" の場合、プロファイル (および関連機能) ログアペンダーの JsonTemplateLayout の maxStringLength を設定します。JSON 形式のプロファイルログ内の文字列フィールド値は、このバイト長に切り捨てられます。非文字列フィールドは影響を受けません。この項目は Log4jConfig の `JsonTemplateLayout maxStringLength` に適用され、プレーンテキストロギングが使用されている場合は無視されます。必要な完全なメッセージに対して十分な大きさに保ちますが、値が大きいとログサイズと I/O が増加することに注意してください。
- 導入時期：v3.2.11

### `sys_log_level`

- デフォルト：INFO
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：システムログエントリが分類される重大度レベル。有効な値: `INFO`、`WARN`、`ERROR`、および `FATAL`。
- 導入時期：-

### `sys_log_roll_interval`

- デフォルト：DAY
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：StarRocks がシステムログエントリをローテーションする時間間隔。有効な値: `DAY` と `HOUR`。
  - このパラメーターが `DAY` に設定されている場合、システムログファイル名に `yyyyMMdd` 形式のサフィックスが追加されます。
  - このパラメーターが `HOUR` に設定されている場合、システムログファイル名に `yyyyMMddHH` 形式のサフィックスが追加されます。
- 導入時期：-

### `sys_log_roll_num`

- デフォルト：10
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：`sys_log_roll_interval` パラメーターで指定された各保持期間内に保持できるシステムログファイルの最大数。
- 導入時期：-

### `sys_log_to_console`

- デフォルト：false (unless the environment variable `SYS_LOG_TO_CONSOLE` is set to "1")
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：この項目が `true` に設定されている場合、システムは Log4j を設定して、すべてのログをファイルベースのアペンダーではなくコンソール (ConsoleErr アペンダー) に送信します。この値はアクティブな Log4j XML 設定を生成する際に読み取られ (ルートロガーおよびモジュールごとのロガーアペンダーの選択に影響します)、プロセス起動時に `SYS_LOG_TO_CONSOLE` 環境変数から取得されます。実行時に変更しても効果はありません。この設定は、stdout/stderr ログ収集がログファイルの書き込みよりも優先されるコンテナ化された環境や CI 環境で一般的に使用されます。
- 導入時期：v3.2.0

### `sys_log_verbose_modules`

- デフォルト：Empty string
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：StarRocks がシステムログを生成するモジュール。このパラメーターが `org.apache.starrocks.catalog` に設定されている場合、StarRocks はカタログモジュールのみのシステムログを生成します。モジュール名をコンマ (,) とスペースで区切ります。
- 導入時期：-

### `sys_log_warn_modules`

- デフォルト：{}
- タイプ：String[]
- 単位：-
- 変更可能：No
- 説明：システムが起動時に WARN レベルのロガーとして設定し、警告アペンダー (SysWF) (`fe.warn.log` ファイル) にルーティングするロガー名またはパッケージプレフィックスのリスト。エントリは生成された Log4j 設定 (org.apache.kafka、org.apache.hudi、org.apache.hadoop.io.compress などの組み込み警告モジュールとともに) に挿入され、`<Logger name="... " level="WARN"><AppenderRef ref="SysWF"/></Logger>` のようなロガー要素を生成します。完全修飾パッケージおよびクラスプレフィックス (例: "com.example.lib") は、通常のログへのノイズの多い INFO/DEBUG 出力を抑制し、警告を個別にキャプチャできるようにするために推奨されます。
- 導入時期：v3.2.13

## サーバー

<EditionSpecificFEItem />

### `brpc_idle_wait_max_time`

- デフォルト：10000
- タイプ：Int
- 単位：ms
- 変更可能：No
- 説明：bRPC クライアントがアイドル状態で待機する最大時間。
- 導入時期：-

### `brpc_inner_reuse_pool`

- デフォルト：true
- タイプ：boolean
- 単位：-
- 変更可能：No
- 説明：下層の BRPC クライアントが接続/チャネルに内部共有再利用プールを使用するかどうかを制御します。StarRocks は BrpcProxy で RpcClientOptions を構築するときに `brpc_inner_reuse_pool` を読み取ります ( `rpcOptions.setInnerResuePool(...)` 経由)。有効な場合 (true)、RPC クライアントは内部プールを再利用して、呼び出しごとの接続作成を減らし、FE-to-BE / LakeService RPC の接続チャーン、メモリ、ファイルディスクリプタの使用量を削減します。無効な場合 (false)、クライアントはより多くの分離されたプールを作成する可能性があります (リソース使用量が増える代わりに並行性分離を向上させます)。この値を変更するには、プロセスを再起動して有効にする必要があります。
- 導入時期：v3.3.11, v3.4.1, v3.5.0

### `brpc_min_evictable_idle_time_ms`

- デフォルト：120000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：No
- 説明：アイドル状態の BRPC 接続が、接続プールで立ち退きの対象になるまでに残っている必要のある時間 (ミリ秒単位)。`BrpcProxy` が使用する RpcClientOptions に適用されます (RpcClientOptions.setMinEvictableIdleTime 経由)。この値を上げるとアイドル接続を長く保持でき (再接続のチャーンを減らす)、下げると未使用のソケットをより速く解放できます (リソース使用量を減らす)。接続の再利用、プールの成長、立ち退きの動作のバランスを取るために、`brpc_connection_pool_size` および `brpc_idle_wait_max_time` とともに調整します。
- 導入時期：v3.3.11, v3.4.1, v3.5.0

### `brpc_reuse_addr`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：true の場合、StarRocks は、brpc RpcClient によって作成されたクライアントソケットにローカルアドレスの再利用を許可するソケットオプションを設定します (RpcClientOptions.setReuseAddress 経由)。これを有効にすると、バインドの失敗が減り、ソケットが閉じられた後にローカルポートの再バインドが高速化され、高レートの接続チャーンや迅速な再起動に役立ちます。false の場合、アドレス/ポートの再利用は無効になり、意図しないポート共有の可能性を減らすことができますが、一時的なバインドエラーが増加する可能性があります。このオプションは、`brpc_connection_pool_size` および `brpc_short_connection` によって設定される接続動作と相互作用します。これは、クライアントソケットを再バインドして再利用できる速度に影響するためです。
- 導入時期：v3.3.11, v3.4.1, v3.5.0

### `cluster_name`

- デフォルト：StarRocks Cluster
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE が属する StarRocks クラスターの名前。クラスター名は Web ページの `Title` に表示されます。
- 導入時期：-

### `dns_cache_ttl_seconds`

- デフォルト：60
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：成功した DNS ルックアップの DNS キャッシュ TTL (Time-To-Live) (秒単位)。これにより、JVM が成功した DNS ルックアップをキャッシュする期間を制御する Java セキュリティプロパティ `networkaddress.cache.ttl` が設定されます。システムが常に情報をキャッシュできるようにするにはこの項目を `-1` に設定し、キャッシュを無効にするには `0` に設定します。これは、Kubernetes デプロイメントや動的 DNS が使用されている場合など、IP アドレスが頻繁に変更される環境で特に役立ちます。
- 導入時期：v3.5.11, v4.0.4

### `enable_http_async_handler`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：システムが HTTP リクエストを非同期で処理することを許可するかどうか。この機能が有効になっている場合、Netty ワーカー スレッドによって受信された HTTP リクエストは、HTTP サーバーのブロックを避けるために、サービスロジック処理のために別のスレッドプールに送信されます。無効になっている場合、Netty ワーカーがサービスロジックを処理します。
- 導入時期：4.0.0

### `enable_http_validate_headers`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：Netty の HttpServerCodec が厳密な HTTP ヘッダー検証を実行するかどうかを制御します。この値は、HttpServer の HTTP パイプラインが初期化されるときに HttpServerCodec に渡されます (UseLocations を参照)。新しい Netty バージョンではより厳密なヘッダー規則が適用されるため (https://github.com/netty/netty/pull/12760)、下位互換性のためにデフォルトは false です。RFC 準拠のヘッダーチェックを強制するには true に設定します。そうすると、レガシークライアントやプロキシからの不正な形式の要求や非準拠の要求が拒否される可能性があります。変更を有効にするには HTTP サーバーの再起動が必要です。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `enable_https`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：FE ノードで HTTP サーバーと HTTPS サーバーを同時に有効にするかどうか。
- 導入時期：v4.0

### `frontend_address`

- デフォルト：0.0.0.0
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：FE ノードの IP アドレス。
- 導入時期：-

### `http_async_threads_num`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：非同期 HTTP リクエスト処理用のスレッドプールのサイズ。エイリアスは `max_http_sql_service_task_threads_num` です。
- 導入時期：4.0.0

### `http_backlog_num`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの HTTP サーバーが保持するバックログキューの長さ。
- 導入時期：-

### `http_max_chunk_size`

- デフォルト：8192
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：FE HTTP サーバーの Netty の HttpServerCodec によって処理される単一の HTTP チャンクの最大許容サイズ (バイト単位) を設定します。これは HttpServerCodec に 3 番目の引数として渡され、チャンク転送またはストリーミング要求/応答中のチャンクの長さを制限します。受信チャンクがこの値を超えると、Netty はフレームが大きすぎるエラー (TooLongFrameException など) を発生させ、要求が拒否される可能性があります。正当な大規模チャンクアップロードの場合はこれを増やし、メモリ圧力を減らしたり、DoS 攻撃の攻撃対象領域を減らしたりするために小さく保ちます。この設定は、`http_max_initial_line_length`、`http_max_header_size`、および `enable_http_validate_headers` とともに使用されます。
- 導入時期：v3.2.0

### `http_max_header_size`

- デフォルト：32768
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：Netty の `HttpServerCodec` によって解析される HTTP 要求ヘッダーブロックの最大許容サイズ (バイト単位)。StarRocks はこの値を `HttpServerCodec` に渡します ( `Config.http_max_header_size` 経由)。受信要求のヘッダー (名前と値の組み合わせ) がこの制限を超えると、コーデックは要求を拒否し (デコーダー例外)、接続/要求は失敗します。クライアントが非常に大きなヘッダー (大きな Cookie または多くのカスタムヘッダー) を正当に送信する場合にのみ増やしてください。値が大きいほど、接続ごとのメモリ使用量が増加します。`http_max_initial_line_length` および `http_max_chunk_size` と合わせて調整してください。変更には FE の再起動が必要です。
- 導入時期：v3.2.0

### `http_max_initial_line_length`

- デフォルト：4096
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：HttpServer で使用される Netty `HttpServerCodec` が受け入れる HTTP 初期リクエスト行 (メソッド + リクエストターゲット + HTTP バージョン) の最大許容長 (バイト単位) を設定します。この値は Netty のデコーダーに渡され、この長さよりも長い初期行を持つリクエストは拒否されます (TooLongFrameException)。非常に長いリクエスト URI をサポートする必要がある場合にのみ、これを増やしてください。値が大きいほどメモリ使用量が増加し、不正な形式の/リクエスト乱用のリスクが高まる可能性があります。`http_max_header_size` および `http_max_chunk_size` と合わせて調整してください。
- 導入時期：v3.2.0

### `http_port`

- デフォルト：8030
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの HTTP サーバーがリッスンするポート。
- 導入時期：-

### `http_web_page_display_hardware`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：true の場合、HTTP インデックスページ (/index) に oshi ライブラリ (CPU、メモリ、プロセス、ディスク、ファイルシステム、ネットワークなど) を介して入力されたハードウェア情報セクションが含まれます。oshi は、システムユーティリティを呼び出したり、間接的にシステムファイルを読み取ったりする場合があります (たとえば、`getent passwd` などのコマンドを実行する可能性があります)。これにより、機密性の高いシステムデータが表面化する可能性があります。より厳格なセキュリティが必要な場合、またはホストでこれらの間接コマンドの実行を回避したい場合は、この設定を false にして、Web UI でのハードウェア詳細の収集と表示を無効にします。
- 導入時期：v3.2.0

### `http_worker_threads_num`

- デフォルト：0
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：HTTP リクエストを処理する HTTP サーバーのワーカー スレッドの数。負の値または 0 の場合、スレッド数は CPU コア数の 2 倍になります。
- 導入時期：v2.5.18, v3.0.10, v3.1.7, v3.2.2

### `https_port`

- デフォルト：8443
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの HTTPS サーバーがリッスンするポート。
- 導入時期：v4.0

### `max_mysql_service_task_threads_num`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの MySQL サーバーがタスクを処理するために実行できる最大スレッド数。
- 導入時期：-

### `max_task_runs_threads_num`

- デフォルト：512
- タイプ：Int
- 単位：Threads
- 変更可能：No
- 説明：タスク実行エグゼキュータースレッドプールの最大スレッド数を制御します。この値は同時タスク実行の上限であり、増やすと並行性が高まりますが、CPU、メモリ、ネットワーク使用量も増加し、減らすとタスク実行のバックログと待ち時間が長くなる可能性があります。この値は、予想される同時スケジュールジョブと利用可能なシステムリソースに応じて調整してください。
- 導入時期：v3.2.0

### `memory_tracker_enable`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：FE メモリートラッカーサブシステムを有効にします。`memory_tracker_enable` が `true` に設定されている場合、`MemoryUsageTracker` は定期的に登録されたメタデータモジュールをスキャンし、メモリ内の `MemoryUsageTracker.MEMORY_USAGE` マップを更新し、合計をログに記録し、`MetricRepo` がメモリー使用量とオブジェクト数ゲージをメトリック出力に公開するようにします。サンプリング間隔を制御するには `memory_tracker_interval_seconds` を使用します。この機能を有効にすると、メモリー消費の監視とデバッグに役立ちますが、CPU と I/O のオーバーヘッド、および追加のメトリックカーディナリティが発生します。
- 導入時期：v3.2.4

### `memory_tracker_interval_seconds`

- デフォルト：60
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：FE の `MemoryUsageTracker` デーモンが FE プロセスと登録された `MemoryTrackable` モジュールのメモリ使用量をポーリングして記録する間隔 (秒単位)。`memory_tracker_enable` が `true` に設定されている場合、トラッカーはこの周期で実行され、`MEMORY_USAGE` を更新し、集計された JVM および追跡対象モジュールの使用状況をログに記録します。
- 導入時期：v3.2.4

### `mysql_nio_backlog_num`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの MySQL サーバーが保持するバックログキューの長さ。
- 導入時期：-

### `mysql_server_version`

- デフォルト：8.0.33
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：クライアントに返される MySQL サーバーバージョン。このパラメーターを変更すると、以下の状況でバージョン情報に影響します。
  1. `select version();`
  2. ハンドシェイクパケットバージョン
  3. グローバル変数 `version` の値 (`show variables like 'version';`)
- 導入時期：-

### `mysql_service_io_threads_num`

- デフォルト：4
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの MySQL サーバーが I/O イベントを処理するために実行できる最大スレッド数。
- 導入時期：-

### `mysql_service_kill_after_disconnect`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：MySQL TCP 接続が閉じられたと検出された場合 (読み取りで EOF) のサーバーによるセッションの処理方法を制御します。`true` に設定されている場合、サーバーはその接続で実行中のクエリを直ちに停止し、即座にクリーンアップを実行します。`false` の場合、サーバーは切断時に実行中のクエリを停止せず、保留中の要求タスクがない場合にのみクリーンアップを実行し、クライアントが切断された後も長時間実行中のクエリを続行できるようにします。注: TCP キープアライブを示唆する簡単なコメントにもかかわらず、このパラメーターは特に切断後の停止動作を管理し、孤立したクエリを終了させるか (信頼性の低い/負荷分散されたクライアントの背後で推奨)、完了させるかを希望するかどうかに応じて設定する必要があります。
- 導入時期：-

### `mysql_service_nio_enable_keep_alive`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：MySQL 接続で TCP Keep-Alive を有効にします。ロードバランサーの背後にある長時間アイドル状態の接続に役立ちます。
- 導入時期：-

### `net_use_ipv6_when_priority_networks_empty`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：`priority_networks` が指定されていない場合に IPv6 アドレスを優先的に使用するかどうかを制御するブール値。`true` は、ノードをホストするサーバーに IPv4 と IPv6 アドレスの両方があり、`priority_networks` が指定されていない場合に、システムが IPv6 アドレスを優先的に使用することを許可することを示します。
- 導入時期：v3.3.0

### `priority_networks`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：複数の IP アドレスを持つサーバーの選択戦略を宣言します。このパラメーターで指定されたリストと一致する IP アドレスは最大 1 つである必要があることに注意してください。このパラメーターの値は、CIDR 表記でセミコロン (;) で区切られたエントリ (例: 10.10.10.0/24) で構成されるリストです。このリストのエントリに一致する IP アドレスがない場合、サーバーの使用可能な IP アドレスがランダムに選択されます。v3.3.0 から、StarRocks は IPv6 に基づくデプロイメントをサポートしています。サーバーに IPv4 と IPv6 アドレスの両方があり、このパラメーターが指定されていない場合、システムはデフォルトで IPv4 アドレスを使用します。`net_use_ipv6_when_priority_networks_empty` を `true` に設定することで、この動作を変更できます。
- 導入時期：-

### `proc_profile_cpu_enable`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `true` に設定されている場合、バックグラウンドの `ProcProfileCollector` は `AsyncProfiler` を使用して CPU プロファイルを収集し、HTML レポートを `sys_log_dir/proc_profile` に書き込みます。各収集実行では、`proc_profile_collect_time_s` で設定された期間 CPU スタックを記録し、Java スタック深度に `proc_profile_jstack_depth` を使用します。生成されたプロファイルは圧縮され、古いファイルは `proc_profile_file_retained_days` および `proc_profile_file_retained_size_bytes` に従ってパージされます。`AsyncProfiler` にはネイティブライブラリ (`libasyncProfiler.so`) が必要です。`/tmp` の noexec 問題を回避するために `one.profiler.extractPath` は `STARROCKS_HOME_DIR/bin` に設定されています。
- 導入時期：v3.2.12

### `qe_max_connection`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードへのすべてのユーザーが確立できる最大接続数。v3.1.12 および v3.2.7 以降、デフォルト値が `1024` から `4096` に変更されました。
- 導入時期：-

### `query_port`

- デフォルト：9030
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの MySQL サーバーがリッスンするポート。
- 導入時期：-

### `rpc_port`

- デフォルト：9020
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの Thrift サーバーがリッスンするポート。
- 導入時期：-

### `slow_lock_stack_trace_reserve_levels`

- デフォルト：15
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：StarRocks が遅いロックまたは保持されているロックのロックデバッグ情報をダンプする際に、いくつのスタックトレースフレームをキャプチャして出力するかを制御します。この値は、排他ロック所有者、現在のスレッド、および最古/共有リーダーの JSON を生成する際に `QueryableReentrantReadWriteLock` によって `LogUtil.getStackTraceToJsonArray` に渡されます。この値を増やすと、遅いロックまたはデッドロックの問題の診断に役立つより多くのコンテキストが得られますが、JSON ペイロードが大きくなり、スタックキャプチャの CPU/メモリがわずかに増加します。減らすとオーバーヘッドが減少します。注: リーダーエントリは、低速ロックのみをログに記録する場合、`slow_lock_threshold_ms` によってフィルタリングできます。
- 導入時期：v3.4.0, v3.5.0

### `ssl_cipher_blacklist`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：IANA 名で SSL 暗号スイートをブラックリストに登録するための、正規表現をサポートするコンマ区切りリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- 導入時期：v4.0

### `ssl_cipher_whitelist`

- デフォルト：Empty string
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：IANA 名で SSL 暗号スイートをホワイトリストに登録するための、正規表現をサポートするコンマ区切りリスト。ホワイトリストとブラックリストの両方が設定されている場合、ブラックリストが優先されます。
- 導入時期：v4.0

### `task_runs_concurrency`

- デフォルト：4
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：同時に実行される TaskRun インスタンスのグローバル制限。`TaskRunScheduler` は、現在の実行数が `task_runs_concurrency` 以上の場合、新しい実行のスケジュールを停止するため、この値はスケジューラー全体で並行 TaskRun 実行を制限します。これは `MVPCTRefreshPartitioner` によって TaskRun ごとのパーティション更新粒度を計算するためにも使用されます。値を増やすと並行性とリソース使用量が増加し、減らすと並行性が減少し、実行ごとのパーティション更新が大きくなります。意図的にスケジューリングを無効にする場合を除き、0 または負の値に設定しないでください。0 (または負の値) は、`TaskRunScheduler` による新しい TaskRun のスケジューリングを事実上妨げます。
- 導入時期：v3.2.0

### `task_runs_queue_length`

- デフォルト：500
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：保留中のキューに保持される保留中の TaskRun 項目の最大数を制限します。`TaskRunManager` は現在の保留中の数をチェックし、有効な保留中の TaskRun の数が `task_runs_queue_length` 以上の場合、新しい送信を拒否します。マージ/承認された TaskRun が追加される前に同じ制限が再チェックされます。メモリとスケジューリングのバックログのバランスをとるためにこの値を調整します。拒否を回避するために、大規模でバースト性の高いワークロードの場合は高く設定し、メモリを制限して保留中のバックログを減らす場合は低く設定します。
- 導入時期：v3.2.0

### `thrift_backlog_num`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：FE ノードの Thrift サーバーが保持するバックログキューの長さ。
- 導入時期：-

### `thrift_client_timeout_ms`

- デフォルト：5000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：No
- 説明：アイドル状態のクライアント接続がタイムアウトするまでの時間。
- 導入時期：-

### `thrift_rpc_max_body_size`

- デフォルト：-1
- タイプ：Int
- 単位：Bytes
- 変更可能：No
- 説明：サーバーの Thrift プロトコルを構築するときに使用される Thrift RPC メッセージ本文の最大許容サイズ (バイト単位) を制御します ( `ThriftServer` の TBinaryProtocol.Factory に渡されます)。値が `-1` の場合、制限が無効になります (無制限)。正の値を設定すると、上限が適用され、これより大きいメッセージは Thrift レイヤーによって拒否され、メモリ使用量を制限し、サイズ超過の要求や DoS のリスクを軽減するのに役立ちます。正当な要求の拒否を避けるために、予想されるペイロード (大きな構造体やバッチデータ) に十分な大きさに設定してください。
- 導入時期：v3.2.0

### `thrift_server_max_worker_threads`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：FE ノードの Thrift サーバーがサポートするワーカー スレッドの最大数。
- 導入時期：-

### `thrift_server_queue_size`

- デフォルト：4096
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：要求が保留されているキューの長さ。Thrift サーバーで処理されているスレッドの数が `thrift_server_max_worker_threads` で指定された値を超えると、新しい要求が保留中のキューに追加されます。
- 導入時期：-

## メタデータとクラスター管理

### `alter_max_worker_queue_size`

- デフォルト：4096
- タイプ：Int
- 単位：Tasks
- 変更可能：No
- 説明：alter サブシステムで使用される内部ワーカー スレッド プール キューの容量を制御します。これは `AlterHandler` で `ThreadPoolManager.newDaemonCacheThreadPool` に `alter_max_worker_threads` とともに渡されます。保留中の alter タスクの数が `alter_max_worker_queue_size` を超えると、新しい送信は拒否され、`RejectedExecutionException` がスローされる可能性があります ( `AlterHandler.handleFinishAlterTask` を参照)。この値を調整して、メモリ使用量と、同時 alter タスクに対して許可するバックログの量のバランスを取ります。
- 導入時期：v3.2.0

### `alter_max_worker_threads`

- デフォルト：4
- タイプ：Int
- 単位：Threads
- 変更可能：No
- 説明：AlterHandler のスレッドプールの最大ワーカー スレッド数を設定します。AlterHandler はこの値を使用してエクゼキューターを構築し、alter 関連のタスクを実行および完了します (例: handleFinishAlterTask 経由で `AlterReplicaTask` を送信)。この値は alter 操作の同時実行を制限します。値を上げると並行性とリソース使用量が増加し、下げると同時 alter が制限され、ボトルネックになる可能性があります。エクゼキューターは `alter_max_worker_queue_size` とともに作成され、ハンドラーのスケジューリングは `alter_scheduler_interval_millisecond` を使用します。
- 導入時期：v3.2.0

### `automated_cluster_snapshot_interval_seconds`

- デフォルト：600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：自動クラスター スナップショット タスクがトリガーされる間隔。
- 導入時期：v3.4.2

### `background_refresh_metadata_interval_millis`

- デフォルト：600000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：連続する 2 回の Hive メタデータ キャッシュ更新の間隔。
- 導入時期：v2.5.5

### `background_refresh_metadata_time_secs_since_last_access_secs`

- デフォルト：3600 * 24
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：Hive メタデータキャッシュ更新タスクの有効期限。アクセスされた Hive カタログの場合、指定された時間以上アクセスされていない場合、StarRocks はそのキャッシュされたメタデータの更新を停止します。アクセスされていない Hive カタログの場合、StarRocks はそのキャッシュされたメタデータを更新しません。
- 導入時期：v2.5.5

### `bdbje_cleaner_threads`

- デフォルト：1
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：StarRocks ジャーナルで使用される Berkeley DB Java Edition (JE) 環境のバックグラウンドクリーナースレッドの数。この値は `BDBEnvironment.initConfigs` で環境初期化中に読み取られ、`Config.bdbje_cleaner_threads` を使用して `EnvironmentConfig.CLEANER_THREADS` に適用されます。これは JE ログクリーニングとスペース再利用の並行性を制御します。値を増やすとクリーニングが高速化される可能性がありますが、追加の CPU とフォアグラウンド操作との I/O 干渉が発生する可能性があります。変更は BDB 環境が (再) 初期化された場合にのみ有効になるため、新しい値を適用するにはフロントエンドの再起動が必要です。
- 導入時期：v3.2.0

### `bdbje_heartbeat_timeout_second`

- デフォルト：30
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：StarRocks クラスター内のリーダー、フォロワー、オブザーバー FE 間でハートビートがタイムアウトするまでの時間。
- 導入時期：-

### `bdbje_lock_timeout_second`

- デフォルト：1
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：BDB JE ベースの FE のロックがタイムアウトするまでの時間。
- 導入時期：-

### `bdbje_replay_cost_percent`

- デフォルト：150
- タイプ：Int
- 単位：Percent
- 変更可能：No
- 説明：BDB JE ログからのトランザクションのリプレイとネットワーク復元による同じデータの取得の相対コスト (パーセンテージ) を設定します。この値は基盤となる JE レプリケーションパラメーター `REPLAY_COST_PERCENT` に提供され、通常 `>100` であり、リプレイが通常ネットワーク復元よりも高価であることを示します。システムは、潜在的なリプレイのためにクリーンアップされたログファイルを保持するかどうかを決定する際に、リプレイコストにログサイズを掛けたものとネットワーク復元のコストを比較します。ネットワーク復元の方が効率的であると判断された場合、ファイルは削除されます。値が 0 の場合、このコスト比較に基づく保持は無効になります。`REP_STREAM_TIMEOUT` 内のレプリカまたはアクティブなレプリケーションに必要なログファイルは常に保持されます。
- 導入時期：v3.2.0

### `bdbje_replica_ack_timeout_second`

- デフォルト：10
- タイプ：Int
- 単位：Seconds
- 変更可能：No
- 説明：メタデータがリーダー FE からフォロワー FE に書き込まれるときに、リーダー FE が指定された数のフォロワー FE から ACK メッセージを待機できる最大時間。単位: 秒。大量のメタデータが書き込まれている場合、フォロワー FE がリーダー FE に ACK メッセージを返すまでに長い時間がかかり、ACK タイムアウトが発生する可能性があります。この状況では、メタデータ書き込みが失敗し、FE プロセスが終了します。この状況を防ぐために、このパラメーターの値を増やすことをお勧めします。
- 導入時期：-

### `bdbje_reserved_disk_size`

- デフォルト：512 * 1024 * 1024 (536870912)
- タイプ：Long
- 単位：Bytes
- 変更可能：No
- 説明：Berkeley DB JE が「保護されていない」(削除可能) ログ/データファイルとして予約するバイト数を制限します。StarRocks はこの値を BDBEnvironment の `EnvironmentConfig.RESERVED_DISK` 経由で JE に渡します。JE の組み込みデフォルトは 0 (無制限) です。StarRocks のデフォルト (512 MiB) は、JE が保護されていないファイルに過剰なディスクスペースを予約するのを防ぎつつ、古いファイルを安全にクリーンアップできるようにします。ディスクが制限されているシステムでこの値を調整します。値を減らすと JE はより多くのファイルをより早く解放でき、増やすと JE はより多くの予約スペースを保持できます。変更を有効にするにはプロセスを再起動する必要があります。
- 導入時期：v3.2.0

### `bdbje_reset_election_group`

- デフォルト：false
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：BDBJE レプリケーショングループをリセットするかどうか。このパラメーターが `TRUE` に設定されている場合、FE は BDBJE レプリケーショングループをリセットし (つまり、すべての選挙可能な FE ノードの情報を削除し)、リーダー FE として起動します。リセット後、この FE はクラスター内の唯一のメンバーとなり、他の FE は `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'` を使用してこのクラスターに再参加できます。ほとんどのフォロワー FE のデータが破損しているためにリーダー FE を選出できない場合にのみこの設定を使用してください。`reset_election_group` は `metadata_failure_recovery` の代替として使用されます。
- 導入時期：-

### `black_host_connect_failures_within_time`

- デフォルト：5
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：ブラックリストに登録された BE ノードに許可される接続失敗のしきい値。BE ノードが自動的に BE ブラックリストに追加された場合、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` で設定されたしきい値よりも少ない接続失敗である場合にのみ、BE ブラックリストから削除できます。
- 導入時期：v3.3.0

### `black_host_history_sec`

- デフォルト：2 * 60
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：BE ブラックリストに登録された BE ノードの過去の接続失敗を保持する期間。BE ノードが自動的に BE ブラックリストに追加された場合、StarRocks はその接続性を評価し、BE ブラックリストから削除できるかどうかを判断します。`black_host_history_sec` 内で、ブラックリストに登録された BE ノードが `black_host_connect_failures_within_time` で設定されたしきい値よりも少ない接続失敗である場合にのみ、BE ブラックリストから削除できます。
- 導入時期：v3.3.0

### `brpc_connection_pool_size`

- デフォルト：16
- タイプ：Int
- 単位：Connections
- 変更可能：No
- 説明：FE の BrpcProxy がエンドポイントごとに使用する BRPC 接続プールの最大数。この値は `setMaxTotoal` および `setMaxIdleSize` を介して RpcClientOptions に適用されるため、各要求はプールから接続を借りる必要があるため、同時発信 BRPC 要求を直接制限します。高並行シナリオでは、要求キューイングを回避するためにこれを増やしてください。増やすとソケットとメモリの使用量が増加し、リモートサーバーの負荷が増加する可能性があります。調整する際には、`brpc_idle_wait_max_time`、`brpc_short_connection`、`brpc_inner_reuse_pool`、`brpc_reuse_addr`、および `brpc_min_evictable_idle_time_ms` などの関連設定を考慮してください。この値を変更することはホットリロード可能ではなく、再起動が必要です。
- 導入時期：v3.2.0

### `brpc_short_connection`

- デフォルト：false
- タイプ：boolean
- 単位：-
- 変更可能：No
- 説明：下層の brpc RpcClient が短命の接続を使用するかどうかを制御します。有効な場合 (`true`)、RpcClientOptions.setShortConnection が設定され、要求完了後に接続が閉じられ、接続設定のオーバーヘッドが増加し、レイテンシーが増加する代わりに、長命のソケットの数が減少します。無効な場合 (`false`、デフォルト)、永続的な接続と接続プールが使用されます。このオプションを有効にすると、接続プール動作に影響するため、`brpc_connection_pool_size`、`brpc_idle_wait_max_time`、`brpc_min_evictable_idle_time_ms`、`brpc_reuse_addr`、および `brpc_inner_reuse_pool` と合わせて検討する必要があります。一般的な高スループットデプロイメントでは無効のままにし、ソケットのライフタイムを制限する必要がある場合や、ネットワークポリシーによって短命の接続が要求される場合にのみ有効にします。
- 導入時期：v3.3.11, v3.4.1, v3.5.0

### `catalog_try_lock_timeout_ms`

- デフォルト：5000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：Yes
- 説明：グローバルロックを取得するためのタイムアウト期間。
- 導入時期：-

### `checkpoint_only_on_leader`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：`true` の場合、CheckpointController はリーダー FE のみをチェックポイントワーカーとして選択します。`false` の場合、コントローラーは任意のフロントエンドを選択し、ヒープ使用量が少ないノードを優先します。`false` の場合、ワーカーは最近の失敗時間と `heapUsedPercent` でソートされます (リーダーは無限のヒープ使用量を持つものとして扱われ、選択されないようにします)。クラスターのスナップショットメタデータを必要とする操作の場合、コントローラーはこのフラグに関係なくリーダーの選択を強制します。`true` を有効にすると、チェックポイント作業がリーダーに集中します (単純ですが、リーダーの CPU/メモリとネットワーク負荷が増加します)。`false` のままにすると、負荷の少ない FE にチェックポイント負荷が分散されます。この設定は、ワーカーの選択と、`checkpoint_timeout_seconds` などのタイムアウトや `thrift_rpc_timeout_ms` などの RPC 設定との相互作用に影響します。
- 導入時期：v3.4.0, v3.5.0

### `checkpoint_timeout_seconds`

- デフォルト：24 * 3600
- タイプ：Long
- 単位：Seconds
- 変更可能：Yes
- 説明：リーダーの CheckpointController がチェックポイントワーカーがチェックポイントを完了するのを待つ最大時間 (秒単位)。コントローラーはこの値をナノ秒に変換し、ワーカーの結果キューをポーリングします。このタイムアウト内に成功した完了が受信されない場合、チェックポイントは失敗と見なされ、createImage は失敗を返します。この値を増やすと、長時間実行されるチェックポイントに対応できますが、失敗検出と後続のイメージ伝播が遅れます。値を減らすと、より高速なフェイルオーバー/再試行が発生しますが、低速なワーカーに対して誤ったタイムアウトが発生する可能性があります。この設定は、チェックポイント作成中の `CheckpointController` での待機期間のみを制御し、ワーカーの内部チェックポイント動作は変更しません。
- 導入時期：v3.4.0, v3.5.0

### `db_used_data_quota_update_interval_secs`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：データベース使用データクォータが更新される間隔。StarRocks は、ストレージ消費量を追跡するために、すべてのデータベースの使用データクォータを定期的に更新します。この値はクォータ強制とメトリック収集に使用されます。過剰なシステム負荷を防ぐため、許容される最小間隔は 30 秒です。30 未満の値は拒否されます。
- 導入時期：-

### `drop_backend_after_decommission`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：BE が停止解除された後に BE を削除するかどうか。`TRUE` は、BE が停止解除された直後に削除されることを示します。`FALSE` は、BE が停止解除された後も削除されないことを示します。
- 導入時期：-

### `edit_log_port`

- デフォルト：9010
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：クラスター内のリーダー、フォロワー、オブザーバー FE 間で通信に使用されるポート。
- 導入時期：-

### `edit_log_roll_num`

- デフォルト：50000
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：メタデータログエントリの最大数。この数に達すると、これらのログエントリ用のログファイルが作成されます。このパラメーターは、ログファイルのサイズを制御するために使用されます。新しいログファイルは BDBJE データベースに書き込まれます。
- 導入時期：-

### `edit_log_type`

- デフォルト：BDB
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：生成できる編集ログの種類。値を `BDB` に設定します。
- 導入時期：-

### `enable_background_refresh_connector_metadata`

- デフォルト：true in v3.0 and later and false in v2.5
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：定期的な Hive メタデータキャッシュ更新を有効にするかどうか。有効にすると、StarRocks は Hive クラスターのメタストア (Hive Metastore または AWS Glue) をポーリングし、頻繁にアクセスされる Hive カタログのキャッシュされたメタデータを更新してデータ変更を認識します。`true` は Hive メタデータキャッシュ更新を有効にすることを示し、`false` は無効にすることを示します。
- 導入時期：v2.5.5

### `refresh_other_fe_dispatch_executor_thread_num`

- デフォルト：4
- タイプ：Integer
- 単位：-
- 変更可能：Yes
- 説明：Connector の書き込みパスから実行される非同期の "refresh other FE" バックグラウンドジョブをスケジュールする、FE グローバルのディスパッチ実行プール内のスレッド数です。これらのスレッドはバックグラウンド更新タスクを起動するだけで、他の FE に対して更新 RPC を直接送信しません。変更は再起動なしで実行中の FE に反映されます。
- 導入時期：-

### `refresh_other_fe_rpc_executor_thread_num`

- デフォルト：4
- タイプ：Integer
- 単位：-
- 変更可能：Yes
- 説明： "refresh other FE" の fan-out に使用される、FE グローバルの RPC 実行プール内のスレッド数です。この実行プールにより、同期および非同期の外部テーブル更新フローで他の FE に同時送信される更新 RPC の数が制限されます。変更は再起動なしで実行中の FE に反映されます。
- 導入時期：-

### `enable_collect_query_detail_info`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：クエリのプロファイルを収集するかどうか。このパラメーターが `TRUE` に設定されている場合、システムはクエリのプロファイルを収集します。このパラメーターが `FALSE` に設定されている場合、システムはクエリのプロファイルを収集しません。
- 導入時期：-

### `enable_create_partial_partition_in_batch`

- デフォルト：false
- タイプ：boolean
- 単位：-
- 変更可能：Yes
- 説明：この項目が `false` (デフォルト) に設定されている場合、StarRocks は、バッチ作成された範囲パーティションが標準の時刻単位境界に揃うことを強制します。穴の作成を避けるために、非整列の範囲は拒否されます。この項目を `true` に設定すると、そのアラインメントチェックが無効になり、バッチで部分的な (非標準の) パーティションを作成できるようになります。これにより、ギャップや誤ったパーティション範囲が生成される可能性があります。意図的に部分的なバッチパーティションが必要であり、関連するリスクを受け入れる場合にのみ `true` に設定する必要があります。
- 導入時期：v3.2.0

### `enable_internal_sql`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：この項目が `true` に設定されている場合、内部コンポーネント (例: SimpleExecutor) によって実行される内部 SQL ステートメントは、内部監査またはログメッセージに保持および書き込まれます ( `enable_sql_desensitize_in_log` が設定されている場合、さらに非機密化できます)。`false` に設定されている場合、内部 SQL テキストは抑制されます。フォーマットコード (SimpleExecutor.formatSQL) は "?" を返し、実際のステートメントは内部監査またはログメッセージに出力されません。この設定は、内部ステートメントの実行セマンティクスを変更しません。プライバシーまたはセキュリティのために内部 SQL のロギングと可視性のみを制御します。
- 導入時期：-

### `enable_legacy_compatibility_for_replication`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：レプリケーションのレガシー互換性を有効にするかどうか。StarRocks は、以前のバージョンと新しいバージョンで異なる動作をする可能性があり、クロスクラスターデータ移行中に問題を引き起こすことがあります。したがって、データ移行前にターゲットクラスターでレガシー互換性を有効にし、データ移行完了後に無効にする必要があります。`true` はこのモードを有効にすることを示します。
- 導入時期：v3.1.10, v3.2.6

### `enable_show_materialized_views_include_all_task_runs`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：SHOW MATERIALIZED VIEWS コマンドに TaskRuns が返される方法を制御します。この項目が `false` に設定されている場合、StarRocks はタスクごとに最新の TaskRun のみを返します (互換性のためのレガシー動作)。`true` (デフォルト) に設定されている場合、`TaskManager` は、同じ開始 TaskRun ID を共有する場合 (たとえば、同じジョブに属する場合) にのみ、同じタスクの追加の TaskRun を含めることができ、無関係な重複実行が表示されるのを防ぎながら、1 つのジョブに紐付けられた複数のステータスを表示できます。単一実行の出力を復元したり、デバッグと監視のために複数実行ジョブの履歴を表示したりするには、この項目を `false` に設定します。
- 導入時期：v3.3.0, v3.4.0, v3.5.0

### `enable_statistics_collect_profile`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：統計クエリのプロファイルを生成するかどうか。この項目を `true` に設定すると、StarRocks がシステム統計に関するクエリのクエリプロファイルを生成できるようになります。
- 導入時期：v3.1.5

### `enable_table_name_case_insensitive`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：カタログ名、データベース名、テーブル名、ビュー名、マテリアライズドビュー名の大文字小文字を区別しない処理を有効にするかどうか。現在、テーブル名は大文字小文字を区別します。
  - この機能を有効にすると、関連するすべての名前は小文字で格納され、これらの名前を含むすべての SQL コマンドは自動的に小文字に変換されます。
  - この機能は、クラスター作成時にのみ有効にできます。**クラスター起動後、この設定の値をいかなる方法でも変更することはできません**。変更しようとするとエラーが発生します。FE は、この設定アイテムの値がクラスターが最初に起動されたときと一致しないことを検出すると、起動に失敗します。
  - 現在、この機能は JDBC カタログおよびテーブル名をサポートしていません。JDBC または ODBC データソースで大文字小文字を区別しない処理を実行する場合は、この機能を有効にしないでください。
- 導入時期：v4.0

### `enable_task_history_archive`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、完了したタスク実行レコードは永続的なタスク実行履歴テーブルにアーカイブされ、編集ログに記録されるため、ルックアップ (例: `lookupHistory`、`lookupHistoryByTaskNames`、`lookupLastJobOfTasks`) にアーカイブされた結果が含まれます。アーカイブは FE リーダーによって実行され、単体テスト中 (`FeConstants.runningUnitTest`) はスキップされます。有効にすると、インメモリの有効期限と強制 GC パスはバイパスされ (コードは `removeExpiredRuns` と `forceGC` から早期にリターンします)、保持/削除は `task_runs_ttl_second` と `task_runs_max_history_number` ではなく永続アーカイブによって処理されます。無効にすると、履歴はメモリ内に残り、これらの設定によってプルーニングされます。
- 導入時期：v3.3.1, v3.4.0, v3.5.0

### `enable_task_run_fe_evaluation`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：有効にすると、FE は `TaskRunsSystemTable.supportFeEvaluation` でシステムテーブル `task_runs` のローカル評価を実行します。FE 側の評価は、列と定数を比較する結合等価述語にのみ許可され、列 `QUERY_ID` と `TASK_NAME` に制限されます。これを有効にすると、対象を絞ったルックアップのパフォーマンスが向上し、より広範なスキャンや追加のリモート処理が回避されます。無効にすると、プランナーは `task_runs` の FE 評価をスキップするため、述語のプルーニングが減少し、これらのフィルターのクエリレイテンシーに影響する可能性があります。
- 導入時期：v3.3.13, v3.4.3, v3.5.0

### `heartbeat_mgr_blocking_queue_size`

- デフォルト：1024
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：Heartbeat Manager によって実行されるハートビートタスクを格納するブロッキングキューのサイズ。
- 導入時期：-

### `heartbeat_mgr_threads_num`

- デフォルト：8
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：Heartbeat Manager がハートビートタスクを実行するために実行できるスレッド数。
- 導入時期：-

### `ignore_materialized_view_error`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：FE がマテリアライズドビューエラーによって引き起こされたメタデータ例外を無視するかどうか。マテリアライズドビューエラーによって引き起こされたメタデータ例外のために FE の起動に失敗した場合、このパラメーターを `true` に設定して FE が例外を無視できるようにすることができます。
- 導入時期：v2.5.10

### `ignore_meta_check`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：非リーダー FE がリーダー FE からのメタデータギャップを無視するかどうか。値が TRUE の場合、非リーダー FE はリーダー FE からのメタデータギャップを無視し、データ読み取りサービスを継続して提供します。このパラメーターは、リーダー FE を長期間停止した場合でも継続的なデータ読み取りサービスを保証します。値が FALSE の場合、非リーダー FE はリーダー FE からのメタデータギャップを無視せず、データ読み取りサービスの提供を停止します。
- 導入時期：-

### `ignore_task_run_history_replay_error`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：StarRocks が `information_schema.task_runs` の TaskRun 履歴行を逆シリアル化する際、破損または無効な JSON 行は通常、逆シリアル化が警告をログに記録し、RuntimeException をスローします。この項目が `true` に設定されている場合、システムは逆シリアル化エラーをキャッチし、不正な形式のレコードをスキップし、クエリを失敗させるのではなく、残りの行の処理を続行します。これにより、`information_schema.task_runs` クエリは `_statistics_.task_run_history` テーブル内の不正なエントリに対して寛容になります。ただし、これを有効にすると、破損した履歴レコードが明示的なエラーを表面化する代わりにサイレントにドロップされることになります (潜在的なデータ損失)。
- 導入時期：v3.3.3, v3.4.0, v3.5.0

### `lock_checker_interval_second`

- デフォルト：30
- タイプ：long
- 単位：Seconds
- 変更可能：Yes
- 説明：LockChecker フロントエンドデーモン ("deadlock-checker" という名前) の実行間の間隔 (秒単位)。デーモンはデッドロック検出と低速ロックのスキャンを実行します。設定値はミリ秒でタイマーを設定するために 1000 倍されます。この値を減らすと検出レイテンシーが短くなりますが、スケジューリングと CPU オーバーヘッドが増加します。増やすとオーバーヘッドが減少しますが、検出と低速ロックレポートが遅れます。変更は、デーモンが実行ごとに間隔をリセットするため、実行時に有効になります。この設定は、`lock_checker_enable_deadlock_check` (デッドロックチェックを有効にする) と `slow_lock_threshold_ms` (低速ロックを構成するものを定義する) と相互作用します。
- 導入時期：v3.2.0

### `master_sync_policy`

- デフォルト：SYNC
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：リーダー FE がログをディスクにフラッシュするポリシー。このパラメーターは、現在の FE がリーダー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされると、ログエントリの生成とフラッシュは同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。

  フォロワー FE を 1 つだけデプロイしている場合、このパラメーターを `SYNC` に設定することをお勧めします。フォロワー FE を 3 つ以上デプロイしている場合、このパラメーターと `replica_sync_policy` の両方を `WRITE_NO_SYNC` に設定することをお勧めします。

- 導入時期：-

### `max_bdbje_clock_delta_ms`

- デフォルト：5000
- タイプ：Long
- 単位：Milliseconds
- 変更可能：No
- 説明：StarRocks クラスター内のリーダー FE とフォロワーまたはオブザーバー FE 間で許可される最大クロックオフセット。
- 導入時期：-

### `meta_delay_toleration_second`

- デフォルト：300
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：フォロワー FE およびオブザーバー FE のメタデータがリーダー FE のメタデータよりも遅延できる最大期間。単位: 秒。この期間を超えると、非リーダー FE はサービスの提供を停止します。
- 導入時期：-

### `meta_dir`

- デフォルト：`StarRocksFE.STARROCKS_HOME_DIR` + "/meta"
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：メタデータを格納するディレクトリ。
- 導入時期：-

### `metadata_ignore_unknown_operation_type`

- デフォルト：false
- タイプ：Boolean
- 単位：-
- 変更可能：Yes
- 説明：未知のログ ID を無視するかどうか。FE がロールバックされた場合、以前のバージョンの FE は一部のログ ID を認識できない可能性があります。値が `TRUE` の場合、FE は未知のログ ID を無視します。値が `FALSE` の場合、FE は終了します。
- 導入時期：-

### `profile_info_format`

- デフォルト：default
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：システムが出力するプロファイルの形式。有効な値: `default` と `json`。`default` に設定すると、プロファイルはデフォルトの形式になります。`json` に設定すると、システムはプロファイルを JSON 形式で出力します。
- 導入時期：v2.5

### `replica_ack_policy`

- デフォルト：`SIMPLE_MAJORITY`
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：ログエントリが有効であると見なされるポリシー。デフォルト値 `SIMPLE_MAJORITY` は、ログエントリがフォロワー FE の過半数が ACK メッセージを返した場合に有効であると見なされることを指定します。
- 導入時期：-

### `replica_sync_policy`

- デフォルト：SYNC
- タイプ：String
- 単位：-
- 変更可能：No
- 説明：フォロワー FE がログをディスクにフラッシュするポリシー。このパラメーターは、現在の FE がフォロワー FE の場合にのみ有効です。有効な値:
  - `SYNC`: トランザクションがコミットされると、ログエントリが生成され、同時にディスクにフラッシュされます。
  - `NO_SYNC`: トランザクションがコミットされると、ログエントリの生成とフラッシュは同時に発生しません。
  - `WRITE_NO_SYNC`: トランザクションがコミットされると、ログエントリが同時に生成されますが、ディスクにはフラッシュされません。
- 導入時期：-

### `start_with_incomplete_meta`

- デフォルト：false
- タイプ：boolean
- 単位：-
- 変更可能：No
- 説明：true の場合、FE はイメージデータが存在するが Berkeley DB JE (BDB) ログファイルが欠落または破損している場合に起動を許可します。`MetaHelper.checkMetaDir()` はこのフラグを使用して、対応する BDB ログのないイメージからの起動を防ぐ安全チェックをバイパスします。この方法で起動すると、古いまたは矛盾したメタデータが生成される可能性があり、緊急復旧にのみ使用する必要があります。`RestoreClusterSnapshotMgr` はクラスターのスナップショットを復元する際に一時的にこのフラグを true に設定し、その後ロールバックします。このコンポーネントは復元中に `bdbje_reset_election_group` も切り替えます。通常の操作では有効にしないでください。破損した BDB データから復旧する場合、またはイメージベースのスナップショットを明示的に復元する場合にのみ有効にしてください。
- 導入時期：v3.2.0

### `table_keeper_interval_second`

- デフォルト：30
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：TableKeeper デーモンの実行間の間隔 (秒単位)。TableKeeperDaemon はこの値 (1000 倍) を使用して内部タイマーを設定し、履歴テーブルが存在すること、正しいテーブルプロパティ (レプリケーション番号) があること、パーティション TTL を更新することを保証するキーパータスクを定期的に実行します。デーモンはリーダーノードでのみ作業を実行し、`table_keeper_interval_second` が変更されると `setInterval` を介して実行時間隔を更新します。スケジューリング頻度と負荷を減らすには値を増やし、欠落または古い履歴テーブルへの反応を速くするには値を減らします。
- 導入時期：v3.3.1, v3.4.0, v3.5.0

### `task_runs_ttl_second`

- デフォルト：7 * 24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：タスク実行履歴の Time-To-Live (TTL) を制御します。この値を小さくすると履歴の保持期間が短くなり、メモリ/ディスク使用量が減ります。値を大きくすると履歴が長く保持されますが、リソース使用量が増加します。予測可能な保持とストレージ動作のために、`task_runs_max_history_number` と `enable_task_history_archive` と一緒に調整してください。
- 導入時期：v3.2.0

### `task_ttl_second`

- デフォルト：24 * 3600
- タイプ：Int
- 単位：Seconds
- 変更可能：Yes
- 説明：タスクの Time-to-live (TTL)。手動タスク (スケジュールが設定されていない場合)、TaskBuilder はこの値を使用してタスクの `expireTime` を計算します (`expireTime = now + task_ttl_second * 1000L`)。TaskRun も、実行のタイムアウトを計算する際にこの値を上限として使用します。実質的な実行タイムアウトは `min(task_runs_timeout_second, task_runs_ttl_second, task_ttl_second)` です。この値を調整すると、手動で作成されたタスクが有効な期間が変更され、タスク実行の最大許容実行時間を間接的に制限できます。
- 導入時期：v3.2.0

### `thrift_rpc_retry_times`

- デフォルト：3
- タイプ：Int
- 単位：-
- 変更可能：Yes
- 説明：Thrift RPC 呼び出しが行う合計試行回数を制御します。この値は `ThriftRPCRequestExecutor` (および `NodeMgr` や `VariableMgr` などの呼び出し元) によって再試行のループカウントとして使用されます。つまり、値 3 は初期試行を含めて最大 3 回の試行を許可します。`TTransportException` の場合、エグゼキューターは接続を再開してこの回数まで再試行します。原因が `SocketTimeoutException` の場合や再開が失敗した場合は再試行しません。各試行は `thrift_rpc_timeout_ms` で設定された試行ごとのタイムアウトの対象となります。この値を増やすと、一時的な接続障害に対する回復力は向上しますが、全体の RPC レイテンシーとリソース使用量が増加する可能性があります。
- 導入時期：v3.2.0

### `thrift_rpc_strict_mode`

- デフォルト：true
- タイプ：Boolean
- 単位：-
- 変更可能：No
- 説明：Thrift サーバーで使用される TBinaryProtocol の「厳密読み取り」モードを制御します。この値は Thrift サーバーのスタックにある org.apache.thrift.protocol.TBinaryProtocol.Factory に最初の引数として渡され、受信 Thrift メッセージの解析と検証方法に影響します。`true` (デフォルト) の場合、サーバーは厳密な Thrift エンコーディング/バージョンチェックを強制し、設定された `thrift_rpc_max_body_size` 制限を尊重します。`false` の場合、サーバーは非厳密な (レガシー/寛容な) メッセージ形式を受け入れます。これにより、古いクライアントとの互換性は向上する可能性がありますが、一部のプロトコル検証がバイパスされる可能性があります。これは変更不可能であり、相互運用性と解析の安全性に影響するため、稼働中のクラスターでこれを変更する場合は注意してください。
- 導入時期：v3.2.0

### `thrift_rpc_timeout_ms`

- デフォルト：10000
- タイプ：Int
- 単位：Milliseconds
- 変更可能：Yes
- 説明：Thrift RPC 呼び出しのデフォルトのネットワーク/ソケットタイムアウトとして使用されるタイムアウト (ミリ秒単位)。`ThriftConnectionPool` (フロントエンドおよびバックエンドプールで使用) で Thrift クライアントを作成するときに TSocket に渡され、また `ConfigBase`、`LeaderOpExecutor`、`GlobalStateMgr`、`NodeMgr`、`VariableMgr`、`CheckpointWorker` などの場所で RPC 呼び出しのタイムアウトを計算するときに操作の実行タイムアウトに追加されます (例: ExecTimeout*1000 + `thrift_rpc_timeout_ms`)。この値を増やすと、RPC 呼び出しは長いネットワークまたはリモート処理の遅延を許容できます。減らすと、低速なネットワークでのフェイルオーバーが高速化されます。この値を変更すると、Thrift RPC を実行する FE コードパス全体で接続作成と要求の期限に影響します。
- 導入時期：v3.2.0

### `txn_latency_metric_report_groups`

- デフォルト：An empty string
- タイプ：String
- 単位：-
- 変更可能：Yes
- 説明：レポートするトランザクションレイテンシーメトリックグループのコンマ区切りリスト。ロードタイプは監視のために論理グループに分類されます。グループが有効になっている場合、その名前はトランザクションメトリックに「type」ラベルとして追加されます。有効な値: `stream_load`、`routine_load`、`broker_load`、`insert`、および `compaction` (共有データクラスターでのみ利用可能)。例: `"stream_load,routine_load"`。
- 導入時期：v4.0

### `txn_rollback_limit`

- デフォルト：100
- タイプ：Int
- 単位：-
- 変更可能：No
- 説明：ロールバックできるトランザクションの最大数。
- 導入時期：-
