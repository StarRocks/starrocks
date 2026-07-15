---
displayed_sidebar: docs
sidebar_label: "セッション・プロトコル・ガバナンス"
sidebar_position: 8
description: "MySQL プロトコル互換、クエリキュー、タイムアウト、リソースグループ、プロファイリングに関するセッション変数。"
---

# システム変数 - セッション・プロトコル・ガバナンス

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### activate_all_roles_on_login (global)

* **説明**: StarRocks ユーザーが StarRocks クラスタに接続する際に、すべてのロール（デフォルトロールと付与されたロールを含む）を有効にするかどうか。
  * 有効にすると（`true`）、ユーザーのすべてのロールがログイン時にアクティブになります。これは [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) で設定されたロールよりも優先されます。
  * 無効にすると（`false`）、SET DEFAULT ROLE で設定されたロールがアクティブになります。
* **デフォルト**: false
* **導入バージョン**: v3.0

セッションで割り当てられたロールをアクティブにしたい場合は、[SET ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) コマンドを使用してください。

### auto_increment_increment

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### binary_encoding_format

* **スコープ**: Session
* **説明**: StarRocks が MySQL テキスト結果で `BINARY` / `VARBINARY` の値をどのようにエンコードするかを制御します。指定可能な値は `raw`、`hex`、`base64` で、デフォルトは `hex` です。この変数は `binary_encoding_level` と組み合わせて動作します。トップレベルのバイナリ値は MySQL クライアントがそのまま扱える場合がありますが、`ARRAY`、`MAP`、`STRUCT` などのネストした型の中にあるバイナリ値は JSON ライクな文字列として返されるため、表示可能で整った形式を保つには追加のエンコードが必要になることがあります。よりコンパクトな表現が必要な場合は `base64` を、追加エンコードを無効にしたい場合は `raw` を使用します。
* **デフォルト**: `hex`
* **データ型**: String
* **導入バージョン**: v4.1

### binary_encoding_level

* **スコープ**: Session
* **説明**: MySQL テキスト結果でどのバイナリ値をエンコードするかを制御します。指定可能な値は `nested` と `all` で、デフォルトは `nested` です。`nested` は既存互換性のための設定で、`ARRAY`、`MAP`、`STRUCT` などのネストした型の中にあるバイナリ値のみをエンコードし、トップレベルのバイナリ列の挙動は従来のまま維持します。すべてのバイナリ出力を統一した形式にしたい場合は `all` を使用すると、トップレベルのバイナリ値もエンコードされます。`binary_encoding_format = raw` の場合は、ここで `nested` または `all` を指定していても追加のバイナリエンコードは行われないため、ネスト結果の可読性が下がることがあります。
* **デフォルト**: `nested`
* **データ型**: String
* **導入バージョン**: v4.1

### big_query_profile_threshold

* **説明**: 大規模なクエリのしきい値を設定するために使用されます。セッション変数 `enable_profile` が `false` に設定されており、クエリの実行時間が変数 `big_query_profile_threshold` で指定されたしきい値を超えた場合、そのクエリのプロファイルが生成されます。

  注: v3.1.5 から v3.1.7、および v3.2.0 から v3.2.2 では、大規模なクエリのしきい値を設定するために `big_query_profile_second_threshold` を導入しました。v3.1.8、v3.2.3、およびそれ以降のリリースでは、このパラメータは `big_query_profile_threshold` に置き換えられ、より柔軟な設定オプションを提供します。
* **デフォルト**: 0
* **単位**: 秒
* **データ型**: 文字列
* **導入バージョン**: v3.1

### catalog

* **説明**: セッションが属するカタログを指定するために使用されます。
* **デフォルト**: default_catalog
* **データ型**: 文字列
* **導入バージョン**: v3.2.4

### character_set_database (global)

* **データ型**: 文字列 StarRocks がサポートする文字セット。UTF8 (`utf8`) のみがサポートされています。
* **デフォルト**: utf8
* **データ型**: 文字列

### collation_server

* **スコープ**: Session
* **説明**: FE がこのセッションに対して MySQL 互換の照合順序動作を提示するために使用するセッションレベルのサーバ照合名。この変数は、FE がクライアントに報告し、`character_set_server` / `collation_connection` / `collation_database` に関連付けられるデフォルトの照合識別子（例: `utf8_general_ci`）を設定します。セッション変数 JSON に永続化されます（SessionVariable#getJsonString / replayFromJson を参照）および変数マネージャーを通じて公開されます（`@VarAttr(name = COLLATION_SERVER)`）、したがって SHOW VARIABLES に表示されセッションごとに変更可能です。値は SessionVariable にプレーンな String として保存され、通常は標準的な MySQL の照合名（例: `utf8_general_ci`, `utf8mb4_unicode_ci`）を保持します。ここでコードは固定列挙型を強制したり追加の検証を行ったりしないため、比較やソートなど照合に敏感な操作の実際の動作は照合名を解釈する下流コンポーネントに依存します。
* **デフォルト**: `utf8_general_ci`
* **データ型**: String
* **導入バージョン**: `v3.2.0`

### custom_query_id (session)

* **説明**: 現在のクエリに外部識別子をバインドするために使用されます。クエリ実行前に `SET SESSION custom_query_id = 'my-query-id';` のように設定できます。クエリ終了後に値はリセットされます。この値は `KILL QUERY 'my-query-id'` に渡すことができます。値は監査ログの `customQueryId` フィールドで確認できます。
* **デフォルト**: ""
* **データタイプ**: String
* **導入バージョン**: v3.4.0

### default_authentication_plugin

* **スコープ**: Session
* **説明**: このセッションのデフォルトの MySQL 認証プラグイン名を指定するセッションスコープの変数です。SessionVariable.defaultAuthenticationPlugin として格納され、サーバーがデフォルトの認証プラグインを告知または使用する必要がある場合（ハンドシェイク時やプラグインが指定されていない場合など）に、StarRocks の MySQL プロトコル互換レイヤーで使用されます。サーバーがサポートする標準的な MySQL 認証プラグイン識別子（例: `mysql_native_password`, `caching_sha2_password`）を受け付けます。この変数はセッション動作にのみ影響し、永続的なユーザーアカウントの認証構成は別途管理されます。関連するセッション変数 `authentication_policy` を参照してください。
* **デフォルト**: `mysql_native_password`
* **データタイプ**: String
* **導入バージョン**: -

### default_rowset_type (global)

コンピューティングノードのストレージエンジンで使用されるデフォルトのストレージ形式を設定するために使用されます。現在サポートされているストレージ形式は `alpha` と `beta` です。

### default_table_compression

* **説明**: テーブルストレージのデフォルト圧縮アルゴリズム。サポートされている圧縮アルゴリズムは `snappy, lz4, zlib, zstd` です。

  CREATE TABLE 文で `compression` プロパティを指定した場合、`compression` で指定された圧縮アルゴリズムが有効になります。

* **デフォルト**: lz4_frame
* **導入バージョン**: v3.0

### default_tmp_storage_engine

* **説明**: セッション変数で、テンポラリテーブル（明示的な `CREATE TEMPORARY TABLE` およびエンジンによって作成される内部/暗黙のテンポラリテーブル）のデフォルトのストレージエンジンを制御します。`SessionVariable.java` に `@VariableMgr.VarAttr` アノテーションで宣言されており、主に MySQL 8.0 互換性のために存在し、MySQL ライクな動作を期待するクライアントやツールがセッションごとに一時テーブルのエンジンを確認または変更できるようにします。この値を変更すると、メモリバックエンドとディスクバックエンドなど、異なるエンジンを尊重するストレージ層でのテンポラリテーブルデータの保存/管理方法に影響します。
* **スコープ**: Session
* **デフォルト**: `InnoDB`
* **データタイプ**: String
* **導入バージョン**: v3.4.2, v3.5.0

### default_view_sql_security

* **説明**: `CREATE VIEW` ステートメントで `SECURITY` 句が指定されていない場合に適用される、デフォルトの SQL SECURITY 特性です。`NONE`（明示的な `SECURITY NONE` 句と同等）はビューを参照する際に実行ユーザーがそのビュー自体に対する `SELECT` 権限を持っていればよく、ビューが参照するテーブルは実行ユーザーに対してチェックされないことを意味します。`INVOKER`（`SECURITY INVOKER` と同等）は実行ユーザーがビューの参照するテーブルに対する `SELECT` 権限も持っている必要があることを意味します。ステートメント内で明示的に指定された `SECURITY NONE` または `SECURITY INVOKER` 句は、常にこの変数より優先されます。この変数は `CREATE VIEW` にのみ影響し、`ALTER VIEW` には影響しません。
* **スコープ**: Session
* **デフォルト**: `NONE`
* **データタイプ**: String
* **有効な値**: `NONE`, `INVOKER`
* **導入バージョン**: v4.1.1

### div_precision_increment

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### enable_color_explain_output

* **スコープ**: Session
* **説明**: テキスト形式の EXPLAIN / PROFILE 出力に ANSI カラーエスケープシーケンスを含めるかどうかを制御します。有効（`true`）のとき、StmtExecutor はセッション設定を explain/profile パイプライン（ExplainAnalyzer への呼び出しを通じて）に渡すため、explain、EXPLAIN ANALYZE、analyze-profile 出力に ANSI 対応端末での可読性を高めるカラー強調表示が含まれます。無効（`false`）のときは ANSI シーケンスなし（プレーンテキスト）で出力され、ログや ANSI をサポートしないクライアント、または出力をファイルにパイプする場合に適しています。これはセッション単位のトグルであり、実行のセマンティクスを変更するものではなく、explain/profile テキストの表示方法のみを変更します。
* **デフォルト**: `true`
* **データタイプ**: boolean
* **導入バージョン**: v3.5.0

### enable_group_level_query_queue (global)

* **説明**: リソースグループレベルの[クエリキュー](../administration/management/resource_management/query_queues.md)を有効にするかどうか。
* **デフォルト**: false、つまりこの機能は無効です。
* **導入バージョン**: v3.1.4

### max_unknown_string_meta_length (global)

* **説明**: 文字列列の最大長が不明な場合にメタデータで使用するフォールバック長。メタデータの長さが実際より小さいと、一部の BI ツールで空値や切り詰めが発生する可能性があります。`0` 以下は `64` にフォールバックします。有効範囲は `1` ～ `1048576`。
* **デフォルト**: 64
* **データ型**: Int
* **導入バージョン**: v3.5.16, v4.0.9

### enable_metadata_profile

* **説明**: Iceberg Catalog のメタデータ収集クエリに対して Profile を有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.3.3

### enable_profile

* **説明**: クエリのプロファイルを分析のために送信するかどうかを指定します。デフォルト値は `false` で、プロファイルは必要ありません。

  デフォルトでは、クエリエラーが BE で発生した場合にのみプロファイルが FE に送信されます。プロファイルの送信はネットワークオーバーヘッドを引き起こし、高い並行性に影響を与えます。

  クエリのプロファイルを分析する必要がある場合、この変数を `true` に設定できます。クエリが完了した後、現在接続されている FE のウェブページ（アドレス：`fe_host:fe_http_port/query`）でプロファイルを表示できます。このページには、`enable_profile` がオンになっている最新の 100 件のクエリのプロファイルが表示されます。

* **デフォルト**: false

### enable_explain_in_profile

* **スコープ**: Session
* **説明**: この変数が `true` で、かつクエリに対してプロファイルが生成される場合、実行された計画の `EXPLAIN COSTS` テキストがプロファイルの `Summary` セクション内に `ExplainPlan` というキーで埋め込まれます。これにより、稼働中のクラスターにアクセスできない状態で保存済みのプロファイル成果物だけを使ってスロークエリを切り分ける際に、ランタイムメトリクスと並べてオプティマイザーの基数推定、列統計、述語、ランタイムフィルター宣言、および全体の計画コストを確認できます。

  プロファイルに埋め込まれる計画は、他の永続化される SQL 成果物と同じ非機密化制御に従います。`FILES(...)` などに含まれる資格情報リテラルは常に編集され、述語や射影のリテラルは、クラスター全体の FE 設定 `enable_sql_desensitize_in_log` またはセッション変数 `enable_desensitize_explain` のいずれかが有効な場合にダイジェスト形式で表示されます。
* **デフォルト**: false
* **データ型**: boolean

### profile_log_latency_threshold_ms

* **スコープ**: Session
* **説明**: FE が `fe.profile.log` にプロファイルを書き込むための最小クエリレイテンシ（ミリ秒）。実行時間がこの値以上の場合にのみプロファイルを記録します。`-1`（デフォルト）に設定すると、FE 設定の `profile_log_latency_threshold_ms` が使用されます。`0` に設定するとすべてのプロファイルを記録します。正の値（例: `1000`）に設定すると、レイテンシがその値（ミリ秒）以上のクエリのみを記録します。このセッション変数で接続ごとにクラスタ全体の設定を上書きできます。
* **デフォルト**: -1
* **データ型**: long
* **単位**: ミリ秒

### enable_query_queue_load (global)

* **説明**: ロードタスクのクエリキューを有効にするためのブール値。
* **デフォルト**: false

### enable_query_queue_select (global)

* **説明**: SELECT クエリのクエリキューを有効にするかどうか。
* **デフォルト**: false

### enable_query_queue_statistic (global)

* **説明**: 統計クエリのクエリキューを有効にするかどうか。
* **デフォルト**: false

### enable_strict_order_by

* **説明**: ORDER BY で参照される列名が曖昧であるかどうかをチェックするために使用されます。この変数がデフォルト値 `TRUE` に設定されている場合、次のようなクエリパターンに対してエラーが報告されます：クエリの異なる式で重複したエイリアスが使用され、このエイリアスが ORDER BY のソートフィールドでもある場合、例：`select distinct t1.* from tbl1 t1 order by t1.k1;`。このロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、そのようなクエリを有効な SQL クエリとして処理します。
* **デフォルト**: true
* **導入バージョン**: v2.5.18 および v3.1.7

### event_scheduler

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### forward_to_leader

一部のコマンドがリーダー FE に転送されて実行されるかどうかを指定するために使用されます。エイリアス: `forward_to_master`。デフォルト値は `false` で、リーダー FE に転送されません。StarRocks クラスタには複数の FE があり、そのうちの 1 つがリーダー FE です。通常、ユーザーは任意の FE に接続してフル機能の操作を行うことができます。ただし、一部の情報はリーダー FE にのみ存在します。

たとえば、SHOW BACKENDS コマンドがリーダー FE に転送されない場合、ノードが生存しているかどうかなどの基本情報のみを表示できます。リーダー FE に転送すると、ノードの起動時間や最終ハートビート時間などの詳細情報を取得できます。

この変数に影響を受けるコマンドは次のとおりです：

* SHOW FRONTENDS: リーダー FE に転送すると、ユーザーは最終ハートビートメッセージを表示できます。

* SHOW BACKENDS: リーダー FE に転送すると、ユーザーは起動時間、最終ハートビート情報、ディスク容量情報を表示できます。

* SHOW BROKER: リーダー FE に転送すると、ユーザーは起動時間と最終ハートビート情報を表示できます。

* SHOW TABLET

* ADMIN SHOW REPLICA DISTRIBUTION

* ADMIN SHOW REPLICA STATUS: リーダー FE に転送すると、ユーザーはリーダー FE のメタデータに保存されているタブレット情報を表示できます。通常、タブレット情報は異なる FE のメタデータで同じであるべきです。エラーが発生した場合、この方法を使用して現在の FE とリーダー FE のメタデータを比較できます。

* Show PROC: リーダー FE に転送すると、ユーザーはメタデータに保存されている PROC 情報を表示できます。これは主にメタデータの比較に使用されます。

### group_concat_max_len

* **説明**: [group_concat](sql-functions/string-functions/group_concat.md) 関数によって返される文字列の最大長。
* **デフォルト**: 1024
* **最小値**: 4
* **単位**: 文字
* **データ型**: Long

### historical_nodes_min_update_interval

* **説明**: ヒストリカルノード記録の2回の更新の間の最小間隔。クラスタのノードが短期間で頻繁に変更される場合（つまり、この変数で設定された値未満）、いくつかの中間状態は有効なヒストリカルノードスナップショットとして記録されません。ヒストリカルノードは、クラスタのスケーリング中に適切なキャッシュノードを選択するためのキャッシュ共有機能の主な基盤となります。
* **デフォルト**: 600
* **単位**: Seconds
* **導入バージョン**: v3.5.1

### init_connect (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### innodb_read_only

* **説明**: セッションレベルのフラグ（MySQL 互換）で、セッションの InnoDB 読み取り専用モードを示します。この変数は `SessionVariable.java` の Java フィールド `innodbReadOnly` として宣言およびセッションに保存され、`isInnodbReadOnly()` および `setInnodbReadOnly(boolean)` を介してアクセス可能です。SessionVariable クラスはフラグを保持するだけであり、実際の強制（InnoDB テーブルへの書き込み/DDL の防止やトランザクション動作の変更など）は、このセッションフラグを参照すべきトランザクション／ストレージ／認可レイヤー側で実装する必要があります。現在のセッション内で読み取り専用の意図を伝えるために、この変数を使用してください。
* **スコープ**: Session
* **デフォルト**: `true`
* **データ型**: boolean
* **導入バージョン**: v3.2.0

### interactive_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### language (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### license (global)

* **説明**: StarRocks のライセンスを表示します。
* **デフォルト**: Apache License 2.0

### lower_case_table_names (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。StarRocks のテーブル名は大文字小文字を区別します。

### max_allowed_packet

* **説明**: JDBC 接続プール C3P0 との互換性のために使用されます。この変数は、クライアントとサーバー間で送信できるパケットの最大サイズを指定します。
* **デフォルト**: 33554432 (32 MB)。クライアントが "PacketTooBigException" を報告する場合、この値を増やすことができます。
* **単位**: バイト
* **データ型**: Int

### net_buffer_length

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### net_read_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### net_write_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### performance_schema (global)

MySQL JDBC バージョン 8.0.16 以降との互換性のために使用されます。実際の用途はありません。

### pipeline_profile_level

* **説明**: クエリプロファイルのレベルを制御します。クエリプロファイルには通常、フラグメント、フラグメントインスタンス、パイプライン、パイプラインドライバー、オペレーターの 5 つのレイヤーがあります。異なるレベルはプロファイルの異なる詳細を提供します：

  * 0: StarRocks はプロファイルのメトリクスを結合し、いくつかのコアメトリクスのみを表示します。
  * 1: デフォルト値。StarRocks はプロファイルを簡略化し、プロファイルのメトリクスを結合してプロファイルレイヤーを減らします。
  * 2: StarRocks はプロファイルのすべてのレイヤーを保持します。このシナリオでは、特に SQL クエリが複雑な場合、プロファイルサイズが大きくなります。この値は推奨されません。

* **デフォルト**: 1
* **データ型**: Int

### query_queue_concurrency_limit (global)

* **説明**: BE 上の同時クエリの上限。`0` より大きい値に設定された場合にのみ有効です。`0` に設定すると、制限が課されないことを示します。
* **デフォルト**: 0
* **データ型**: Int

### query_queue_cpu_used_permille_limit (global)

* **説明**: BE 上の CPU 使用率の上限（CPU 使用率 * 1000）。`0` より大きい値に設定された場合にのみ有効です。`0` に設定すると、制限が課されないことを示します。
* **値の範囲**: [0, 1000]
* **デフォルト**: `0`

### query_queue_max_queued_queries (global)

* **説明**: キュー内のクエリの上限。このしきい値に達すると、受信クエリは拒否されます。`0` より大きい値に設定された場合にのみ有効です。`0` に設定すると、制限が課されないことを示します。
* **デフォルト**: `1024`

### query_queue_mem_used_pct_limit (global)

* **説明**: BE 上のメモリ使用率の上限。`0` より大きい値に設定された場合にのみ有効です。`0` に設定すると、制限が課されないことを示します。
* **値の範囲**: [0, 1]
* **デフォルト**: 0

### query_queue_pending_timeout_second (global)

* **説明**: キュー内の保留中のクエリの最大タイムアウト。このしきい値に達すると、対応するクエリは拒否されます。
* **デフォルト**: 300
* **単位**: 秒

### query_timeout

* **説明**: クエリのタイムアウトを「秒」で設定するために使用されます。この変数は、現在の接続のすべてのクエリ文に影響を与えます。デフォルト値は 300 秒です。v3.4.0以降では、`query_timeout`は INSERT を含む操作（例えば、UPDATE、DELETE、CTAS、マテリアライズドビューの更新、統計情報の収集、PIPE）には適用されません。
* **値の範囲**: [1, 259200]
* **デフォルト**: 300
* **データ型**: Int
* **単位**: 秒

### resource_group 

* **説明**: このセッションの指定されたリソースグループ
* **デフォルト**: ""
* **データタイプ**: String
* **導入バージョン**: 3.2.0

### runtime_profile_report_interval

* **説明**: ランタイムプロファイルが報告される時間間隔。
* **デフォルト**: 10
* **単位**: 秒
* **データ型**: Int
* **導入バージョン**: v3.1.0

### sql_dialect

* **説明**: 使用される SQL ダイアレクト。たとえば、`set sql_dialect = 'trino';` コマンドを実行して SQL ダイアレクトを Trino に設定すると、クエリで Trino 固有の SQL 構文と関数を使用できます。

  > **注意**
  >
  > StarRocks を Trino ダイアレクトで使用するように設定した後、クエリ内の識別子はデフォルトで大文字小文字を区別しません。したがって、データベースやテーブルの作成時に名前を小文字で指定する必要があります。データベースやテーブル名を大文字で指定すると、これらのデータベースやテーブルに対するクエリが失敗します。

* **データ型**: StarRocks
* **導入バージョン**: v3.0

### sql_mode

特定の SQL ダイアレクトに対応する SQL モードを指定するために使用されます。有効な値には以下が含まれます：

* `PIPES_AS_CONCAT`: パイプ記号 `|` を使用して文字列を連結します。例：`select 'hello ' || 'world'`。
* `ONLY_FULL_GROUP_BY`（デフォルト）: SELECT リストには GROUP BY 列または集計関数のみを含めることができます。
* `ALLOW_THROW_EXCEPTION`: 型変換が失敗した場合に NULL の代わりにエラーを返します。
* `FORBID_INVALID_DATE`: 無効な日付を禁止します。
* `MODE_DOUBLE_LITERAL`: 浮動小数点型を DECIMAL ではなく DOUBLE として解釈します。
* `SORT_NULLS_LAST`: ソート後に NULL 値を最後に配置します。
* `ERROR_IF_OVERFLOW`: 算術オーバーフローが発生した場合に NULL の代わりにエラーを返します。現在、このオプションは DECIMAL データ型にのみ対応しています。
* `GROUP_CONCAT_LEGACY`: v2.5 およびそれ以前の `group_concat` 構文を使用します。このオプションは v3.0.9 および v3.1.6 からサポートされています。
* `FORBID_INVALID_IMPLICIT_CAST`: プラン時に Trino 風の厳格な型チェックを有効にします。暗黙的に許可されるのは同一型ファミリ内の拡張（widening）変換のみです（例: `TINYINT`→`INT`→`BIGINT`→`DECIMAL`→`DOUBLE`、`DATE`→`DATETIME`）。`VARCHAR`/`CHAR` 間の暗黙的キャストは宣言された長さを問わず許可されます。型ファミリをまたぐキャスト（`string`↔`numeric`、`string`↔`date`、`numeric`↔`date`、`boolean` と他の型など）や数値の縮小（narrowing）キャスト（`BIGINT`→`INT`、`DOUBLE`→`FLOAT` など）はセマンティックエラーとして拒否されます。これらの変換が必要な場合は、明示的な `CAST` を使用してください。
* `STRUCT_CAST_BY_NAME`: STRUCT 型間のキャストにおいて、デフォルトの位置ベースのマッチングではなく、名前ベースのフィールドマッチングを有効にします。このモードが有効になっている場合、ソース構造体のフィールドは、宣言順序に関係なく、フィールド名（大文字小文字を区別しない）に基づいてターゲット構造体のフィールドと照合されます。ソース側には存在するがターゲット側には存在しないフィールドは無視され、ターゲット側には存在するがソース側には存在しないフィールドは NULL で埋められます。このモードは、FE型の解決（UNION ALL における共通のスーパータイプの計算やキャスト可能性のチェック）と、BEキャストの評価（CastStructExpr における実行時のフィールドの順序変更）の両方に影響します。これは、分岐間でフィールドの定義順序が異なるSTRUCT列に対して UNION ALL を実行する場合に特に有用です。

1 つの SQL モードのみを設定できます。例：

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

または、複数のモードを一度に設定することもできます。例：

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_safe_updates

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### sql_select_limit

* **説明**: クエリによって返される最大行数を制限するために使用されます。これにより、クエリが大量のデータを返すことによって発生するメモリ不足やネットワーク混雑などの問題を防ぐことができます。
* **デフォルト**: 無制限
* **データ型**: Long

### storage_engine

StarRocks がサポートするエンジンの種類：

* olap（デフォルト）: StarRocks システム所有のエンジン。
* mysql: MySQL 外部テーブル。
* broker: ブローカープログラムを介して外部テーブルにアクセス。
* elasticsearch または es: Elasticsearch 外部テーブル。
* hive: Hive 外部テーブル。
* iceberg: Iceberg 外部テーブル（v2.1 からサポート）。
* hudi: Hudi 外部テーブル（v2.2 からサポート）。
* jdbc: JDBC 互換データベースの外部テーブル（v2.3 からサポート）。

### system_time_zone

現在のシステムのタイムゾーンを表示するために使用されます。変更できません。

### time_zone

現在のセッションのタイムゾーンを設定するために使用されます。タイムゾーンは、特定の時間関数の結果に影響を与える可能性があります。

### version (global)

クライアントに返される MySQL サーバーバージョン。値は FE パラメータ `mysql_server_version` と同じです。

### version_comment (global)

StarRocks のバージョン。変更できません。

### wait_timeout

* **説明**: サーバーが非対話型接続でアクティビティを待機する秒数。この時間が経過してもクライアントが StarRocks と対話しない場合、StarRocks は接続を積極的に閉じます。
* **デフォルト**: 28800（8 時間）。
* **単位**: 秒
* **データ型**: Int
