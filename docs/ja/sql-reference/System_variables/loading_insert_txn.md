---
displayed_sidebar: docs
sidebar_label: "ロード・INSERT・トランザクション"
sidebar_position: 7
description: "データロード、INSERT の挙動、トランザクション処理に関するセッション変数。"
---

# システム変数 - ロード・INSERT・トランザクション

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### dynamic_overwrite

* **説明**: パーティションテーブルを使用した INSERT OVERWRITE の [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) セマンティクスを有効にするかどうか。有効な値:
  * `true`: Dynamic Overwrite を有効にします。
  * `false`: Dynamic Overwrite を無効にし、デフォルトのセマンティクスを使用します。
* **デフォルト**: false
* **導入バージョン**: v3.4.0

### enable_insert_partial_update

* **説明**：主キーテーブルに対するINSERTステートメントの部分更新を有効化するかどうか。この項目が `true`（デフォルト）に設定されている場合、INSERT ステートメントで指定された列がサブセット（テーブル内のすべての非生成列の数より少ない）であるとき、システムは部分更新を実行し、指定された列のみを更新しながら他の列の既存値を保持します。`false` に設定すると、システムは既存の値を保持する代わりに、指定されていない列に対してデフォルト値を使用します。この機能は、主キーテーブルの特定の列を更新する際に他の列の値に影響を与えない場合に特に有用です。
* **デフォルト値**：true
* **導入バージョン**：v3.3.20、v3.4.9、v3.5.8、v4.0.2

### enable_insert_strict

* **説明**: Files() からの INSERT を使用してデータをロードする際に厳密モードを有効にするかどうか。有効な値: `true` および `false`（デフォルト）。厳密モードが有効な場合、システムは資格のある行のみをロードします。不適格な行をフィルタリングし、不適格な行の詳細を返します。詳細は [Strict mode](../loading/load_concept/strict_mode.md) を参照してください。v3.4.0 より前のバージョンでは、`enable_insert_strict` が `true` に設定されている場合、不適格な行があると INSERT ジョブが失敗します。
* **デフォルト**: true

### enable_load_profile

* **スコープ**: Session
* **説明**: 有効にすると、FE はロードジョブのランタイムプロファイルの収集を要求し、ロード完了後にロードコーディネータがプロファイルを収集/エクスポートします。ストリームロードの場合、FE は `TQueryOptions.enable_profile = true` を設定し、`stream_load_profile_collect_threshold_second` からの `load_profile_collect_second` をバックエンドに渡します。コーディネータは条件に応じてプロファイル収集を呼び出します（StreamLoadTask.collectProfile() を参照）。実際の振る舞いは、このセッション変数と宛先テーブルのテーブルレベルプロパティ `enable_load_profile` の論理 OR です。収集はさらに `load_profile_collect_interval_second`（FE 側のサンプリング間隔）によって制御され、頻繁な収集を回避します。セッションフラグは `SessionVariable.isEnableLoadProfile()` を介して読み取られ、`setEnableLoadProfile(...)` で接続ごとに設定できます。
* **デフォルト**: `false`
* **データ型**: boolean
* **導入バージョン**: v3.2.0

### insert_max_filter_ratio

* **説明**: Files() からの INSERT の最大エラー許容率。データ品質が不十分なためにフィルタリングされるデータレコードの最大比率です。不適格なデータレコードの比率がこのしきい値に達すると、ジョブは失敗します。範囲: [0, 1]。
* **デフォルト**: 0
* **導入バージョン**: v3.4.0

### insert_timeout

* **説明**: INSERT ジョブのタイムアウト時間。単位: 秒。v3.4.0 以降、`insert_timeout` は INSERT に関与する操作（例: UPDATE、DELETE、CTAS、マテリアライズドビューのリフレッシュ、統計収集、PIPE）に適用され、`query_timeout` を置き換えます。
* **デフォルト**: 14400
* **導入バージョン**: v3.4.0

### load_mem_limit

インポート操作のメモリ制限を指定します。デフォルト値は 0 で、この変数は使用されず、`query_mem_limit` が代わりに使用されます。

この変数は、クエリとインポートの両方を含む `INSERT` 操作にのみ使用されます。ユーザーがこの変数を設定しない場合、クエリとインポートのメモリ制限は `exec_mem_limit` に設定されます。それ以外の場合、クエリのメモリ制限は `exec_mem_limit` に設定され、インポートのメモリ制限は `load_mem_limit` に設定されます。

他のインポート方法（`BROKER LOAD`、`STREAM LOAD` など）は引き続き `exec_mem_limit` をメモリ制限として使用します。

### log_rejected_record_num (v3.1 以降)

ログに記録できる不適格なデータ行の最大数を指定します。有効な値: `0`、`-1`、および任意の非ゼロ正の整数。デフォルト値: `0`。

* 値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。
* 値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。
* 非ゼロ正の整数 `n` は、フィルタリングされたデータ行が各 BE で最大 `n` 行までログに記録されることを指定します。

### partial_update_mode

* **説明**: 部分更新のモードを制御するために使用されます。有効な値:

  * `auto`（デフォルト）: システムは UPDATE 文と関与する列を分析して部分更新のモードを自動的に決定します。
  * `column`: 部分更新にカラムモードを使用します。これは、少数の列と多数の行を含む部分更新に特に適しています。

  詳細については、[UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#partial-updates-in-column-mode-since-v31) を参照してください。
* **デフォルト**: auto
* **導入バージョン**: v3.1

### transaction_read_only

* **説明**: MySQL 5.8 互換性のために使用されます。エイリアスは `tx_read_only` です。この変数はトランザクションのアクセスモードを指定します。`ON` は読み取り専用を示し、`OFF` は読み取りおよび書き込み可能を示します。
* **デフォルト**: OFF
* **導入バージョン**: v2.5.18, v3.0.9, v3.1.7

### tx_isolation

MySQL クライアント互換性のために使用されます。実際の用途はありません。エイリアスは `transaction_isolation` です。

### tx_visible_wait_timeout

* **説明**: コミット済みトランザクションが可視（公開）になるまでサーバが待機する時間（秒単位）を制御するセッションスコープのタイムアウトです。`StmtExecutor` および `TransactionStmtExecutor` のコミットフローで使用されるミリ秒単位の publish 待機時間はこの値に1000を掛けて算出されます。可視待機が期限切れになると、トランザクションは COMMITTED と扱われますがまだ VISIBLE ではないと見なされます。マテリアライズドビューのリフレッシュロジック（`MVTaskRunProcessor`）は、可視性を事実上無期限に待つために一時的にこの変数を `Long.MAX_VALUE / 1000` に設定し、リフレッシュ後に元の値を復元します。`enable_sync_publish` が有効な場合、この変数は無視され、publish 待機はジョブのデッドラインから派生されます。
* **スコープ**: Session
* **デフォルト**: `10`
* **データ型**: long
* **導入バージョン**: v3.2.0

