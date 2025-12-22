---
displayed_sidebar: docs
---

# システム変数

StarRocks は、多くのシステム変数を提供しており、要件に応じて設定や変更が可能です。このセクションでは、StarRocks がサポートする変数について説明します。これらの変数の設定を確認するには、MySQL クライアントで [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) コマンドを実行します。また、[SET](sql-statements/cluster-management/config_vars/SET.md) コマンドを使用して、変数を動的に設定または変更することもできます。これらの変数は、システム全体でグローバルに、現在のセッションのみで、または単一のクエリ文でのみ有効にすることができます。

StarRocks の変数は、MySQL の変数セットを参照していますが、**一部の変数は MySQL クライアントプロトコルとの互換性のみを持ち、MySQL データベースでは機能しません**。

> **注意**
>
> どのユーザーでも SHOW VARIABLES を実行し、セッションレベルで変数を有効にする権限があります。ただし、SYSTEM レベルの OPERATE 権限を持つユーザーのみが、変数をグローバルに有効にすることができます。グローバルに有効な変数は、すべての将来のセッション（現在のセッションを除く）で有効になります。
>
> 現在のセッションの設定変更を行い、さらにその設定変更をすべての将来のセッションに適用したい場合は、`GLOBAL` 修飾子を使用せずに一度、使用してもう一度変更を行うことができます。例：
>
> ```SQL
> SET query_mem_limit = 137438953472; -- 現在のセッションに適用。
> SET GLOBAL query_mem_limit = 137438953472; -- すべての将来のセッションに適用。
> ```

## 変数の階層と種類

StarRocks は、グローバル変数、セッション変数、`SET_VAR` ヒントの3種類（レベル）の変数をサポートしています。それらの階層関係は次のとおりです：

* グローバル変数はグローバルレベルで有効であり、セッション変数や `SET_VAR` ヒントによって上書きされることがあります。
* セッション変数は現在のセッションでのみ有効であり、`SET_VAR` ヒントによって上書きされることがあります。
* `SET_VAR` ヒントは、現在のクエリ文でのみ有効です。

## 変数の表示

`SHOW VARIABLES [LIKE 'xxx']` を使用して、すべてまたは一部の変数を表示できます。例：

```SQL
-- システム内のすべての変数を表示。
SHOW VARIABLES;

-- 特定のパターンに一致する変数を表示。
SHOW VARIABLES LIKE '%time_zone%';
```

## 変数の設定

### 変数をグローバルまたは単一のセッションで設定

変数を **グローバルに** または **現在のセッションのみで** 有効に設定できます。グローバルに設定すると、新しい値はすべての将来のセッションで使用されますが、現在のセッションは元の値を使用します。「現在のセッションのみ」に設定すると、変数は現在のセッションでのみ有効になります。

`SET <var_name> = xxx;` で設定された変数は、現在のセッションでのみ有効です。例：

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

`SET GLOBAL <var_name> = xxx;` で設定された変数はグローバルに有効です。例：

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

以下の変数はグローバルにのみ有効です。単一のセッションで有効にすることはできません。これらの変数には `SET GLOBAL <var_name> = xxx;` を使用する必要があります。単一のセッションでそのような変数を設定しようとすると（`SET <var_name> = xxx;`）、エラーが返されます。

* activate_all_roles_on_login
* character_set_database
* cngroup_low_watermark_cpu_used_permille
* cngroup_low_watermark_running_query_count
* cngroup_resource_usage_fresh_ratio
* cngroup_schedule_mode
* default_rowset_type
* enable_group_level_query_queue
* enable_query_history
* enable_query_queue_load
* enable_query_queue_select
* enable_query_queue_statistic
* enable_table_name_case_insensitive
* enable_tde
* init_connect
* language
* license
* lower_case_table_names
* performance_schema
* query_cache_size
* query_history_keep_seconds
* query_history_load_interval_seconds
* query_queue_concurrency_limit
* query_queue_cpu_used_permille_limit
* query_queue_driver_high_water
* query_queue_driver_low_water
* query_queue_fresh_resource_usage_interval_ms
* query_queue_max_queued_queries
* query_queue_mem_used_pct_limit
* query_queue_pending_timeout_second
* system_time_zone
* version
* version_comment


さらに、変数設定は定数式もサポートしています。例：

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### 単一のクエリ文で変数を設定

特定のクエリに対して変数を設定する必要がある場合があります。`SET_VAR` ヒントを使用することで、単一の文内でのみ有効なセッション変数を設定できます。

StarRocks は、以下の文で `SET_VAR` の使用をサポートしています：

- SELECT
- INSERT (v3.1.12 および v3.2.0 以降)
- UPDATE (v3.1.12 および v3.2.0 以降)
- DELETE (v3.1.12 および v3.2.0 以降)

`SET_VAR` は、上記のキーワードの後にのみ配置され、`/*+...*/` で囲まれます。

例：

```sql
SELECT /*+ SET_VAR(query_mem_limit = 8589934592) */ name FROM people ORDER BY name;

SELECT /*+ SET_VAR(query_timeout = 1) */ sleep(3);

UPDATE /*+ SET_VAR(insert_timeout=100) */ tbl SET c1 = 2 WHERE c1 = 1;

DELETE /*+ SET_VAR(query_mem_limit = 8589934592) */
FROM my_table PARTITION p1
WHERE k1 = 3;

INSERT /*+ SET_VAR(insert_timeout = 10000000) */
INTO insert_wiki_edit
    SELECT * FROM FILES(
        "path" = "s3://inserttest/parquet/insert_wiki_edit_append.parquet",
        "format" = "parquet",
        "aws.s3.access_key" = "XXXXXXXXXX",
        "aws.s3.secret_key" = "YYYYYYYYYY",
        "aws.s3.region" = "us-west-2"
);
```

単一の文で複数の変数を設定することもできます。例：

```sql
SELECT /*+ SET_VAR
  (
  exec_mem_limit = 515396075520,
  query_timeout=10000000,
  batch_size=4096,
  parallel_fragment_exec_instance_num=32
  )
  */ * FROM TABLE;
```

### ユーザーのプロパティとして変数を設定

[ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md) を使用して、セッション変数をユーザーのプロパティとして設定できます。この機能は v3.3.3 からサポートされています。

例：

```SQL
-- ユーザー jack に対してセッション変数 `query_timeout` を `600` に設定。
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## 変数の説明

変数は **アルファベット順** に説明されています。`global` ラベルが付いた変数はグローバルにのみ有効です。他の変数はグローバルまたは単一のセッションで有効にすることができます。

### activate_all_roles_on_login (global)

* **説明**: StarRocks ユーザーが StarRocks クラスタに接続する際に、すべてのロール（デフォルトロールと付与されたロールを含む）を有効にするかどうか。
  * 有効にすると（`true`）、ユーザーのすべてのロールがログイン時にアクティブになります。これは [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) で設定されたロールよりも優先されます。
  * 無効にすると（`false`）、SET DEFAULT ROLE で設定されたロールがアクティブになります。
* **デフォルト**: false
* **導入バージョン**: v3.0

セッションで割り当てられたロールをアクティブにしたい場合は、[SET ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) コマンドを使用してください。

### auto_increment_increment

MySQL クライアント互換性のために使用されます。実際の用途はありません。

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

### cbo_cte_force_reuse_node_count

* **説明**: Common Table Expressions (CTE) に対するオプティマイザのショートカットを制御するセッションスコープの閾値。RelationTransformer.visitCTE 内でプランナは CTE プロデューサーツリーのノード数をカウントします（cteContext.getCteNodeCount）。そのカウントがこの閾値以上かつ閾値が 0 より大きい場合、transformer は CTE の再利用を強制します：プロデューサープランのインライン化／変換をスキップし、事前計算された式マッピング（入力なし）で consume 演算子を構築し、代わりに生成されたカラム参照を使用します。これにより非常に大きな CTE プロデューサーツリーに対するオプティマイザの時間を削減できますが、物理プランの最適性が若干低下する可能性があります。値を `0` に設定すると force-reuse 最適化が無効になります。この変数は SessionVariable に getter/setter があり、セッション単位で適用されます。
* **スコープ**: Session
* **デフォルト**: `2000`
* **タイプ**: int
* **導入バージョン**: v3.5.3

### cbo_cte_reuse

* **説明**: オプティマイザが共通テーブル式（CTE）を再利用して multi-distinct 集計クエリを書き換えることを許可するかを制御します（CBO の CTE 再利用リライト）。有効にすると、プランナー（RewriteMultiDistinctRule）はマルチカラム DISTINCT、スキューした集計、または統計により CTE リライトの方が効率的であると示される場合に CTE ベースのリライトを選択することがあり、`prefer_cte_rewrite` ヒントを尊重します。無効にすると CTE ベースのリライトは許可されず、プランナーは multi-function リライトを試みます。クエリが CTE を必要とする場合（例：マルチカラム DISTINCT や multi-function リライトで扱えない関数など）は、プランナーはユーザーエラーを発生させます。注意：オプティマイザが参照する実際の設定はこのフラグとパイプラインエンジンフラグの論理 AND です — すなわち `isCboCteReuse()` はこの変数と `enablePipelineEngine` の両方を返す必要があるため、`enablePipelineEngine` がオンのときのみ CTE 再利用が有効になります。
* **デフォルト**: `true`
* **タイプ**: Boolean
* **導入バージョン**: `v3.2.0`

### cbo_disabled_rules

* **説明**: 現在のセッションで無効にするオプティマイザルール名のカンマ区切りリスト。各名前は `RuleType` 列挙値に一致する必要があり、無効化できるのは名前が `TF_`（変換ルール）または `GP_`（グループ結合ルール）で始まるルールのみです。セッション変数は `SessionVariable` に格納され（`getCboDisabledRules` / `setCboDisabledRules`）、オプティマイザは `OptimizerOptions.applyDisableRuleFromSessionVariable()` を通じて適用します。同関数はリストを解析し、対応するルールスイッチをクリアしてプランニング時にそれらのルールをスキップします。SET ステートメントを通じて設定された場合、値は検証され、サーバは不明な名前や `TF_`/`GP_` で始まらない名前を明確なエラーメッセージ（例: "Unknown rule name(s): ..." や "Only TF_ ... and GP_ ... can be disabled"）で拒否します。プランナ実行時には不明なルール名は警告とともに無視されます（"Ignoring unknown rule name: ... (may be from different version)" とログに残る）。名前は列挙子識別子と正確に一致する必要があります（大文字小文字を区別）。名前の前後の空白はトリムされ、空のエントリは無視されます。
* **スコープ**: Session
* **デフォルト**: `""` (無効化されたルールなし)
* **データ型**: String
* **導入バージョン**: -

### cbo_enable_low_cardinality_optimize

* **説明**: 低基数最適化を有効にするかどうか。この機能を有効にすると、STRING 列のクエリパフォーマンスが約 3 倍向上します。
* **デフォルト**: true

### cbo_eq_base_type

* **説明**: DECIMAL データと STRING データの間でデータ比較に使用されるデータ型を指定します。デフォルト値は `DECIMAL` であり、VARCHAR も有効な値です。**この変数は `=` および `!=` 比較にのみ有効です。**
* **データ型**: 文字列
* **導入バージョン**: v2.5.14

### cbo_json_v2_dict_opt

* **説明**: JSON v2 のパス書き換えで生成される Flat JSON の文字列サブカラムに対して、低カーディナリティ辞書最適化を有効にするかどうか。有効にすると、オプティマイザはそれらのサブカラムにグローバル辞書を構築・利用し、文字列式、GROUP BY、JOIN などを高速化できます。
* **デフォルト**: true
* **データ型**: Boolean

### cbo_json_v2_rewrite

* **説明**: オプティマイザで JSON v2 のパス書き換えを有効にするかどうか。有効にすると、JSON 関数（`get_json_*` など）を Flat JSON のサブカラムへの直接アクセスに書き換え、述語プッシュダウン、カラムプルーニング、辞書最適化を有効化します。
* **デフォルト**: true
* **データ型**: Boolean

### cbo_max_reorder_node_use_greedy

* **説明**: コストベースオプティマイザが greedy join-reorder アルゴリズムを検討するマルチジョイン内の結合入力（アトム）の最大数。オプティマイザは候補となるリオーダーアルゴリズムのリストを構築する際にこの制限（`cbo_enable_greedy_join_reorder` と併せて）をチェックします：もし `multiJoinNode.getAtoms().size()` がこの値以下であれば、`JoinReorderGreedy` のインスタンスが追加され実行されます。この変数は `JoinReorderFactory.createJoinReorderAdaptive()` と `ReorderJoinRule` によって、ジョインリオーダー段階での greedy リオーダリングの適用可否を制御するために使用されます。セッションごとに適用され、統計情報が利用可能でかつ greedy が有効な場合に greedy リオーダリングが試行されるかに影響します。多数の結合されたリレーションを含むクエリに対するオプティマイザの時間/複雑度のトレードオフを制御するために調整してください。
* **スコープ**: Session (セッションごとに変更可能)
* **デフォルト**: `16`
* **データタイプ**: long
* **導入バージョン**: v3.4.0, v3.5.0

### cbo_use_correlated_predicate_estimate

* **説明**: セッションフラグ。オプティマイザが、複数列にまたがる結合された等価述語の選択率を推定する際に相関を考慮したヒューリスティックを適用するかを制御します。有効（デフォルト）の場合、推定器はプライマリのマルチカラム統計や最も選択的な述語を除く追加列に対して指数減衰重みを適用し、追加述語の乗算的影響を軽減します（重み：追加最大3列に対して 0.5、0.25、0.125）。無効の場合、減衰は適用されず（減衰係数 = 1）、これらの列の完全な選択率を乗算します（より強い独立仮定）。このフラグは StatisticsEstimateUtils.estimateConjunctiveEqualitySelectivity により確認され、マルチカラム統計経路とフォールバック経路の両方で減衰係数を選択するため、CBO が使用するカーディナリティ推定に影響します。
* **スコープ**: Session
* **デフォルト**: `true`
* **タイプ**: boolean
* **導入バージョン**: v3.5.0

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

### computation_fragment_scheduling_policy

* **スコープ**: セッション
* **説明**: 計算フラグメントの実行インスタンスを選択するために使用されるスケジューラポリシーを制御します。有効な値（大文字小文字を区別しない）は次のとおりです:
  * `compute_nodes_only` — フラグメントを compute ノードのみにスケジュールします（デフォルト）。
  * `all_nodes` — compute ノードと従来のバックエンドノードの両方でのスケジューリングを許可します。
  この変数は enum `SessionVariableConstants.ComputationFragmentSchedulingPolicy` によってバックされます。設定されると値は enum に対して検証（大文字化して比較）され、無効な値はエラーを引き起こします（API 経由で設定した場合は `IllegalArgumentException`、SET 文で使用した場合は `SemanticException`）。ゲッターは対応する enum 値を返し、未設定または認識できない場合は `COMPUTE_NODES_ONLY` にフォールバックします。この設定はプラン作成／デプロイ時に FE がフラグメント配置先ノードを選択する方法に影響します。
* **デフォルト**: `COMPUTE_NODES_ONLY`
* **タイプ**: String
* **導入バージョン**: v3.2.7

### connector_io_tasks_per_scan_operator

* **説明**: 外部テーブルクエリ中にスキャンオペレーターによって発行される最大同時 I/O タスク数。値は整数です。現在、StarRocks は外部テーブルをクエリする際に同時 I/O タスクの数を適応的に調整できます。この機能は、デフォルトで有効になっている変数 `enable_connector_adaptive_io_tasks` によって制御されます。
* **デフォルト**: 16
* **データ型**: Int
* **導入バージョン**: v2.5

### connector_sink_compression_codec

* **説明**: Hive テーブルまたは Iceberg テーブルにデータを書き込む際、または Files() でデータをエクスポートする際に使用される圧縮アルゴリズムを指定します。
* **有効な値**: `uncompressed`, `snappy`, `lz4`, `zstd`, および `gzip`。
* **デフォルト**: uncompressed
* **データ型**: 文字列
* **導入バージョン**: v3.2.3

### connector_sink_target_max_file_size

* **説明**: Hive テーブルまたは Iceberg テーブルにデータを書き込む際、または Files() でデータをエクスポートする際のターゲットファイルの最大サイズを指定します。この制限は厳密ではなく、ベストエフォートで適用されます。
* **単位**: バイト
* **デフォルト**: 1073741824
* **データ型**: Long
* **導入バージョン**: v3.3.0

### count_distinct_column_buckets

* **説明**: グループバイカウントディスティンクトクエリでの COUNT DISTINCT 列のバケット数。この変数は `enable_distinct_column_bucketization` が `true` に設定されている場合にのみ有効です。
* **デフォルト**: 1024
* **導入バージョン**: v2.5

### custom_query_id (session)

* **説明**: 現在のクエリに外部識別子をバインドするために使用されます。クエリ実行前に `SET SESSION custom_query_id = 'my-query-id';` のように設定できます。クエリ終了後に値はリセットされます。この値は `KILL QUERY 'my-query-id'` に渡すことができます。値は監査ログの `customQueryId` フィールドで確認できます。
* **デフォルト**: ""
* **データタイプ**: String
* **導入バージョン**: v3.4.0

### datacache_sharing_work_period

* **説明**: キャッシュ共有が有効になる期間。各クラスタースケーリング操作の後、キャッシュ共有機能が有効になっている場合、この期間内のリクエストのみが他のノードからキャッシュデータにアクセスしようとします。
* **デフォルト**: 600
* **単位**: Seconds
* **導入バージョン**: v3.5.1

### default_rowset_type (global)

コンピューティングノードのストレージエンジンで使用されるデフォルトのストレージ形式を設定するために使用されます。現在サポートされているストレージ形式は `alpha` と `beta` です。

### default_table_compression

* **説明**: テーブルストレージのデフォルト圧縮アルゴリズム。サポートされている圧縮アルゴリズムは `snappy, lz4, zlib, zstd` です。

  CREATE TABLE 文で `compression` プロパティを指定した場合、`compression` で指定された圧縮アルゴリズムが有効になります。

* **デフォルト**: lz4_frame
* **導入バージョン**: v3.0

### disable_colocate_join

* **説明**: Colocation Join を有効にするかどうかを制御するために使用されます。デフォルト値は `false` で、機能が有効です。この機能が無効になっている場合、クエリプランニングは Colocation Join を実行しようとしません。
* **デフォルト**: false

### disable_join_reorder

* **スコープ**: Session
* **説明**: コストベースオプティマイザが結合の並べ替え（join reordering）を行うかどうかを制御します。`false`（デフォルト）では、オプティマイザは新しいプランナ経路（`SPMOptimizer` や `QueryOptimizer` に見られる）における論理最適化中に結合並べ替え変換（例：`ReorderJoinRule`、join transformation や outer-join transformation ルール）を適用する場合があります。`true` の場合、結合の並べ替えおよび関連する outer-join reorder ルールはスキップされ、オプティマイザが結合順序を変更することを防ぎます。これは最適化時間を短縮したり、安定／再現可能な結合順序を得たり、CBO の並べ替えが非最適なプランを生成するケースを回避するのに有用です。この設定は `cbo_max_reorder_node`、`cbo_max_reorder_node_use_exhaustive`、`enable_outer_join_reorder` のような他の CBO／セッション制御と相互作用します。
* **デフォルト**: `false`
* **データタイプ**: boolean
* **導入バージョン**: v3.2.0

### div_precision_increment

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### dynamic_overwrite

* **説明**: パーティションテーブルを使用した INSERT OVERWRITE の [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) セマンティクスを有効にするかどうか。有効な値:
  * `true`: Dynamic Overwrite を有効にします。
  * `false`: Dynamic Overwrite を無効にし、デフォルトのセマンティクスを使用します。
* **デフォルト**: false
* **導入バージョン**: v3.4.0

### enable_adaptive_sink_dop

* **説明**: データロードの適応並行性を有効にするかどうかを指定します。この機能を有効にすると、システムは INSERT INTO および Broker Load ジョブのロード並行性を自動的に設定し、`pipeline_dop` のメカニズムと同等になります。新しくデプロイされた v2.5 StarRocks クラスタでは、デフォルトで `true` に設定されています。v2.4 からアップグレードされた v2.5 クラスタでは、デフォルトで `false` に設定されています。
* **デフォルト**: false
* **導入バージョン**: v2.5

### enable_bucket_aware_execution_on_lake

* **説明**: データレイク（例：Iceberg テーブル）に対するクエリにおいて、Bucket-aware 実行を有効にするかどうか。この機能を有効にすると、システムはバケット情報を活用してデータシャッフルを削減し、パフォーマンスを向上させることでクエリの実行を最適化します。この最適化は、バケット化されたテーブルにおける Join 操作や集計処理において特に効果的です。
* **デフォルト**: true
* **タイプ**: Boolean
* **導入バージョン**: v4.0

### enable_cbo_based_mv_rewrite
* **説明**: CBO フェーズでマテリアライズドビューの書き換えを有効にするかどうか。これにより、クエリ書き換えの成功率を最大化できます（例：マテリアライズドビューとクエリの結合順序が異なる場合）が、オプティマイザフェーズの実行時間が増加します。
* **デフォルト**: true
* **導入バージョン**: v3.5.5, v4.0.1

### enable_cbo_table_prune

* **説明**: 有効にすると、オプティマイザはメモ最適化中に CBO テーブルプルーニングルール（CboTablePruneRule）を追加して、カーディナリティを保持する結合に対するコストベースのテーブルプルーニングを行います。ルールはオプティマイザ内で条件付きに追加されます（QueryOptimizer.memoOptimize および SPMOptimizer.memoOptimize を参照）— 結合ツリー内の結合ノード数が小さい場合（10 未満の結合ノード）にのみ追加されます。このオプションはルールベースのプルーニング切替 `enable_rbo_table_prune` を補完し、Cost-Based Optimizer が結合処理から不要なテーブルや入力を取り除いてプランと実行の複雑さを軽減できるようにします。プルーニングはプラン形状を変更する可能性があるためデフォルトはオフです。代表的なワークロードで検証してから有効化してください。
* **スコープ**: Session
* **デフォルト**: `false`
* **タイプ**: boolean
* **導入バージョン**: v3.2.0

### enable_color_explain_output

* **スコープ**: Session
* **説明**: テキスト形式の EXPLAIN / PROFILE 出力に ANSI カラーエスケープシーケンスを含めるかどうかを制御します。有効（`true`）のとき、StmtExecutor はセッション設定を explain/profile パイプライン（ExplainAnalyzer への呼び出しを通じて）に渡すため、explain、EXPLAIN ANALYZE、analyze-profile 出力に ANSI 対応端末での可読性を高めるカラー強調表示が含まれます。無効（`false`）のときは ANSI シーケンスなし（プレーンテキスト）で出力され、ログや ANSI をサポートしないクライアント、または出力をファイルにパイプする場合に適しています。これはセッション単位のトグルであり、実行のセマンティクスを変更するものではなく、explain/profile テキストの表示方法のみを変更します。
* **デフォルト**: `true`
* **データタイプ**: boolean
* **導入バージョン**: v3.5.0

### enable_connector_adaptive_io_tasks

* **説明**: 外部テーブルをクエリする際に同時 I/O タスクの数を適応的に調整するかどうか。デフォルト値は `true` です。この機能が有効でない場合、変数 `connector_io_tasks_per_scan_operator` を使用して同時 I/O タスクの数を手動で設定できます。
* **デフォルト**: true
* **導入バージョン**: v2.5

### enable_datacache_async_populate_mode

* **説明**: データキャッシュを非同期モードでポピュレートするかどうか。デフォルトでは、システムは同期モードを使用してデータキャッシュをポピュレートします。つまり、データをクエリしながらキャッシュをポピュレートします。
* **デフォルト**: false
* **導入バージョン**: v3.2.7

### enable_datacache_io_adaptor

* **説明**: Data Cache I/O アダプタを有効にするかどうか。この機能を有効にすると、ディスク I/O 負荷が高い場合にシステムが一部のキャッシュ要求をリモートストレージに自動的にルーティングし、ディスクの負荷を軽減します。
* **デフォルト**: true
* **導入バージョン**: v3.3.0

### enable_datacache_sharing

* **説明**: キャッシュ共有を有効にするかどうか。これを `true` に設定すると、この機能が有効になります。キャッシュ共有は、ネットワークを介して他のノードからキャッシュデータにアクセスすることをサポートするために使用されます。これは、クラスタのスケーリング中にキャッシュが無効になることによって発生するパフォーマンスのジッタを軽減するのに役立ちます。この変数は FE パラメータ `enable_trace_historical_node` が `true` に設定されている場合にのみ有効になります。
* **デフォルト**: true
* **導入バージョン**: v3.5.1

### enable_distinct_agg_over_window

* **説明**: WINDOW句上の DISTINCT 集約呼び出しを等価な join ベースのプランに変換するオプティマイザのリライトを制御します。有効（`true`、デフォルト）の場合、QueryOptimizer.invoke convertDistinctAggOverWindowToNullSafeEqualJoin は以下を行います:
  * LogicalWindowOperator を含むクエリを検出、
  * project-merge リライトを実行し、論理プロパティを導出、
  * DistinctAggregationOverWindowRule を適用して DISTINCT-OVER-WINDOW パターンを null-safe equality join に変換（プラン形状を変更してさらに push-down や集約最適化を有効にする）、
  * その後 SeparateProjectRule を実行してプロパティを再導出。
  無効（`false`）の場合、オプティマイザはこの変換をスキップし、ウィンドウ上の DISTINCT 集約を変更せずに残します。この設定はセッションスコープで、オプティマイザのリライトフェーズ（QueryOptimizer.convertDistinctAggOverWindowToNullSafeEqualJoin を参照）にのみ影響します。
* **デフォルト**: `true`
* **データタイプ**: boolean
* **導入バージョン**: -

### enable_distinct_column_bucketization

* **説明**: グループバイカウントディスティンクトクエリで COUNT DISTINCT 列のバケット化を有効にするかどうか。クエリ `select a, count(distinct b) from t group by a;` を例にとります。GROUP BY 列 `a` が低基数列で、COUNT DISTINCT 列 `b` が高基数列でデータスキューが激しい場合、パフォーマンスボトルネックが発生します。この状況では、COUNT DISTINCT 列のデータを複数のバケットに分割してデータをバランスさせ、データスキューを防ぐことができます。この変数は、変数 `count_distinct_column_buckets` と一緒に使用する必要があります。

  クエリに `skew` ヒントを追加することで、COUNT DISTINCT 列のバケット化を有効にすることもできます。例：`select a,count(distinct [skew] b) from t group by a;`。

* **デフォルト**: false、つまりこの機能は無効です。
* **導入バージョン**: v2.5

### enable_force_rule_based_mv_rewrite

* **説明**: オプティマイザのルールベース最適化フェーズで複数テーブルに対するクエリの書き換えを有効にするかどうか。この機能を有効にすると、クエリ書き換えの堅牢性が向上します。ただし、クエリがマテリアライズドビューを見逃した場合、時間消費が増加します。
* **デフォルト**: true
* **導入バージョン**: v3.3.0

### enable_gin_filter

* **説明**: クエリ中に[全文逆インデックス](../table_design/indexes/inverted_index.md)を利用するかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.3.0

### enable_global_runtime_filter

グローバルランタイムフィルタ（RF の略）を有効にするかどうか。RF は実行時にデータをフィルタリングします。データフィルタリングは通常、ジョイン段階で発生します。複数テーブルのジョイン中に、述語プッシュダウンなどの最適化を使用してデータをフィルタリングし、ジョインのスキャン行数とシャッフル段階の I/O を削減し、クエリを高速化します。

StarRocks は 2 種類の RF を提供します：ローカル RF とグローバル RF。ローカル RF は Broadcast Hash Join に適しており、グローバル RF は Shuffle Join に適しています。

デフォルト値: `true`、つまりグローバル RF が有効です。この機能が無効になっている場合、グローバル RF は効果を発揮しません。ローカル RF は引き続き機能します。

### enable_group_by_compressed_key

* **説明**: GROUP BY キー列を圧縮するために正確な統計情報を使用するかどうか。有効な値: `true` と `false`。
* **デフォルト**: true
* **導入バージョン**: v4.0

### enable_group_execution

* **説明**: Colocate Group Executionを有効化するかどうか。Colocate Group Executionは物理的なデータ分割を活用する実行パターンであり、固定数のスレッドがそれぞれのデータ範囲を順次処理することで局所性とスループットを向上させます。この機能を有効化するとメモリ使用量を削減できます。
* **デフォルト**: true
* **導入バージョン**: v3.3

### enable_group_level_query_queue (global)

* **説明**: リソースグループレベルの[クエリキュー](../administration/management/resource_management/query_queues.md)を有効にするかどうか。
* **デフォルト**: false、つまりこの機能は無効です。
* **導入バージョン**: v3.1.4

### enable_insert_partial_update

* **説明**：主キーテーブルに対するINSERTステートメントの部分更新を有効化するかどうか。この項目が `true`（デフォルト）に設定されている場合、INSERT ステートメントで指定された列がサブセット（テーブル内のすべての非生成列の数より少ない）であるとき、システムは部分更新を実行し、指定された列のみを更新しながら他の列の既存値を保持します。`false` に設定すると、システムは既存の値を保持する代わりに、指定されていない列に対してデフォルト値を使用します。この機能は、主キーテーブルの特定の列を更新する際に他の列の値に影響を与えない場合に特に有用です。
* **デフォルト値**：true
* **導入バージョン**：v3.3.20、v3.4.9、v3.5.8、v4.0.2

### enable_insert_strict

* **説明**: Files() からの INSERT を使用してデータをロードする際に厳密モードを有効にするかどうか。有効な値: `true` および `false`（デフォルト）。厳密モードが有効な場合、システムは資格のある行のみをロードします。不適格な行をフィルタリングし、不適格な行の詳細を返します。詳細は [Strict mode](../loading/load_concept/strict_mode.md) を参照してください。v3.4.0 より前のバージョンでは、`enable_insert_strict` が `true` に設定されている場合、不適格な行があると INSERT ジョブが失敗します。
* **デフォルト**: true

### enable_lake_tablet_internal_parallel

* **説明**: 共有データクラスタ内のクラウドネイティブテーブルに対する並列スキャンを有効にするかどうか。
* **デフォルト**: true
* **データ型**: Boolean
* **導入バージョン**: v3.3.0

### enable_load_profile

* **スコープ**: Session
* **説明**: 有効にすると、FE はロードジョブのランタイムプロファイルの収集を要求し、ロード完了後にロードコーディネータがプロファイルを収集/エクスポートします。ストリームロードの場合、FE は `TQueryOptions.enable_profile = true` を設定し、`stream_load_profile_collect_threshold_second` からの `load_profile_collect_second` をバックエンドに渡します。コーディネータは条件に応じてプロファイル収集を呼び出します（StreamLoadTask.collectProfile() を参照）。実際の振る舞いは、このセッション変数と宛先テーブルのテーブルレベルプロパティ `enable_load_profile` の論理 OR です。収集はさらに `load_profile_collect_interval_second`（FE 側のサンプリング間隔）によって制御され、頻繁な収集を回避します。セッションフラグは `SessionVariable.isEnableLoadProfile()` を介して読み取られ、`setEnableLoadProfile(...)` で接続ごとに設定できます。
* **デフォルト**: `false`
* **データ型**: boolean
* **導入バージョン**: v3.2.0

### enable_materialized_view_agg_pushdown_rewrite

* **説明**: マテリアライズドビュークエリ書き換えのための集計プッシュダウンを有効にするかどうか。`true` に設定されている場合、集計関数はクエリ実行中に Scan Operator にプッシュダウンされ、Join Operator が実行される前にマテリアライズドビューによって書き換えられます。これにより、Join によるデータ拡張が軽減され、クエリパフォーマンスが向上します。この機能のシナリオと制限の詳細については、[Aggregation pushdown](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#aggregation-pushdown) を参照してください。
* **デフォルト**: false
* **導入バージョン**: v3.3.0

### enable_materialized_view_for_insert

* **説明**: StarRocks が INSERT INTO SELECT 文でクエリを書き換えることを許可するかどうか。
* **デフォルト**: false、つまりそのようなシナリオでのクエリ書き換えはデフォルトで無効です。
* **導入バージョン**: v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_text_match_rewrite

* **説明**: テキストベースのマテリアライズドビュー書き換えを有効にするかどうか。この項目が `true` に設定されている場合、オプティマイザはクエリを既存のマテリアライズドビューと比較します。マテリアライズドビューの定義の抽象構文ツリーがクエリまたはそのサブクエリと一致する場合、クエリは書き換えられます。
* **デフォルト**: true
* **導入バージョン**: v3.2.5, v3.3.0

### enable_materialized_view_union_rewrite

* **説明**: マテリアライズドビューのユニオン書き換えを有効にするかどうか。この項目が `true` に設定されている場合、マテリアライズドビューの述語がクエリの述語を満たさない場合、システムは UNION ALL を使用して述語を補完しようとします。
* **デフォルト**: true
* **導入バージョン**: v2.5.20, v3.1.9, v3.2.7, v3.3.0

### enable_metadata_profile

* **説明**: Iceberg Catalog のメタデータ収集クエリに対して Profile を有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.3.3

### enable_multicolumn_global_runtime_filter

マルチカラムグローバルランタイムフィルタを有効にするかどうか。デフォルト値: `false`、つまりマルチカラムグローバル RF は無効です。

ジョイン（Broadcast Join および Replicated Join を除く）に複数の等値ジョイン条件がある場合：

* この機能が無効な場合、ローカル RF のみが機能します。
* この機能が有効な場合、マルチカラムグローバル RF が有効になり、パーティション by 句に `multi-column` を含みます。

### enable_mv_planner

* **スコープ**: Session
* **説明**: 有効にすると、現在のセッションに対して Materialized View (MV) planner モードを有効にします。このモードではオプティマイザは以下のように動作します：
  - 通常の join 実装ルール（QueryOptimizer）ではなく、`context.getRuleSet().addRealtimeMVRules()` を通じた MV 固有のルールセットを使用します。
  - stream 実装ルールが適用されることを許可します（`StreamImplementationRule.check` は MV planner がオンのときのみ true を返します）。
  - 論理プラン変換時の scan/operator 構築を変更します（例：RelationTransformer は MV planner が有効な場合、ネイティブテーブル/マテリアライズドビューに対して `LogicalBinlogScanOperator` を選択します）。
  - 一部の標準変換を無効化またはバイパスします（例：`SplitMultiPhaseAggRule.check` は MV planner がオンのとき false を返します）。
  Materialized view のプラン生成コード（MaterializedViewAnalyzer）は MV プランニング作業の前後でこのフラグを設定します（プランニング前に true にし、終了後に false に戻す）ので、主に MV プラン生成とテスト用を意図しています。このセッション変数を設定しても、影響は現在のセッションのオプティマイザ動作にのみ及びます。
* **デフォルト**: `false`
* **データ型**: boolean
* **導入バージョン**: v3.2.0

### enable_parallel_merge

* **説明**: ソートの Parallel Merge を有効にするかどうか。この機能を有効にすると、ソートのマージフェーズでマージ操作に複数のスレッドが使用されます。
* **デフォルト**: true
* **導入バージョン**: v3.3

### enable_parquet_reader_bloom_filter

* **デフォルト**: true
* **データ型**: Boolean
* **単位**: -
* **説明**: Parquet ファイルの読み込み時にブルームフィルターの最適化を有効にするかどうか。
  * `true` (デフォルト): Parquet ファイルの読み込み時にブルームフィルタの最適化を有効にする。
  * `false`: Parquet ファイルの読み込み時にブルームフィルターの最適化を有効にしない。
* **導入バージョン**: v3.5.0

### enable_parquet_reader_page_index

* **デフォルト**: true
* **データ型**: Boolean
* **単位**: -
* **説明**: Parquet ファイルの読み込み時にページインデックスの最適化を有効にするかどうか。
  * `true` (デフォルト): Parquet ファイルの読み込み時にページインデックスの最適化を有効にする。
  * `false`: Parquet ファイルの読み込み時にページインデックスの最適化を有効にしない。
* **導入バージョン**: v3.5.0

### enable_partition_hash_join

* **説明**: 適応型 Partition Hash Join を有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.4

### enable_per_bucket_optimize

* **説明**: バケット化計算を有効化するかどうか。この機能を有効にすると、第 1 段階の集計をバケット順に計算でき、メモリ使用量を削減できます。
* **デフォルト**: true
* **導入バージョン**: v3.0

### enable_phased_scheduler

* **説明**: マルチフェーズスケジューリングを有効にするかどうか。マルチフェーズスケジューリングを有効にすると、フラグメントは依存関係に基づいてスケジューリングされます。例えば、システムはまず Shuffle Join の Build side のフラグメントをスケジュールし、次に Probe side のフラグメントをスケジュールします（注：ステージごとのスケジュールとは異なり、フェーズごとのスケジュールは依然として MPP 実行モード下で実行されます）。マルチフェーズスケジューリングを有効にすると、多数のUNION ALLクエリにおけるメモリ使用量を大幅に削減できます。
* **デフォルト**: false
* **導入バージョン**: v3.3

### enable_pipeline_engine

* **説明**: パイプライン実行エンジンを有効にするかどうかを指定します。`true` は有効を示し、`false` は無効を示します。デフォルト値: `true`。
* **デフォルト**: true

### enable_plan_advisor

* **説明**: 遅いクエリや手動でマークされたクエリに対するクエリフィードバック機能を有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.4.0

### enable_predicate_reorder

* **スコープ**: Session
* **説明**: 有効にすると、オプティマイザは論理／物理プランの書き換え時に AND（連言）述語に対して Predicate Reorder ルールを適用します。ルールは `Utils.extractConjuncts` を使って連言の各項を抽出し、`DefaultPredicateSelectivityEstimator` で各項の選択性を推定し、推定選択性の昇順（制約の緩いものを先）で項を並べ替えて新しい `CompoundPredicateOperator`（AND）を構成します。オペレータが複数の項を持つ `CompoundPredicateOperator` の場合にのみルールは実行されます。統計は子の `OptExpression` の統計から取得され、`PhysicalOlapScanOperator` の場合は `GlobalStateMgr.getCurrentState().getStatisticStorage()` からカラム統計を取得します。子の統計が欠けておりスキャンが OLAP スキャンでない場合、ルールは並べ替えをスキップします。セッション変数は `SessionVariable.isEnablePredicateReorder()` を通じて公開され、`enablePredicateReorder()` および `disablePredicateReorder()` のヘルパーメソッドがあります。
* **デフォルト**: false
* **データタイプ**: boolean
* **導入バージョン**: v3.2.0

### enable_profile

* **説明**: クエリのプロファイルを分析のために送信するかどうかを指定します。デフォルト値は `false` で、プロファイルは必要ありません。

  デフォルトでは、クエリエラーが BE で発生した場合にのみプロファイルが FE に送信されます。プロファイルの送信はネットワークオーバーヘッドを引き起こし、高い並行性に影響を与えます。

  クエリのプロファイルを分析する必要がある場合、この変数を `true` に設定できます。クエリが完了した後、現在接続されている FE のウェブページ（アドレス：`fe_host:fe_http_port/query`）でプロファイルを表示できます。このページには、`enable_profile` がオンになっている最新の 100 件のクエリのプロファイルが表示されます。

* **デフォルト**: false

### enable_query_cache

* **説明**: Query Cache 機能を有効にするかどうかを指定します。有効な値: true および false。`true` はこの機能を有効にし、`false` はこの機能を無効にします。この機能が有効な場合、[Query Cache](../using_starrocks/caching/query_cache.md#application-scenarios) の適用シナリオで指定された条件を満たすクエリに対してのみ機能します。
* **デフォルト**: false
* **導入バージョン**: v2.5

### enable_query_queue_load (global)

* **説明**: ロードタスクのクエリキューを有効にするためのブール値。
* **デフォルト**: false

### enable_query_queue_select (global)

* **説明**: SELECT クエリのクエリキューを有効にするかどうか。
* **デフォルト**: false

### enable_query_queue_statistic (global)

* **説明**: 統計クエリのクエリキューを有効にするかどうか。
* **デフォルト**: false

### enable_query_tablet_affinity

* **説明**: 同じタブレットに対する複数のクエリを固定レプリカに向けるかどうかを制御するためのブール値。

  クエリするテーブルに多数のタブレットがあるシナリオでは、この機能によりクエリパフォーマンスが大幅に向上します。なぜなら、タブレットのメタ情報とデータがメモリにより迅速にキャッシュされるからです。

  ただし、ホットスポットタブレットがある場合、この機能はクエリパフォーマンスを低下させる可能性があります。なぜなら、クエリを同じ BE に向けるため、高い並行性シナリオで複数の BE のリソースを十分に活用できなくなるからです。

* **デフォルト**: false、つまりシステムは各クエリに対してレプリカを選択します。
* **導入バージョン**: v2.5.6, v3.0.8, v3.1.4, および v3.2.0

### enable_query_trigger_analyze

* **デフォルト**: true
* **タイプ**: Boolean
* **説明**: 外部テーブルに対してクエリトリガー ANALYZE タスクを有効化するかどうか。
* **導入バージョン**: v3.4.0

### enable_rbo_table_prune

* **説明**: セッションで有効にすると、オプティマイザは現在のセッションでカーディナリティを保持する結合に対してルールベース（RBO）によるテーブルプルーニングを適用します。オプティマイザは一連のリライトおよびプルーニングステップ（パーティションプルーニング、project のマージ/分離、`UniquenessBasedTablePruneRule`、結合順序入れ替え、`RboTablePruneRule`）を実行して不要なテーブルスキャンの候補を除去し、結合における走査パーティション/行数を削減します。このオプションを有効にすると、論理ルールのリライト実行中に競合する変換を避けるために結合同値性導出（`context.setEnableJoinEquivalenceDerive(false)`）が無効化されます。プルーニングフローは、`enable_table_prune_on_update` が設定されている場合に UPDATE 文に対して `PrimaryKeyUpdateTableRule` を追加で実行することがあります。ルールは、クエリがプルーニング可能な結合を含む場合にのみ実行されます（`Utils.hasPrunableJoin(tree)` でチェックされます）。
* **スコープ**: Session
* **デフォルト**: `false`
* **タイプ**: boolean
* **導入バージョン**: v3.2.0

### enable_runtime_adaptive_dop

* **スコープ**: Session
* **説明**: セッションで有効にすると、planner と fragment builder はランタイムの adaptive DOP をサポートするパイプライン対応フラグメントに対して、実行時に adaptive degree-of-parallelism を使用するようマークします。このオプションは `enable_pipeline_engine` が true のときのみ有効になります。有効にするとフラグメントはプラン構築中に `enableAdaptiveDop()` を呼び出すようになり、ランタイム上の影響があります：join の probe はすべての build フェーズが完了するのを待つ可能性があり（これは `group_execution` の動作と衝突します）、ランタイム adaptive DOP を有効にするとパイプラインレベルの multi-partitioned runtime filters が無効になります（setter は `enablePipelineLevelMultiPartitionedRf` をクリアします）。このフラグはクエリプロファイルに記録され、セッション単位で切り替えることができます。
* **デフォルト**: `false`
* **データ型**: boolean
* **導入バージョン**: v3.2.0

### enable_scan_datacache

* **説明**: Data Cache 機能を有効にするかどうかを指定します。この機能が有効になると、StarRocks は外部ストレージシステムから読み取ったホットデータをブロックにキャッシュし、クエリと分析を加速します。詳細については、[Data Cache](../data_source/data_cache.md) を参照してください。バージョン 3.2 より前では、この変数は `enable_scan_block_cache` として名前が付けられていました。
* **デフォルト**: true
* **導入バージョン**: v2.5

### enable_short_circuit

* **説明**: クエリのショートサーキットを有効にするかどうか。デフォルト: `false`。`true` に設定されている場合、クエリが条件を満たすとき（クエリがポイントクエリであるかどうかを評価するための条件）、WHERE 句の条件列がすべての主キー列を含み、WHERE 句の演算子が `=` または `IN` の場合、クエリはショートサーキットを取ります。
* **デフォルト**: false
* **導入バージョン**: v3.2.3

### enable_sort_aggregate

* **説明**: ソートされたストリーミングを有効にするかどうかを指定します。`true` はソートされたストリーミングが有効で、データストリーム内のデータをソートします。
* **デフォルト**: false
* **導入バージョン**: v2.5

### enable_spill

* **説明**: 中間結果のスピルを有効にするかどうか。デフォルト: `false`。`true` に設定されている場合、StarRocks はクエリ内の集計、ソート、またはジョインオペレーターを処理する際にメモリ使用量を削減するために中間結果をディスクにスピルします。
* **デフォルト**: false
* **導入バージョン**: v3.0

### enable_spill_to_remote_storage

* **説明**: 中間結果をオブジェクトストレージにスピルするかどうか。`true` に設定されている場合、StarRocks はローカルディスクの容量制限に達した後、`spill_storage_volume` で指定されたストレージボリュームに中間結果をスピルします。詳細については、[Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage) を参照してください。
* **デフォルト**: false
* **導入バージョン**: v3.3.0

### enable_spm_rewrite

* **説明**: SQL Plan Manager (SPM) クエリ改写を有効にするかどうか。有効にすると、StarRocks は自動的にクエリを拘束されたクエリプランに改写し、クエリのパフォーマンスと安定性を向上させます。
* **デフォルト**: false

### enable_strict_order_by

* **説明**: ORDER BY で参照される列名が曖昧であるかどうかをチェックするために使用されます。この変数がデフォルト値 `TRUE` に設定されている場合、次のようなクエリパターンに対してエラーが報告されます：クエリの異なる式で重複したエイリアスが使用され、このエイリアスが ORDER BY のソートフィールドでもある場合、例：`select distinct t1.* from tbl1 t1 order by t1.k1;`。このロジックは v2.3 およびそれ以前と同じです。この変数が `FALSE` に設定されている場合、緩やかな重複排除メカニズムが使用され、そのようなクエリを有効な SQL クエリとして処理します。
* **デフォルト**: true
* **導入バージョン**: v2.5.18 および v3.1.7

### enable_sync_materialized_view_rewrite

* **説明**: 同期マテリアライズドビューに基づくクエリの書き換えを有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.1.11, v3.2.5

### enable_table_prune_on_update

* **説明**: セッションレベルのブールで、オプティマイザが UPDATE 文に対して主キー固有のテーブルプルーニング規則を適用するかどうかを制御します。有効にすると、QueryOptimizer（pruneTables ステージ中）が `PrimaryKeyUpdateTableRule` を呼び出して更新プランを書き換え／プルーニングし、主キー更新パターンに対するプルーニングを改善する可能性があります。このフラグは rule-based/CBO によるテーブルプルーニングが有効な場合にのみ効果があります（`enable_rbo_table_prune` を参照）。デフォルトで無効になっているのは、この変換がデータレイアウトやプランの形状（例：OlapTableSink のバケットシャッフルレイアウト）を変える可能性があり、同時更新時に正確性や性能の後退を引き起こす恐れがあるためです。
* **デフォルト**: `false`
* **データタイプ**: boolean
* **導入バージョン**: v3.2.4

### enable_tablet_internal_parallel

* **説明**: タブレットの適応並列スキャンを有効にするかどうか。この機能を有効にすると、複数のスレッドを使用してタブレットをセグメントごとにスキャンし、スキャンの並行性を高めることができます。
* **デフォルト**: true
* **導入バージョン**: v2.3

### enable_topn_runtime_filter

* **説明**: TopN Runtime Filter を有効にするかどうか。この機能を有効にすると、ORDER BY LIMIT クエリに対して動的に Runtime Filter が構築され、Scan 段階にプッシュダウンされてフィルタリングに利用されます。
* **デフォルト**: true
* **導入バージョン**: v3.3

### enable_ukfk_opt

* **説明**: Unique-Key / Foreign-Key (UK/FK) に基づく変換と統計の拡張に対するオプティマイザのサポートを有効にします。有効にすると、オプティマイザは `UKFKConstraintsCollector` を実行して一番下から（bottom‑up に）ユニークキーおよび外部キー制約を収集し、それらを計画ノード（OptExpressions）に付加します。収集された制約は `PruneUKFKJoinRule`（結合の UK 側をプルーニングしたり、述語を UK から FK 列へ書き換え、外部結合の場合に IS NULL チェックを追加する）や `PruneUKFKGroupByKeysRule`（UK/FK 関係から導出される冗長な GROUP BY キーを削除する）などの変換ルールで利用されます。収集された UK/FK 情報は `StatisticsCalculator` において UK‑FK ジョインの結合基数推定をより厳密にするためにも使われ、より精密な場合はデフォルトの推定を置き換えることがあります。これらの最適化は宣言されたスキーマ制約に依存し、プランの形状や述語の配置を変更する可能性があるため、デフォルトは保守的に `false` です。
* **デフォルト**: `false`
* **スコープ**: セッション
* **データタイプ**: boolean
* **導入バージョン**: v3.2.4

### enable_view_based_mv_rewrite

* **説明**: ビューに基づくマテリアライズドビューのクエリ書き換えを有効にするかどうか。この項目が `true` に設定されている場合、ビューは統一されたノードとして使用され、クエリのパフォーマンスを向上させるために自身に対するクエリを書き換えます。この項目が `false` に設定されている場合、システムはビューに対するクエリを物理テーブルまたはマテリアライズドビューに対するクエリに書き換えます。
* **デフォルト**: false
* **導入バージョン**: v3.1.9, v3.2.5, v3.3.0

### enable_wait_dependent_event

* **説明**: パイプラインが、同じフラグメント内で依存するオペレーターの実行が完了するまで待機するかどうか。例えば、Left Join クエリにおいて、この機能が有効になっている場合、Probe side オペレーターは Build side オペレーターの実行が完了するまで待機し、その後実行を開始します。この機能を有効にするとメモリ使用量を削減できますが、クエリの遅延が増加する可能性があります。ただし、CTE で再利用されるクエリの場合、この機能を有効にするとメモリ使用量が増加する可能性があります。
* **デフォルト**: false
* **導入バージョン**: v3.3

### enable_write_hive_external_table

* **説明**: Hive の外部テーブルにデータをシンクすることを許可するかどうか。
* **デフォルト**: false
* **導入バージョン**: v3.2

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

### group_execution_max_groups

* **説明**: Group Execution で許可されるグループの最大数。分割の粒度を制限し、過剰なグループ数によるスケジューリングのオーバーヘッドを防ぐために使用されます。
* **デフォルト**: 128
* **導入バージョン**: v3.3

### group_execution_min_scan_rows

* **説明**: Group Execution におけるグループごとの最小処理行数。
* **デフォルト**: 5000000
* **導入バージョン**: v3.3

### hash_join_push_down_right_table

* **説明**: ジョインクエリで右テーブルに対するフィルタ条件を使用して左テーブルのデータをフィルタリングできるかどうかを制御するために使用されます。これにより、クエリ中に処理する必要のあるデータ量を削減できます。
* **デフォルト**: `true` は操作が許可され、システムが左テーブルをフィルタリングできるかどうかを決定します。`false` は操作が無効です。デフォルト値は `true` です。

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

### insert_max_filter_ratio

* **説明**: Files() からの INSERT の最大エラー許容率。データ品質が不十分なためにフィルタリングされるデータレコードの最大比率です。不適格なデータレコードの比率がこのしきい値に達すると、ジョブは失敗します。範囲: [0, 1]。
* **デフォルト**: 0
* **導入バージョン**: v3.4.0

### insert_timeout

* **説明**: INSERT ジョブのタイムアウト時間。単位: 秒。v3.4.0 以降、`insert_timeout` は INSERT に関与する操作（例: UPDATE、DELETE、CTAS、マテリアライズドビューのリフレッシュ、統計収集、PIPE）に適用され、`query_timeout` を置き換えます。
* **デフォルト**: 14400
* **導入バージョン**: v3.4.0

### interactive_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### interpolate_passthrough

* **説明**: 特定の演算子に対して local-exchange-passthrough を追加するかどうか。現在サポートされている演算子には streaming aggregates などがある。local-exchange を追加すると、データの偏りが計算に与える影響を軽減できるが、メモリ使用量がわずかに増加する。
* **デフォルト**: true
* **導入バージョン**: v3.2

### io_tasks_per_scan_operator

* **説明**: スキャンオペレーターによって発行される同時 I/O タスクの数。この値を増やすと、HDFS や S3 などのリモートストレージシステムにアクセスする際のレイテンシーが高い場合に役立ちます。ただし、値が大きいとメモリ消費が増加します。
* **デフォルト**: 4
* **データ型**: Int
* **導入バージョン**: v2.5

### jit_level

* **説明**: 式の JIT コンパイルが有効になるレベル。有効な値:
  * `1`: システムはコンパイル可能な式に対して適応的に JIT コンパイルを有効にします。
  * `-1`: JIT コンパイルはすべてのコンパイル可能な非定数式に対して有効です。
  * `0`: JIT コンパイルは無効です。この機能にエラーが発生した場合、手動で無効にすることができます。
* **デフォルト**: 1
* **データ型**: Int
* **導入バージョン**: -

### lake_bucket_assign_mode

* **説明**: データレイク内のテーブルに対するクエリにおけるバケット割り当てモード。この変数は、クエリ実行中に Bucket-aware 実行が有効になった際に、バケットがワーカーノードにどのように割り当てられるかを制御します。有効な値:
  * `balance`: ワーカーノードにバケットを均等に割り当て、バランスの取れたワークロードとより良いパフォーマンスを実現します。
  * `elastic`: 一貫性ハッシュを使用してバケットをワーカーノードに割り当て、弾力的な環境においてより良い負荷分散を実現できます。
* **デフォルト**: balance
* **タイプ**: String
* **導入バージョン**: v4.0

### language (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### license (global)

* **説明**: StarRocks のライセンスを表示します。
* **デフォルト**: Apache License 2.0

### load_mem_limit

インポート操作のメモリ制限を指定します。デフォルト値は 0 で、この変数は使用されず、`query_mem_limit` が代わりに使用されます。

この変数は、クエリとインポートの両方を含む `INSERT` 操作にのみ使用されます。ユーザーがこの変数を設定しない場合、クエリとインポートのメモリ制限は `exec_mem_limit` に設定されます。それ以外の場合、クエリのメモリ制限は `exec_mem_limit` に設定され、インポートのメモリ制限は `load_mem_limit` に設定されます。

他のインポート方法（`BROKER LOAD`、`STREAM LOAD` など）は引き続き `exec_mem_limit` をメモリ制限として使用します。

### log_rejected_record_num (v3.1 以降)

ログに記録できる不適格なデータ行の最大数を指定します。有効な値: `0`、`-1`、および任意の非ゼロ正の整数。デフォルト値: `0`。

* 値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。
* 値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。
* 非ゼロ正の整数 `n` は、フィルタリングされたデータ行が各 BE で最大 `n` 行までログに記録されることを指定します。

### low_cardinality_optimize_on_lake

* **デフォルト**: true
* **データ型**: Boolean
* **単位**: -
* **説明**: データレイクのクエリで低基数最適化を有効にするかどうか。有効な値：
  * `true` (デフォルト)：データレイクのクエリで低基数最適化を有効にします。
  * `false`：データレイクのクエリで低基数最適化を無効にします。
* **導入バージョン**: v3.5.0

### lower_case_table_names (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。StarRocks のテーブル名は大文字小文字を区別します。

### materialized_view_rewrite_mode (v3.2 以降)

非同期マテリアライズドビューのクエリ書き換えモードを指定します。有効な値:

* `disable`: 非同期マテリアライズドビューの自動クエリ書き換えを無効にします。
* `default`（デフォルト値）: 非同期マテリアライズドビューの自動クエリ書き換えを有効にし、オプティマイザがコストに基づいてクエリをマテリアライズドビューを使用して書き換えるかどうかを決定します。クエリが書き換えられない場合、ベーステーブルのデータを直接スキャンします。
* `default_or_error`: 非同期マテリアライズドビューの自動クエリ書き換えを有効にし、オプティマイザがコストに基づいてクエリをマテリアライズドビューを使用して書き換えるかどうかを決定します。クエリが書き換えられない場合、エラーが返されます。
* `force`: 非同期マテリアライズドビューの自動クエリ書き換えを有効にし、オプティマイザがマテリアライズドビューを使用してクエリを書き換えることを優先します。クエリが書き換えられない場合、ベーステーブルのデータを直接スキャンします。
* `force_or_error`: 非同期マテリアライズドビューの自動クエリ書き換えを有効にし、オプティマイザがマテリアライズドビューを使用してクエリを書き換えることを優先します。クエリが書き換えられない場合、エラーが返されます。

### materialized_view_subquery_text_match_max_count

* **説明**: クエリのサブクエリがマテリアライズドビューの定義と一致するかどうかをシステムがチェックする最大回数を指定します。
* **デフォルト**: 4
* **導入バージョン**: v3.2.5, v3.3.0

### max_allowed_packet

* **説明**: JDBC 接続プール C3P0 との互換性のために使用されます。この変数は、クライアントとサーバー間で送信できるパケットの最大サイズを指定します。
* **デフォルト**: 33554432 (32 MB)。クライアントが "PacketTooBigException" を報告する場合、この値を増やすことができます。
* **単位**: バイト
* **データ型**: Int

### max_pushdown_conditions_per_column

* **説明**: 列に対してプッシュダウンできる述語の最大数。
* **デフォルト**: -1、`be.conf` ファイルの値が使用されることを示します。この変数が 0 より大きい値に設定されている場合、`be.conf` の値は無視されます。
* **データ型**: Int

### max_scan_key_num

* **説明**: 各クエリによってセグメント化されるスキャンキーの最大数。
* **デフォルト**: -1、`be.conf` ファイルの値が使用されることを示します。この変数が 0 より大きい値に設定されている場合、`be.conf` の値は無視されます。

### metadata_collect_query_timeout

* **説明**: Iceberg Catalog メタデータ収集クエリのタイムアウト時間。
* **単位**: 秒
* **デフォルト**: 60
* **導入バージョン**: v3.3.3

### nested_mv_rewrite_max_level

* **説明**: クエリ書き換えに使用できるネストされたマテリアライズドビューの最大レベル。
* **値の範囲**: [1, +∞)。値 `1` は、ベーステーブルに作成されたマテリアライズドビューのみがクエリ書き換えに使用できることを示します。
* **デフォルト**: 3
* **データ型**: Int

### net_buffer_length

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### net_read_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### net_write_timeout

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### new_planner_optimize_timeout

* **説明**: クエリオプティマイザのタイムアウト時間。オプティマイザがタイムアウトすると、エラーが返され、クエリが停止され、クエリパフォーマンスに影響を与えます。この変数をクエリに基づいて大きな値に設定するか、StarRocks の技術サポートに連絡してトラブルシューティングを行うことができます。タイムアウトは、クエリにジョインが多すぎる場合に発生することがあります。
* **デフォルト**: 3000
* **単位**: ms

### optimizer_materialized_view_timelimit

* **説明**: マテリアライズドビューの書き換えルールが消費できる最大時間を指定します。しきい値に達すると、このルールはクエリ書き換えに使用されません。
* **デフォルト**: 1000
* **単位**: ms
* **導入バージョン**: v3.1.9, v3.2.5

### orc_use_column_names

* **説明**: StarRocks が Hive から ORC ファイルを読み取る際に列がどのように一致するかを指定するために使用されます。デフォルト値は `false` で、ORC ファイル内の列は Hive テーブル定義内の順序位置に基づいて読み取られます。この変数が `true` に設定されている場合、列は名前に基づいて読み取られます。
* **デフォルト**: false
* **導入バージョン**: v3.1.10

### parallel_exchange_instance_num

上位ノードが実行プランで下位ノードからデータを受け取るために使用するエクスチェンジノードの数を設定するために使用されます。デフォルト値は -1 で、エクスチェンジノードの数は下位ノードの実行インスタンスの数と等しいことを意味します。この変数が 0 より大きく、下位ノードの実行インスタンスの数より小さい場合、エクスチェンジノードの数は設定値と等しくなります。

分散クエリ実行プランでは、上位ノードには通常、下位ノードの実行インスタンスからデータを受け取るための 1 つ以上のエクスチェンジノードがあります。通常、エクスチェンジノードの数は下位ノードの実行インスタンスの数と等しいです。

集計クエリシナリオで、集計後にデータ量が大幅に減少する場合、この変数を小さい値に変更してリソースオーバーヘッドを削減することができます。例として、重複キーテーブルを使用して集計クエリを実行することが挙げられます。

### parallel_fragment_exec_instance_num

各 BE でノードをスキャンするために使用されるインスタンスの数を設定するために使用されます。デフォルト値は 1 です。

クエリプランは通常、一連のスキャン範囲を生成します。このデータは複数の BE ノードに分散されています。BE ノードには 1 つ以上のスキャン範囲があり、デフォルトでは各 BE ノードのスキャン範囲セットは 1 つの実行インスタンスによって処理されます。マシンリソースが十分な場合、この変数を増やして、より多くの実行インスタンスがスキャン範囲を同時に処理できるようにすることで効率を向上させることができます。

スキャンインスタンスの数は、上位レベルの他の実行ノード（集計ノードやジョインノードなど）の数を決定します。したがって、クエリプラン実行全体の並行性を高めます。この変数を変更することで効率を向上させることができますが、値が大きいとマシンリソース（CPU、メモリ、ディスク IO など）が多く消費されます。

### parallel_merge_late_materialization_mode

* **説明**: ソートの Parallel Merge の遅延マテリアライゼーションモード。有効な値：
  * `AUTO`
  * `ALWAYS`
  * `NEVER`
* **デフォルト**: `AUTO`
* **導入バージョン**: v3.3

### partial_update_mode

* **説明**: 部分更新のモードを制御するために使用されます。有効な値:

  * `auto`（デフォルト）: システムは UPDATE 文と関与する列を分析して部分更新のモードを自動的に決定します。
  * `column`: 部分更新にカラムモードを使用します。これは、少数の列と多数の行を含む部分更新に特に適しています。

  詳細については、[UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#partial-updates-in-column-mode-since-v31) を参照してください。
* **デフォルト**: auto
* **導入バージョン**: v3.1

### performance_schema (global)

MySQL JDBC バージョン 8.0.16 以降との互換性のために使用されます。実際の用途はありません。

### phased_scheduler_max_concurrency

* **説明**: フェーズドスケジューラーによるリーフノード フラグメントのスケジューリングにおける並列処理。例えば、デフォルト値は、多数の UNION ALL スキャン クエリにおいて、同時にスケジューリングされるスキャン フラグメントは最大で 2 つまで許可されることを意味します。
* **デフォルト**: 2
* **導入バージョン**: v3.3

### pipeline_dop

* **説明**: パイプラインインスタンスの並行性を指定し、クエリの並行性を調整します。デフォルト値: 0、システムが各パイプラインインスタンスの並行性を自動的に調整することを示します。この変数は、OLAP テーブルへのロードジョブの並列処理も制御します。この変数を 0 より大きい値に設定することもできます。一般的に、物理 CPU コア数の半分の値に設定します。v3.0 以降、StarRocks はクエリの並行性に基づいてこの変数を適応的に調整します。

* **デフォルト**: 0
* **データ型**: Int

### pipeline_profile_level

* **説明**: クエリプロファイルのレベルを制御します。クエリプロファイルには通常、フラグメント、フラグメントインスタンス、パイプライン、パイプラインドライバー、オペレーターの 5 つのレイヤーがあります。異なるレベルはプロファイルの異なる詳細を提供します：

  * 0: StarRocks はプロファイルのメトリクスを結合し、いくつかのコアメトリクスのみを表示します。
  * 1: デフォルト値。StarRocks はプロファイルを簡略化し、プロファイルのメトリクスを結合してプロファイルレイヤーを減らします。
  * 2: StarRocks はプロファイルのすべてのレイヤーを保持します。このシナリオでは、特に SQL クエリが複雑な場合、プロファイルサイズが大きくなります。この値は推奨されません。

* **デフォルト**: 1
* **データ型**: Int

### pipeline_sink_dop

* **説明**: Iceberg テーブルおよび Hive テーブルへのデータロードにおけるシンクの並列処理、ならびに INSERT INTO FILES() を使用したデータアンロードの並列処理。これらのロードジョブの同時実行数を調整するために使用されます。デフォルト値：0（システムが自動的に並列処理を調整することを示します）。この変数を 0 より大きい値に設定することも可能です。
* **デフォルト**: 0
* **データ型**: Int

### plan_mode

* **説明**: Iceberg Catalog のメタデータ取得戦略。詳細は [Iceberg Catalog metadata retrieval strategy](../data_source/catalog/iceberg/iceberg_catalog.md#appendix-periodic-metadata-refresh-strategy) を参照してください。有効な値:
  * `auto`: システムが自動的に取得プランを選択します。
  * `local`: ローカルキャッシュプランを使用します。
  * `distributed`: 分散プランを使用します。
* **デフォルト**: auto
* **導入バージョン**: v3.3.3

### populate_datacache_mode

* **説明**: 外部ストレージシステムからデータブロックを読み取る際の Data Cache のポピュレーション動作を指定します。有効な値:
  * `auto`（デフォルト）: システムはポピュレーションルールに基づいてデータを選択的にキャッシュします。
  * `always`: 常にデータをキャッシュします。
  * `never`: データをキャッシュしません。
* **デフォルト**: auto
* **導入バージョン**: v3.3.2

### prefer_compute_node

* **説明**: FEs がクエリ実行プランを CN ノードに配布するかどうかを指定します。有効な値:
  * `true`: FEs がクエリ実行プランを CN ノードに配布することを示します。
  * `false`: FEs がクエリ実行プランを CN ノードに配布しないことを示します。
* **デフォルト**: false
* **導入バージョン**: v2.4

### query_cache_agg_cardinality_limit

* **説明**: Query Cache における GROUP BY の基数の上限。GROUP BY によって生成される行がこの値を超える場合、Query Cache は有効になりません。デフォルト値: 5000000。`query_cache_entry_max_bytes` または `query_cache_entry_max_rows` が 0 に設定されている場合、関与するタブレットから計算結果が生成されていない場合でもパススルーモードが使用されます。
* **デフォルト**: 5000000
* **データ型**: Long
* **導入バージョン**: v2.5

### query_cache_entry_max_bytes

* **説明**: パススルーモードをトリガーするしきい値。特定のタブレットがアクセスするクエリの計算結果のバイト数または行数が `query_cache_entry_max_bytes` または `query_cache_entry_max_rows` で指定されたしきい値を超えると、クエリはパススルーモードに切り替わります。
* **有効な値**: 0 から 9223372036854775807 まで
* **デフォルト**: 4194304
* **単位**: バイト
* **導入バージョン**: v2.5

### query_cache_entry_max_rows

* **説明**: キャッシュできる行の上限。`query_cache_entry_max_bytes` の説明を参照してください。デフォルト値: 。
* **デフォルト**: 409600
* **導入バージョン**: v2.5

### query_cache_size (global)

MySQL クライアント互換性のために使用されます。実際の用途はありません。

### query_cache_type

JDBC 接続プール C3P0 との互換性のために使用されます。実際の用途はありません。

### query_mem_limit

* **説明**: 各 BE ノードでのクエリのメモリ制限を設定するために使用されます。デフォルト値は 0 で、制限がないことを意味します。この項目はパイプラインエンジンが有効になった後にのみ有効です。`Memory Exceed Limit` エラーが発生した場合、この変数を増やすことができます。`0` に設定すると、制限が課されないことを示します。
* **デフォルト**: 0
* **単位**: バイト

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

### range_pruner_max_predicate

* **説明**: 範囲パーティションプルーニングに使用できる IN 述語の最大数。デフォルト値: 100。100 より大きい値は、システムがすべてのタブレットをスキャンする可能性があり、クエリパフォーマンスを損なう可能性があります。
* **デフォルト**: 100
* **導入バージョン**: v3.0

### resource_group 

* **説明**: このセッションの指定されたリソースグループ
* **デフォルト**: ""
* **データタイプ**: String
* **導入バージョン**: 3.2.0

### runtime_filter_on_exchange_node

* **説明**: GRF が Exchange オペレーターを越えて下位レベルのオペレーターにプッシュダウンされた後、Exchange ノードに配置されるかどうか。デフォルト値は `false` で、GRF は Exchange オペレーターを越えて下位レベルのオペレーターにプッシュダウンされた後、Exchange ノードに配置されません。これにより、GRF の繰り返し使用が防止され、計算時間が短縮されます。

  ただし、GRF の配信は「最善を尽くす」プロセスです。下位レベルのオペレーターが GRF を受け取れない場合、GRF が Exchange ノードに配置されていないと、データがフィルタリングされず、フィルタパフォーマンスが損なわれます。`true` は、GRF が Exchange オペレーターを越えて下位レベルのオペレーターにプッシュダウンされた後でも、Exchange ノードに配置されることを意味します。

* **デフォルト**: false

### runtime_join_filter_push_down_limit

* **説明**: Bloom フィルターローカル RF が生成されるハッシュテーブルの最大行数。ローカル RF は、この値を超えると生成されません。この変数は、過度に長いローカル RF の生成を防ぎます。
* **デフォルト**: 1024000
* **データ型**: Int

### runtime_profile_report_interval

* **説明**: ランタイムプロファイルが報告される時間間隔。
* **デフォルト**: 10
* **単位**: 秒
* **データ型**: Int
* **導入バージョン**: v3.1.0

### scan_olap_partition_num_limit

* **説明**: 実行プランで単一のテーブルに対してスキャンされるパーティションの数。
* **デフォルト**: 0（制限なし）
* **導入バージョン**: v3.3.9

### skip_local_disk_cache

* **説明**: FE がスキャンレンジを構築するときに、各タブレットの内部スキャンレンジに `skip_disk_cache` をマークするよう指示するセッションフラグです。`true` に設定すると、`OlapScanNode.addScanRangeLocations()` は作成された `TInternalScanRange` オブジェクトに対して `internalRange.setSkip_disk_cache(true)` を設定するため、下流の BE スキャンノードはそのスキャンでローカルディスクキャッシュをバイパスするよう指示されます。この設定はセッション単位で適用され、プラン／スキャンレンジ構築時に評価されます。ページキャッシュスキップの制御には `skip_page_cache` と組み合わせて使用し、データキャッシュ関連の変数（`enable_scan_datacache` / `enable_populate_datacache`）と併せて適切に利用してください。
* **スコープ**: セッション
* **デフォルト**: `false`
* **タイプ**: boolean
* **導入バージョン**: v3.3.9, v3.4.0, v3.5.0

### spill_encode_level

* **スコープ**: セッション
* **説明**: オペレータのスピルファイルに適用されるエンコード／圧縮の振る舞いを制御します。整数はビットフラグレベルで、意味は `transmission_encode_level` と同様です:
  * bit 1 (値 `1`) — 適応エンコーディングを有効にする;
  * bit 2 (値 `2`) — 整数のような列を streamvbyte でエンコードする;
  * bit 4 (値 `4`) — バイナリ／文字列列を LZ4 で圧縮する。
  関連する `transmission_encode_level` のコメントからの例の意味: `7` は数値と文字列の適応エンコーディングを有効にし、`6` は数値と文字列のエンコードを強制します。この値を変更するとスピル時の CPU とディスク I/O のトレードオフが調整されます（エンコードレベルが高いほど CPU 負荷は増えるがスピルサイズ／I/O は減少します）。
  `SessionVariable.java` のセッション変数として注釈された `SPILL_ENCODE_LEVEL`（getter `getSpillEncodeLevel()`）として実装されており、`spill_mem_table_size` のような他のスピル調整可能項目の近くに文書化されています。
* **デフォルト**: `7`
* **タイプ**: int
* **導入バージョン**: v3.2.0

### spill_mode (3.0 以降)

中間結果のスピルの実行モード。有効な値:

* `auto`: メモリ使用量のしきい値に達したときにスピルが自動的にトリガーされます。
* `force`: StarRocks はメモリ使用量に関係なく、すべての関連オペレーターに対してスピルを強制的に実行します。

この変数は、変数 `enable_spill` が `true` に設定されている場合にのみ有効です。

### spill_storage_volume

* **説明**: スピルをトリガーしたクエリの中間結果を保存するために使用するストレージボリューム。詳細については、[Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage) を参照してください。
* **デフォルト**: 空の文字列
* **導入バージョン**: v3.3.0

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

### streaming_preaggregation_mode

GROUP BY の最初のフェーズの事前集計モードを指定するために使用されます。最初のフェーズでの事前集計効果が満足できない場合、ストリーミングモードを使用できます。これは、データをストリーミング先に送信する前に簡単なデータシリアル化を行います。有効な値：

* `auto`: システムは最初にローカル事前集計を試みます。効果が満足できない場合、ストリーミングモードに切り替えます。これはデフォルト値です。
* `force_preaggregation`: システムは直接ローカル事前集計を行います。
* `force_streaming`: システムは直接ストリーミングを行います。

### system_time_zone

現在のシステムのタイムゾーンを表示するために使用されます。変更できません。

### time_zone

現在のセッションのタイムゾーンを設定するために使用されます。タイムゾーンは、特定の時間関数の結果に影響を与える可能性があります。

### transaction_read_only

* **説明**: MySQL 5.8 互換性のために使用されます。エイリアスは `tx_read_only` です。この変数はトランザクションのアクセスモードを指定します。`ON` は読み取り専用を示し、`OFF` は読み取りおよび書き込み可能を示します。
* **デフォルト**: OFF
* **導入バージョン**: v2.5.18, v3.0.9, v3.1.7

### tx_isolation

MySQL クライアント互換性のために使用されます。実際の用途はありません。エイリアスは `transaction_isolation` です。

### use_compute_nodes

* **説明**: 使用できる CN ノードの最大数。この変数は `prefer_compute_node=true` の場合に有効です。有効な値：

  * `-1`: すべての CN ノードが使用されることを示します。
  * `0`: CN ノードが使用されないことを示します。
* **デフォルト**: -1
* **データ型**: Int
* **導入バージョン**: v2.4

### version (global)

クライアントに返される MySQL サーバーバージョン。値は FE パラメータ `mysql_server_version` と同じです。

### version_comment (global)

StarRocks のバージョン。変更できません。

### wait_timeout

* **説明**: サーバーが非対話型接続でアクティビティを待機する秒数。この時間が経過してもクライアントが StarRocks と対話しない場合、StarRocks は接続を積極的に閉じます。
* **デフォルト**: 28800（8 時間）。
* **単位**: 秒
* **データ型**: Int


