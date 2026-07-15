---
displayed_sidebar: docs
sidebar_label: "結合・集計・マテリアライズドビュー"
sidebar_position: 2
description: "結合の実行、集計、Top-N、マテリアライズドビューのクエリリライトに関するセッション変数。"
---

# システム変数 - 結合・集計・マテリアライズドビュー

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### count_distinct_column_buckets

* **説明**: グループバイカウントディスティンクトクエリでの COUNT DISTINCT 列のバケット数。この変数は `enable_distinct_column_bucketization` が `true` に設定されている場合にのみ有効です。
* **デフォルト**: 1024
* **導入バージョン**: v2.5

### disable_colocate_join

* **説明**: Colocation Join を有効にするかどうかを制御するために使用されます。デフォルト値は `false` で、機能が有効です。この機能が無効になっている場合、クエリプランニングは Colocation Join を実行しようとしません。
* **デフォルト**: false

### disable_join_reorder

* **スコープ**: Session
* **説明**: コストベースオプティマイザが結合の並べ替え（join reordering）を行うかどうかを制御します。`false`（デフォルト）では、オプティマイザは新しいプランナ経路（`SPMOptimizer` や `QueryOptimizer` に見られる）における論理最適化中に結合並べ替え変換（例：`ReorderJoinRule`、join transformation や outer-join transformation ルール）を適用する場合があります。`true` の場合、結合の並べ替えおよび関連する outer-join reorder ルールはスキップされ、オプティマイザが結合順序を変更することを防ぎます。これは最適化時間を短縮したり、安定／再現可能な結合順序を得たり、CBO の並べ替えが非最適なプランを生成するケースを回避するのに有用です。この設定は `cbo_max_reorder_node`、`cbo_max_reorder_node_use_exhaustive`、`enable_outer_join_reorder` のような他の CBO／セッション制御と相互作用します。
* **デフォルト**: `false`
* **データタイプ**: boolean
* **導入バージョン**: v3.2.0

### enable_cbo_based_mv_rewrite
* **説明**: CBO フェーズでマテリアライズドビューの書き換えを有効にするかどうか。これにより、クエリ書き換えの成功率を最大化できます（例：マテリアライズドビューとクエリの結合順序が異なる場合）が、オプティマイザフェーズの実行時間が増加します。
* **デフォルト**: true
* **導入バージョン**: v3.5.5, v4.0.1

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

### enable_group_by_compressed_key

* **説明**: GROUP BY キー列を圧縮するために正確な統計情報を使用するかどうか。有効な値: `true` と `false`。
* **デフォルト**: true
* **導入バージョン**: v4.0

### enable_incremental_mv

* **説明**: セッションフラグで、サーバーが増分リフレッシュを使用するマテリアライズドビューに対してプランを生成し、インメモリのプランを保持するかを制御します。有効にすると、`MaterializedViewAnalyzer.planMVQuery` はリフレッシュスキームが `IncrementalRefreshSchemeDesc` である create-MV ステートメントに対して処理を行います：ビュークエリの論理・物理プランを構築し、セッションの `enableMVPlanner` フラグを設定します（`setMVPlanner(true)`）。無効にすると、増分リフレッシュ MV のプラン作成はスキップされます。`SessionVariable` の `isEnableIncrementalRefreshMV()` および `setEnableIncrementalRefreshMv(boolean)` からアクセス可能です。
* **スコープ**: セッション（接続ごと）
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

### enable_partition_hash_join

* **説明**: 適応型 Partition Hash Join を有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.4

### enable_sort_aggregate

* **説明**: ソートされたストリーミングを有効にするかどうかを指定します。`true` はソートされたストリーミングが有効で、データストリーム内のデータをソートします。
* **デフォルト**: false
* **導入バージョン**: v2.5

### enable_sync_materialized_view_rewrite

* **説明**: 同期マテリアライズドビューに基づくクエリの書き換えを有効にするかどうか。
* **デフォルト**: true
* **導入バージョン**: v3.1.11, v3.2.5

### enable_view_based_mv_rewrite

* **説明**: ビューに基づくマテリアライズドビューのクエリ書き換えを有効にするかどうか。この項目が `true` に設定されている場合、ビューは統一されたノードとして使用され、クエリのパフォーマンスを向上させるために自身に対するクエリを書き換えます。この項目が `false` に設定されている場合、システムはビューに対するクエリを物理テーブルまたはマテリアライズドビューに対するクエリに書き換えます。
* **デフォルト**: false
* **導入バージョン**: v3.1.9, v3.2.5, v3.3.0

### hash_join_push_down_right_table

* **説明**: ジョインクエリで右テーブルに対するフィルタ条件を使用して左テーブルのデータをフィルタリングできるかどうかを制御するために使用されます。これにより、クエリ中に処理する必要のあるデータ量を削減できます。
* **デフォルト**: `true` は操作が許可され、システムが左テーブルをフィルタリングできるかどうかを決定します。`false` は操作が無効です。デフォルト値は `true` です。

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

### nested_mv_rewrite_max_level

* **説明**: クエリ書き換えに使用できるネストされたマテリアライズドビューの最大レベル。
* **値の範囲**: [1, +∞)。値 `1` は、ベーステーブルに作成されたマテリアライズドビューのみがクエリ書き換えに使用できることを示します。
* **デフォルト**: 3
* **データ型**: Int

### optimizer_materialized_view_timelimit

* **説明**: マテリアライズドビューの書き換えルールが消費できる最大時間を指定します。しきい値に達すると、このルールはクエリ書き換えに使用されません。
* **デフォルト**: 1000
* **単位**: ms
* **導入バージョン**: v3.1.9, v3.2.5

### streaming_preaggregation_mode

GROUP BY の最初のフェーズの事前集計モードを指定するために使用されます。最初のフェーズでの事前集計効果が満足できない場合、ストリーミングモードを使用できます。これは、データをストリーミング先に送信する前に簡単なデータシリアル化を行います。有効な値：

* `auto`: システムは最初にローカル事前集計を試みます。効果が満足できない場合、ストリーミングモードに切り替えます。これはデフォルト値です。
* `force_preaggregation`: システムは直接ローカル事前集計を行います。
* `force_streaming`: システムは直接ストリーミングを行います。

