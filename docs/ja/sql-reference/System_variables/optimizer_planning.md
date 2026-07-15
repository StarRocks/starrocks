---
displayed_sidebar: docs
sidebar_label: "クエリオプティマイザとプランニング"
sidebar_position: 1
description: "コストベースオプティマイザ、統計情報に基づくプランニング、プルーニング、リライト、CTE 再利用を制御するセッション変数。"
---

# システム変数 - クエリオプティマイザとプランニング

変数の表示および設定方法については、[システム変数の概要](../System_variable.md) を参照してください。

### array_low_cardinality_optimize

* **スコープ**: Session
* **説明**: オプティマイザが `array<varchar>` カラムを low-cardinality（辞書ベース）のデコードおよび関連最適化の対象として検討するかどうかを制御します。有効にすると、オプティマイザの low-cardinality ルール（例: `DecodeCollector`）は辞書カラムを定義し、型が `varchar` または `array<varchar>` の式に対して辞書デコードを適用することがあります。無効にすると、スカラーの `varchar` カラムのみが対象となり、`array<varchar>` 型はこれらの low-cardinality 最適化によって無視されます。この変数は配列サポートの判定に `DecodeCollector.supportAndEnabledLowCardinality(...)` によって読み取られ、`SessionVariable` の getter/setter を介して公開されます。
* **デフォルト**: `true`
* **タイプ**: boolean
* **導入バージョン**: v3.3.0, v3.4.0, v3.5.0

### cbo_cte_force_reuse_limit_without_order_by

* **説明**: 定義に `ORDER BY` のない `LIMIT` を含む共通テーブル式（CTE）をオプティマイザがどう扱うかを制御します。`true`（デフォルト）の場合、オプティマイザ（`ForceCTEReuseRule`）はそのような CTE の再利用を強制し、すべてのコンシューマが同じ行を読み取るようにして、順序付けされていない `LIMIT` の再評価による非決定的な結果を回避します。`false` の場合、オプティマイザは代わりに CTE をインライン化することがあり、コンシューマ間で結果が一致しない可能性があります。
* **スコープ**: Session
* **デフォルト**: `true`
* **タイプ**: Boolean
* **導入バージョン**: v3.5.10、v4.0.3、v4.1.0

### cbo_cte_force_reuse_node_count

* **説明**: Common Table Expressions (CTE) に対するオプティマイザのショートカットを制御するセッションスコープの閾値。RelationTransformer.visitCTE 内でプランナは CTE プロデューサーツリーのノード数をカウントします（cteContext.getCteNodeCount）。そのカウントがこの閾値以上かつ閾値が 0 より大きい場合、transformer は CTE の再利用を強制します：プロデューサープランのインライン化／変換をスキップし、事前計算された式マッピング（入力なし）で consume 演算子を構築し、代わりに生成されたカラム参照を使用します。これにより非常に大きな CTE プロデューサーツリーに対するオプティマイザの時間を削減できますが、物理プランの最適性が若干低下する可能性があります。値を `0` に設定すると force-reuse 最適化が無効になります。この変数は SessionVariable に getter/setter があり、セッション単位で適用されます。
* **スコープ**: Session
* **デフォルト**: `2000`
* **タイプ**: int
* **導入バージョン**: v3.5.3

### cbo_cte_max_limit

* **説明**: オプティマイザが CTE 再利用の対象として考慮する、1 クエリ内の共通テーブル式（CTE）の数を制限します。この値はプランニング中に CTE transformer（`CTETransformerContext`）へ渡されます。この上限を超える数の CTE を含むクエリは、再利用ベースのプランではなくインライン化にフォールバックします。値を大きくすると、より多くの CTE を含むクエリにも CTE 再利用を適用できますが、プランニング時間が長くなります。
* **スコープ**: Session
* **デフォルト**: `10`
* **タイプ**: int
* **導入バージョン**: v2.5.0

### cbo_cte_reuse

* **説明**: オプティマイザが共通テーブル式（CTE）を再利用して multi-distinct 集計クエリを書き換えることを許可するかを制御します（CBO の CTE 再利用リライト）。有効にすると、プランナー（RewriteMultiDistinctRule）はマルチカラム DISTINCT、スキューした集計、または統計により CTE リライトの方が効率的であると示される場合に CTE ベースのリライトを選択することがあり、`prefer_cte_rewrite` ヒントを尊重します。無効にすると CTE ベースのリライトは許可されず、プランナーは multi-function リライトを試みます。クエリが CTE を必要とする場合（例：マルチカラム DISTINCT や multi-function リライトで扱えない関数など）は、プランナーはユーザーエラーを発生させます。注意：オプティマイザが参照する実際の設定はこのフラグとパイプラインエンジンフラグの論理 AND です — すなわち `isCboCteReuse()` はこの変数と `enablePipelineEngine` の両方を返す必要があるため、`enablePipelineEngine` がオンのときのみ CTE 再利用が有効になります。
* **デフォルト**: `true`
* **タイプ**: Boolean
* **導入バージョン**: `v3.2.0`

### cbo_cte_reuse_rate

* **説明**: 共通テーブル式（CTE）を再利用する方がインライン化よりもコストが低いかどうかをオプティマイザが判断する際に使用するコスト比率を制御します。コストベースで判断する場合、この値が大きいほど CTE 再利用の推定コストが高くなり（`CostModel` では CTE アンカーのメモリコストは `produce_size * (1 + rate)` として推定されます）、オプティマイザは CTE を再利用しにくくなります。この値には次の 2 つの特殊なケースがあります。
  * `< 0`：CTE 再利用を無効にし、常に CTE をインライン化します。
  * `0`：常に CTE を再利用します。
  * `> 0`：この比率で重み付けしたコストに基づいて、再利用とインライン化を選択します。

  内部実装名は `cbo_cte_reuse_rate_v2` です。`cbo_cte_reuse_rate` は `SHOW VARIABLES` で表示され、変数の設定に使用される名前です。
* **スコープ**: Session
* **デフォルト**: `1.15`
* **タイプ**: double
* **導入バージョン**: v2.2.0

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

### cbo_max_reorder_node_use_dp

* **説明**: コストベースオプティマイザ（CBO）が DP（動的計画法）join-reorder アルゴリズムを含めるかを制御するセッションスコープの上限です。オプティマイザは join 入力数（MultiJoinNode.atoms.size()）をこの値と比較し、`multiJoinNode.getAtoms().size() <= cbo_max_reorder_node_use_dp` かつ `cbo_enable_dp_join_reorder` が有効な場合にのみ DP reorder を実行または追加します。JoinReorderFactory.createJoinReorderAdaptive（候補アルゴリズムに JoinReorderDP を追加するため）および ReorderJoinRule.transform/rewrite（メモにプランをコピーするときに JoinReorderDP を実行するか決定するため）で使用されます。デフォルト値の 10 は実用的な性能のカットオフを反映しています（コード内コメント: "10 table join reorder takes more than 100ms"）。この設定はオプティマイザの実行時間（DP は高コスト）と大規模な multi-join クエリに対するプラン品質のトレードオフを調整するためにチューニングしてください。`cbo_enable_dp_join_reorder` および 貪欲法の閾値 `cbo_max_reorder_node_use_greedy` と相互作用します。比較は包含比較（`<=`）です。
* **スコープ**: セッション
* **デフォルト**: `10`
* **データタイプ**: long
* **導入バージョン**: `v3.2.0`

### cbo_max_reorder_node_use_exhaustive

* **スコープ**: セッション
* **説明**: CBO における join-reorder アルゴリズム選択の閾値を制御します。オプティマイザはクエリ内の inner/cross join ノードをカウントし、その数がこの値より大きい場合、プランナは transform ベースの（より積極的な）reorder パスを取ります：CTE 統計の収集を強制し、ReorderJoinRule.transform および関連する可換性ルールを呼び出します。カウントがこの値以下の場合、プランナはより安価な join-transformation ルールを適用します（特定の semi/anti-join ケースでは INNER_JOIN_LEFT_ASSCOM_RULE を追加することがあります）。このセッション変数はオプティマイザ（`SPMOptimizer`, `QueryOptimizer`）で参照され、セッションレベルでは `setMaxTransformReorderJoins` を介して設定できます。
* **デフォルト**: `4`
* **データタイプ**: int
* **導入バージョン**: v3.2.0

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

### enable_cbo_table_prune

* **説明**: 有効にすると、オプティマイザはメモ最適化中に CBO テーブルプルーニングルール（CboTablePruneRule）を追加して、カーディナリティを保持する結合に対するコストベースのテーブルプルーニングを行います。ルールはオプティマイザ内で条件付きに追加されます（QueryOptimizer.memoOptimize および SPMOptimizer.memoOptimize を参照）— 結合ツリー内の結合ノード数が小さい場合（10 未満の結合ノード）にのみ追加されます。このオプションはルールベースのプルーニング切替 `enable_rbo_table_prune` を補完し、Cost-Based Optimizer が結合処理から不要なテーブルや入力を取り除いてプランと実行の複雑さを軽減できるようにします。プルーニングはプラン形状を変更する可能性があるためデフォルトはオフです。代表的なワークロードで検証してから有効化してください。
* **スコープ**: Session
* **デフォルト**: `false`
* **タイプ**: boolean
* **導入バージョン**: v3.2.0

### enable_reduce_cast_varchar_length_inheritance (global)

* **説明**: `ReduceCastRule` が同一型の `VARCHAR -> VARCHAR` cast を削除する際に、ターゲットの `VARCHAR(N)` 長を保持するかどうかを制御します。これを有効にすると、`CAST(col AS VARCHAR(N))` のような文で prepare と execute の結果セットメタデータを一致させることができます。
* **デフォルト**: false
* **データ型**: Boolean
* **導入バージョン**: v3.5.16, v4.0.9

### enable_reduce_cast_varchar_expr_sync_type (global)

* **説明**: `ReduceCastRule` が同一型の `VARCHAR -> VARCHAR` cast を削除した後、再利用される planner `Expr` の `type` と `originType` を書き換え後の `VARCHAR(N)` に同期するかどうかを制御します。
* **デフォルト**: true
* **データ型**: Boolean
* **導入バージョン**: v3.5.16, v4.0.9

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

### enable_spm_rewrite

* **説明**: SQL Plan Manager (SPM) クエリ改写を有効にするかどうか。有効にすると、StarRocks は自動的にクエリを拘束されたクエリプランに改写し、クエリのパフォーマンスと安定性を向上させます。
* **デフォルト**: false

### enable_table_prune_on_update

* **説明**: セッションレベルのブールで、オプティマイザが UPDATE 文に対して主キー固有のテーブルプルーニング規則を適用するかどうかを制御します。有効にすると、QueryOptimizer（pruneTables ステージ中）が `PrimaryKeyUpdateTableRule` を呼び出して更新プランを書き換え／プルーニングし、主キー更新パターンに対するプルーニングを改善する可能性があります。このフラグは rule-based/CBO によるテーブルプルーニングが有効な場合にのみ効果があります（`enable_rbo_table_prune` を参照）。デフォルトで無効になっているのは、この変換がデータレイアウトやプランの形状（例：OlapTableSink のバケットシャッフルレイアウト）を変える可能性があり、同時更新時に正確性や性能の後退を引き起こす恐れがあるためです。
* **デフォルト**: `false`
* **データタイプ**: boolean
* **導入バージョン**: v3.2.4

### enable_ukfk_opt

* **説明**: Unique-Key / Foreign-Key (UK/FK) に基づく変換と統計の拡張に対するオプティマイザのサポートを有効にします。有効にすると、オプティマイザは `UKFKConstraintsCollector` を実行して一番下から（bottom‑up に）ユニークキーおよび外部キー制約を収集し、それらを計画ノード（OptExpressions）に付加します。収集された制約は `PruneUKFKJoinRule`（結合の UK 側をプルーニングしたり、述語を UK から FK 列へ書き換え、外部結合の場合に IS NULL チェックを追加する）や `PruneUKFKGroupByKeysRule`（UK/FK 関係から導出される冗長な GROUP BY キーを削除する）などの変換ルールで利用されます。収集された UK/FK 情報は `StatisticsCalculator` において UK‑FK ジョインの結合基数推定をより厳密にするためにも使われ、より精密な場合はデフォルトの推定を置き換えることがあります。これらの最適化は宣言されたスキーマ制約に依存し、プランの形状や述語の配置を変更する可能性があるため、デフォルトは保守的に `false` です。
* **デフォルト**: `false`
* **スコープ**: セッション
* **データタイプ**: boolean
* **導入バージョン**: v3.2.4

### low_cardinality_optimize_on_lake

* **デフォルト**: true
* **データ型**: Boolean
* **単位**: -
* **説明**: データレイクのクエリで低基数最適化を有効にするかどうか。有効な値：
  * `true` (デフォルト)：データレイクのクエリで低基数最適化を有効にします。
  * `false`：データレイクのクエリで低基数最適化を無効にします。
* **導入バージョン**: v3.5.0

### low_cardinality_optimize_v2

* **スコープ**: Session
* **説明**: オプティマイザが適用する low-cardinality 最適化のどのリライトを選択するかを制御するセッションレベルの boolean。`true` の場合、オプティマイザは `LowCardinalityRewriteRule` によって実装された新しい V2 リライト（`DecodeCollector` / `DecodeRewriter` を使用して low-cardinality な VARCHAR カラムをエンコード/デコード）を試行します。`false` の場合、オプティマイザは `AddDecodeNodeForDictStringRule` によって実装されたレガシーなリライトにフォールバックします。どちらのリライトを実行する場合でも `enableLowCardinalityOptimize` が有効である必要があり、`enableLowCardinalityOptimize` が無効な場合は low-cardinality のリライトは実行されません。この変数は適切な変換を選択するためにオプティマイザのツリー書き換え経路でチェックされます。
* **デフォルト**: `true`
* **タイプ**: boolean
* **導入バージョン**: v3.3.0, v3.4.0, v3.5.0

### new_planner_optimize_timeout

* **説明**: クエリオプティマイザのタイムアウト時間。オプティマイザがタイムアウトすると、エラーが返され、クエリが停止され、クエリパフォーマンスに影響を与えます。この変数をクエリに基づいて大きな値に設定するか、StarRocks の技術サポートに連絡してトラブルシューティングを行うことができます。タイムアウトは、クエリにジョインが多すぎる場合に発生することがあります。
* **デフォルト**: 3000
* **単位**: ms

### plan_mode

* **説明**: Iceberg Catalog のメタデータ取得戦略。詳細は [Iceberg Catalog metadata retrieval strategy](../data_source/catalog/iceberg/iceberg_catalog.md#appendix-periodic-metadata-refresh-strategy) を参照してください。有効な値:
  * `auto`: システムが自動的に取得プランを選択します。
  * `local`: FE がローカルで Iceberg manifest ファイルを解析し、解析しながら scan range を段階的に BE に配信します。すべての manifest の解析完了を待つ必要がなく、メモリ使用量と初回レイテンシを削減できます。
  * `distributed`: manifest の解析を複数の BE に分散して並列処理しますが、FE はすべての BE の結果が揃うまで scan range を配信できません。manifest ファイルが多い大規模テーブルでは、メモリ使用量の増大と待ち時間の長期化を招く可能性があります。FE の CPU がボトルネックになっている場合にのみ推奨します。
* **デフォルト**: local（v3.5 より `auto` から変更。v3.5 以降、増分 scan range 配信がデフォルトで有効になったため、`local` モードはほとんどのワークロードで `distributed` より低メモリ・低レイテンシで動作します）
* **導入バージョン**: v3.3.3

### prefer_cte_rewrite

* **説明**: multi-distinct 集計クエリに対して、オプティマイザが共通テーブル式（CTE）ベースの書き換えを優先するかを制御します。`true` かつ `cbo_cte_reuse` も有効な場合、`RewriteMultiDistinctRule` は複数の distinct 集計を含むクエリに対して multi-function 書き換えではなく CTE ベースの書き換えを選択します。`false`（デフォルト）の場合、オプティマイザは通常のコスト／統計情報のヒューリスティックに基づいて、CTE 書き換えと multi-function 書き換えのどちらを使うかを判断します。
* **スコープ**: Session
* **デフォルト**: `false`
* **タイプ**: Boolean
* **導入バージョン**: v3.2.0

### range_pruner_max_predicate

* **説明**: 範囲パーティションプルーニングに使用できる IN 述語の最大数。デフォルト値: 100。100 より大きい値は、システムがすべてのタブレットをスキャンする可能性があり、クエリパフォーマンスを損なう可能性があります。
* **デフォルト**: 100
* **導入バージョン**: v3.0

