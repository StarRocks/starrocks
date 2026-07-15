---
displayed_sidebar: docs
sidebar_label: "连接、聚合与物化视图"
sidebar_position: 2
description: "用于连接执行、聚合、Top-N 以及物化视图查询改写的会话变量。"
---

# 系统变量 - 连接、聚合与物化视图

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### count_distinct_column_buckets

* 描述：group-by-count-distinct 查询中为 count distinct 列设置的分桶数。该变量只有在 `enable_distinct_column_bucketization` 设置为 `true` 时才会生效。
* 默认值：1024
* 引入版本：v2.5

### disable_colocate_join

* 描述：控制是否启用 Colocate Join 功能。默认值为 false，表示启用该功能。true 表示禁用该功能。当该功能被禁用后，查询规划将不会尝试执行 Colocate Join。
* 默认值：false

### disable_join_reorder

* **范围**: Session
* **描述**: 控制基于代价的优化器是否执行连接重排序。当 `false`（默认）时，优化器可能在新规划路径的逻辑优化阶段应用连接重排序转换（例如 `ReorderJoinRule`、join transformation 和 outer-join transformation 规则），这些路径见于 `SPMOptimizer` 和 `QueryOptimizer`。当为 `true` 时，会跳过连接重排序及相关的外连接重排序规则，阻止优化器改变连接顺序。此选项有助于减少优化时间、获得稳定/可复现的连接顺序，或规避 CBO 重排序产生次优计划的情况。此设置会与其他 CBO/会话控制项交互，例如 `cbo_max_reorder_node`、`cbo_max_reorder_node_use_exhaustive` 和 `enable_outer_join_reorder`。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_cbo_based_mv_rewrite

* 描述：是否在 CBO 阶段启用物化视图改写，这可以最大化查询改写成功的可能性（例如，当物化视图和查询之间的连接顺序不同时），但这会增加优化器阶段的执行时间。
* 默认值：true
* 引入版本：v3.5.5，v4.0.1

### enable_distinct_agg_over_window

* **描述**: 控制优化器重写，将带有 WINDOW 子句的 DISTINCT 聚合调用转换为等价的基于 join 的计划。启用时（`true`，默认值），QueryOptimizer.invoke convertDistinctAggOverWindowToNullSafeEqualJoin 将会：
  * 检测包含 LogicalWindowOperator 的查询，
  * 运行投影合并（project-merge）重写，并推导逻辑属性，
  * 应用 DistinctAggregationOverWindowRule 将 DISTINCT-OVER-WINDOW 模式转换为 null-safe 等值连接（改变计划形状以便进一步下推和聚合优化），
  * 然后运行 SeparateProjectRule 并重新推导属性。
  禁用时（`false`），优化器跳过此转换，保留窗口上的 DISTINCT 聚合不变。该设置是会话级的，仅影响优化器重写阶段（参见 QueryOptimizer.convertDistinctAggOverWindowToNullSafeEqualJoin）。
* **默认值**: `true`
* **类型**: boolean
* **引入版本**: -

### enable_distinct_column_bucketization

* 描述：是否在 group-by-count-distinct 查询中开启对 count distinct 列的分桶优化。在类似 `select a, count(distinct b) from t group by a;` 的查询中，如果 group by 列 a 为低基数列，count distinct 列 b 为高基数列且发生严重数据倾斜时，会引发查询性能瓶颈。可以通过对 count distinct 列进行分桶来平衡数据，规避数据倾斜。

  该变量需要与 `count_distinct_column_buckets` 配合使用。

  您也可以通过添加 `skew` hint 来开启 count distinct 列的分桶优化，例如 `select a,count(distinct [skew] b) from t group by a;`。
* 默认值：false，表示不开启。
* 引入版本：v2.5

### enable_force_rule_based_mv_rewrite

* 描述：在优化器的 RBO（rule-based optimization）阶段是否针对多表查询启用查询改写。启用此功能将提高查询改写的鲁棒性。但如果查询未命中物化视图，则会增加优化耗时。
* 默认值：true
* 引入版本：v3.3

### enable_group_by_compressed_key

* 描述：是否利用准确的统计信息来压缩 GROUP BY Key 列。有效值：`true` 和 `false`。
* 默认值：true
* 引入版本：v4.0

### enable_incremental_mv

* **描述**: 会话变量，用于控制服务器是否会为使用增量刷新（incremental refresh）的物化视图规划并保留内存中的计划。当启用时，对于刷新方案为增量刷新的物化视图创建语句，系统会为视图查询构建逻辑和物理计划并设置会话的 `enableMVPlanner` 标志（`setMVPlanner(true)`）。禁用时，增量刷新物化视图的规划将被跳过。
* **范围**: Session（每连接）
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_materialized_view_agg_pushdown_rewrite

* 描述：是否为物化视图查询改写启用聚合函数下推。如果设置为 `true`，聚合函数将在查询执行期间下推至 Scan Operator，并在执行 Join Operator 之前被物化视图改写。此举可以缓解 Join 操作导致的数据膨胀，从而提高查询性能。有关此功能的具体场景和限制的详细信息，请参见 [聚合函数下推](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#聚合下推)。
* 默认值：false
* 引入版本：v3.3.0

### enable_materialized_view_for_insert

* 描述：是否允许 StarRocks 改写 INSERT INTO SELECT 语句中的查询。
* 默认值：false，即默认关闭该场景下的物化视图查询改写。
* 引入版本：v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_text_match_rewrite

* 描述：是否启用基于文本的物化视图改写。当此项设置为 `true` 时，优化器将查询与现有的物化视图进行比较。如果物化视图定义的抽象语法树与查询或其子查询的抽象语法树匹配，则会对查询进行改写。
* 默认值：true
* 引入版本：v3.2.5，v3.3.0

### enable_materialized_view_union_rewrite

* 描述：是否启用物化视图 UNION 改写。如果此项设置为 true，则系统在物化视图的谓词不能满足查询的谓词时，会尝试使用 UNION ALL 来补偿谓词。
* 默认值：true
* 引入版本：v2.5.20，v3.1.9，v3.2.7，v3.3.0

### enable_mv_planner

* **范围**: Session
* **描述**: 启用后，为当前 Session 激活 Materialized View (MV) planner 模式。在此模式下，优化器将：
  - 使用物化视图专用规则集，替代常规的 Join 实现规则。
  - 允许流（stream）实现规则生效。
  - 在逻辑计划转换过程中改变 Scan/Operator 的构造（例如，当启用 MV planner 时，RelationTransformer 会为原生表/物化视图选择 `LogicalBinlogScanOperator`）。
  - 禁用或绕过一些标准转换（例如，当 MV planner 开启时，`SplitMultiPhaseAggRule.check` 返回 false）。
  Materialized view 规划代码（MaterializedViewAnalyzer）在 MV 规划工作前后会设置该标志（在规划前设为 true，之后重置为 false），因此主要用于 MV 计划生成和测试。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_partition_hash_join

* 描述: 是否启用自适应 Partition Hash Join。
* 默认值: true
* 引入版本: v3.4

### enable_sort_aggregate

* 描述：是否开启 sorted streaming 聚合。`true` 表示开启 sorted streaming 聚合功能，对流中的数据进行排序。
* 默认值：false
* 引入版本：v2.5

### enable_sync_materialized_view_rewrite

* 描述：是否启用基于同步物化视图的查询改写。
* 默认值：true
* 引入版本：v3.1.11，v3.2.5

### enable_view_based_mv_rewrite

* 描述：是否为基于逻辑视图创建的物化视图启用查询改写。如果此项设置为 `true`，则逻辑视图被用作统一节点进行查询改写，从而获得更好的性能。如果此项设置为 `false`，则系统将针对逻辑视图的查询展开变为针对物理表或物化视图的查询，然后进行改写。
* 默认值：false
* 引入版本：v3.1.9，v3.2.5，v3.3.0

### hash_join_push_down_right_table

* 描述：用于控制在 Join 查询中是否可以使用针对右表的过滤条件来过滤左表的数据，可以减少 Join 过程中需要处理的左表的数据量。取值为 `true` 时表示允许该操作，系统将根据实际情况决定是否能对左表进行过滤；取值为 `false` 表示禁用该操作。
* 默认值：true

### materialized_view_rewrite_mode（3.2 及以后）

指定异步物化视图的查询改写模式。有效值：

* `disable`：禁用异步物化视图的自动查询改写。
* `default`（默认值）：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `default_or_error`：启用异步物化视图的自动查询改写，并允许优化器根据 Cost 决定是否可以使用物化视图改写查询。如果查询无法改写，将返回错误。
* `force`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，则直接查询基表中的数据。
* `force_or_error`：启用异步物化视图的自动查询改写，并且优化器优先使用物化视图改写查询。如果查询无法改写，将返回错误。

### materialized_view_subquery_text_match_max_count

* 描述：指定系统比对查询的子查询是否与物化视图定义匹配的最大次数。
* 默认值：4
* 引入版本：v3.2.5，v3.3.0

### nested_mv_rewrite_max_level

* 描述：可用于查询改写的嵌套物化视图的最大层数。
* 取值范围：[1, +∞)。取值为 `1` 表示只可使用基于基表创建的物化视图用于查询改写。
* 默认值：`3`
* 类型：Int

### optimizer_materialized_view_timelimit

* 描述：指定一个物化视图改写规则可消耗的最大时间。当达到阈值时，将不再使用该规则进行查询改写。
* 默认值：1000
* 单位：毫秒
* 类型：Long

### streaming_preaggregation_mode

用于设置多阶段聚合时，group-by 第一阶段的预聚合方式。如果第一阶段本地预聚合效果不好，则可以关闭预聚合，走流式方式，把数据简单序列化之后发出去。取值含义如下：

* `auto`（默认值）：先探索本地预聚合，如果预聚合效果好，则进行本地预聚合；否则切换成流式。默认值，建议保留。
* `force_preaggregation`: 不进行探索，直接进行本地预聚合。
* `force_streaming`: 不进行探索，直接做流式。

