---
displayed_sidebar: docs
sidebar_label: "Joins, Aggregation, and Materialized Views"
sidebar_position: 2
description: "Session variables for join execution, aggregation, top-N, and materialized view query rewrite."
---

# System Variables - Joins, Aggregation, and Materialized Views

For how to view and set variables, see the [System variables overview](../System_variable.md).

import Restricted from '../../_assets/commonMarkdown/_restricted.mdx'

### agg_in_filter_limit

<Restricted />

* **Description**: Limits the maximum number of values in an IN filter generated from aggregation operations.
* **Scope**: Session
* **Default**: `1024`
* **Data type**: `int`
* **Mutable**: Yes

### array_agg_low_cardinality_optimize

* **Description**: Enables optimization of ARRAY_AGG function when processing columns with low cardinality values.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### broadcast_row_limit

* **Scope**: Session
* **Description**: Limits the right-side table's output row count for a broadcast join. The optimizer (see `EnforceAndCostTask`) treats a candidate as ineligible for broadcast when the right table's row count exceeds this limit or when combined size/scale checks indicate the right table is not sufficiently smaller than the left. A non‑positive value disables this limit. `PushDownAggregateCollector` also uses this variable to identify "small broadcast" joins for aggregate push‑down: it requires the rightRows less than or equal to this limit and less than or equal to `cbo_push_down_aggregate_on_broadcast_join_row_count_limit` before allowing push‑down. It interacts with `broadcast_right_table_scale_factor` and the number of BE nodes when comparing left/right sizes. For example, when `leftOutputSize` is less than `rightOutputSize * beNum * broadcast_right_table_scale_factor` and `rightRowCount` is greater than `broadcast_row_limit`, broadcast is rejected.
* **Default**: `15000000`
* **Data Type**: long
* **Introduced in**: v3.2.0

### count_distinct_column_buckets

* **Description**: The number of buckets for the COUNT DISTINCT column in a group-by-count-distinct query. This variable takes effect only when `enable_distinct_column_bucketization` is set to `true`.
* **Default**: 1024
* **Introduced in**: v2.5

### count_distinct_implementation

* **Description**: Determines which algorithm to use for computing COUNT(DISTINCT) operations.
* **Scope**: Session
* **Default**: `"default"`
* **Data type**: `String`
* **Mutable**: Yes

### cross_join_cost_penalty

<Restricted />

* **Description**: Specifies the cost penalty applied to cross join operations during query optimization to discourage their use.
* **Scope**: Session
* **Default**: `1000000`
* **Data type**: `long`
* **Mutable**: Yes

### disable_colocate_join

* **Description**: Used to control whether the Colocation Join is enabled. The default value is `false`, meaning the feature is enabled. When this feature is disabled, query planning will not attempt to execute Colocation Join.
* **Default**: false

### disable_colocate_set

* **Scope**: Session
* **Description**: When false (default), the optimizer may apply "colocate set" handling for set operations (e.g., UNION / UNION DISTINCT) when the first child's hash distribution is local: the planner attempts to keep children colocated — avoiding full repartitioning by checking pairwise colocation and either converting to compatible bucket shuffles or keeping colocated execution. When true, this session flag disables that colocate-set optimization path; the planner will not rely on colocated-set guarantees and will instead fall back to converting the set operation to round-robin distribution or enforce explicit bucket shuffle conversions for non-colocated children. This flag is consulted by the planner (see ChildOutputPropertyGuarantor.visitPhysicalSetOperation) and exposed on the session via SessionVariable getter/setter (`isDisableColocateSet` / `setDisableColocateSet`).
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.5.0

### disable_join_reorder

* **Scope**: Session
* **Description**: Controls whether the cost-based optimizer performs join reordering. When `false` (default) the optimizer may apply join-reorder transformations (e.g. `ReorderJoinRule`, join transformation and outer-join transformation rules) during logical optimization in the new planner paths (seen in `SPMOptimizer` and `QueryOptimizer`). When `true`, join reordering and related outer-join reorder rules are skipped, preventing the optimizer from changing join order. This is useful to reduce optimization time, to obtain stable/reproducible join ordering, or to work around cases where CBO reordering produces suboptimal plans. This setting interacts with other CBO/session controls such as `cbo_max_reorder_node`, `cbo_max_reorder_node_use_exhaustive`, and `enable_outer_join_reorder`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_agg_spill_preaggregation

<Restricted />

* **Description**: Enables pre-aggregation during spill operations to reduce memory usage in aggregate queries.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_aggregation_pipeline_share_limit

<Restricted />

* **Description**: Enables sharing of pipeline resource limits across aggregation operations to optimize memory usage.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_array_distinct_after_agg_opt

* **Description**: Enables optimization to apply array distinct operations after aggregation instead of before.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_cbo_based_mv_rewrite

* **Description**: Whether to enable materialized view rewrite in CBO phase which can maximize the likelihood of successful query rewriting (e.g., when the join order differs between materialized views and queries), but it will increase the execution time of the optimizer phase.
* **Default**: true
* **Introduced in**: v3.5.5, v4.0.1

### enable_cbo_view_based_mv_rewrite

* **Description**: Enables cost-based optimizer rewrites of queries using materialized views based on view definitions.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_cost_based_multi_stage_agg

* **Description**: Controls whether the new planner uses cost-based decisions to generate and compare multi-stage aggregation plans for queries with DISTINCT aggregates. When enabled, the optimizer may produce alternative 3-stage and 4-stage aggregation candidates and rely on cost estimates to pick the better plan. It also enables post-processing in `PruneAggregateNodeRule` to merge or prune split aggregate nodes when beneficial (that is, reducing unnecessary serialization or deserialization). Note that the effective check in code is gated by `new_planner_agg_stage` — the helper `isEnableCostBasedMultiStageAgg()` returns true only when `new_planner_agg_stage` is set to `AUTO` and this parameter is set to `true`; if `new_planner_agg_stage` is non-`AUTO`, this parameter will not enable cost-based multi-stage behavior. Disabling this flag forces the planner to prefer the simpler 3-stage transformation for distinct aggregations and skips cost-driven candidate generation and certain aggregate-node merges.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_count_distinct_rewrite_by_hll_bitmap

* **Description**: By default, we always use the created mv's bitmap/hll to rewrite count distinct, but result is not exactly matched with the original result. If we want to get the exactly matched result, we can disable this.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_cross_join

* **Description**: Enables or disables the execution of cross join operations in queries.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_defer_project_after_topn

* **Description**: Enables deferring column projection operations until after TOP-N execution to optimize query performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_distinct_agg_over_window

* **Description**: Controls the optimizer rewrite that transforms DISTINCT aggregate calls over WINDOW clauses into an equivalent join-based plan. When enabled (`true`, the default) QueryOptimizer.invoke convertDistinctAggOverWindowToNullSafeEqualJoin will:
  * detect queries containing a LogicalWindowOperator,
  * run project-merge rewrites, derive logical properties,
  * apply DistinctAggregationOverWindowRule to convert the DISTINCT-OVER-WINDOW pattern into a null-safe equality join (changing plan shape to enable further push-downs and aggregation optimizations),
  * then run SeparateProjectRule and re-derive properties.
  When disabled (`false`) the optimizer skips this transformation and leaves DISTINCT aggregates over windows unchanged. This setting is session-scoped and affects only the optimizer rewrite phase (see QueryOptimizer.convertDistinctAggOverWindowToNullSafeEqualJoin).
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_distinct_column_bucketization

* **Description**: Whether to enable bucketization for the COUNT DISTINCT colum in a group-by-count-distinct query. Use the `select a, count(distinct b) from t group by a;` query as an example. If the GROUP BY colum `a` is a low-cardinality column and the COUNT DISTINCT column `b` is a high-cardinality column which has severe data skew, performance bottleneck will occur. In this situation, you can split data in the COUNT DISTINCT column into multiple buckets to balance data and prevent data skew. You must use this variable with the variable `count_distinct_column_buckets`.

  You can also enable bucketization for the COUNT DISTINCT column by adding the `skew` hint to your query, for example, `select a,count(distinct [skew] b) from t group by a;`.

* **Default**: false, which means this feature is disabled.
* **Introduced in**: v2.5

### enable_drop_table_check_mv_dependency

* **Description**: Enables validation of materialized view dependencies when dropping tables to prevent orphaned materialized views.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_eliminate_agg

* **Description**: Controls optimizer transformations that remove or simplify aggregation operators when it is safe to do so. When enabled, the planner applies rules (EliminateAggRule and EliminateAggFunctionRule) to replace a LogicalAggregationOperator with a LogicalProjectOperator (and optionally a LogicalFilterOperator) in two cases:
  - Whole-aggregation elimination (EliminateAggRule): when grouping keys form a unique key on the child (unique/UKFK constraints) and all aggregate calls are supported, non-distinct functions (SUM, COUNT, AVG, FIRST_VALUE, MAX, MIN, GROUP_CONCAT). COUNT is rewritten to an IF/CAST expression (COUNT(col) -> IF(col IS NULL, 0, 1); COUNT(*) -> 1).
  - Per-function elimination (EliminateAggFunctionRule): when individual non-distinct aggregate functions over a grouped column (FIRST_VALUE, LAST_VALUE, ANY_VALUE, MAX, MIN) can be replaced by the column itself while preserving other aggregations.
  The optimization requires non-empty group-by keys, supported function sets, and presence of relevant unique constraints or column relationships; it does not apply to DISTINCT aggregates.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.8, v3.4.0, v3.5.0

### enable_force_group_by_skew_eliminate_when_skewed

* **Description**: Enables forced elimination of data skew in GROUP BY operations when skew is detected.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_force_rule_based_mv_rewrite

* **Description**: Whether to enable query rewrite for queries against multiple tables in the optimizer's rule-based optimization phase. Enabling this feature will improve the robustness of the query rewrite. However, it will also increase the time consumption if the query misses the materialized view.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_group_by_compressed_key

* **Description**: Whether to use accurate statistical information to compress the GROUP BY Key column. Valid values: `true` and `false`.
* **Default**: true
* **Introduced in**: v4.0

### enable_hash_join_linear_chained_opt

* **Description**: Enables linear chained hash table optimization for hash join operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_hash_join_range_direct_mapping_opt

* **Description**: Enables direct mapping optimization for hash joins with range-based key distribution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_hash_join_serialize_fixed_size_string

* **Description**: Enables serialization of fixed-size strings during hash join operations.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_incremental_mv

* **Description**: Session flag that controls whether the server will plan and keep an in-memory plan for materialized views that use incremental refresh. When enabled, `MaterializedViewAnalyzer.planMVQuery` will proceed for create-MV statements whose refresh scheme is an `IncrementalRefreshSchemeDesc`: it builds the logical and physical plan for the view query and sets the session `enableMVPlanner` flag (`setMVPlanner(true)`). When disabled, planning for incremental-refresh MVs is skipped. Accessible via `isEnableIncrementalRefreshMV()` and `setEnableIncrementalRefreshMv(boolean)` in `SessionVariable`.
* **Scope**: Session (per-connection)
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_inner_join_to_semi

* **Description**: Enables automatic transformation of inner joins to semi joins for query optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_join_reorder_before_deduplicate

* **Description**: Enables reordering of join operations before deduplication in query optimization.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_join_runtime_bitset_filter

<Restricted />

* **Description**: Enables the use of bitset filters during join operations to reduce data scanned at runtime.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_join_runtime_filter_push_down

<Restricted />

* **Description**: Enables pushing down join runtime filters to table scan operators during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_agg_pushdown_rewrite

* **Description**: Whether to enable aggregation pushdown for materialized view query rewrite. If it is set to `true`, aggregate functions will be pushed down to Scan Operator during query execution and rewritten by the materialized view before the Join Operator is executed. This will relieve the data expansion caused by Join and thereby improve the query performance. For detailed information about the scenarios and limitations of this feature, see [Aggregation pushdown](../../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#aggregation-pushdown).
* **Default**: `true`
* **Introduced in**: v3.3.0

### enable_materialized_view_concurrent_prepare

* **Description**: whether to use materialized view concurrent prepare to reduce mv rewrite time cost
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_for_insert

* **Description**: Whether to allow StarRocks to rewrite queries in INSERT INTO SELECT statements.
* **Default**: false, which means Query Rewrite in such scenarios is disabled by default.
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_multi_stages_rewrite

* **Description**: Enables rewriting queries to use materialized views through multi-stage optimization strategies.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_plan_cache

<Restricted />

* **Description**: whether to use materialized view plan context cache to reduce mv rewrite time cost
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_rewrite

* **Description**: Enables the query optimizer to rewrite queries using materialized views to improve performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_rewrite_greedy_mode

* **Description**: Enable greedy mode in mv rewrite to cut down optimizer time for mv rewrite: - Use plan cache if possible to avoid regenerating plan tree. - Use the max plan tree to rewrite in view-delta mode to avoid too many rewrites.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_rewrite_partition_compensate

<Restricted />

* **Description**: Whether to compensate partition predicates in mv rewrite, see `Materialization#isCompensatePartitionPredicate` for more details. NOTE: if set it false, it will be rewritten by the mv defined sql with user's query and will not add extra compensated predicates which can rewrite more cases but may lose consistency check.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_single_table_view_delta_rewrite

<Restricted />

* **Description**: Whether to enable view delta compensation for single table, - try to rewrite single table query into candidate view-delta mvs if enabled which will choose plan by cost. - otherwise not try to write single table query by using candidate view-delta mvs which only try to rewrite by single table mvs and is determined by rule rather than by cost.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_text_match_rewrite

* **Description**: Whether to enable text-based materialized view rewrite. When this item is set to true, the optimizer will compare the query with the existing materialized views. A query will be rewritten if the abstract syntax tree of the materialized view's definition matches that of the query or its sub-query.
* **Default**: true
* **Introduced in**: v3.2.5, v3.3.0

### enable_materialized_view_timeseries_agg_pushdown_rewrite

* **Description**: Enables query rewriting to push down time-series aggregations to materialized views.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_transparent_union_rewrite

* **Description**: Whether to enable transparent union rewrite for materialized view which treats materialized view as always-consistent and then union rewrite.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_materialized_view_union_rewrite

* **Description**: Whether to enable materialized view union rewrite. If this item is set to `true`, the system seeks to compensate the predicates using UNION ALL when the predicates in the materialized view cannot satisfy the query's predicates.
* **Default**: true
* **Introduced in**: v2.5.20, v3.1.9, v3.2.7, v3.3.0

### enable_materialized_view_view_delta_rewrite

* **Description**: Enables materialized view rewrite optimization using view delta computation for incremental data changes.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_mv_planner

* **Scope**: Session
* **Description**: When enabled, activates the Materialized View (MV) planner mode for the current session. In this mode the optimizer:
  - Uses MV-specific rule set via `context.getRuleSet().addRealtimeMVRules()` instead of the regular join implementation rules (QueryOptimizer).
  - Allows stream implementation rules to apply (see `StreamImplementationRule.check` which returns true only when MV planner is on).
  - Alters scan/operator construction during logical plan transformation (e.g., RelationTransformer chooses `LogicalBinlogScanOperator` for native tables/materialized views when MV planner is enabled).
  - Disables or bypasses some standard transformations (for example, `SplitMultiPhaseAggRule.check` returns false when MV planner is on).
  Materialized view planning code (MaterializedViewAnalyzer) sets this flag around MV planning work (sets to true before planning and resets to false afterward), so it is primarily intended for MV plan generation and testing. Setting this session variable affects only the current session’s optimizer behavior.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_nested_loop_join

* **Description**: Enables the query optimizer to use nested loop join operations when executing queries.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_optimize_skew_join_v1

* **Scope**: Session
* **Description**: Controls which skew-join optimization strategy the optimizer uses. When set to `true`, the optimizer enables the query-rewrite based skew join optimization: QueryOptimizer checks `sessionVariable.isEnableOptimizerSkewJoinByQueryRewrite()` after join-expression pushdown and, if enabled and `enableOptimizerSkewJoinByBroadCastSkewValues` is disabled, invokes `skewJoinOptimize(...)` which applies `SkewJoinOptimizeRule`. If `isEnableStatsToOptimizeSkewJoin` is enabled, `skewJoinOptimize` first merges projects and computes statistics (`Utils.calculateStatistics`) before applying the rule. The session setters enforce mutual exclusivity between `enableOptimizerSkewJoinByQueryRewrite` and `enableOptimizerSkewJoinByBroadCastSkewValues` (setting one flips the other), so only one skew strategy is active at a time.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_optimize_skew_join_v2

* **Description**: When enabled, the optimizer uses the broadcasted-skew-values strategy (Skew Join v2) to handle skewed joins. In the optimizer this flag activates the SkewShuffleJoinEliminationRule during dynamic rewrite and disables the query-rewrite based skew-join path (skewJoinOptimize). The two skew-join strategies are mutually exclusive: enabling this variable sets `enable_optimize_skew_join_v1` off and vice versa. This is a session-level variable intended to switch optimizer behavior for queries that benefit from using broadcasted skew statistics instead of query-rewrite transformations. Usage locations: `QueryOptimizer.dynamicRewrite(...)` and the main optimization flow in `QueryOptimizer` where skew-join optimization is applied.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: -

### enable_outer_join_reorder

* **Description**: Enables reordering of outer joins during query optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_partition_hash_join

* **Description**: Whether to enable adaptive Partition Hash Join.
* **Default**: true
* **Introduced in**: v3.4

### enable_push_down_pre_agg_with_rank

* **Description**: Enables pushing down pre-aggregation with rank window functions to improve query performance.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_rewrite_bitmap_union_to_bitamp_agg

* **Description**: Enables automatic rewriting of bitmap_union(to_bitmap(x)) expressions to bitmap_agg(x) for query optimization.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_rewrite_groupingsets_to_union_all

* **Scope**: Session
* **Description**: When enabled, the optimizer applies the RewriteGroupingSetsByCTERule to transform SQL GROUPING SETS (including ROLLUP and CUBE semantics) into equivalent plans expressed as multiple aggregation branches combined with UNION ALL (implemented via CTEs). The system will conditionally run the iterative rewrite pass. This rewrite can improve compatibility with existing aggregation planning rules and enable further rule-based optimizations, but it typically expands the plan into multiple aggregation/union branches which may increase intermediate rows, memory usage, and planning time. If `cbo_push_down_grouping_set` is also set to `true`, the optimizer may additionally attempt push-down of grouping-set aggregates after or instead of this rewrite.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_rewrite_or_to_union_all_join

* **Description**: Enables rewriting OR conditions in queries to UNION ALL joins for query optimization.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_rewrite_simple_agg_to_meta_scan

* **Description**: Enables rewriting simple aggregate queries to use metadata scans instead of full table scans.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_sort_aggregate

* **Description**: Specifies whether to enable sorted streaming. `true` indicates sorted streaming is enabled to sort data in data streams.
* **Default**: false
* **Introduced in**: v2.5

### enable_split_topn_agg

* **Description**: Controls whether the optimizer may apply the SplitTopNAggregate transformation (implemented in `SplitTopNAggregateRule.java`). When enabled, the optimizer can rewrite a plan that has a TopN on top of an aggregation over an OLAP scan into a plan that:
  - computes a restricted TopN-aggregation on a right-side partial scan (push-down),
  - joins the TopN result back to the full scan,
  - and finalizes aggregation on the join result.
  This rewrite is intended to reduce work when TopN orders by aggregated columns and the limit is small. The rule is gated by several precise checks:
  - TopN limit is set and less than or equal to `split_topn_agg_limit` (session `splitTopNAggLimit`, default 10000).
  - scan/agg projections are identity (no column remapping).
  - statistics/row-count heuristics (skip when outputRowCount < limit * 10 and stats are reliable, unless running unit tests).
  - duplicated columns to be read twice ≤ 3.
  - duplicated columns are not long strings or complex types (string with averageRowSize ≥ 5 or missing stats is treated as long).
  - predicate complexity limits (≤ 2 conjuncts and ≤ 2 disjuncts).
  - only a subset (not zero or all) of aggregations are referenced by ORDER BY.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_split_topn_agg_limit

* **Description**: Session-level threshold (row count) that controls whether the SplitTopN aggregate optimization may be applied. When `enable_split_topn_agg` is on, the optimizer's SplitTopNAggregateRule will skip the split transformation if the TopN operator's LIMIT is equal to the default unlimited value or greater than this threshold. This variable only governs the numeric cutoff (number of rows); the rule still enforces other correctness and cost checks (projections, predicates, column/type constraints and scan statistics) before applying the transformation.
* **Scope**: Session
* **Default**: `10000`
* **Data Type**: long
* **Introduced in**: -

### enable_split_window_skew_to_union

* **Description**: Enables splitting skewed window functions into separate queries combined with UNION to improve performance.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_stats_to_optimize_skew_join

* **Description**: Enables the query optimizer to use table statistics for detecting and optimizing skewed joins.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_sync_materialized_view_rewrite

* **Description**: Whether to enable query rewrite based on synchronous materialized views.
* **Default**: true
* **Introduced in**: v3.1.11, v3.2.5

### enable_ukfk_join_reorder

* **Scope**: Session
* **Description**: When enabled, the optimizer collects Unique Key (UK) / Foreign Key (FK) column constraints for the two join sides and uses that information to bias join reordering. If a join has an intact UK constraint, the optimizer may choose the FK table as the right child (probe/input) instead of using plain row-count comparison. The decision uses the helper allowFKAsRightTable(), which:
  * rejects reordering when the FK side is ordered by the FK column (`fkConstraint.isOrderByFK`),
  * computes normalized row counts by scaling row counts with summed column type sizes,
  * computes a scale ratio = fkNormalizedRows / max(1, ukNormalizedRows), and
  * allows the FK as right table only when the scale ratio and FK rows are below session thresholds (`max_ukfk_join_reorder_scale_ratio` and `max_ukfk_join_reorder_fk_rows`).
  If UK/FK conditions are not met, join ordering falls back to the default smaller-table-as-right-child heuristic (rowCount comparison). The variable is accessed via SessionVariable getters/setters and used in the JoinOrder optimizer rule.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.4

### enable_view_based_mv_rewrite

* **Description**: Whether to enable query rewrite for logical view-based materialized views. If this item is set to `true`, the logical view is used as a unified node to rewrite the queries against itself for better performance. If this item is set to `false`, the system transcribes the queries against logical views into queries against physical tables or materialized views and then rewrites them.
* **Default**: `true`
* **Introduced in**: v3.1.9, v3.2.5, v3.3.0

### hash_join_interpolate_passthrough

<Restricted />

* **Description**: _Description pending._
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### hash_join_push_down_right_table

* **Description**: Used to control whether the data of the left table can be filtered by using the filter condition against the right table in the Join query. If so, it can reduce the amount of data that needs to be processed during the query.
**Default**: `true` indicates the operation is allowed and the system decides whether the left table can be filtered. `false` indicates the operation is disabled. The default value is `true`.

### join_implementation_mode_v2

* **Description**: Specifies the join algorithm implementation strategy (auto, broadcast, shuffle, or bucket) for query execution.
* **Scope**: Session
* **Default**: `"auto"`
* **Data type**: `String`
* **Mutable**: Yes

### join_late_materialization

* **Scope**: Session
* **Description**: Controls whether the planner enables "late materialization" on join operators. When set to `true`, PlanFragmentBuilder reads the session flag and calls `joinNode.setEnableLateMaterialization(...)` so join execution can defer full row/payload materialization until after join/key-based filtering. This reduces memory usage and I/O for joins with large payload columns or highly selective join predicates by carrying only join keys through the join and materializing payloads for matched rows. The flag is defined in `SessionVariable.java` as `JOIN_LATE_MATERIALIZATION` and defaults to `false`. Enabling this may interact with column-trimming and scan-stage pruning optimizations (for example `enable_filter_unused_columns_in_scan_stage`) and can change join runtime behavior; test queries for correctness and performance before enabling broadly.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### join_reorder_driving_table_max_element

* **Description**: Limits the maximum number of tables in the driving table set for join reorder optimization.
* **Scope**: Session
* **Default**: `5`
* **Data type**: `int`
* **Mutable**: Yes

### materialized_view_join_same_table_permutation_limit

<Restricted />

* **Description**: Limits the number of permutations considered when rewriting queries with joins on the same table using materialized views.
* **Scope**: Session
* **Default**: `5`
* **Data type**: `int`
* **Mutable**: Yes

### materialized_view_max_relation_mapping_size

* **Description**: Limits the maximum number of relation mappings evaluated during materialized view matching.
* **Scope**: Session
* **Default**: `10`
* **Data type**: `int`
* **Mutable**: Yes

### materialized_view_rewrite_mode (v3.2 and later)

Specifies the query rewrite mode of asynchronous materialized views. Valid values:

* `disable`: Disable automatic query rewrite of asynchronous materialized views.
* `default` (Default value): Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, it directly scans the data in the base table.
* `default_or_error`: Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, an error is returned.
* `force`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, it directly scans the data in the base table.
* `force_or_error`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, an error is returned.

### materialized_view_subquery_text_match_max_count

* **Description**: Specifies the maximum number of times that the system checks whether a query's sub-query matches the materialized views' definition.
* **Default**: 4
* **Introduced in**: v3.2.5, v3.3.0

### materialized_view_union_rewrite_mode

* **Description**: see `MaterializedViewUnionRewriteMode` for more details.
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### max_or_to_union_all_join_predicates

* **Description**: Limits the maximum number of OR conditions in join predicates that can be converted to UNION ALL operations.
* **Scope**: Session
* **Default**: `3`
* **Data type**: `int`
* **Mutable**: Yes

### max_ukfk_join_reorder_fk_rows

<Restricted />

* **Description**: Limits the maximum number of rows in a foreign key table for join reorder optimization with unique key-foreign key relationships.
* **Scope**: Session
* **Default**: `100000000`
* **Data type**: `int`
* **Mutable**: Yes

### max_ukfk_join_reorder_scale_ratio

<Restricted />

* **Description**: Limits the maximum ratio of table sizes for unique key/foreign key join reordering optimization.
* **Scope**: Session
* **Default**: `100`
* **Data type**: `int`
* **Mutable**: Yes

### nested_mv_rewrite_max_level

* **Description**: The maximum levels of nested materialized views that can be used for query rewrite.
* **Value range**: [1, +∞). The value of `1` indicates that only materialized views created on base tables can be used for query rewrite.
* **Default**: 3
* **Data type**: Int

### new_planner_agg_stage

* **Scope**: Session
* **Description**: Controls how the new planner selects aggregation phase decomposition. Valid integer values (0–4):
  * `0` (AUTO) — allow the optimizer to choose the aggregation stage selection. When `0` is set, cost-based multi-stage decisions can be enabled via `enable_cost_based_multi_stage_agg`.
  * `1` (ONE_STAGE) — force a single-stage aggregate.
  * `2` (TWO_STAGE) — force a two-stage aggregate.
  * `3` (THREE_STAGE) — force a three-stage aggregate (only producible for single-column DISTINCT scenarios).
  * `4` (FOUR_STAGE) — force a four-stage aggregate (only producible for single-column DISTINCT scenarios).
  Setting a forced stage overrides automatic selection logic used by optimizer rules (for example, SplitMultiPhaseAggRule and related cost checks). When `0` is set, the planner may still be constrained by `enable_cost_based_multi_stage_agg`. The variable is consulted in cost enforcement (EnforceAndCostTask), aggregation-splitting rules, and plan-fragment construction to influence exchange/partitioning and pruning decisions.
* **Default**: `0`
* **Data Type**: int
* **Introduced in**: v3.2.0

### optimize_distinct_agg_over_framed_window

<Restricted />

* **Description**: 0: auto, 1: opimize, -1: do not optimize
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### optimizer_materialized_view_timelimit

* **Description**: Specifies the maximum time that one materialized view rewrite rule can consume. When the threshold is reached, this rule will not be used for query rewrite.
* **Default**: 1000
* **Unit**: ms
* **Introduced in**: v3.1.9, v3.2.5

### query_excluding_mv_names

<Restricted />

* **Description**: Specifies materialized view names to exclude from query rewriting optimization.
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### query_including_mv_names

<Restricted />

* **Description**: Specifies which materialized views to consider for query rewrite optimization.
* **Scope**: Session
* **Default**: `""`
* **Data type**: `String`
* **Mutable**: Yes

### semi_join_deduplicat_mode

* **Description**: this sv controls whether to create distinct agg below semi join -1 means disable this optimization 0 means auto, and the optimizer will decide whether to enable this optimization based on cardinality 1 means froce, optimizer will add distinct agg below semi-join regardless of cardinality
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### skew_join_data_skew_threshold

<Restricted />

* **Description**: Controls the threshold ratio for detecting data skewness in join operations to trigger skew optimization.
* **Scope**: Session
* **Default**: `0.2`
* **Data type**: `double`
* **Mutable**: Yes

### skew_join_max_other_side_overlap_row_count

<Restricted />

* **Description**: Maximum number of overlapping MCV rows on the other side of the join. When exceeding the overlap, the skew join optimization is skipped as this can lead to a row explosion. With the default value of `skewJoinRandRange` = 1000, an overlap of 1M leads to 1Bn rows.
* **Scope**: Session
* **Default**: `1_000_000`
* **Data type**: `long`
* **Mutable**: Yes

### skew_join_mcv_min_input_rows

<Restricted />

* **Description**: Minimal input rows (estimated) to enable MCV-based skew join elimination rewrite.
* **Scope**: Session
* **Default**: `10000000`
* **Data type**: `long`
* **Mutable**: Yes

### skew_join_mcv_single_threshold

<Restricted />

* **Description**: A single MCV value must exceed this total-domain ratio to be considered as a skew value candidate.
* **Scope**: Session
* **Default**: `0.1`
* **Data type**: `double`
* **Mutable**: Yes

### skew_join_rand_range

<Restricted />

* **Description**: Specifies the range for random values used in skew join optimization to distribute skewed join keys.
* **Scope**: Session
* **Default**: `1000`
* **Data type**: `int`
* **Mutable**: Yes

### skew_join_use_mcv_count

<Restricted />

* **Description**: mcv means most common value in histogram statistics
* **Scope**: Session
* **Default**: `5`
* **Data type**: `int`
* **Mutable**: Yes

### spill_hash_join_probe_op_max_bytes

<Restricted />

* **Description**: Limits the maximum memory size for spilling operations in hash join probe operations.
* **Scope**: Session
* **Default**: `1L << 31`
* **Data type**: `long`
* **Mutable**: Yes

### spill_partitionwise_agg_partition_num

<Restricted />

* **Description**: Specifies the number of partitions used when spilling partitionwise aggregation operations to disk.
* **Scope**: Session
* **Default**: `32`
* **Data type**: `int`
* **Mutable**: Yes

### spill_partitionwise_agg_skew_elimination

<Restricted />

* **Description**: Enables skew elimination for partitionwise aggregation spilling to improve performance on data with uneven partition distribution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### streaming_preaggregation_mode

Used to specify the preaggregation mode for the first phase of GROUP BY. If the preaggregation effect in the first phase is not satisfactory, you can use the streaming mode, which performs simple data serialization before streaming data to the destination. Valid values:

* `auto`: The system first tries local preaggregation. If the effect is not satisfactory, it switches to the streaming mode. This is the default value.
* `force_preaggregation`: The system directly performs local preaggregation.
* `force_streaming`: The system directly performs streaming.

### topn_back_pressure_max_rounds

* **Description**: Throttle window parameters for the lake/connector self-enabled back-pressure path (tuned defaults).
* **Scope**: Session
* **Default**: `8`
* **Data type**: `int`
* **Mutable**: Yes

### topn_back_pressure_num_rows

* **Description**: Specifies the row threshold that triggers back pressure mechanism for TOP-N query operations.
* **Scope**: Session
* **Default**: `1024`
* **Data type**: `long`
* **Mutable**: Yes

### topn_back_pressure_throttle_time_ms

* **Description**: Controls the throttle delay time in milliseconds applied when back pressure is triggered during top-N query operations.
* **Scope**: Session
* **Default**: `8`
* **Data type**: `long`
* **Mutable**: Yes

### topn_back_pressure_throttle_time_upper_bound_ms

* **Description**: Limits the maximum duration in milliseconds for throttling back pressure during top-N query operations.
* **Scope**: Session
* **Default**: `100`
* **Data type**: `long`
* **Mutable**: Yes

### topn_filter_back_pressure_io_tasks

* **Description**: Read-ahead IO-task cap while a TopN runtime filter is still pending; &lt;=0 disables the clamp.
* **Scope**: Session
* **Default**: `1`
* **Data type**: `int`
* **Mutable**: Yes

### topn_filter_back_pressure_mode

* **Description**: Controls the back-pressure mode for top-N runtime filters: 0 disables it, 1 applies pressure when too much data is present, and 2 forces it.
* **Scope**: Session
* **Default**: `0`
* **Data type**: `int`
* **Mutable**: Yes

### topn_push_down_agg_mode

<Restricted />

* **Description**: Specifies the aggregation push-down strategy for top-N queries to optimize execution.
* **Scope**: Session
* **Default**: `1`
* **Data type**: `int`
* **Mutable**: Yes

### window_partition_mode

<Restricted />

* **Description**: 1: sort based, 2: hash based
* **Scope**: Session
* **Default**: `1`
* **Data type**: `int`
* **Mutable**: Yes

