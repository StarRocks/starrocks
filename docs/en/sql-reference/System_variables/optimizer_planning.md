---
displayed_sidebar: docs
sidebar_label: "Query Optimizer and Planning"
sidebar_position: 1
description: "Session variables that control the cost-based optimizer, statistics-driven planning, pruning, rewrite, and CTE reuse."
---

# System Variables - Query Optimizer and Planning

For how to view and set variables, see the [System variables overview](../System_variable.md).

### array_low_cardinality_optimize

* **Scope**: Session
* **Description**: Controls whether the optimizer will consider `ARRAY<VARCHAR>` columns for low-cardinality (dictionary-based) decoding and related optimizations. When enabled, the optimizer's low-cardinality rules (for example, `DecodeCollector`) may define dictionary columns and apply dictionary decoding to expressions whose type is VARCHAR or `ARRAY<VARCHAR>`. When disabled, only scalar VARCHAR columns are eligible and `ARRAY<VARCHAR>` types are ignored by those low-cardinality optimizations.
* **Default**: true
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### cbo_cte_force_reuse_limit_without_order_by

* **Description**: Controls how the optimizer handles a Common Table Expression (CTE) whose definition contains a `LIMIT` without an `ORDER BY`. When `true` (default), the optimizer (`ForceCTEReuseRule`) forces reuse of such a CTE so that every consumer reads the same rows, avoiding non-deterministic results caused by re-evaluating an unordered `LIMIT`. When `false`, the optimizer may inline the CTE instead, which can produce inconsistent results across consumers.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: Boolean
* **Introduced in**: v3.5.10, v4.0.3, v4.1.0

### cbo_cte_force_reuse_node_count

* **Description**: Session-scoped threshold that controls an optimizer shortcut for Common Table Expressions (CTEs). In RelationTransformer.visitCTE the planner counts nodes in a CTE producer tree (cteContext.getCteNodeCount). If that count is greater than or equal to this threshold and the threshold is greater than 0, the transformer forces reuse of the CTE: it skips inlining/transforming the producer plan, builds a consume operator with precomputed expression mappings (no inputs) and uses generated column refs instead. This reduces optimizer time for very large CTE producer trees at the cost of potentially less optimal physical plans. Setting the value to `0` disables the force-reuse optimization. This variable has a getter/setter in SessionVariable and is applied per session.
* **Scope**: Session
* **Default**: `2000`
* **Data Type**: int
* **Introduced in**: v3.5.3

### cbo_cte_max_limit

* **Description**: Limits the number of Common Table Expressions (CTEs) in a query that the optimizer will consider for CTE reuse. The value is passed to the CTE transformer (`CTETransformerContext`) during planning; queries containing more CTEs than this limit fall back to inlining rather than the reuse-based plan. Raising the value lets the optimizer apply CTE reuse to queries with more CTEs, at the cost of longer planning time.
* **Scope**: Session
* **Default**: `10`
* **Data Type**: int
* **Introduced in**: v2.5.0

### cbo_cte_reuse

* **Description**: Controls whether the optimizer may rewrite multi-distinct aggregate queries by reusing a Common Table Expression (CTE) (the CBO CTE‑reuse rewrite). When enabled, the planner (RewriteMultiDistinctRule) may choose a CTE-based rewrite for multi-column distincts, skewed aggregations, or when statistics indicate the CTE rewrite is more efficient; it also respects the `prefer_cte_rewrite` hint. When disabled, CTE-based rewrite is not allowed and the planner will attempt the multi-function rewrite; if a query requires CTE (for example, multi-column DISTINCT or functions that cannot be handled by multi-function rewrite) the planner will raise a user error. Note: the effective setting checked by the optimizer is the logical AND of this flag and the pipeline engine flag — i.e. `isCboCteReuse()` returns this variable AND `enablePipelineEngine`, so CTE reuse is only effective when `enablePipelineEngine` is on.
* **Default**: `true`
* **Data Type**: Boolean
* **Introduced in**: `v3.2.0`

### cbo_cte_reuse_rate

* **Description**: Controls the cost ratio the optimizer uses to decide whether reusing a Common Table Expression (CTE) is cheaper than inlining it. When cost-based, a larger value raises the estimated cost of CTE reuse (the CTE anchor memory cost is estimated as `produce_size * (1 + rate)` in `CostModel`), making the optimizer less likely to reuse the CTE. The value also has two special cases:
  * `< 0`: disable CTE reuse and always inline the CTE.
  * `0`: always reuse the CTE.
  * `> 0`: choose reuse or inline based on cost, weighted by this ratio.

  The internal implementation name is `cbo_cte_reuse_rate_v2`; `cbo_cte_reuse_rate` is the name shown in `SHOW VARIABLES` and used to set the variable.
* **Scope**: Session
* **Default**: `1.15`
* **Data Type**: double
* **Introduced in**: v2.2.0

### cbo_disabled_rules

* **Description**: Comma-separated list of optimizer rule names to disable for the current session. Each name must match a `RuleType` enum value and only rules whose names start with `TF_` (transformation rules) or `GP_` (group-combination rules) may be disabled. The session variable is stored on `SessionVariable` (`getCboDisabledRules` / `setCboDisabledRules`) and is applied by the optimizer via `OptimizerOptions.applyDisableRuleFromSessionVariable()`, which parses the list and clears the corresponding rule switches so those rules are skipped during planning. When set through a SET statement, values are validated and the server will reject unknown names or names not starting with `TF_`/`GP_` with clear error messages (e.g. "Unknown rule name(s): ..." or "Only TF_ ... and GP_ ... can be disabled"). At planner runtime, unknown rule names are ignored with a warning (logged as "Ignoring unknown rule name: ... (may be from different version)"). Names must match enum identifiers exactly (case-sensitive). Whitespace around names is trimmed; empty entries are ignored.
* **Scope**: Session
* **Default**: `""` (no disabled rules)
* **Data Type**: String
* **Introduced in**: -

### cbo_enable_low_cardinality_optimize

* **Description**: Whether to enable low cardinality optimization. After this feature is enabled, the performance of querying STRING columns improves by about three times.
* **Default**: true

### cbo_enable_low_cardinality_optimize_for_join

* **Scope**: Session
* **Description**: Controls whether the optimizer rewrites join operators to take advantage of low-cardinality (dictionary-encoded) string columns. When enabled (default), the optimizer (DecodeCollector / DecodeRewriter) will:
  * rewrite join ON predicates, join predicates and projections to use dictionary-encoded column refs for low-cardinality string columns (affects hash joins);
  * extract equality groups for join columns and enable decoding-based optimizations for supported join patterns.
  When disabled, join-related low-cardinality rewrites are skipped and string columns are left unchanged for join processing. Note: the implementation currently targets broadcast joins and avoids rewrite when both sides use shuffle distribution.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### cbo_eq_base_type

* **Description**: Specifies the data type used for data comparison between DECIMAL data and STRING data. The default value is `DECIMAL`, and VARCHAR is also a valid value. **This variable takes effect only for `=` and `!=` comparison.**
* **Data type**: String
* **Introduced in**: v2.5.14

### cbo_json_v2_dict_opt

* **Description**: Whether to enable low-cardinality dictionary optimization for Flat JSON (JSON v2) extended string subcolumns created by JSON path rewrite. When enabled, the optimizer may build and use global dictionaries for those subcolumns to accelerate string expressions, GROUP BY, and JOIN operations.
* **Default**: true
* **Data type**: Boolean

### cbo_json_v2_rewrite

* **Description**: Whether to enable JSON v2 path rewrite in the optimizer. When enabled, JSON functions (such as `get_json_*`) can be rewritten to direct access of Flat JSON subcolumns, enabling predicate pushdown, column pruning, and dictionary optimization.
* **Default**: true
* **Data type**: Boolean

### cbo_max_reorder_node_use_dp

* **Description**: Session-scoped limit that controls when the cost-based optimizer (CBO) will include the DP (dynamic programming) join-reorder algorithm. The optimizer compares the number of join inputs (MultiJoinNode.atoms.size()) against this value and only runs or adds the DP reorder when `multiJoinNode.getAtoms().size() <= cbo_max_reorder_node_use_dp` and `cbo_enable_dp_join_reorder` is enabled. Used in JoinReorderFactory.createJoinReorderAdaptive (to add JoinReorderDP to the candidate algorithms) and in ReorderJoinRule.transform/rewrite (to decide whether to execute JoinReorderDP when copying plans into the memo). Default value 10 reflects a practical performance cutoff (comment in code: "10 table join reorder takes more than 100ms"). Tune this to trade optimizer runtime (DP is expensive) versus potential plan quality for larger multi-join queries. Interacts with `cbo_enable_dp_join_reorder` and the greedy threshold `cbo_max_reorder_node_use_greedy`. The comparison is inclusive (`<=`).
* **Scope**: Session
* **Default**: `10`
* **Data Type**: long
* **Introduced in**: `v3.2.0`

### cbo_max_reorder_node_use_exhaustive

* **Scope**: Session
* **Description**: Controls the join-reorder algorithm selection threshold in the CBO. The optimizer counts inner/cross join nodes in the query; when that count is greater than this value the planner takes the transform-based (more aggressive) reorder path: it forces collection of CTE statistics and calls ReorderJoinRule.transform and related commutativity rules. When the count is less than or equal to this value the planner applies the cheaper join-transformation rules (and may add the INNER_JOIN_LEFT_ASSCOM_RULE for certain semi/anti-join cases). This session variable is read by the optimizer (`SPMOptimizer`, `QueryOptimizer`) and can be set at the session level via `setMaxTransformReorderJoins`.
* **Default**: `4`
* **Data Type**: int
* **Introduced in**: v3.2.0

### cbo_max_reorder_node_use_greedy

* **Description**: Maximum number of join inputs (atoms) in a multi-join for which the cost-based optimizer will consider the greedy join-reorder algorithm. The optimizer checks this limit (together with `cbo_enable_greedy_join_reorder`) when building the list of candidate reorder algorithms: if `multiJoinNode.getAtoms().size()` is less than or equal to this value, a `JoinReorderGreedy` instance will be added and executed. This variable is used by `JoinReorderFactory.createJoinReorderAdaptive()` and `ReorderJoinRule` to gate greedy reordering during join-reorder phases; it applies per session and affects whether greedy reordering is attempted (if statistics are available and greedy is enabled). Adjust this to control optimizer time/complexity trade-offs for queries with many joined relations.
* **Scope**: Session (can be changed per-session)
* **Default**: `16`
* **Data type**: long
* **Introduced in**: v3.4.0, v3.5.0

### cbo_prune_json_subfield

* **Scope**: Session
* **Description**: When enabled, the cost-based optimizer collects and prunes JSON subfield expressions so that JSON access paths (subfields) are recognized and converted into ColumnAccessPath for scan operators. This enables flat-JSON path optimizations and push-down of JSON subfield access into the scan layer (see PruneSubfieldRule and SubfieldExpressionCollector). Note: normalization of cast-from-JSON expressions is gated by the general `cbo_prune_subfield` optimization; both work together to produce `get_json_xxx(...)` or cast-wrapped calls so BE can apply flat JSON optimizations. Enabling `cbo_prune_json_subfield` without backend support for `flat json` may degrade performance; disable it if the BE does not support flat JSON path pushdown.
* **Default**: `true`
* **Data type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### cbo_use_correlated_predicate_estimate

* **Description**: Session flag that controls whether the optimizer applies a correlation-aware heuristic when estimating selectivity for conjunctive equality predicates across multiple columns. When enabled (default), the estimator applies exponential-decay weights to the selectivities of additional columns beyond the primary multi-column stats or most selective predicate, reducing the multiplicative impact of further predicates (weights: 0.5, 0.25, 0.125 for up to three additional columns). When disabled, no decay is applied (decay factor = 1) and the estimator multiplies full selectivities for those columns (stronger independence assumption). This flag is checked by StatisticsEstimateUtils.estimateConjunctiveEqualitySelectivity to choose the decay factor in both the multi-column-statistics path and the fallback path, thereby affecting cardinality estimates used by the CBO.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.5.0

### enable_cbo_table_prune

* **Description**: When enabled, the optimizer will add the CBO table pruning rule (CboTablePruneRule) during memo optimization to perform cost-based table pruning for cardinality-preserving joins. The rule is conditionally added in the optimizer (see QueryOptimizer.memoOptimize and SPMOptimizer.memoOptimize) only when the join-node count in the join tree is small (fewer than 10 join nodes). This option complements the rule-based pruning toggle `enable_rbo_table_prune` and lets the Cost-Based Optimizer try to remove unnecessary tables or inputs from join processing to reduce planning and execution complexity. Default is off because pruning can change plan shape; enable it only after validating on representative workloads.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_filter_unused_columns_in_scan_stage

* **Description**: Controls pruning of columns produced by Scan nodes so the scan stage only outputs columns that are actually needed downstream (either as outputs or for non-pushable predicates). When enabled, PlanFragmentBuilder.setUnUsedOutputColumns will mark scan output columns that are exclusively used in pushdownable predicates and not required later, allowing the scan to trim those columns and reduce I/O and network transfer. The pruning is guarded: it will not apply for aggregation-family indexes in the non-skip-aggregation (non-skip-aggr) scan stage (keys/value columns must be retained to merge/aggregate), and the planner always ensures at least one column is returned from a scan. See `isEnableFilterUnusedColumnsInScanStage()` and the enable/disable helpers `enableTrimOnlyFilteredColumnsInScanStage()` / `disableTrimOnlyFilteredColumnsInScanStage()` in SessionVariable.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_lambda_pushdown

* **Description**: Session-scoped boolean toggle that controls predicate pushdown behavior in the optimizer. Specifically, the `PushDownPredicateProjectRule` consults this flag: when `true` (default) the rule may push predicates through `Project` operators even if those projects contain `LambdaFunctionOperator` expressions; when `false` the rule inspects the project's expressions and aborts the pushdown if any lambda is present (the rule returns no transformation). This affects only the optimizer transformation phase (planning) and can be changed per session via the `SessionVariable` getter/setter (`getEnableLambdaPushDown` / `setEnableLambdaPushdown`).
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.6, v3.4.0, v3.5.0

### enable_large_in_predicate

* **Scope**: Session
* **Description**: When enabled, the parser will convert IN-lists whose literal count meets or exceeds `large_in_predicate_threshold` into a special `LargeInPredicate` (handled in `AstBuilder`). The optimizer rule `LargeInPredicateToJoinRule` then converts that predicate into a `LEFT_SEMI_JOIN` (for IN) or `NULL_AWARE_LEFT_ANTI_JOIN` (for NOT IN) against a `RawValues` constant table, reducing FE memory and planning cost for very large IN lists by avoiding one expression node per constant. The transformation has correctness restrictions (no OR compound predicates, only one large-IN per query); if these restrictions or other conditions cause the optimization to fail, the planner throws `LargeInPredicateException` and upper layers (via `StmtExecutor` / `ConnectProcessor`) retry the query from the parser stage with `enable_large_in_predicate` disabled so the query falls back to the traditional expression-based IN handling. Use `large_in_predicate_threshold` to control the minimum literal count that triggers this behavior.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_reduce_cast_varchar_length_inheritance (global)

* **Description**: Whether to preserve the target `VARCHAR(N)` length when `ReduceCastRule` eliminates a same-type `VARCHAR -> VARCHAR` cast. Enable this variable to keep prepare and execute result-set metadata consistent for statements such as `CAST(col AS VARCHAR(N))`.
* **Default**: false
* **Data Type**: Boolean
* **Introduced in**: v3.5.16, v4.0.9

### enable_reduce_cast_varchar_expr_sync_type (global)

* **Description**: Whether to synchronize the reused planner `Expr` type and origin type with the rewritten `VARCHAR(N)` type after `ReduceCastRule` eliminates a same-type `VARCHAR -> VARCHAR` cast.
* **Default**: true
* **Data Type**: Boolean
* **Introduced in**: v3.5.16, v4.0.9

### enable_plan_advisor

* **Description**: Whether to enable Query Feedback feature for slow queries and manually marked queries.
* **Default**: true
* **Introduced in**: v3.4.0

### enable_predicate_reorder

* **Scope**: Session
* **Description**: When enabled, the optimizer applies the Predicate Reorder rule to AND (conjunctive) predicates during logical/physical plan rewrite. The rule extracts conjuncts via `Utils.extractConjuncts`, estimates each conjunct's selectivity with `DefaultPredicateSelectivityEstimator`, and reorders the conjuncts in ascending order of estimated selectivity (less restrictive first) to form a new `CompoundPredicateOperator` (AND). The rule only runs when the operator has a `CompoundPredicateOperator` with more than one conjunct. Statistics are gathered from child `OptExpression` statistics when available; for `PhysicalOlapScanOperator` it will fetch column statistics from `GlobalStateMgr.getCurrentState().getStatisticStorage()`. If child statistics are missing and the scan is not an OLAP scan, the rule skips reordering. The session variable is exposed via `SessionVariable.isEnablePredicateReorder()`, with `enablePredicateReorder()` and `disablePredicateReorder()` helper methods.
* **Default**: false
* **Data type**: boolean
* **Introduced in**: v3.2.0

### enable_query_trigger_analyze

* **Default**: true
* **Type**: Boolean
* **Description**: Whether to enable query-trigger ANALYZE tasks on tables under external catalogs.
* **Introduced in**: v3.4.0

### enable_rbo_table_prune

* **Description**: When enabled, the optimizer applies Rule-Based (RBO) table pruning for cardinality-preserving joins in the current session. The optimizer runs a sequence of rewrite and pruning steps (partition pruning, project merge/separate, `UniquenessBasedTablePruneRule`, join reorder, and `RboTablePruneRule`) to remove unnecessary table scan alternatives and reduce scanned partitions/rows in joins. Enabling this option also disables join-equivalence derivation (`context.setEnableJoinEquivalenceDerive(false)`) while logical rule rewrite is running to avoid conflicting transformations. The pruning flow may additionally run `PrimaryKeyUpdateTableRule` for update statements if `enable_table_prune_on_update` is set. The rule is only executed when a query contains prunable joins (checked via `Utils.hasPrunableJoin(tree)`).
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_spm_rewrite

* **Description**: Whether to enable SQL Plan Manager (SPM) query rewrite. When enabled, StarRocks automatically rewrites queries to use bound query plans, improving query performance and stability.
* **Default**: false

### enable_table_prune_on_update

* **Description**: Session-level boolean that controls whether the optimizer applies primary-key-specific table pruning rules for UPDATE statements. When enabled, QueryOptimizer (during the pruneTables stage) invokes `PrimaryKeyUpdateTableRule` to rewrite/prune update plans — potentially improving pruning for primary-key update patterns. This flag is only effective when rule-based/CBO table pruning is active (see `enable_rbo_table_prune`). It is disabled by default because the transformation can change data-layout/plan shape (e.g., bucket-shuffle layout for OlapTableSink) and may cause correctness or performance regressions for concurrent updates.
* **Default**: `false`
* **Data type**: boolean
* **Introduced in**: v3.2.4

### enable_ukfk_opt

* **Description**: Enables optimizer support for Unique-Key / Foreign-Key (UK/FK) based transformations and statistics enhancements. When set, the optimizer runs `UKFKConstraintsCollector` to collect unique and foreign-key constraints bottom‑up and attach them to plan nodes (OptExpressions). The collected constraints are consumed by transformation rules such as `PruneUKFKJoinRule` (which can prune the UK-side of joins, rewrite predicates from UK to FK columns and add IS NULL checks for outer-join cases) and `PruneUKFKGroupByKeysRule` (which can remove redundant GROUP BY keys derived from UK/FK relationships). The collected UK/FK information is also used in `StatisticsCalculator` to produce tighter join cardinality estimates for UK‑FK joins and may replace default estimates when more precise. Default is conservative (`false`) because these optimizations rely on declared schema constraints and can change plan shape and predicate placement.
* **Default**: `false`
* **Scope**: Session
* **Data Type**: boolean
* **Introduced in**: v3.2.4

### large_in_predicate_threshold

* **Scope**: Session
* **Description**: Threshold (number of constants) at which the planner switches a regular IN-list predicate into the compact LargeInPredicate representation to avoid building very large ASTs and heavy FE analysis/planning overhead. When `enable_large_in_predicate` is true and an IN-list (string or integer) contains >= this many literals, the parser (AstBuilder) creates a LargeInPredicate containing the raw text and a compact value list (and keeps only a minimal representative Expr in the AST). For integer lists the parser additionally verifies all literals can be parsed as integral values and will fall back to a normal InPredicate if parsing fails. LargeInPredicate is later transformed into a left semi/anti join for execution, improving parse/analyze/deploy performance for queries with very large constant IN lists. The variable is exposed as a session-level `VariableMgr.VarAttr` and can be get/set per session.
* **Default**: `100000`
* **Data Type**: int
* **Introduced in**: -

### low_cardinality_optimize_on_lake

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable low cardinality optimization on data lake queries. Valid values:
  * `true` (Default): Enable low cardinality optimization on data lake queries.
  * `false`: Disable low cardinality optimization on data lake queries.
* **Introduced in**: v3.5.0

### low_cardinality_optimize_v2

* **Scope**: Session
* **Description**: Session-level boolean that selects which low-cardinality optimization rewrite strategy the optimizer applies. When `true` the optimizer will attempt the V2 rewrite strategy implemented by `LowCardinalityRewriteRule` (uses `DecodeCollector` / `DecodeRewriter` to encode/decode low-cardinality VARCHAR columns). When `false` the optimizer falls back to the legacy rewrite strategy implemented by `AddDecodeNodeForDictStringRule`. Execution of either rewrite also requires `enableLowCardinalityOptimize` to be enabled; if `enableLowCardinalityOptimize` is disabled, no low-cardinality rewrite is performed. The variable is checked in optimizer tree-rewrite paths to choose the appropriate transformation.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### new_planner_optimize_timeout

* **Description**: The timeout duration of the query optimizer. When the optimizer times out, an error is returned and the query is stopped, which affects the query performance. You can set this variable to a larger value based on your query or contact StarRocks technical support for troubleshooting. A timeout often occurs when a query has too many joins.
* **Default**: 3000
* **Unit**: ms

### plan_mode

* **Description**: The metadata retrieval strategy of Iceberg Catalog. For more information, see [Iceberg Catalog metadata retrieval strategy](../data_source/catalog/iceberg/iceberg_catalog.md#appendix-periodic-metadata-refresh-strategy). Valid values:
  * `auto`: The system will automatically select the retrieval plan.
  * `local`: The FE parses Iceberg manifest files locally and streams scan ranges to BEs incrementally as manifests are processed. This avoids collecting all splits before execution begins, reducing memory usage and first-byte latency.
  * `distributed`: Manifest parsing is offloaded to multiple BEs in parallel. The FE must wait for all BEs to finish before delivering any scan ranges, which can cause high memory usage and long wait times on large tables with many manifest files. Prefer this only if FE CPU is a bottleneck and the table has a very large number of manifests.
* **Default**: local (changed from `auto` in v3.5; with incremental scan range delivery enabled by default since v3.5, `local` mode provides lower memory usage and lower latency than `distributed` for most workloads)
* **Introduced in**: v3.3.3

#### enable_iceberg_column_statistics

* **Description**: Whether to obtain column statistics, such as `min`, `max`, `null count`, `row size`, and `ndv` (if a puffin file exists). When this item is set to `false`, only the row count information will be collected.
* **Default**: false
* **Introduced in**: v3.4

### prefer_cte_rewrite

* **Description**: Controls whether the optimizer prefers a Common Table Expression (CTE) based rewrite for multi-distinct aggregate queries. When `true` and `cbo_cte_reuse` is also enabled, `RewriteMultiDistinctRule` chooses the CTE-based rewrite over the multi-function rewrite for queries with multiple distinct aggregates. When `false` (default), the optimizer relies on its normal cost/statistics heuristics to decide between the CTE and multi-function rewrites.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: Boolean
* **Introduced in**: v3.2.0

### range_pruner_max_predicate

* **Description**: The maximum number of IN predicates that can be used for Range partition pruning. Default value: 100. A value larger than 100 may cause the system to scan all tablets, which compromises the query performance.
* **Default**: 100
* **Introduced in**: v3.0

