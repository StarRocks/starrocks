---
displayed_sidebar: docs
keywords: ['session variable']
---

# System variables

StarRocks provides many system variables that can be set and modified to suit your requirements. This section describes the variables supported by StarRocks. You can view the settings of these variables by running the [SHOW VARIABLES](sql-statements/cluster-management/config_vars/SHOW_VARIABLES.md) command on your MySQL client. You can also use the [SET](sql-statements/cluster-management/config_vars/SET.md) command to dynamically set or modify variables. You can make these variables take effect globally on the entire system, only in the current session, or only in a single query statement.

The variables in StarRocks refer to the variable sets in MySQL, but **some variables are only compatible with the MySQL client protocol and do not function on the MySQL database**.

> **NOTE**
>
> Any user has the privilege to run SHOW VARIABLES and make a variable take effect at session level. However, only users with the SYSTEM-level OPERATE privilege can make a variable take effect globally. Globally effective variables take effect on all the future sessions (excluding the current session).
>
> If you want to make a setting change for the current session and also make that setting change apply to all future sessions, you can make the change twice, once without the `GLOBAL` modifier and once with it. For example:
>
> ```SQL
> SET query_mem_limit = 137438953472; -- Apply to the current session.
> SET GLOBAL query_mem_limit = 137438953472; -- Apply to all future sessions.
> ```

## Variable hierarchy and types

StarRocks supports three types (levels) of variables: global variables, session variables, and `SET_VAR` hints. Their hierarchical relationship is as follows:

* Global variables take effect on global level, and can be overridden by session variables and `SET_VAR` hints.
* Session variables take effect only on the current session, and can be overridden by `SET_VAR` hints.
* `SET_VAR` hints take effect only on the current query statement.

## View variables

You can view all or some variables by using `SHOW VARIABLES [LIKE 'xxx']`. Example:

```SQL
-- Show all variables in the system.
SHOW VARIABLES;

-- Show variables that match a certain pattern.
SHOW VARIABLES LIKE '%time_zone%';
```

## Set variables

### Set variables globally or for a single session

You can set variables to take effect **globally** or **only on the current session**. When set to global, the new value will be used for all the future sessions, while the current session still uses the original value. When set to "current session only", the variable will only take effect on the current session.

A variable set by `SET <var_name> = xxx;` only takes effect for the current session. Example:

```SQL
SET query_mem_limit = 137438953472;

SET forward_to_master = true;

SET time_zone = "Asia/Shanghai";
```

A variable set by `SET GLOBAL <var_name> = xxx;` takes effect globally. Example:

```SQL
SET GLOBAL query_mem_limit = 137438953472;
```

The following variables only take effect globally. They cannot take effect for a single session, which means you must use `SET GLOBAL <var_name> = xxx;` for these variables. If you try to set such a variable for a single session (`SET <var_name> = xxx;`), an error is returned.

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


In addition, variable settings also support constant expressions, such as:

```SQL
SET query_mem_limit = 10 * 1024 * 1024 * 1024;
```

 ```SQL
SET forward_to_master = concat('tr', 'u', 'e');
```

### Set variables in a single query statement

In some scenarios, you may need to set variables specifically for certain queries. By using the `SET_VAR` hint, you can set session variables that will take effect only within a single statement.

StarRocks supports using `SET_VAR` in the following statements;

- SELECT
- INSERT (from v3.1.12 and v3.2.0 onwards)
- UPDATE (from v3.1.12 and v3.2.0 onwards)
- DELETE (from v3.1.12 and v3.2.0 onwards)

`SET_VAR` can only be placed after the above keywords and enclosed in `/*+...*/`.

Example:

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

You can also set multiple variables in a single statement. Example:

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

### Set variables as user properties

You can set session variables as user properties using the [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md). This feature is supported from v3.3.3.

Example:

```SQL
-- Set the session variable `query_timeout` to `600` for the user jack.
ALTER USER 'jack' SET PROPERTIES ('session.query_timeout' = '600');
```

## Descriptions of variables

The variables are described **in alphabetical order**. Variables with the `global` label can only take effect globally. Other variables can take effect either globally or for a single session.

### activate_all_roles_on_login (global)

* **Description**: Whether to enable all roles (including default roles and granted roles) for a StarRocks user when the user connects to the StarRocks cluster.
  * If enabled (`true`), all roles of the user are activated at user login. This takes precedence over the roles set by [SET DEFAULT ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md).
  * If disabled (`false`), the roles set by SET DEFAULT ROLE are activated.
* **Default**: false
* **Introduced in**: v3.0

If you want to activate the roles assigned to you in a session, use the [SET ROLE](sql-statements/account-management/SET_DEFAULT_ROLE.md) command.

### array_low_cardinality_optimize

* **Scope**: Session
* **Description**: When enabled, the query optimizer may apply LowCardinality (dictionary) decoding to array string columns — specifically arrays whose item type is `varchar` — allowing expressions over `varchar` and `array&lt;varchar&gt;` columns to be considered for dictionary-based optimizations. The flag is consulted by the low-cardinality decode logic (e.g., DecodeCollector.supportAndEnabledLowCardinality): scalar `varchar` columns are always eligible, while `array&lt;varchar&gt;` columns are eligible only if this session variable is `true`. Toggle this to include or exclude array-of-string types from low-cardinality rewrites without affecting non-array varchar behavior. The variable has a session-level setter/getter in SessionVariable.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### authentication_policy

* **Scope**: Session
* **Description**: Per-session string that records the authentication policy token(s) associated with the client connection. The value is stored as comma-separated tokens (the default contains two commas, i.e., three tokens). The variable is provided for MySQL-protocol compatibility and to carry the authentication policy information alongside `default_authentication_plugin`. The repository-level declaration lives in the session variable set (see SessionVariable), and this value is intended to be preserved as part of session state rather than being a query planner/execution parameter.
* **Default Value**: `*,,`
* **Type**: String
* **Introduced in**: -

### auto_increment_increment

Used for MySQL client compatibility. No practical usage.

### big_query_profile_threshold

* **Description**: Used to set the threshold for big queries. When the session variable `enable_profile` is set to `false` and the amount of time taken by a query exceeds the threshold specified by the variable `big_query_profile_threshold`, a profile is generated for that query.

  Note: In versions v3.1.5 to v3.1.7, as well as v3.2.0 to v3.2.2, we introduced the `big_query_profile_second_threshold` for setting the threshold for big queries. In versions v3.1.8, v3.2.3, and subsequent releases, this parameter has been replaced by `big_query_profile_threshold` to offer more flexible configuration options.
* **Default**: 0
* **Unit**: Second
* **Data type**: String
* **Introduced in**: v3.1

### broadcast_row_limit

* **Scope**: Session
* **Description**: Session-level threshold (row count) used by the optimizer to decide whether a join's right-side input is small enough for a broadcast join and to enable aggregate pushdown on broadcast joins. The planner checks the right-side output row count against this limit and also factors in cluster parallelism and `broadcast_right_table_scale_factor`: broadcast is disallowed when
  leftOutputSize &lt; rightOutputSize * beNum * `broadcast_right_table_scale_factor` AND rightRows &gt; `broadcast_row_limit`.
  If `broadcast_row_limit` &lt;= 0 the check treats broadcast as disabled for those cases. This variable is also consulted by `cbo_push_down_aggregate_on_broadcast_join_row_count_limit` when determining whether to push aggregates down into a broadcast join. Tune this to control memory/traffic trade-offs for broadcasting the right table across backends.
* **Default**: `15000000`
* **Data Type**: long
* **Introduced in**: v3.2.0

### catalog

* **Description**: Used to specify the catalog to which the session belongs.
* **Default**: default_catalog
* **Data type**: String
* **Introduced in**: v3.2.4

### cbo_cte_force_reuse_node_count

* **Scope**: Session  
* **Description**: Controls a threshold on the number of nodes in a CTE producer tree that triggers forced reuse of the CTE instead of fully transforming/inlining it. The optimizer measures the producer node count (from `cteContext.getCteNodeCount`) and, when that count is greater than the configured threshold, RelationTransformer creates a consume operator that reuses the already-registered CTE expressions (avoiding visiting and building the full producer plan). This reduces optimizer time for very large CTE producer trees. A value of 0 disables the force-reuse behavior and forces normal transformation/inlining regardless of producer size.  
* **Default**: 2000  
* **Data Type**: int  
* **Introduced in**: v3.5.3

### cbo_cte_reuse

* **Scope**: Session
* **Description**: Enable or disable the CBO rewrite that reuses Common Table Expressions (CTEs) when transforming multi-distinct aggregations. When enabled, the optimizer (see `RewriteMultiDistinctRule`) may convert DISTINCT aggregations into CTE-based plans to share computation across multiple distinct calls; this rewrite can improve performance for multi-column DISTINCTs or when statistics indicate CTE is more efficient. When disabled, the optimizer prefers multi-distinct-function rewrites and, for cases that cannot be rewritten (for example multi-column DISTINCT, complex/JSON types, `GROUP_CONCAT`, or certain `ARRAY_AGG` decimal usages), the planner will raise a user error stating the query is unsupported with `cbo_cte_reuse` disabled. The effective check in code is `isCboCteReuse()` which returns the session flag AND the `enable_pipeline_engine` state, so `enable_pipeline_engine` must also be true for CTE reuse to take effect. This setting interacts with `prefer_cte_rewrite`, `cbo_cte_reuse_rate`, `cbo_cte_max_limit` and related CTE tuning variables.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### cbo_disabled_rules

* **Description**: Session-only string containing a comma-separated list of optimizer rule enum names to disable. Each name must match a RuleType enum constant exactly (case-sensitive) and only rules whose names start with `TF_` (transformation rules) or `GP_` (group-combination rules) can be disabled. The statement analyzer (`SetStmtAnalyzer.validateCboDisabledRules`) validates the list and rejects unknown names or names that are not `TF_`/`GP_`. At runtime `OptimizerOptions.applyDisableRuleFromSessionVariable` reads `cbo_disabled_rules`, `parseDisabledRules` splits and trims entries, converts valid enum names to RuleType and clears corresponding rule switches so the optimizer will skip those rules. Unknown names may be ignored with a logged warning by the optimizer parsing path, but attempting to SET an invalid value will raise a semantic error. Use an empty string to disable no rules. Example:
  SET `cbo_disabled_rules` = 'TF_JOIN_COMMUTATIVITY,GP_PRUNE_COLUMNS';
* **Default**: `""`
* **Data Type**: String
* **Introduced in**: -

### cbo_enable_intersect_add_distinct

* **Description**: Enables a CBO transformation that attempts to insert a global DISTINCT (aggregation) on each input branch of a `LOGICAL_INTERSECT` when the optimizer deems it beneficial. When enabled, `IntersectAddDistinctRule` will create a `LogicalAggregationOperator` (AggType.GLOBAL) for each child, invoke statistics estimation, and compare the estimated distinct-row count against the original child row count using `StatisticsEstimateCoefficient.MEDIUM_AGGREGATE_EFFECT_COEFFICIENT`. The aggregation is kept only if the estimated distinct rows are positive and sufficiently smaller than the original (i.e., estimatedAggRows &gt; 0, originalRows &gt; 0, and estimatedAggRows * coefficient &lt;= originalRows). When disabled, the rule returns the original `INTERSECT` without inserting DISTINCTs. This is a session-level toggle accessed via `SessionVariable.isCboEnableIntersectAddDistinct()` / `setCboEnableIntersectAddDistinct(...)`.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.12, v3.4.2, v3.5.0

### cbo_enable_low_cardinality_optimize

* **Description**: Whether to enable low cardinality optimization. After this feature is enabled, the performance of querying STRING columns improves by about three times.
* **Default**: true

### cbo_enable_low_cardinality_optimize_for_join

* **Description**: Session toggle that enables rewriting of joins to use dictionary-encoded (low-cardinality) representations for string columns. When enabled, the optimizer (see `DecodeRewriter.visitPhysicalHashJoin`) will attempt to replace string columns with their dictionary ID counterparts in join ON-predicates, projections and predicates to avoid expensive string comparisons. The collector (`DecodeCollector.visitPhysicalJoin`) also uses this flag to decide whether to extract join equality groups for low-cardinality columns; note the collector currently only applies this optimization for broadcast-style joins (it skips rewrite when both sides are shuffle). If disabled, join-related low-cardinality rewrites are turned off and involved ON columns are marked to prevent rewrite. Also exposed via `SessionVariable.isEnableLowCardinalityOptimizeForJoin()`.
* **Scope**: Session
* **Default Value**: `true`
* **Type**: boolean
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

* **Description**: Session-level threshold that limits when the CBO uses the DP (dynamic programming) join reordering algorithm. If a join's number of input tables (MultiJoinNode.getAtoms().size()) is &lt;= this value and `cbo_enable_dp_join_reorder` is enabled, the optimizer will include the DP reorder (`JoinReorderDP`) during planning. This cap exists for performance reasons (the code notes a 10-table DP can exceed 100ms). The variable is consulted in `JoinReorderFactory.createJoinReorderAdaptive` and `ReorderJoinRule.transform`. It interacts with `cbo_enable_dp_join_reorder`, `cbo_enable_greedy_join_reorder`, and `cbo_max_reorder_node_use_greedy` to decide which reorder algorithms are attempted.
* **Scope**: Session
* **Default**: `10`
* **Data Type**: long
* **Introduced in**: v3.2.0

### cbo_max_reorder_node_use_exhaustive

* **Description**: Session-level integer threshold that controls which join-reorder strategy the CBO uses based on the number of inner/cross join nodes in a query. The optimizer (see `QueryOptimizer.memoOptimize` and `SPMOptimizer.memoOptimize`) counts inner/cross join nodes and compares that count against `cbo_max_reorder_node` and this variable. When the inner/cross join count is &lt; `cbo_max_reorder_node`:
  * if the count &gt; `cbo_max_reorder_node_use_exhaustive`, the optimizer runs `ReorderJoinRule()` (non‑exhaustive path), collects forced CTE statistics and applies commutativity rules;  
  * otherwise (count &lt;= `cbo_max_reorder_node_use_exhaustive`) it enables transformation‑based join reordering rules (the exhaustive transformation rules) and may enable `JoinLeftAsscomRule` when semi/anti joins are below this threshold.  
  This variable counts only inner or cross join nodes (not outer/semi/anti); it works together with `cbo_max_reorder_node` and `cbo_reorder_threshold_use_exhaustive` to select appropriate join-reorder algorithms.
* **Scope**: Session
* **Default**: `4`
* **Data Type**: int
* **Introduced in**: v3.2.0

### cbo_max_reorder_node_use_greedy

* **Scope**: Session
* **Description**: Specifies the maximum number of join nodes (atoms) in a multi-join for which the cost-based optimizer will run the greedy join reorder algorithm. The query planner checks the multi-join size and, when `multiJoinNode.getAtoms().size()` &lt;= this value and `cbo_enable_greedy_join_reorder` is enabled, it includes and executes `JoinReorderGreedy` (see ReorderJoinRule and JoinReorderFactory). This threshold limits planner work and controls when the greedy strategy is applied; use it together with `cbo_max_reorder_node_use_dp` and `cbo_enable_dp_join_reorder` to tune which reordering algorithms (DP vs. greedy) are considered.
* **Default**: `16`
* **Data Type**: long
* **Introduced in**: v3.4.0, v3.5.0

### cbo_prune_json_subfield

* **Scope**: Session
* **Description**: Controls whether the optimizer collects and uses JSON subfield access patterns to create column access paths for CBO pruning and pushdown. When enabled the planner (see `PruneSubfieldRule` and `PlanFragmentBuilder`) will:
  * collect subfield expressions and JSON function calls from project and predicate expressions,
  * normalize JSON cast/access patterns (e.g. rewrite `cast(k['a'] as ...)` to `get_json_xxx(k, "a")`) to allow flat-JSON access,
  * produce `ColumnAccessPath` entries so the scan operator can prune/limit reads to only required JSON children or push predicates down.
  Note: this optimization targets JSON/complex-type pruning; it works alongside `cbo_prune_subfield` and is bounded by `cbo_prune_json_subfield_depth`. Because execution benefits from BE support for flat JSON, ensure the backend has flat JSON enabled; otherwise enabling this may cause worse performance due to non-flat JSON access overhead.
* **Default**: ``true``
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### cbo_push_down_aggregate_with_multi_column_stats

* **Description**: Controls whether the Cost-Based Optimizer (CBO) includes multi-column combined statistics when deciding to push aggregation into scans. When enabled, `PushDownAggregateCollector` queries `statistics.getLargestSubsetMCStats(...)` and, if a multi-column combined stat exists, treats its NDV as an additional ColumnStatistic alongside single-column statistics computed by `ExpressionStatisticCalculator`. This influences grouping-cardinality estimation (including `maxMultiColumnDistinct`) and the push-down decision path that respects `sessionVariable.getCboPushDownAggregateMode()` (e.g., `PUSH_DOWN_ALL_AGG`, `DISABLE_PUSH_DOWN_AGG`). When disabled, only single-column statistics are used. Enables more accurate NDV estimation for correlated group-by columns and affects checks against broadcast-join and combined-NDV thresholds (e.g., `SMALL_BROADCAST_JOIN_MAX_COMBINED_NDV_LIMIT`, `LOWER_AGGREGATE_EFFECT_COEFFICIENT`).
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.5.0

### cbo_push_down_distinct_limit

* **Description**: Session-level threshold (row count limit) used by the optimizer when transforming a global DISTINCT aggregation into a two‑phase (local + global) aggregation. During the SplitTwoPhaseAggRule rewrite, if the query is a pure GROUP BY with a LIMIT and the query LIMIT is less than this variable, the optimizer will push that LIMIT down to the local aggregation stage (setting the local agg limit to the query LIMIT) to reduce data processed by the global stage. Concretely the rule checks a condition equivalent to `aggOp.getLimit() &lt; context.getSessionVariable().cboPushDownDistinctLimit()` and uses it to decide whether to set a non‑default localAggLimit. Smaller values make this pushdown less aggressive; larger values make it more aggressive. See `SplitTwoPhaseAggRule` for the usage site.
* **Scope**: Session
* **Default**: `4096`
* **Data Type**: long
* **Introduced in**: v3.4.1, v3.5.0

### cbo_push_down_topn_limit

* **Scope**: Session
* **Description**: Maximum TopN LIMIT that the optimizer will consider for pushing TopN operators down into lower plan nodes. The optimizer checks the TopN LIMIT against this session variable (e.g., the rules in PushDownTopNToPreAggRule, PushDownTopNBelowUnionRule and PushDownTopNBelowOuterJoinRule). If a TopN has no LIMIT or its LIMIT &gt; this value, push-down is not performed. This variable only gates the numeric LIMIT check; other rule conditions (for example, presence of OFFSET, dependencies on aggregation results, or join/union constraints) remain enforced by the individual transformation rules. Setting this value to 0 effectively disables TopN push-down for typical positive LIMITs.
* **Default**: `1000`
* **Type**: long
* **Introduced in**: v3.2.0

### cbo_use_correlated_predicate_estimate

* **Scope**: Session
* **Description**: Controls whether the optimizer applies exponential-decay weighting to additional column selectivities when estimating selectivity for conjunctive equality predicates. When enabled, `StatisticsEstimateUtils.estimateConjunctiveEqualitySelectivity` uses decay weights (0.5, 0.25, 0.125, ...) for up to 3 additional uncorrelated columns in the multi-column-statistics path and up to 3 additional columns (positions 1..3) in the fallback path (the fallback also considers up to 4 columns overall). This reduces the influence of extra columns and mitigates over‑aggressive filtering when combining multi-column combined statistics with individual column selectivities. When disabled, the decay weight is 1.0 and each additional column’s selectivity is applied fully, which can produce more aggressive (smaller) estimates. The flag is checked via `ConnectContext.get().getSessionVariable().isUseCorrelatedPredicateEstimate()` in the estimation code paths that compute conjunctive equality selectivity and compound-statistics optimization.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.5.0

### character_set_database (global)

* **Data type**: StringThe character set supported by StarRocks. Only UTF8 (`utf8`) is supported.
* **Default**: utf8
* **Data type**: String

### collation_connection

* **Description**: Session variable that specifies the connection (session) collation name returned by SHOW statements and INFORMATION_SCHEMA views. It is the session-level identifier for how the connection's collation is reported (for example in `SHOW CREATE VIEW`, `SHOW ENGINES`, `INFORMATION_SCHEMA.VIEWS` and other SHOW result metadata). The variable is referenced by `SessionVariable.COLLATION_CONNECTION` and used as a default constant in system tables (see `ViewsSystemTable.CONSTANT_MAP`) and in show-result metadata builders (`ShowResultMetaFactory`, `ShowExecutor`). Set this to a valid collation name (for example `utf8_general_ci`) to change the reported connection collation for the current session.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_database

* **Description**: Session-scoped string that holds the current session's database collation name. It represents the default collation associated with the active database and is used for client compatibility and metadata reporting (for example, what SHOW VARIABLES returns). It is related to connection- and server-level settings: `collation_connection`, `collation_server` and the character set variables such as `character_set_client`. Changing this session variable affects how the session reports the database collation and may influence client-side expectations for string comparison and display; engine-side behavior for string comparison/sorting is governed by the catalog/table column collations at runtime. This variable is exposed via the session variable machinery in SessionVariable and is serialized in session JSON/Thrift where applicable.
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_server

* **Description**: Session-scoped server collation name. This variable sets the default collation used by the SQL layer for string comparison, sorting (ORDER BY), pattern matching (LIKE) and other collation-sensitive operations when no explicit collation is provided. It is stored in the SessionVariable object (`collationServer`) and works together with `character_set_server`, `collation_connection` and `collation_database`. Accepted values are MySQL-style collation identifiers (for example `utf8_general_ci`). Changing it affects locale-sensitive behavior for the current session only.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### computation_fragment_scheduling_policy

* **Scope**: Session
* **Default**: `COMPUTE_NODES_ONLY`
* **Data Type**: String
* **Description**: Controls which node types the query planner/scheduler may choose for placing computation fragments. Valid, case‑insensitive values are:
  * `compute_nodes_only` — schedule fragments only on compute nodes (default)
  * `all_nodes` — allow scheduling on both compute nodes and BE/backend nodes
  The session setter validates the value (SetStmtAnalyzer) and will raise a SemanticException for unsupported values when executing a SET statement. The SessionVariable setter also validates and throws an IllegalArgumentException for invalid names; valid values are mapped to the enum SessionVariableConstants.ComputationFragmentSchedulingPolicy and stored in uppercase. The getter returns the corresponding enum and falls back to `COMPUTE_NODES_ONLY` if the stored value is missing or unrecognized.
* **Introduced in**: v3.2.7

### connector_io_tasks_per_scan_operator

* **Description**: The maximum number of concurrent I/O tasks that can be issued by a scan operator during external table queries. The value is an integer. Currently, StarRocks can adaptively adjust the number of concurrent I/O tasks when querying external tables. This feature is controlled by the variable `enable_connector_adaptive_io_tasks`, which is enabled by default.
* **Default**: 16
* **Data type**: Int
* **Introduced in**: v2.5

### connector_sink_compression_codec

* **Description**: Specifies the compression algorithm used for writing data into Hive tables or Iceberg tables, or exporting data with Files().
* **Valid values**: `uncompressed`, `snappy`, `lz4`, `zstd`, and `gzip`.
* **Default**: uncompressed
* **Data type**: String
* **Introduced in**: v3.2.3

### connector_sink_target_max_file_size

* **Description**: Specifies the maximum size of target file for writing data into Hive tables or Iceberg tables, or exporting data with Files(). The limit is not exact and is applied on a best-effort basis.
* **Unit**: Bytes
* **Default**: 1073741824
* **Data type**: Long
* **Introduced in**: v3.3.0

### count_distinct_column_buckets

* **Description**: The number of buckets for the COUNT DISTINCT column in a group-by-count-distinct query. This variable takes effect only when `enable_distinct_column_bucketization` is set to `true`.
* **Default**: 1024
* **Introduced in**: v2.5

### count_distinct_implementation

* **Description**: Controls the session-level mode that the planner/optimizer uses for implementing COUNT(DISTINCT ...). Valid modes (case-insensitive) are: `DEFAULT` (keep the original implementation), `NDV` (use HyperLogLog to estimate distinct count), and `MULTI_COUNT_DISTINCT` (use the multi-count-distinct implementation). The setter is validated by the analyzer (SetStmtAnalyzer) against SessionVariableConstants.CountDistinctImplMode; SessionVariable.getCountDistinctImplementation() parses the stored string into that enum. This variable interacts with `enable_count_distinct_rewrite_by_hll_bitmap`, which controls whether created materialized views' bitmap/HLL are used for rewriting COUNT DISTINCT (such rewrites may be approximate).
* **Default**: ``default``
* **Data Type**: String
* **Introduced in**: v3.3.6, v3.4.0, v3.5.0

### custom_query_id (session)

* **Description**: Used to bind some external identifier to a current query. Can be set using `SET SESSION custom_query_id = 'my-query-id';` before executing a query. The value is reset after query is finished. This value can be passed to `KILL QUERY 'my-query-id'`. Value can be found in audit logs as a `customQueryId` field.
* **Default**: ""
* **Data type**: String
* **Introduced in**: v3.4.0

### datacache_sharing_work_period

* **Description**: The period of time that Cache Sharing takes effect. After each cluster scaling operation, only the requests within this period of time will try to access the cache data from other nodes if the Cache Sharing feature is enabled.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### decimal_overflow_to_double

* **Scope**: Session
* **Description**: Controls analyzer behavior when a decimal expression's required precision exceeds the engine's maximum decimal precision. If enabled (`true`), the analyzer converts the expression result and operands to `DOUBLE` (floating-point) when the computed return precision &gt; maximum supported decimal precision while return scale &lt;= maximum supported decimal precision, avoiding decimal-width overflow during type derivation. If disabled (`false`, default), the analyzer prefers the widest available decimal type (e.g., `DECIMAL128`/`DECIMAL256`) or raises a `SemanticException` if the return scale cannot be represented. This flag is consulted in `DecimalV3FunctionAnalyzer` during decimal arithmetic/type resolution and exposed on the session via `SessionVariable`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: -

### default_authentication_plugin

* **Scope**: Session
* **Description**: Session-level variable that holds the default authentication plugin name for this session. It is declared in `SessionVariable` (annotated with `@VariableMgr.VarAttr`) and therefore exposed to clients and included when session variables are serialized to JSON. Typical values are MySQL-compatible plugin identifiers such as `mysql_native_password` or `caching_sha2_password`. Setting this variable changes the value stored for the current session only and does not modify global server configuration. The variable is primarily for MySQL-compatibility with client libraries and for storing which authentication plugin should be used/considered for operations in this session; the actual authentication behavior depends on the server’s authentication implementation and `authentication_policy`.
* **Default**: `mysql_native_password`
* **Data Type**: String
* **Introduced in**: -

### default_rowset_type (global)

Used to set the default storage format used by the storage engine of the computing node. The currently supported storage formats are `alpha` and `beta`.

### default_storage_engine

* **Description**: Session-scoped variable (exposed as `default_storage_engine`) provided for MySQL 8.0 client compatibility. It records the default storage engine name reported to clients/connectors. The variable is declared in SessionVariable.java with `@VariableMgr.VarAttr`, so it can be read and set per session. There is no evidence in the codebase that StarRocks uses this value to change internal storage behavior; it exists to satisfy MySQL client/driver expectations (see the companion `default_tmp_storage_engine` variable).
* **Scope**: Session
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

### default_table_compression

* **Description**: The default compression algorithm for table storage. Supported compression algorithms are `snappy, lz4, zlib, zstd`.

  Note that if you specified the `compression` property in a CREATE TABLE statement, the compression algorithm specified by `compression` takes effect.

* **Default**: lz4_frame
* **Introduced in**: v3.0

### default_tmp_storage_engine

* **Description**: Session-level string that sets the default storage engine name used for temporary tables when CREATE TEMPORARY TABLE statements omit an explicit ENGINE. Declared in code for MySQL-compatibility (annotated with `@VariableMgr.VarAttr`) and initialized to `InnoDB`. Changing this variable affects only the current session and influences how the system selects the engine for implicitly created temporary tables; it does not alter global server defaults.
* **Scope**: Session
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

### disable_colocate_join

* **Description**: Used to control whether the Colocation Join is enabled. The default value is `false`, meaning the feature is enabled. When this feature is disabled, query planning will not attempt to execute Colocation Join.
* **Default**: false

### disable_colocate_set

* **Description**: Session flag to disable the "colocate set" optimization for set operations (UNION / INTERSECT / EXCEPT). The planner checks this variable in ChildOutputPropertyGuarantor and, when the flag is false (default), will attempt to use colocated execution if child fragments expose a local hash distribution with matching distribution columns. When this flag is true, the optimizer will not try to use colocated set execution and will instead enforce alternative distributions (bucket-shuffle or round-robin for DISTINCT union) so that children satisfy required shuffle properties. Accessors: `isDisableColocateSet()` / `setDisableColocateSet(boolean)`. See usage in `SessionVariable.java` and `ChildOutputPropertyGuarantor.java`.
* **Default**: false
* **Data Type**: boolean
* **Scope**: Session
* **Introduced in**: v3.5.0

### disable_join_reorder

* **Scope**: Session
* **Description**: When set to `true`, the cost-based optimizer skips all join reordering phases. Specifically, the planner will not apply `ReorderJoinRule`, join transformation rules, outer-join transformation rules, or related join-commutativity expansions that are normally added when reordering is allowed. Code paths in `QueryOptimizer` and `SPMOptimizer` check this flag and guard reordering when evaluating conditions such as innerCrossJoinNode &lt; `CBO_MAX_REORDER_NODE` and outer-join reorderability. Use this to force the optimizer to preserve the original join order (for debugging, stable plans, or when reorder produces worse plans). Helper methods available on `SessionVariable` include `isDisableJoinReorder()`, `disableJoinReorder()`, `enableJoinReorder()`, and `enableJoinReorder(boolean)`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### disable_spill_to_local_disk

* **Scope**: Session
* **Description**: When enabled (`true`), instructs the engine to avoid writing spill files to local disk and force spill to remote storage paths configured for the session. This setting is only meaningful when `enable_spill_to_remote_storage` is `true` and a valid `spill_storage_volume` is found; in that code path the session value is mapped to the Thrift field TSpillToRemoteStorageOptions.disable_spill_to_local_disk. When disabled (`false`, the default), local disk may be used as a fallback for spill I/O. The variable affects only the current session's spill behavior and is ignored if remote spill is not enabled or no remote storage volume is configured.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### div_precision_increment

Used for MySQL client compatibility. No practical usage.

### dynamic_overwrite

* **Description**: Whether to enable the [Dynamic Overwrite](./sql-statements/loading_unloading/INSERT.md#dynamic-overwrite) semantic for INSERT OVERWRITE with partitioned tables. Valid values:
  * `true`: Enables Dynamic Overwrite.
  * `false`: Disables Dynamic Overwrite and uses the default semantic.
* **Default**: false
* **Introduced in**: v3.4.0

### enable_adaptive_sink_dop

* **Description**: Specifies whether to enable adaptive parallelism for data loading. After this feature is enabled, the system automatically sets load parallelism for INSERT INTO and Broker Load jobs, which is equivalent to the mechanism of `pipeline_dop`. For a newly deployed v2.5 StarRocks cluster, the value is `true` by default. For a v2.5 cluster upgraded from v2.4, the value is `false`.
* **Default**: false
* **Introduced in**: v2.5

### enable_bucket_aware_execution_on_lake

* **Description**: Whether to enable bucket-aware execution for queries against data lakes (such as Iceberg tables). When this feature is enabled, the system optimizes query execution by leveraging bucketing information to reduce data shuffling and improve performance. This optimization is particularly effective for join operations and aggregations on bucketed tables.
* **Default**: true
* **Data type**: Boolean
* **Introduced in**: v4.0

### enable_cbo_based_mv_rewrite

* **Description**: Whether to enable materialized view rewrite in CBO phase which can maximize the likelihood of successful query rewriting (e.g., when the join order differs between materialized views and queries), but it will increase the execution time of the optimizer phase.
* **Default**: true
* **Introduced in**: v3.5.5, v4.0.1

### enable_cbo_table_prune

* **Scope**: Session
* **Description**: Enable CBO-driven table pruning for cardinality-preserving joins. When set to true, the optimizer (both QueryOptimizer and SPMOptimizer) will add the CboTablePruneRule into the memo optimization rule set if the count of join nodes for the CBO prune join types is &lt; 10. This flag only controls whether the pruning rule is considered during memo optimization; it does not change the pruning rule's internal thresholds or join-reordering behavior. Use `enable_rbo_table_prune` to enable the RBO variant and `enable_table_prune_on_update` to control pruning on UPDATE statements.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_color_explain_output

* **Scope**: Session
* **Description**: When enabled, the server emits ANSI color escape sequences in EXPLAIN and profile-based explain outputs so terminals and clients that support ANSI colors display a colored, more readable plan. The session variable is consulted by StmtExecutor when formatting explain results (including profile-driven explains in handleDMLStmtWithProfile and AnalyzeProfileStmt) via calls such as ExplainAnalyzer.analyze(..., context.getSessionVariable().getColorExplainOutput()). Toggle this per-session when connecting from clients that do or do not support ANSI color sequences.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.5.0

### enable_connector_adaptive_io_tasks

* **Description**: Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value is `true`. If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.
* **Default**: true
* **Introduced in**: v2.5

### enable_cost_based_multi_stage_agg

* **Description**: Controls whether the planner uses a cost-driven strategy to choose multi-stage aggregation shapes for DISTINCT aggregates. When enabled (default), the optimizer (via `SplitMultiPhaseAggRule`) may produce alternative multi-stage candidates (e.g. 3-stage and 4-stage variants such as Local → DistinctGlobal → Local → Global) and let the cost model pick the cheapest. The pruning/merge logic in `PruneAggregateNodeRule` also considers this flag when merging split two-phase aggregates to avoid unnecessary serialization. The effective check in code is gated by `new_planner_agg_stage` (the helper `isEnableCostBasedMultiStageAgg()` requires `new_planner_agg_stage` to be `auto` and this flag to be true). Set to `false` to force the simpler, non-cost-based transformation path (fewer alternative candidates and reduced aggressive merging).
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_count_distinct_rewrite_by_hll_bitmap

* **Scope**: Session
* **Description**: Controls whether the optimizer may rewrite distinct-count aggregates to use existing HLL/Bitmap materialized views (MVs) or MV-built HLL/Bitmap functions. When enabled, the materialization rewrite rules (e.g., HLLRewriteEquivalent and BitmapRewriteEquivalent) can transform queries such as `COUNT(DISTINCT ...)`, `APPROX_COUNT_DISTINCT`, `NDV`, `MULTI_DISTINCT_COUNT`, `BITMAP_UNION_COUNT`, and `BITMAP_AGG` to use the MV's precomputed HLL/Bitmap operations (for example, `HLL_UNION_AGG`, `HLL_CARDINALITY`, `BITMAP_UNION`, `BITMAP_COUNT`, etc.) to push down or replace the original aggregate. Because HLL/Bitmap-based rewrites can produce approximate results that do not exactly match the original exact `COUNT(DISTINCT)` semantics, disable this variable to force the optimizer to avoid HLL/Bitmap rewrites and preserve exact results.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.6, v3.4.0, v3.5.0

### enable_datacache_async_populate_mode

* **Description**: Whether to populate the data cache in asynchronous mode. By default, the system uses the synchronous mode to populate data cache, that is, populating the cache while querying data.
* **Default**: false
* **Introduced in**: v3.2.7

### enable_datacache_io_adaptor

* **Description**: Whether to enable the Data Cache I/O Adaptor. Setting this to `true` enables the feature. When this feature is enabled, the system automatically routes some cache requests to remote storage when the disk I/O load is high, reducing disk pressure.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_datacache_sharing

* **Description**: Whether to enable Cache Sharing. Setting this to `true` enables the feature. Cache Sharing is used to support accessing cache data from other nodes through the network, which can help to reduce performance jitter caused by cache invalidation during cluster scaling. This variable takes effect only when the FE parameter `enable_trace_historical_node` is set to `true`.
* **Default**: true
* **Introduced in**: v3.5.1

### enable_distinct_agg_over_window

* **Scope**: Session
* **Description**: Controls an optimizer rewrite that transforms DISTINCT aggregations computed over window functions into an equivalent null-safe-equality join plan. When enabled, QueryOptimizer runs preparatory rewrites (merge/separate project rules and logical property derivation) and applies DistinctAggregationOverWindowRule to replace DISTINCT aggregation-over-window patterns (LogicalWindowOperator present in the plan) with a join-based implementation that can be more efficient or enable further optimizations. When disabled, the optimizer skips this rewrite and leaves DISTINCT-over-window expressions intact. This variable interacts with `optimize_distinct_agg_over_framed_window`, which governs framed-window specific optimizations.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_distinct_column_bucketization

* **Description**: Whether to enable bucketization for the COUNT DISTINCT colum in a group-by-count-distinct query. Use the `select a, count(distinct b) from t group by a;` query as an example. If the GROUP BY colum `a` is a low-cardinality column and the COUNT DISTINCT column `b` is a high-cardinality column which has severe data skew, performance bottleneck will occur. In this situation, you can split data in the COUNT DISTINCT column into multiple buckets to balance data and prevent data skew. You must use this variable with the variable `count_distinct_column_buckets`.

  You can also enable bucketization for the COUNT DISTINCT column by adding the `skew` hint to your query, for example, `select a,count(distinct [skew] b) from t group by a;`.

* **Default**: false, which means this feature is disabled.
* **Introduced in**: v2.5

### enable_eliminate_agg

* **Description**: Controls optimizer transformations that eliminate or simplify aggregation when it is provably redundant. When enabled, the optimizer (see `EliminateAggRule` and `EliminateAggFunctionRule`) will:
    * Replace a `LogicalAggregationOperator` with a `LogicalProjectOperator` if grouping keys are unique for the child (uses UK/FK and unique-key constraints). Supported functions in this rule: `SUM`, `COUNT`, `AVG`, `FIRST_VALUE`, `MAX`, `MIN`, `GROUP_CONCAT`. COUNT is rewritten to `IF(col IS NULL, 0, 1)` or a constant for count(*); other aggregates are rewritten to the underlying column (with cast if needed).
    * Simplify aggregate functions that directly return a grouped column (supported: `FIRST_VALUE`, `LAST_VALUE`, `ANY_VALUE`, `MAX`, `MIN`) by projecting the column and leaving remaining aggregates unchanged.
    Both `EliminateAggRule.check(...)` and `EliminateAggFunctionRule.check(...)` first test this session flag and will skip these transformations if it is false.
* **Default**: `true`
* **Data Type**: boolean
* **Scope**: Session
* **Introduced in**: v3.3.8, v3.4.0, v3.5.0

### enable_evaluate_schema_scan_rule

* **Scope**: Session
* **Description**: When enabled, the optimizer may evaluate simple schema/system-table queries inside the FE instead of generating a plan sent to BE. This flag gates the `SchemaTableEvaluateRule`, which:
    * activates for `LogicalSchemaScan` over `SystemTable` instances;
    * requires `systemTable.supportFeEvaluation(predicate)` to return true for the scan predicate;
    * calls `systemTable.evaluate(predicate)` to produce rows and replaces the scan with a `LogicalValuesOperator` containing constant result rows, avoiding RPC/plan execution on BE.
  If `evaluate` is not implemented (throws `NotImplementedException`) or the predicate is not supported, the rule does not transform the plan. Use this session-scoped variable to enable or disable FE-side evaluation of eligible metadata/system-table queries to reduce RPC overhead and latency.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.3, v3.4.0, v3.5.0

### enable_filter_unused_columns_in_scan_stage

* **Description**: When enabled, the planner (PlanFragmentBuilder) computes columns that are only used by predicates that can be pushed down to the storage layer and are not needed by later plan stages or the final output. It marks those column ids on the scan node (via OlapScanNode.setUnUsedOutputStringColumns) so the scan stage can avoid materializing/fetching them, reducing IO and memory. The pruning logic:
  - Builds requiredColumns from output columns and materialized view (MV) use columns.
  - Collects predicateColumns from pushdownable predicates.
  - Marks unused = predicateColumns minus requiredColumns for pruning.
  - Does not prune key/value columns in non-skip-aggregation scan stages; for aggregation-family indexes it skips pruning when pre-aggregation is not enabled.
  Control methods in SessionVariable: isEnableFilterUnusedColumnsInScanStage(), enableTrimOnlyFilteredColumnsInScanStage(), disableTrimOnlyFilteredColumnsInScanStage().

* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_force_rule_based_mv_rewrite

* **Description**: Whether to enable query rewrite for queries against multiple tables in the optimizer's rule-based optimization phase. Enabling this feature will improve the robustness of the query rewrite. However, it will also increase the time consumption if the query misses the materialized view.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_gin_filter

* **Description**: Whether to utilize the [fulltext inverted index](../table_design/indexes/inverted_index.md) during queries.
* **Default**: true
* **Introduced in**: v3.3.0

### enable_global_runtime_filter

Whether to enable global runtime filter (RF for short). RF filters data at runtime. Data filtering often occurs in the Join stage. During multi-table joins, optimizations such as predicate pushdown are used to filter data, in order to reduce the number of scanned rows for Join and the I/O in the Shuffle stage, thereby speeding up the query.

StarRocks offers two types of RF: Local RF and Global RF. Local RF is suitable for Broadcast Hash Join and Global RF is suitable for Shuffle Join.

Default value: `true`, which means global RF is enabled. If this feature is disabled, global RF does not take effect. Local RF can still work.

### enable_group_by_compressed_key

* **Description**: Whether to use accurate statistical information to compress the GROUP BY Key column. Valid values: `true` and `false`.
* **Default**: true
* **Introduced in**: v4.0

### enable_group_execution

* **Scope**: Session
* **Description**: Enable the planner optimization that groups compatible fragments into colocated execution groups to reduce shuffle and enable colocated execution. When active, PlanFragmentBuilder will mark ExecGroups as colocate groups and merge left/right groups for colocate joins, enable group execution for aggregation stages (unless a local shuffle is present), and set colocate behavior for set operations. This variable is consulted throughout plan construction (e.g., aggregation, join and set-operation handling). Note: it is implicitly disabled when runtime adaptive DOP is enabled because runtime DOP requires join probes to wait for builds and conflicts with group execution. Related session configs: `group_execution_group_scale`, `group_execution_max_groups`, `group_execution_min_scan_rows`.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### enable_group_level_query_queue (global)

* **Description**: Whether to enable resource group-level [query queue](../administration/management/resource_management/query_queues.md).
* **Default**: false, which means this feature is disabled.
* **Introduced in**: v3.1.4

### enable_groupby_use_output_alias

* **Description**: Controls whether the analyzer rewrites references that match SELECT output aliases to use the output alias (and its expression) as the GROUP BY / HAVING / ORDER BY key instead of the original table column. When enabled, SelectAnalyzer's alias-rewrite logic will:
  - Resolve SlotRef against the projection (output) scope and replace it with the corresponding output expression when an output alias exists with the same name.
  - Remove table name from SlotRef when safe (to favor the output alias).
  This behavior is provided for compatibility with the old planner and to support client tools (for example, Tableau queries against `information_schema.tables` are temporarily allowed by toggling this session variable during analysis). Default behavior (when disabled) keeps the original column reference semantics and avoids alias-based rewriting.

* **Scope**: Session

* **Default**: `false`

* **Data type**: boolean

* **Introduced in**: v3.2.0

### enable_hash_join_linear_chained_opt

* **Description**: Session toggle that controls a hash-join implementation optimization named "linear chained". When enabled, the session variable is serialized into TQueryOptions as `enable_hash_join_linear_chained_opt` (see SessionVariable.toThrift) so the execution engine on BE/CN can choose the linear-chained variant of the hash-join operator (i.e., a different hash table layout / collision-resolution and probe behavior compared to the baseline). This flag is evaluated at execution time and only affects the current session's queries.
* **Scope**: Session (session-only; changing it affects only the current session)
* **Default Value**: `true`
* **Type**: boolean
* **Introduced in**: v4.0.0

Use locations: defined and read in fe-core at com/starrocks/qe/SessionVariable.java (`enableHashJoinLinearChainedOpt`) and forwarded to query runtime via TQueryOptions.

### enable_hash_join_range_direct_mapping_opt (Session)

* **Description**: Enables an execution-time optimization for hash joins that uses a direct-mapping (array-indexed) lookup when the build-side join key domain is dense and small. When enabled, the planner/runtime can replace a general-purpose hash table with a direct mapping table for eligible join keys (typically integral types with a compact value range), which removes hashing overhead and reduces probe latency. If the key domain is large or sparse, the engine falls back to the normal hash table implementation. This setting is per-session and can be disabled to force the default hash-table code path. Works alongside other hash-join optimizations such as `enable_hash_join_linear_chained_opt` and `enable_hash_join_serialize_fixed_size_string`.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v4.0.0

### enable_hash_join_serialize_fixed_size_string

* **Description**: Session boolean that controls whether fixed-size string keys are serialized into a fixed-length representation for hash join operators. When `enable_hash_join_serialize_fixed_size_string` is true (default), the FE includes this flag in the session Thrift payload (see `tResult.setEnable_hash_join_serialize_fixed_size_string(...)`), instructing the execution layer to use a fixed-size serialization/layout for fixed-length string types (for example, CHAR or other fixed-width string encodings) when building or probing hash tables. Enabling this can improve memory layout predictability and comparison speed for hash joins; disabling it forces the engine to use the standard (variable-length) serialization path and may be used for compatibility or correctness reasons.
* **Default**: true
* **Data Type**: boolean
* **Introduced in**: -

### enable_hive_column_stats

* **Description**: Controls whether StarRocks retrieves per-column statistics from external Hive/Hudi metadata when building table statistics. When enabled, Hive and Hudi connectors call their statistics provider to fetch column-level stats used by the optimizer (see `HiveMetadata.getTableStatistics` and `HudiMetadata.getTableStatistics`). When disabled, StarRocks skips fetching external column stats and falls back to default/unknown statistics, which may reduce optimizer accuracy and change plan selection. Note: external-statistics retrieval also requires the connector-level setting `enable_get_table_stats_from_external_metadata` to be enabled; if that setting is disabled, column stats will not be fetched even when this session variable is true.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_hive_metadata_cache_with_insert

* **Description**: When enabled, StarRocks will consult and (where applicable) update the Hive metadata cache when planning and executing INSERT operations into Hive external tables. This reduces calls to the Hive metastore for partition discovery and table/format metadata during INSERT, improving write planning and execution latency. The setting is session-scoped and affects only the current session; it only applies to operations targeting Hive external tables and requires `enable_write_hive_external_table` to be enabled and a configured Hive connector. Be aware that using cached metadata can return slightly stale partition information; disable this flag when strict, immediate metastore consistency is required.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_hyperscan_vec

* **Scope**: Session
* **Description**: Enable or disable Hyperscan-based vectorized pattern/predicate scanning during query execution. When set to true, the FE includes this flag in the serialized query options (sent to BE via `TQueryOptions.enable_hyperscan_vec`), allowing backends to use the Hyperscan acceleration path for applicable predicates (e.g., regex or multi-pattern matching). Turn this off to avoid Hyperscan-specific behavior or compatibility issues on BEs that do not support the feature or when debugging query execution differences.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.7

### enable_incremental_mv

* **Scope**: Session
* **Description**: Session-level boolean toggle that enables planning of an incremental refresh execution plan for CREATE MATERIALIZED VIEW statements that specify an incremental refresh scheme. When set to `true`, MaterializedViewAnalyzer.planMVQuery attempts to build and optimize a logical and physical plan and create an ExecPlan (via PlanFragmentBuilder.createPhysicalPlanForMV) for views whose refresh scheme is an IncrementalRefreshSchemeDesc. When `false`, the analyzer returns immediately and no plan is produced or stored. The session getter is `isEnableIncrementalRefreshMV()` and the setter is `setEnableIncrementalRefreshMv(boolean)`. This flag is used only during the MV creation analysis path and does not persist beyond the session.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_insert_partial_update

* **Description**: Whether to enable Partial Update for INSERT statements on Primary Key tables. When this item is set to `true` (default), if an INSERT statement specifies only a subset of columns (fewer than the number of all non-generated columns in the table), the system performs a Partial Update to update only the specified columns while preserving existing values in other columns. When set to `false`, the system uses default values for unspecified columns instead of preserving existing values. This feature is particularly useful for updating specific columns in Primary Key tables without affecting other column values.
* **Default**: true
* **Introduced in**: v3.3.20, v3.4.9, v3.5.8, v4.0.2

### enable_insert_strict

* **Description**: Whether to enable strict mode while loading data using INSERT from files(). Valid values: `true` and `false` (Default). When strict mode is enabled, the system loads only qualified rows. It filters out unqualified rows and returns details about the unqualified rows. For more information, see [Strict mode](../loading/load_concept/strict_mode.md). In versions earlier than v3.4.0, when `enable_insert_strict` is set to `true`, the INSERT jobs fails when there is an unqualified rows.
* **Default**: true

### enable_lake_tablet_internal_parallel

* **Description**: Whether to enable Parallel Scan for Cloud-native tables in a shared-data cluster.
* **Default**: true
* **Data type**: Boolean
* **Introduced in**: v3.3.0

### enable_lambda_pushdown

* **Scope**: Session
* **Description**: Controls whether the optimizer is allowed to push filter predicates down through Project operators that contain lambda expressions. When enabled (`true`), the PushDownPredicateProjectRule may rewrite and push deterministic predicates below a Project even if the Project defines expressions with `LambdaFunctionOperator`. When disabled (`false`), the rule aborts predicate pushdown if any project expression contains a lambda (the rule returns no transformation), preventing potentially unsafe rewrites involving lambda functions. This variable is read via `SessionVariable.getEnableLambdaPushDown()` and written by `setEnableLambdaPushdown(...)` and is directly referenced in PushDownPredicateProjectRule to guard the pushdown logic.
* **Default**: `true`
* **Data type**: boolean
* **Introduced in**: v3.3.6, v3.4.0, v3.5.0

### enable_large_in_predicate

* **Scope**: Session
* **Description**: When true, the parser recognizes IN/NOT IN predicates that contain many literal constants (count &gt;= `large_in_predicate_threshold`) and constructs a compact LargeInPredicate representation using raw constant lists instead of individual expression nodes. During optimization this pattern is converted to a `RawValues`-backed plan and implemented as a left-semi (IN) or null-aware left-anti (NOT IN) join, reducing FE memory usage and planning time for very large IN lists. The transformation has strict restrictions (no OR compound predicates, only one LargeInPredicate per query, and compatible types); if the rule cannot safely apply it throws LargeInPredicateException. Upper layers (StmtExecutor / ConnectProcessor) catch that exception and re-parse/re-run the query with `enable_large_in_predicate` disabled so the query uses the traditional expression-based IN path. The session variable has a getter/setter and can be toggled per-session.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_load_profile

* **Scope**: Session
* **Description**: When set to true, the session requests generation and collection of a runtime profile for load jobs after they finish. The session variable is checked by the load coordinator (e.g., StreamLoadTask uses `coord.isEnableLoadProfile()` to decide whether to call `collectProfile(false)`), and it controls profile-related behavior during stream/load operations (used in DefaultCoordinator, StreamLoadMgr, StreamLoadPlanner, QueryRuntimeProfile, StatisticUtils). Enabling this option causes the FE to collect/upload additional profiling information for the load, which incurs extra work and I/O; it does not affect regular query profiling controlled by other profile variables.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_local_shuffle_agg

* **Scope**: Session
* **Description**: Controls whether the optimizer and cost model may generate a one-phase local aggregation using a local shuffle operator (e.g., ScanNode-&gt;LocalShuffleNode-&gt;OnePhaseAggNode) even when grouping keys differ from the scan distribution keys. When enabled, the planner can:
  - In CostModel: prefer one-phase local aggregation and ignore network cost for `SHUFFLE` in single-backend single-compute-node clusters when `enable_pipeline_engine` is true.
  - In rewrite rules (PruneShuffleDistributionNodeRule): replace an Exchange/Shuffle between Scan and Global Agg with a local shuffle and mark the aggregate as local.
  - In various planners (Insert/Update/Delete/MaterializedViewOptimizer): allow plan shapes that use local shuffle aggregation when safe.
  This optimization is only applied when the cluster is a single backend-and-compute-node and `enable_pipeline_engine` is enabled. Disable to force conventional shuffle/exchange behavior.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_materialized_view_agg_pushdown_rewrite

* **Description**: Whether to enable aggregation pushdown for materialized view query rewrite. If it is set to `true`, aggregate functions will be pushed down to Scan Operator during query execution and rewritten by the materialized view before the Join Operator is executed. This will relieve the data expansion caused by Join and thereby improve the query performance. For detailed information about the scenarios and limitations of this feature, see [Aggregation pushdown](../using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views.md#aggregation-pushdown).
* **Default**: false
* **Introduced in**: v3.3.0

### enable_materialized_view_for_insert

* **Description**: Whether to allow StarRocks to rewrite queries in INSERT INTO SELECT statements.
* **Default**: false, which means Query Rewrite in such scenarios is disabled by default.
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7, v3.2.2

### enable_materialized_view_text_match_rewrite

* **Description**: Whether to enable text-based materialized view rewrite. When this item is set to true, the optimizer will compare the query with the existing materialized views. A query will be rewritten if the abstract syntax tree of the materialized view's definition matches that of the query or its sub-query.
* **Default**: true
* **Introduced in**: v3.2.5, v3.3.0

### enable_materialized_view_union_rewrite

* **Description**: Whether to enable materialized view union rewrite. If this item is set to `true`, the system seeks to compensate the predicates using UNION ALL when the predicates in the materialized view cannot satisfy the query's predicates.
* **Default**: true
* **Introduced in**: v2.5.20, v3.1.9, v3.2.7, v3.3.0

### enable_metadata_profile

* **Description**: Whether to enabled Profile for Iceberg Catalog metadata.
* **Default**: true
* **Introduced in**: v3.3.3

### enable_multicolumn_global_runtime_filter

Whether to enable multi-column global runtime filter. Default value: `false`, which means multi-column global RF is disabled.

If a Join (other than Broadcast Join and Replicated Join) has multiple equi-join conditions:

* If this feature is disabled, only Local RF works.
* If this feature is enabled, multi-column Global RF takes effect and carries `multi-column` in the partition by clause.

### enable_mv_planner

* **Description**: Session-level boolean switch that enables the Materialized View (MV) planner path for the current session. When enabled, the optimizer activates MV-specific behaviors: the QueryOptimizer adds realtime MV rewrite/implementation rules instead of the normal join implementation rules; aggregation-splitting rules such as `SplitTwoPhaseAggRule` and `SplitMultiPhaseAggRule` are skipped; `StreamImplementationRule` and other MV-only implementation rules become applicable; and `RelationTransformer` uses MV/binlog scan operators for native tables during MV plan construction. This flag is set temporarily by `MaterializedViewAnalyzer` (via `setMVPlanner(true)`) when planning or preparing incremental MV refresh to ensure the planner produces MV-aware logical and physical plans. Default is `false` so normal query planning is used unless explicitly turned on for MV planning tasks.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_optimize_skew_join_v1

* **Scope**: Session
* **Description**: Controls whether the optimizer applies skew-join optimization using a query-rewrite path. When enabled, QueryOptimizer invokes the skew-join rewrite (skewJoinOptimize) after pushing down join-on expressions to child projects so that child statistics (for example subfield stats) can be used to detect and rewrite skewed joins. This option is mutually exclusive with `enableOptimizerSkewJoinByBroadCastSkewValues`: setting one will flip the other (the SessionVariable setters keep them inverse). Use this to prefer query-rewrite based skew-join handling instead of the broadcast-skew-values strategy.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_optimize_skew_join_v2

* **Description**: Session-level boolean flag that selects the "broadcast skew values" variant of skew-join optimization (v2). This variable is mutually exclusive with the query-rewrite variant `enable_optimize_skew_join_v1`: setting one flips the other (the setter for this variable sets the query-rewrite flag to the logical negation). When `false` (default) the optimizer follows the query-rewrite path and may invoke `skewJoinOptimize(...)` (see QueryOptimizer), which applies the `SkewJoinOptimizeRule` to detect skewed values from column histograms and rewrite joins by adding salts/unnest. When `true`, the query-rewrite path is explicitly disabled (`isEnableOptimizerSkewJoinByQueryRewrite()` becomes `false`), enabling the alternative broadcast-skew-values strategy instead of the `SkewJoinOptimizeRule` rewrite.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: -

### enable_parallel_merge

* **Scope**: Session
* **Description**: Controls whether the query planner can choose a parallel-merge strategy for ORDER BY / TopN operations. When enabled, the planner computes a threshold = degreeOfParallelism * chunkSize (using the current session's DOP and chunk size); if disabled the threshold is set to -1 and parallel merge is never used. For full-sort (no LIMIT) the planner will use parallel merge when the input row count is unknown or inputRowCount &gt; threshold. For TopN (LIMIT present) the planner uses parallel merge when topN &gt; threshold (where topN = limit + offset). The variable is read from the session via SessionVariable.isEnableParallelMerge() and is exposed as the session configuration `enable_parallel_merge`.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_parquet_reader_bloom_filter

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable Bloom Filter optimization when reading Parquet files.
  * `true` (Default): Enable Bloom Filter optimization when reading Parquet files.
  * `false`: Disable Bloom Filter optimization when reading Parquet files.
* **Introduced in**: v3.5.0

### enable_parquet_reader_page_index

* **Default**: true
* **Type**: Boolean
* **Unit**: -
* **Description**: Whether to enable Page Index optimization when reading Parquet files.
  * `true` (Default): Enable Page Index optimization when reading Parquet files.
  * `false`: Disable Page Index optimization when reading Parquet files.
* **Introduced in**: v3.5.0

### enable_partition_hash_join

* **Description**: Whether to enable adaptive Partition Hash Join.
* **Default**: true
* **Introduced in**: v3.4

### enable_per_bucket_optimize

* **Scope**: Session
* **Description**: Enables the "per-bucket" aggregation optimization applied by the planner for eligible queries. When set, the optimizer (PhysicalDistributionAggOptRule) marks one-stage global hash aggregates that have an OLAP scan child and non-empty GROUP BY to use per-bucket execution: it sets the aggregate flag (use per-bucket optimize) and instructs the scan to output chunks by bucket. The optimization is skipped for cloud-native tables and is guarded by other session flags — the rule is not applied when `enable_query_cache` is on, when `enable_sort_aggregate` is chosen, or when `enable_spill` with spill mode set to "force". If `enable_partition_bucket_optimize` is also enabled and there is no colocate requirement, the rule can clear colocate requirements for the aggregate/scan when partition columns are covered by GROUP BY. Note: this per-bucket path is slated to be replaced by group execution (planned removal in v4.0).
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_phased_scheduler

* **Description**: Whether to enable multi-phased scheduling. When multi-phased scheduling is enabled, it will schedule fragments according to their dependencies. For example, the system will first schedule the fragment on the build side of a Shuffle Join, and then the fragment on the probe side (Note that, unlike stage-by-stage scheduling, phased scheduling is still under the MPP execution mode). Enabling multi-phased scheduling can significantly reduce memory usage for a large number of UNION ALL queries.
* **Default**: false
* **Introduced in**: v3.3

### enable_pipeline_engine

* **Description**: Specifies whether to enable the pipeline execution engine. `true` indicates enabled and `false` indicates the opposite. Default value: `true`.
* **Default**: true

### enable_pipeline_level_multi_partitioned_rf

* **Scope**: Session
* **Description**: When enabled, the planner produces pipeline-level multi-partitioned runtime-filter layouts instead of the legacy 1-level/global singleton layouts. RuntimeFilterDescription uses this flag to choose local and global layouts:
  - BROADCAST / REPLICATED =&gt; `SINGLETON`
  - PARTITIONED / SHUFFLE_HASH_BUCKET =&gt; local `PIPELINE_SHUFFLE`, global `GLOBAL_SHUFFLE_2L`
  - COLOCATE / LOCAL_HASH_BUCKET =&gt; `PIPELINE_BUCKET` or `PIPELINE_BUCKET_LX` (local) and `GLOBAL_BUCKET_2L` or `GLOBAL_BUCKET_2L_LX` (global), depending on bucket-to-driver mapping presence
  - otherwise =&gt; `NONE`
  When disabled, the planner falls back to the previous layouts (e.g., `GLOBAL_SHUFFLE_1L`, `GLOBAL_BUCKET_1L`). Enabling sets the `pipeline_level_multi_partitioned` field on the runtime-filter thrift layout (see `toLayout()`/`toThrift()`), and the session setter will also disable `enable_runtime_adaptive_dop`. The effective value is only true if `enable_pipeline_engine` is also enabled (the getter returns the flag AND `enable_pipeline_engine`).
  (Used in: RuntimeFilterDescription.computeLocalLayout, computeGlobalLayout, toLayout/toThrift; SessionVariable setter/getter.)
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### enable_plan_advisor

* **Description**: Whether to enable Query Feedback feature for slow queries and manually marked queries.
* **Default**: true
* **Introduced in**: v3.4.0

### enable_plan_advisor_blacklist

* **Scope**: Session
* **Description**: Controls whether the Plan Tuning Advisor will add ineffective tuning-guide patterns to the advisor blacklist for the current session. When enabled, code paths in `ApplyTuningGuideRule` check tuning guide usefulness and call `PlanTuningAdvisor.deleteTuningGuides(..., preventFutureTuning)`; `PlanTuningAdvisor` then records the matching plan pattern in its internal `tuningGuideBlacklist` to prevent storing or applying future tuning guides for that pattern. When disabled, useless tuning guides are removed from the in-memory cache but are not recorded in the permanent blacklist, allowing future analysis to re-record tuning guides for the same pattern. This setting is independent per session and complements `enable_plan_advisor`.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.5.2

### enable_predicate_reorder

* **Scope**: Session
* **Description**: When enabled, the optimizer applies the PredicateReorderRule to reorder conjunctive predicates (AND) inside an operator's predicate based on estimated selectivity. The rule builds a Statistics object from child statistics or, for a PhysicalOlapScanOperator, from table column statistics and then uses DefaultPredicateSelectivityEstimator to estimate selectivity of each conjunct. Conjuncts are sorted so more selective predicates (lower estimated selectivity) are evaluated earlier, producing a new compound AND predicate. The rewrite runs only when the predicate is a `CompoundPredicateOperator` and is invoked via `PredicateReorderRule.rewrite(...)` guarded by `SessionVariable.isEnablePredicateReorder()`. If required statistics are unavailable or the predicate is not compound, no reorder is performed. SessionVariable provides `isEnablePredicateReorder()`, `enablePredicateReorder()`, and `disablePredicateReorder()` accessors.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_profile

* **Description**: Specifies whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required.

  By default, a profile is sent to the FE only when a query error occurs in the BE. Profile sending causes network overhead and therefore affects high concurrency.

  If you need to analyze the profile of a query, you can set this variable to `true`. After the query is completed, the profile can be viewed on the web page of the currently connected FE (address: `fe_host:fe_http_port/query`). This page displays the profiles of the latest 100 queries with `enable_profile` turned on.

* **Default**: false

### enable_query_cache

* **Description**: Specifies whether to enable the Query Cache feature. Valid values: true and false. `true` specifies to enable this feature, and `false` specifies to disable this feature. When this feature is enabled, it works only for queries that meet the conditions specified in the application scenarios of [Query Cache](../using_starrocks/caching/query_cache.md#application-scenarios).
* **Default**: false
* **Introduced in**: v2.5

### enable_query_dump

* **Description**: Per-session boolean switch that enables server-side query dumping for debugging. When true, ConnectContext.shouldDumpQuery() will return true (in addition to HTTP-triggered dumps) and StmtExecutor.dumpException() records the session's DumpInfo to the centralized QueryDumpLog (serialized via Gson). This causes exception stack traces and collected dump information for that session's queries to be persisted for later inspection. The flag is stored on the session (per-connection) and does not affect the separate HTTP query-dump path (`isHTTPQueryDump`) except that either path can trigger dumping.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_query_queue_load (global)

* **Description**: Boolean value to enable query queues for loading tasks.
* **Default**: false

### enable_query_queue_select (global)

* **Description**: Whether to enable query queues for SELECT queries.
* **Default**: false

### enable_query_queue_statistic (global)

* **Description**: Whether to enable query queues for statistics queries.
* **Default**: false

### enable_query_tablet_affinity

* **Description**: Boolean value to control whether to direct multiple queries against the same tablet to a fixed replica.

  In scenarios where the table to query has a large number of tablets, this feature significantly improves query performance because the meta information and data of the tablet can be cached in memory more quickly.

  However, if there are some hotspot tablets, this feature may degrade the query performance because it directs the queries to the same BE, making it unable to fully use the resources of multiple BEs in high-concurrency scenarios.

* **Default**: false, which means the system selects a replica for each query.
* **Introduced in**: v2.5.6, v3.0.8, v3.1.4, and v3.2.0.

### enable_query_trigger_analyze

* **Default**: true
* **Type**: Boolean
* **Description**: Whether to enable query-trigger ANALYZE tasks on tables under external catalogs.
* **Introduced in**: v3.4.0

### enable_rbo_table_prune

* **Scope**: Session
* **Description**: When enabled, activates rule-based table pruning for cardinality-preserving joins in the query optimizer. The QueryOptimizer checks this flag in pruneTables() and, when true and prunable joins exist, runs a sequence of rewrite rules (partition pruning, projection merges, `UniquenessBasedTablePruneRule`, join reorder, optionally `PrimaryKeyUpdateTableRule` when `enable_table_prune_on_update` is set) and finally applies `RboTablePruneRule` to remove or simplify unnecessary table inputs for eligible joins. Enabling this option also disables join-equivalence derivation in the optimizer context (calls setEnableJoinEquivalenceDerive(false)), which changes how join predicates/equivalences are derived during optimization. Intended for cases where rule-based (tree-rewrite) pruning produces better pruning results for cardinality-preserving join patterns.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_rewrite_groupingsets_to_union_all

* **Scope**: Session
* **Description**: When enabled, the optimizer applies the `RewriteGroupingSetsByCTERule` during query rewrite to transform SQL GROUPING SETS constructs into an equivalent UNION ALL form (implemented via CTE-based rewriting). This flag is checked in `QueryOptimizer.rewriteGroupingSets(...)` and only triggers the iterative rewrite when true. It is independent of `cbo_push_down_grouping_set` (which controls push-down of grouping-set aggregates); both can be active but perform different rewrite strategies.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_rewrite_partition_column_minmax

* **Description**: Controls the optimizer rule PartitionColumnMinMaxRewriteRule that rewrites simple MIN/MAX aggregations on a table's partition column for better performance. When enabled, the optimizer may:
  * prune partitions and scan only the candidate partition(s) (for DUP_KEYS tables without deletes),
  * evaluate MAX/MIN from partition metadata/values for LIST partitions,
  * or rewrite the aggregation into a TopN-style plan when applicable.
  The rule is only applied when the target is a native partitioned table with partition column count &lt;= 1, the aggregation(s) are pure MIN or MAX that reference exactly the partition column, there is no GROUP-BY, no HAVING/predicate, no explicit LIMIT, and the scan has no specified partition names or tablet/replica hints. Partition-prune paths additionally require DUP_KEYS and no deletes. This session flag is read from `SessionVariable` and gates the transformation in `PartitionColumnMinMaxRewriteRule`.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.3, v3.4.0, v3.5.0

### enable_rewrite_simple_agg_to_hdfs_scan

* **Scope**: Session
* **Description**: Enables an optimizer transformation that rewrites simple COUNT aggregations over HDFS-backed tables (Hive, Iceberg, or generic file scans) into a specialized scan + aggregation that leverages scan-level count optimization. When enabled, the rule rewrites queries that meet all conditions into a plan that projects IFNULL(SUM(___count___), 0) over a scan producing a placeholder meta column named `___count___`, and sets the scan option `canUseCountOpt` to true. The rewrite applies only when:
  - the scan has no LIMIT,
  - all GROUP BY keys are partition columns,
  - aggregation functions are COUNT (no DISTINCT) with no referenced table columns (e.g., `COUNT()` / `COUNT(*)` or `COUNT` of a non-null constant),
  - there are no materialized (non-partition) columns referenced in scan or aggregation predicates,
  - the aggregation map contains a single aggregation entry.
  The rule implementation is in RewriteSimpleAggToHDFSScanRule and guarded by the session getter/setter in SessionVariable.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.7

### enable_rewrite_simple_agg_to_meta_scan

* **Scope**: Session
* **Description**: Enable the optimizer transformation that rewrites eligible simple aggregation queries into a LogicalMetaScan + aggregation over metadata (e.g., zonemap/column stats) to avoid scanning base data. When enabled, RewriteSimpleAggToMetaScanRule may replace queries such as MIN/MAX/COUNT (and COLUMN_SIZE / COLUMN_COMPRESSED_SIZE) with a MetaScan and SUM-based aggregation, greatly reducing I/O for suitable DUPLICATE_KEY OLAP tables. The rule only applies when all of the following hold:
  * target table is DUP_KEYS and has no deletions;
  * no GROUP BY keys, no HAVING, no query-level filters, and no LIMIT;
  * only MIN, MAX, COUNT (non-distinct), COLUMN_SIZE or COLUMN_COMPRESSED_SIZE are used;
  * aggregator arguments are single primitive columns with no expressions and zonemap index present (not string/complex types for min/max);
  * no partition pruning or incompatible schema changes (in shared-data mode with schema evolution, min/max is disabled).
  Note: Some components (for example, SPMPlanBuilder when generating baseline plans with hints) temporarily disable this variable to ensure consistent plan generation.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_rewrite_sum_by_associative_rule

* **Scope**: Session
* **Description**: Controls an optimizer rewrite that transforms aggregate expressions of the form sum(expr +/- const) into sum(expr) +/- count(expr_other) * const to avoid per-row projection cost. The rule applies only when the aggregate is `SUM`, not `DISTINCT`, and not DecimalV2; the pre-aggregation projection for the SUM argument must be an `ADD` or `SUBTRACT` call with at least one constant operand. The rewriter will:
    * remove redundant casts when the underlying types are fully compatible and a matching SUM signature exists,
    * generate new aggregations (SUM on the non-constant part and COUNT on the argument needed for multiplication),
    * preserve original aggregations that cannot be rewritten.
  For grouped aggregations the rule skips rewriting if the number of unique aggregation functions would not be reduced (to avoid increasing cost). The optimizer checks `context.getSessionVariable().isEnableRewriteSumByAssociativeRule()` before attempting the rewrite.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_runtime_adaptive_dop

* **Scope**: Session
* **Description**: When enabled, allow the pipeline execution engine to activate runtime adaptive degree-of-parallelism (DOP) for eligible plan fragments. During plan building (in PlanFragmentBuilder), if query cache is not used and `enable_runtime_adaptive_dop` is true, fragments that report `canUseRuntimeAdaptiveDop()` will have `enableAdaptiveDop()` applied so their DOP can be adjusted at runtime based on observed execution metrics. This setting is only effective when `enable_pipeline_engine` is also true. Enabling this option will automatically disable `enable_pipeline_level_multi_partitioned_rf` (the setter clears that flag). The variable value is recorded in query runtime profiles for auditing.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_scan_datacache

* **Description**: Specifies whether to enable the Data Cache feature. After this feature is enabled, StarRocks caches hot data read from external storage systems into blocks, which accelerates queries and analysis. For more information, see [Data Cache](../data_source/data_cache.md). In versions prior to 3.2, this variable was named as `enable_scan_block_cache`.
* **Default**: true 
* **Introduced in**: v2.5

### enable_shared_scan

* **Scope**: Session
* **Description**: Session flag that historically controlled enabling "shared scan" for pipeline execution—allowing multiple pipeline drivers/instances to share scan work. The flag is propagated from FE to BE via the thrift field `enable_shared_scan` (see `TFragmentInstanceFactory`), but FE-side support is disabled: `SessionVariable.isEnableSharedScan()` is hard-coded to return false and the feature is not active in FE. The code contains a TODO and a note that the variable has been effectively disabled since v3.5 because it is incompatible with event-based scheduling. In practice, setting this variable has no effect on query behavior in current releases; it remains present for compatibility/telemetry only. It is only considered when `enable_pipeline` is true on the session.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_short_circuit

* **Description**: Whether to enable short circuiting for queries. Default: `false`. If it is set to `true`, when the query meets the criteria (to evaluate whether the query is a point query): the conditional columns in the WHERE clause include all primary key columns, and the operators in the WHERE clause are `=` or `IN`, the query takes the short circuit.
* **Default**: false
* **Introduced in**: v3.2.3

### enable_sort_aggregate

* **Description**: Specifies whether to enable sorted streaming. `true` indicates sorted streaming is enabled to sort data in data streams.
* **Default**: false
* **Introduced in**: v2.5

### enable_spill

* **Description**: Whether to enable intermediate result spilling. Default: `false`. If it is set to `true`, StarRocks spills the intermediate results to disk to reduce the memory usage when processing aggregate, sort, or join operators in queries.
* **Default**: false
* **Introduced in**: v3.0

### enable_spill_to_remote_storage

* **Description**: Whether to enable intermediate result spilling to object storage. If it is set to `true`, StarRocks spills the intermediate results to the storage volume specified in `spill_storage_volume` after the capacity limit of the local disk is reached. For more information, see [Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage).
* **Default**: false
* **Introduced in**: v3.3.0

### enable_split_topn_agg

* **Description**: Controls the optimizer transformation that splits a TopN over Aggregate into a two-stage plan (a bounded TopN+aggregate subquery joined back to the original scan and a final aggregation). When enabled the `SplitTopNAggregateRule` may rewrite eligible plans to reduce work and memory by limiting rows early. The rule additionally enforces several constraints before applying the rewrite: the TopN limit must be set and not exceed `enable_split_topn_agg_limit`; scan/aggregation projections must not remap column refs; duplicated columns read twice are limited (small number and not long-string/complex types); predicate and statistics checks are satisfied (simple predicates, available/compatible column statistics, and estimated row counts). When disabled the transformation is never attempted.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: -

### enable_split_topn_agg_limit

* **Scope**: Session
* **Description**: Maximum TopN `limit` value for which the optimizer may apply the `SplitTopNAggregateRule`. The rule is eligible only when `enable_split_topn_agg` is true and the TopN operator's limit is not `Operator.DEFAULT_LIMIT` and the TopN limit &lt;= `splitTopNAggLimit`. If the TopN limit &gt; `splitTopNAggLimit`, the rule will not split the TopN+Aggregate pattern. Note: even when this threshold allows splitting, `SplitTopNAggregateRule` performs additional checks (projections, predicates, column types, statistics and duplicated-column cost heuristics) that can still prevent the transformation.
* **Default**: `10000`
* **Data Type**: long
* **Introduced in**: -

### enable_spm_rewrite

* **Description**: Whether to enable SQL Plan Manager (SPM) query rewrite. When enabled, StarRocks automatically rewrites queries to use bound query plans, improving query performance and stability.
* **Default**: false

### enable_strict_order_by

* **Description**: Used to check whether the column name referenced in ORDER BY is ambiguous. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries.
* **Default**: true
* **Introduced in**: v2.5.18 and v3.1.7

### enable_sync_materialized_view_rewrite

* **Description**: Whether to enable query rewrite based on synchronous materialized views.
* **Default**: true
* **Introduced in**: v3.1.11, v3.2.5

### enable_table_prune_on_update

* **Scope**: Session
* **Description**: When enabled, the optimizer will apply primary-key–aware table pruning for update queries by running the `PrimaryKeyUpdateTableRule` in the prune stage (see `QueryOptimizer.pruneTables`). This flag is only effective when rule-based table pruning is enabled (for example, `enable_rbo_table_prune`). It is provided as a session-level, opt-in switch because pruning during UPDATE/primary-key write paths can change join/layout decisions (e.g., bucket-shuffle join interpolation) and may introduce performance regressions or concurrency/race effects on `OlapTableSink`. Use this to experiment with or temporarily enable table-prune optimizations for update statements; keep it disabled for production workloads unless the behavior is validated.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.4

### enable_tablet_internal_parallel

* **Description**: Whether to enable adaptive parallel scanning of tablets. After this feature is enabled, multiple threads can be used to scan one tablet by segment, increasing the scan concurrency.
* **Default**: true
* **Introduced in**: v2.3

### enable_topn_runtime_filter

* **Description**: Whether to enable TopN Runtime Filter. If this feature is enabled, a runtime filter will be dynamically constructed for ORDER BY LIMIT queries and pushed down to the scan for filtering.
* **Default**: true
* **Introduced in**: v3.3

### enable_ukfk_join_reorder

* **Description**: When enabled, the join reorderer considers unique-key / foreign-key (UK/FK) constraints to prefer placing the foreign-key side as the right child of an inner join. The optimizer (in JoinOrder) invokes `UKFKConstraintsCollector` to build join column constraints and derives a `JoinProperty`. If the UK constraint is intact and additional checks pass (FK is not ordered by the FK column and the FK-to-UK size "scale ratio" and FK row count are within limits), the reorderer biases join direction to use the FK table on the right; otherwise it falls back to size-based decisions. This flag is consulted by `JoinOrder.buildJoinExpr()` and `JoinOrder.allowFKAsRightTable()`. Use `max_ukfk_join_reorder_scale_ratio` and `max_ukfk_join_reorder_fk_rows` to control allowed scale and FK size. Requires `enable_ukfk_opt` for UK/FK constraint collection to be meaningful.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.4

### enable_ukfk_opt

* **Scope**: Session
* **Description**: Enables optimizer use of table Unique-Key (UK) and Foreign-Key (FK) metadata to derive plan constraints and apply UK/FK-aware rewrites and estimations. When set, the planner invokes `UKFKConstraintsCollector.collectColumnConstraints(...)` to attach `UKFKConstraints` to OptExpressions; this enables transformation rules such as `PruneUKFKGroupByKeysRule` (removes redundant GROUP BY keys derived from UKs) and `PruneUKFKJoinRule` (prunes or rewrites the UK side of joins by mapping UK predicates to the FK side). The flag also allows `StatisticsCalculator` to consider UK/FK relationships to produce tighter join cardinality estimates for UK–FK joins. Constraint collection is plan-structure sensitive (constraints are attached to OptExpressions and are invalidated when the plan changes); the collector provides `collectColumnConstraintsForce` to force collection regardless of the flag. Default is conservative (`false`) because these optimizations change plan shape and estimate behavior.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.4

### enable_view_based_mv_rewrite

* **Description**: Whether to enable query rewrite for logical view-based materialized views. If this item is set to `true`, the logical view is used as a unified node to rewrite the queries against itself for better performance. If this item is set to `false`, the system transcribes the queries against logical views into queries against physical tables or materialized views and then rewrites them.
* **Default**: false
* **Introduced in**: v3.1.9, v3.2.5, v3.3.0

### enable_wait_dependent_event

* **Description**: Whether Pipeline waits for a dependent operator to finish execution before continuing within the same fragment. For example, in a left join query, when this feature is enabled, the probe-side operator waits for the build-side operator to finish before it starts executing. Enabling this feature can reduce memory usage, but may increase the query latency. However, for queries reused in CTE, enabling this feature may increase memory usage.
* **Default**: false
* **Introduced in**: v3.3

### enable_write_hive_external_table

* **Description**: Whether to allow for sinking data to external tables of Hive.
* **Default**: false
* **Introduced in**: v3.2

### event_scheduler

Used for MySQL client compatibility. No practical usage.

### exec_mode

* **Description**: Session execution mode. Valid (case-insensitive) values are `DEFAULT` and `ETL`. Setting `exec_mode` to `ETL` enables `enable_phased_scheduler` and `enable_spill` for the session; any other value (including `DEFAULT`) disables those two features. The setter stores the provided string in the session and applies the boolean toggles immediately (case-insensitive comparison against `SessionVariableConstants.ETL`).
* **Scope**: Session
* **Default**: `SessionVariableConstants.DEFAULT`
* **Data Type**: String
* **Introduced in**: -

### force_schedule_local

* **Scope**: Session
* **Description**: When enabled, the Hybrid/HDFS backend selector will prefer and, when possible, force assignment of HDFS/connector (currently Hive and other external table) scan ranges to a local compute backend if one exists. This influences scan-range assignment in HDFSBackendSelector and is read by HiveConnectorScanRangeSource during scan-range generation. Default behavior (disabled) lets the selector balance assignments by assigned bytes/scan-range counts and other policies; enabling this variable biases scheduling for data locality in hybrid deployments with datanodes.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### forward_to_leader

Used to specify whether some commands will be forwarded to the leader FE for execution. Alias: `forward_to_master`. The default value is `false`, meaning not forwarding to the leader FE. There are multiple FEs in a StarRocks cluster, one of which is the leader FE. Normally, users can connect to any FE for full-featured operations. However, some information is only available on the leader FE.

For example, if the SHOW BACKENDS command is not forwarded to the leader FE, only basic information (for example, whether the node is alive) can be viewed. Forwarding to the leader FE can get more detailed information including the node start time and last heartbeat time.

The commands affected by this variable are as follows:

* SHOW FRONTENDS: Forwarding to the leader FE allows users to view the last heartbeat message.

* SHOW BACKENDS: Forwarding to the leader FE allows users to view the boot time, last heartbeat information, and disk capacity information.

* SHOW BROKER: Forwarding to the leader FE allows users to view the boot time and last heartbeat information.

* SHOW TABLET

* ADMIN SHOW REPLICA DISTRIBUTION

* ADMIN SHOW REPLICA STATUS: Forwarding to the leader FE allows users to view the tablet information stored in the metadata of the leader FE. Normally, the tablet information should be the same in the metadata of different FEs. If an error occurs, you can use this method to compare the metadata of the current FE and the leader FE.

* Show PROC: Forwarding to the leader FE allows users to view the PROC information stored in the metadata. This is mainly used for metadata comparison.

### group_concat_max_len

* **Description**: The maximum length of string returned by the [group_concat](sql-functions/string-functions/group_concat.md) function.
* **Default**: 1024
* **Min value**: 4
* **Unit**: Characters
* **Data type**: Long

### group_execution_group_scale

* **Scope**: Session
* **Description**: Controls the per-instance scaling factor used to compute the logical degree-of-parallelism (DOP) when a fragment uses `group execution`. During instance assignment (see fragment scheduling logic), StarRocks computes an upper bound:
  - initial bound = `group_execution_group_scale` * physical DOP (the fragment's expected pipeline/sink DOP)
  - then bounded by `group_execution_max_groups`
  - additionally limited by data size: bound &= floor(total scan rows / `group_execution_min_scan_rows`)
  - final logical DOP is at least the physical DOP and not greater than the number of available driver sequences; for colocated bucket assignment it is further aligned to an integer multiple of the physical DOP.
  The variable only takes effect when `enable_group_execution` is enabled (and `enable_runtime_adaptive_dop` is disabled). Raising this value allows larger logical DOP (more parallel groups) subject to `group_execution_max_groups` and data-size thresholds.
* **Default**: `64`
* **Data Type**: int
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### group_execution_max_groups

* **Description**: Upper bound on the number of groups (logical degree of parallelism) that the group-execution planner can create for a fragment when `fragment.isUseGroupExecution()` is true. StarRocks uses this value inside `LocalFragmentAssignmentStrategy` to cap the computed logical DOP. The runtime computation first limits DOP by `group_execution_group_scale * expectedDop` and then by this `group_execution_max_groups` value. It is further constrained by scan-row heuristics using `group_execution_min_scan_rows` (the per-group minimum scan rows), and the final logical DOP is clamped to available bucket/scan-range counts and then aligned to an integer multiple of the physical pipeline DOP. In short, this variable prevents excessive group splitting and bounds memory/CPU growth from overly fine-grained group execution while allowing larger logical DOP when scan volume justifies it.
* **Scope**: Session
* **Default**: 128
* **Data Type**: int
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### group_execution_min_scan_rows

* **Scope**: Session
* **Description**: Minimum number of scan-rows used as the base unit when computing group-execution parallelism. When a fragment uses group execution, the planner computes an upper bound for logical parallelism (logicalDop) using:
  * `maxDop = min(group_execution_group_scale * expectedDop, group_execution_max_groups)`
  * then `maxDop = min(instanceAvgScanRows / group_execution_min_scan_rows, maxDop)`
  The resulting `logicalDop` is capped by available buckets/scan ranges and aligned to a multiple of the physical DOP. In short, larger `group_execution_min_scan_rows` produces fewer (coarser) groups; smaller values permit more (finer) groups. This parameter is consulted in both colocated and normal scan assignment paths inside `LocalFragmentAssignmentStrategy`.
* **Default Value**: `5000000`
* **Type**: long
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### hash_join_push_down_right_table

* **Description**: Used to control whether the data of the left table can be filtered by using the filter condition against the right table in the Join query. If so, it can reduce the amount of data that needs to be processed during the query.
**Default**: `true` indicates the operation is allowed and the system decides whether the left table can be filtered. `false` indicates the operation is disabled. The default value is `true`.

### historical_nodes_min_update_interval

* **Description**: The minimum interval between two updates of historical node records. If the nodes of a cluster change frequently in a short period of time (that is, less than the value set in this variable), some intermediate states will not be recorded as valid historical node snapshots. The historical nodes are the main basis for the Cache Sharing feature to choose the right cache nodes during cluster scaling.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### hive_partition_stats_sample_size

* **Scope**: Session
* **Description**: Controls how many partitions to sample when retrieving partition-level column statistics from a Hive metastore for a query. The Hive statistics collector uses this session value to limit the number of partition entries requested (and sampled) to avoid metastore RPC failures (for example StackOverflow or transport errors) when a table has many partitions. Lowering this value reduces the number of partitions included in `getPartitionColumnStatistics` calls and can improve reliability at the cost of less-complete stats; it is referenced by HiveStatisticsProvider and returned by SessionVariable.getHivePartitionStatsSampleSize(). The codebase also implements retry/slicing logic in HiveMetaClient when large partition lists cause errors, but tuning `hive_partition_stats_sample_size` is the primary session knob to limit initial partition-stat requests (e.g., temporarily set to `100` for troubleshooting GC/timeout issues).
* **Default**: `3000`
* **Data Type**: int
* **Introduced in**: v3.2.0

### init_connect (global)

Used for MySQL client compatibility. No practical usage.

### innodb_read_only

* **Scope**: Session
* **Description**: Session-scoped boolean flag exposed as the system variable `innodb_read_only`. It is stored on the session object (field `innodbReadOnly` in `SessionVariable`) and accessible via `isInnodbReadOnly()` / `setInnodbReadOnly()`. The variable indicates whether the session should treat InnoDB as read-only for compatibility with MySQL clients and tools. StarRocks does not automatically imply storage-layer enforcement here; other components must explicitly check this session flag to honor read-only semantics.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### insert_max_filter_ratio

* **Description**: The maximum error tolerance of INSERT from files(). It's the maximum ratio of data records that can be filtered out due to inadequate data quality. When the ratio of unqualified data records reaches this threshold, the job fails. Range: [0, 1].
* **Default**: 0
* **Introduced in**: v3.4.0

### insert_timeout

* **Description**: The timeout duration of the INSERT job. Unit: Seconds. From v3.4.0 onwards, `insert_timeout` applies to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE), replacing `query_timeout`.
* **Default**: 14400
* **Introduced in**: v3.4.0

### interactive_timeout

Used for MySQL client compatibility. No practical usage.

### interleaving_group_size

* **Description**: Session-level integer controlling interleaving behavior for grouped operations during query execution. Semantics:
  * A value of 0 disables interleaving.
  * A positive value N sets the group size to N and enables interleaving adaptively (the engine decides whether to interleave based on group size and runtime conditions).
  * A negative value -N forces interleaving for groups whose size is less than the absolute value N.
  This variable is declared in SessionVariable (annotated with VariableMgr.VarAttr) and affects per-session runtime scheduling/processing of grouped tasks. Default positive setting (`10`) enables adaptive interleaving with a group size of 10.
* **Scope**: Session
* **Default**: `10`
* **Data Type**: int
* **Introduced in**: v3.2.0

### io_tasks_per_scan_operator

* **Description**: The number of concurrent I/O tasks that can be issued by a scan operator. Increase this value if you want to access remote storage systems such as HDFS or S3 but the latency is high. However, a larger value causes more memory consumption.
* **Default**: 4
* **Data type**: Int
* **Introduced in**: v2.5

### jit_level

* **Description**: The level at which JIT compilation for expressions is enabled. Valid values:
  * `1`: The system adaptively enables JIT compilation for compilable expressions.
  * `-1`: JIT compilation is enabled for all compilable, non-constant expressions.
  * `0`: JIT compilation is disabled. You can disable it manually if any error is returned for this feature.
* **Default**: 1
* **Data type**: Int
* **Introduced in**: -

### join_implementation_mode_v2

* **Description**: Controls which join implementation rules the optimizer applies during plan optimization. Accepted, case-insensitive values:
  * `auto` — let the optimizer choose join implementations via heuristics (default).
  * `merge` — force the optimizer to add merge-join implementation rules (`addMergeJoinImplementationRule`).
  * `hash` — force hash-join implementation rules (`addHashJoinImplementationRule`).
  * `nestloop` — force nested-loop join implementation rules (`addNestLoopJoinImplementationRule`).
  Unknown or unrecognized values fall back to `auto`. The variable is read from the session (via `SessionVariable.getJoinImplementationMode()`) in the optimizer (e.g., `SPMOptimizer.memoOptimize`) to decide which implementation rule set to add. Use this setting for targeted tuning or debugging when you need to force a specific join strategy. It complements the older `join_implementation_mode` configuration.
* **Scope**: Session
* **Default**: `auto`
* **Data Type**: String
* **Introduced in**: v3.2.0

### join_late_materialization

* **Description**: Controls whether join operators enable late materialization at the session level. When set to true, the planner (see `PlanFragmentBuilder`) calls `joinNode.setEnableLateMaterialization(true)`, instructing JoinNode implementations to defer building or materializing non-join output columns until after the join when possible. This can reduce memory usage and network transfer for joins that only need a subset of columns for matching, but may change execution characteristics (CPU/workload pattern). The variable is stored and exposed via `SessionVariable` (field `joinLateMaterialization`) and read by the planner through `isJoinLateMaterialization()`.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### lake_bucket_assign_mode

* **Description**: The bucket assignment mode for queries against tables in data lakes. This variable controls how buckets are distributed among worker nodes when bucket-aware execution takes effect during query execution. Valid values:
  * `balance`: Distributes buckets evenly across worker nodes to achieve balanced workload and better performance.
  * `elastic`: Uses consistent hashing to assign buckets to worker nodes, which can provide better load distribution in elastic environments.
* **Default**: balance
* **Data type**: String
* **Introduced in**: v4.0

### language (global)

Used for MySQL client compatibility. No practical usage.

### large_in_predicate_threshold

* **Scope**: Session
* **Description**: Threshold (count of literal elements) used by the FE parser to decide when to convert a regular IN predicate into a `LargeInPredicate` optimization. When `enable_large_in_predicate` is true and the number of constants in an IN list is greater than or equal to this threshold, AstBuilder constructs a `LargeInPredicate` (applies to both string and integer literal lists). For integer lists, if any integer cannot be parsed as a long the parser falls back to a normal `InPredicate`. `LargeInPredicate` reduces FE parsing/analysis/planning overhead for very large IN lists and is later transformed into an execution plan (left semi/anti join) for more efficient execution. The comparison in code is literalCount &gt;= this threshold.
* **Default**: `100000`
* **Data Type**: int
* **Introduced in**: -

### license (global)

* **Description**: Displays the license of StarRocks.
* **Default**: Apache License 2.0

### load_mem_limit

Specifies the memory limit for the import operation. The default value is 0, meaning that this variable is not used and `query_mem_limit` is used instead.

This variable is only used for the `INSERT` operation which involves both query and import. If the user does not set this variable, the memory limit for both query and import will be set as `exec_mem_limit`. Otherwise, the memory limit for query will be set as `exec_mem_limit` and the memory limit for import will be as `load_mem_limit`.

Other import methods such as `BROKER LOAD`, `STREAM LOAD` still use `exec_mem_limit` for memory limit.

### load_transmission_compression_type

* **Description**: Controls the compression algorithm used for data transmission during load operations. This is a per-session setting and is persisted into load jobs (see `BulkLoadJob` and `RoutineLoadJob`) so the same compression choice is applied when the job runs or is replayed. The session string is mapped to a Thrift `TCompressionType` via `CompressionUtils.findTCompressionByName` and included in RPC/Thrift payloads as `load_transmission_compression_type`. Note that column encoding behavior is controlled separately by `transmission_encode_level`.  
  Valid names (case-insensitive): `NO_COMPRESSION`, `AUTO`, `LZ4`, `LZ4_FRAME`, `SNAPPY`, `ZLIB`, `ZSTD`, `GZIP`, `DEFLATE`, `BZIP2`. The runtime may only enable a subset of compressors for actual payload compression; refer to `CompressionUtils` for the mappings and supported runtime compressors.
* **Scope**: Session
* **Default**: `NO_COMPRESSION`
* **Data Type**: String
* **Introduced in**: v3.2.0

### log_rejected_record_num (v3.1 and later)

Specifies the maximum number of unqualified data rows that can be logged. Valid values: `0`, `-1`, and any non-zero positive integer. Default value: `0`.

* The value `0` specifies that data rows that are filtered out will not be logged.
* The value `-1` specifies that all data rows that are filtered out will be logged.
* A non-zero positive integer such as `n` specifies that up to `n` data rows that are filtered out can be logged on each BE.

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
* **Description**: Session-level toggle that selects the low-cardinality optimization implementation used by the optimizer. When enabled, the optimizer applies the V2 rewrite implemented in `LowCardinalityRewriteRule` (the rule collects decode candidates and rewrites the plan using decode/encode operators). When disabled, the optimizer falls back to the legacy path (`AddDecodeNodeForDictStringRule`) or skips V2-specific rewrites. The planner checks both `isEnableLowCardinalityOptimize()` and this variable to decide whether to run the V2 rewrite. It can be changed per session via `setUseLowCardinalityOptimizeV2`.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### lower_case_table_names (global)

Used for MySQL client compatibility. No practical usage. Table names in StarRocks are case-sensitive.

### materialized_view_rewrite_mode (v3.2 and later)

Specifies the query rewrite mode of asynchronous materialized views. Valid values:

* `disable`: Disable automatic query rewrite of asynchronous materialized views.
* `default` (Default value): Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, it directly scans the data in the base table.
* `default_or_error`: Enable automatic query rewrite of asynchronous materialized views, and allow the optimizer to decide whether a query can be rewritten using the materialized view based on the cost. If the query cannot be rewritten, an error is returned.
* `force`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, it directly scans the data in the base table.
* `force_or_error`: Enable automatic query rewrite of asynchronous materialized views, and the optimizer prioritizes query rewrite using the materialized view. If the query cannot be rewritten, an error is returned.

### materialized_view_subuqery_text_match_max_count

* **Description**: Specifies the maximum number of times that the system checks whether a query's sub-query matches the materialized views' definition.
* **Default**: 4
* **Introduced in**: v3.2.5, v3.3.0

### max_allowed_packet

* **Description**: Used for compatibility with the JDBC connection pool C3P0. This variable specifies the maximum size of packets that can be transmitted between the client and server.
* **Default**: 33554432 (32 MB). You can raise this value if the client reports "PacketTooBigException".
* **Unit**: Byte
* **Data type**: Int

### max_parallel_scan_instance_num

* **Scope**: Session  
* **Description**: Controls the maximum number of parallel scan instances that a single scan node may create when executing a query. The session value is serialized into the plan (see `OlapScanNode#setMax_parallel_scan_instance_num`) and propagated to backends/compute nodes so they can cap scan parallelism for that session. Typical uses: limit concurrent tablet/file scan tasks for very wide scans, or override cluster defaults for debugging/tuning. When the value is &lt; 0 the variable is disabled and the system selects the scan instance count based on other settings and BE defaults. When set to an integer value &gt;= 0 it enforces an upper bound. This variable is included in runtime profiles (`StmtExecutor`, `LoadLoadingTask`) for visibility in query/load diagnostics. It is a session-scoped override and does not change global defaults; consider adjusting `parallel_fragment_exec_instance_num` or cluster BE defaults for system-wide changes.  
* **Default**: `-1`  
* **Data Type**: int  
* **Introduced in**: v3.2.0

### max_pipeline_dop

* **Scope**: Session
* **Description**: Limits the pipeline engine's degree-of-parallelism (DOP) when `pipeline_dop` is not explicitly set. Behavior:
    * If `pipeline_dop` &gt; 0 then `pipeline_dop` is used directly.
    * If `pipeline_dop` &lt;= 0 and `max_pipeline_dop` &lt;= 0 then the system falls back to the backend default DOP (`BackendResourceStat` default).
    * If `pipeline_dop` &lt;= 0 and `max_pipeline_dop` &gt; 0 then the effective DOP is min(`max_pipeline_dop`, backend default DOP). The same cap is applied to sink DOP using the sink default DOP.
  This session-level cap exists to avoid scheduling overhead on very large multi-core machines; setting `max_pipeline_dop` to a non-positive value disables the cap.
* **Default**: `64`
* **Data Type**: int
* **Introduced in**: v3.2.0

### max_pushdown_conditions_per_column

* **Description**: The maximum number of predicates that can be pushed down for a column.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.
* **Data type**: Int

### max_scan_key_num

* **Description**: The maximum number of scan key segmented by each query.
* **Default**: -1, indicating that the value in the `be.conf` file is used. If this variable is set to a value greater than 0, the value in `be.conf` is ignored.

### metadata_collect_query_timeout

* **Description**: The timeout duration for Iceberg Catalog metadata collection queries.
* **Unit**: Second
* **Default**: 60
* **Introduced in**: v3.3.3

### nested_mv_rewrite_max_level

* **Description**: The maximum levels of nested materialized views that can be used for query rewrite.
* **Value range**: [1, +∞). The value of `1` indicates that only materialized views created on base tables can be used for query rewrite.
* **Default**: 3
* **Data type**: Int

### net_buffer_length

Used for MySQL client compatibility. No practical usage.

### net_read_timeout

Used for MySQL client compatibility. No practical usage.

### net_write_timeout

Used for MySQL client compatibility. No practical usage.

### new_planner_agg_stage

* **Scope**: Session
* **Description**: Controls the aggregation staging strategy used by the new planner. Valid integer values:
  * `0` — automatic selection (planner decides; when `enable_cost_based_multi_stage_agg` is `true` the planner may choose multi-stage aggregation based on cost analysis).
  * `1` — force one-stage aggregation.
  * `2` — force two-stage aggregation.
  * `3` — force three-stage aggregation (can only be produced in single-column DISTINCT scenarios).
  * `4` — force four-stage aggregation (can only be produced in single-column DISTINCT scenarios).
  The variable is consulted by optimizer rules and tasks such as `SplitAggregateRule`, `SplitMultiPhaseAggRule`, `EnforceAndCostTask`, `PlanFragmentBuilder` and by `FunctionAnalyzer` (some functions require a specific stage, e.g., checks that `new_planner_agg_stage == 1`). Setting a non-AUTO value forces the planner to use the corresponding aggregation plan; AUTO delegates choice to the optimizer and cost-based transformations.
* **Default**: `SessionVariableConstants.AggregationStage.AUTO.ordinal()`
* **Data Type**: int
* **Introduced in**: v3.2.0

### new_planner_optimize_timeout

* **Description**: The timeout duration of the query optimizer. When the optimizer times out, an error is returned and the query is stopped, which affects the query performance. You can set this variable to a larger value based on your query or contact StarRocks technical support for troubleshooting. A timeout often occurs when a query has too many joins.
* **Default**: 3000
* **Unit**: ms

### optimizer_materialized_view_timelimit

* **Description**: Specifies the maximum time that one materialized view rewrite rule can consume. When the threshold is reached, this rule will not be used for query rewrite.
* **Default**: 1000
* **Unit**: ms
* **Introduced in**: v3.1.9, v3.2.5

### orc_use_column_names

* **Description**: Used to specify how columns are matched when StarRocks reads ORC files from Hive. The default value is `false`, which means columns in ORC files are read based on their ordinal positions in the Hive table definition. If this variable is set to `true`, columns are read based on their names.
* **Default**: false
* **Introduced in**: v3.1.10

### parallel_exchange_instance_num

Used to set the number of exchange nodes that an upper-level node uses to receive data from a lower-level node in the execution plan. The default value is -1, meaning the number of exchange nodes is equal to the number of execution instances of the lower-level node. When  this variable is set to be greater than 0 but smaller than the number of execution instances of the lower-level node, the number of exchange nodes equals the set value.

In a distributed query execution plan, the upper-level node usually has one or more exchange nodes to receive data from the execution instances of the lower-level node on different BEs. Usually the number of exchange nodes is equal to the number of execution instances of the lower-level node.

In some aggregation query scenarios where the amount of data decreases drastically after aggregation, you can try to modify this variable to a smaller value to reduce the resource overhead. An example would be running aggregation queries using the Duplicate Key table.

### parallel_fragment_exec_instance_num

Used to set the number of instances used to scan nodes on each BE. The default value is 1.

A query plan typically produces a set of scan ranges. This data is distributed across multiple BE nodes. A BE node will have one or more scan ranges, and by default, each BE node's set of scan ranges is processed by only one execution instance. When machine resources suffice, you can increase this variable to allow more execution instances to process a scan range simultaneously for efficiency purposes.

The number of scan instances determines the number of other execution nodes in the upper level, such as aggregation nodes and join nodes. Therefore, it increases the concurrency of the entire query plan execution. Modifying this variable will help  improve efficiency, but larger values will consume more machine resources, such as CPU, memory, and disk IO.

### parallel_merge_late_materialization_mode

* **Description**: Session-level switch that controls late-materialization behavior for parallel-merge execution paths (used by `SortNode` and `ExchangeNode`). Valid values: `AUTO` / `ALWAYS` / `NEVER`.
  * `AUTO`: Let the planner / BE decide per-query whether to apply late materialization (default heuristic-driven behavior).
  * `ALWAYS`: Force late materialization during parallel merge — non-key columns (or full rows) are deferred and only materialized after merge/ordering to reduce network, memory and I/O pressure.
  * `NEVER`: Disable late materialization for parallel merge — full rows are materialized before merge.
  This variable is serialized into the plan as the Thrift field `parallel_merge_late_materialize_mode` (`TLateMaterializeMode`) in `SortNode.toThrift()` and `ExchangeNode.toThrift()` and only affects nodes where `enable_parallel_merge` is active. Use this to override automatic decisions when you need to force or prevent late materialization for specific sessions or queries.

* **Default**: `SessionVariableConstants.AUTO`
* **Data Type**: String
* **Introduced in**: v3.3.10, v3.4.1, v3.5.0

### parse_tokens_limit

* **Description**: Session-level integer limit controlling the maximum number of tokens the SQL parser accepts. The value is read from the session (`SessionVariable.parseTokensLimit`) and used in SqlParser.invokeParser to compute `tokenLimit = Math.max(MIN_TOKEN_LIMIT, sessionVariable.getParseTokensLimit())`. The parser installs a PostProcessListener with this token limit; the listener throws a ParsingException if a token's index is &gt;= the configured limit, preventing parsing of excessively large/complex inputs and protecting the server from parser OOM or DoS. The limit can be set per-session (also parsed from session properties, e.g., in CreateRoutineLoadStmt.buildSessionVariables). Note: `MIN_TOKEN_LIMIT` (100) is enforced as the lower bound.
* **Default**: `3500000`
* **Data Type**: int
* **Introduced in**: v3.2.0

### partial_update_mode

* **Description**: Used to control the mode of partial updates. Valid values:

  * `auto` (default): The system automatically determines the mode of partial updates by analyzing the UPDATE statement and the columns involved.
  * `column`: The column mode is used for the partial updates, which is particularly suitable for the partial updates which involve a small number of columns and a large number of rows.

  For more information, see [UPDATE](sql-statements/table_bucket_part_index/UPDATE.md#partial-updates-in-column-mode-since-v31).
* **Default**: auto
* **Introduced in**: v3.1

### performance_schema (global)

Used for compatibility with MySQL JDBC versions 8.0.16 and above. No practical usage.

### phased_scheduler_max_concurrency

* **Description**: The concurrency for phased scheduler scheduling leaf node fragments. For example, the default value means that, in a large number of UNION ALL Scan queries, at most two scan fragments are allowed to be scheduled at the same time.
* **Default**: 2
* **Introduced in**: v3.3

### pipeline_dop

* **Description**: The parallelism of a pipeline instance, which is used to adjust the query concurrency. Default value: 0, indicating the system automatically adjusts the parallelism of each pipeline instance. This variable also controls the parallelism of loading jobs on OLAP tables. You can also set this variable to a value greater than 0. Generally, set the value to half the number of physical CPU cores. From v3.0 onwards, StarRocks adaptively adjusts this variable based on query parallelism.

* **Default**: 0
* **Data type**: Int

### pipeline_profile_level

* **Description**: Controls the level of the query profile. A query profile often has five layers: Fragment, FragmentInstance, Pipeline, PipelineDriver, and Operator. Different levels provide different details of the profile:

  * 0: StarRocks combines metrics of the profile and shows only a few core metrics.
  * 1: default value. StarRocks simplifies the profile and combines metrics of the profile to reduce profile layers.
  * 2: StarRocks retains all the layers of the profile. The profile size is large in this scenario, especially when the SQL query is complex. This value is not recommended.

* **Default**: 1
* **Data type**: Int

### pipeline_sink_dop

* **Description**: The parallelism of sink for loading data into Iceberg tables and Hive tables, and unloading data using INSERT INTO FILES(). It is used to adjust the concurrency of these loading jobs. Default value: 0, indicating the system automatically adjusts the parallelism. You can also set this variable to a value greater than 0.
* **Default**: 0
* **Data type**: Int

### plan_mode

* **Description**: The metadata retrieval strategy of Iceberg Catalog. For more information, see [Iceberg Catalog metadata retrieval strategy](../data_source/catalog/iceberg/iceberg_catalog.md#appendix-periodic-metadata-refresh-strategy). Valid values:
  * `auto`: The system will automatically select the retrieval plan.
  * `local`: Use the local cache plan.
  * `distributed`: Use the distributed plan.
* **Default**: auto
* **Introduced in**: v3.3.3

#### enable_iceberg_column_statistics

* **Description**: Whether to obtain column statistics, such as `min`, `max`, `null count`, `row size`, and `ndv` (if a puffin file exists). When this item is set to `false`, only the row count information will be collected.
* **Default**: false
* **Introduced in**: v3.4

### populate_datacache_mode

* **Description**: Specifies the population behavior of Data Cache when reading data blocks from external storage systems. Valid values:
  * `auto` (default): the system automatically caches data selectively based on the population rule.
  * `always`: Always cache the data.
  * `never`: Never cache the data.
* **Default**: auto
* **Introduced in**: v3.3.2

### prefer_compute_node

* **Description**: Specifies whether the FEs distribute query execution plans to CN nodes. Valid values:
  * `true`: indicates that the FEs distribute query execution plans to CN nodes.
  * `false`: indicates that the FEs do not distribute query execution plans to CN nodes.
* **Default**: false
* **Introduced in**: v2.4

### query_cache_agg_cardinality_limit

* **Description**: The upper limit of cardinality for GROUP BY in Query Cache. Query Cache is not enabled if the rows generated by GROUP BY exceeds this value. Default value: 5000000. If `query_cache_entry_max_bytes` or `query_cache_entry_max_rows` is set to 0, the Passthrough mode is used even when no computation results are generated from the involved tablets.
* **Default**: 5000000
* **Data type**: Long
* **Introduced in**: v2.5

### query_cache_entry_max_bytes

* **Description**: The threshold for triggering the Passthrough mode. When the number of bytes or rows from the computation results of a specific tablet accessed by a query exceeds the threshold specified by `query_cache_entry_max_bytes` or `query_cache_entry_max_rows`, the query is switched to Passthrough mode.
* **Valid values**: 0 to 9223372036854775807
* **Default**: 4194304
* **Unit**: Byte
* **Introduced in**: v2.5

### query_cache_entry_max_rows

* **Description**: The upper limit of rows that can be cached. See the description in `query_cache_entry_max_bytes`. Default value: .
* **Default**: 409600
* **Introduced in**: v2.5

### query_cache_size (global)

Used for MySQL client compatibility. No practical use.

### query_cache_type

Used for compatibility with JDBC connection pool C3P0. No practical use.

### query_delivery_timeout

* **Description**: Timeout (in seconds) for the first phase of query execution: delivering all plan fragment instances to Backends (BEs). StarRocks divides execution into two phases: 1) deliver fragments to BEs; 2) pull result data after fragments are prepared and ready to execute. `query_delivery_timeout` controls the maximum wait time for phase 1. When `enablePhasedScheduler` is true, this variable is ignored and the delivery timeout is set to the overall query timeout. When serialized to Thrift, the value is capped to avoid integer overflow using Math.min(Integer.MAX_VALUE / 1000, queryDeliveryTimeoutS).
* **Scope**: Session
* **Default**: 300
* **Data Type**: int
* **Introduced in**: v3.2.0

### query_mem_limit

* **Description**: Used to set the memory limit of a query on each BE node. The default value is 0, which means no limit for it. This item takes effect only after Pipeline Engine is enabled. When the `Memory Exceed Limit` error happens, you could try to increase this variable. Setting it to `0` indicates no limit is imposed.
* **Default**: 0
* **Unit**: Byte

### query_queue_concurrency_limit (global)

* **Description**: The upper limit of concurrent queries on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Default**: 0
* **Data type**: Int

### query_queue_cpu_used_permille_limit (global)

* **Description**: The upper limit of CPU usage permille (CPU usage * 1000) on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Value range**: [0, 1000]
* **Default**: `0`

### query_queue_max_queued_queries (global)

* **Description**: The upper limit of queries in a queue. When this threshold is reached, incoming queries are rejected. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Default**: `1024`.

### query_queue_mem_used_pct_limit (global)

* **Description**: The upper limit of memory usage percentage on a BE. It takes effect only after being set greater than `0`. Setting it to `0` indicates no limit is imposed.
* **Value range**: [0, 1]
* **Default**: 0

### query_queue_pending_timeout_second (global)

* **Description**: The maximum timeout of a pending query in a queue. When this threshold is reached, the corresponding query is rejected.
* **Default**: 300
* **Unit**: Second

### query_timeout

* **Description**: Used to set the query timeout in "seconds". This variable will act on all query statements in the current connection. The default value is 300 seconds. From v3.4.0 onwards, `query_timeout` does not apply to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE).
* **Value range**: [1, 259200]
* **Default**: 300
* **Data type**: Int
* **Unit**: Second

### range_pruner_max_predicate

* **Description**: The maximum number of IN predicates that can be used for Range partition pruning. Default value: 100. A value larger than 100 may cause the system to scan all tablets, which compromises the query performance.
* **Default**: 100
* **Introduced in**: v3.0

### resource_group

* **Scope**: Session
* **Description**: Session-level name of the Resource Group to use for query scheduling and resource isolation. When non-empty and `enable_resource_group` is effective, CoordinatorPreprocessor first tries to resolve this name via ResourceGroupMgr.chooseResourceGroupByName and, if found, assigns that group to the ConnectContext (also recorded in the audit event). If the named group is not set or not found, the system falls back to the session's `workgroup_id` and then to classifier/default selection logic in ResourceGroupMgr. The value is stored on SessionVariable (get/set) and is consulted by scheduler components (slot selection, fragment factory, coordinator) to determine group-level concurrency, slot allocation, and resource-overload checks.
* **Default**: `""`
* **Data Type**: String
* **Introduced in**: v3.2.0

### runtime_filter_on_exchange_node

* **Description**: Whether to place GRF on Exchange Node after GRF is pushed down across the Exchange operator to a lower-level operator. The default value is `false`, which means GRF will not be placed on Exchange Node after it is pushed down across the Exchange operator to a lower-level operator. This prevents repetitive use of GRF and reduces the computation time.

  However, GRF delivery is a "try-best" process. If the lower-level operator fails to receive the GRF but the GRF is not placed on Exchange Node, data cannot be filtered, which compromises filter performance. `true` means GRF will still be placed on Exchange Node even after it is pushed down across the Exchange operator to a lower-level operator.

* **Default**: false

### runtime_join_filter_push_down_limit

* **Description**: The maximum number of rows allowed for the Hash table based on which Bloom filter Local RF is generated. Local RF will not be generated if this value is exceeded. This variable prevents the generation of an excessively long Local RF.
* **Default**: 1024000
* **Data type**: Int

### runtime_profile_report_interval

* **Description**: The time interval at which runtime profiles are reported.
* **Default**: 10
* **Unit**: Second
* **Data type**: Int
* **Introduced in**: v3.1.0

### scan_olap_partition_num_limit

* **Description**: The number of partitions allowed to be scanned for a single table in the execution plan.
* **Default**: 0 (No limit)
* **Introduced in**: v3.3.9

### skip_local_disk_cache

* **Scope**: Session
* **Description**: Session-level boolean that instructs the frontend to mark scan ranges so backends skip the local disk cache. When set to `true`, OlapScanNode (while building TScanRange/TInternalScanRange) sets the `skip_disk_cache` flag on each scan range sent to BE, causing the storage engine on the backend to avoid reading from or populating the local disk cache for that query. It is applied per session and affects subsequent scans for that session. Commonly used together with `skip_page_cache` when bypassing caches to obtain fresh reads or to avoid cache pollution.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### skip_page_cache

* **Scope**: Session
* **Description**: When enabled, the frontend marks scan ranges with the skip-page-cache flag so that backends are instructed to bypass the page cache when reading tablet data. The session variable is read by scan planning (see `OlapScanNode.addScanRangeLocations`) and applied to each TInternalScanRange via `setSkip_page_cache`. Use this to avoid polluting OS or local page caches or to enforce direct I/O behavior for a specific session. It is evaluated together with related session flags such as `skip_local_disk_cache` and frontend-level `use_page_cache`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### spill_encode_level

* **Scope**: Session
* **Default**: `7`
* **Data Type**: int
* **Description**: Controls the encoding/compression level applied to data written to spill files. Its bit meanings follow the same convention as `transmission_encode_level`:
  * bit 2 (value 2) — enable integer-type encoding (streamvbyte) for integer-like columns.
  * bit 4 (value 4) — enable compression for binary/string columns (lz4).
  * bit 1 (value 1) — enable adaptive encoding (decide per-column based on compression ratio).
  Example interpretations:
  * `7` (default) = 1|2|4: adaptively encode integers and string/binary columns (adaptive decision using compression ratio &lt; 0.9).
  * `6` = 2|4: force integer and string/binary encoding (no adaptive decision).
  * `2` or `4`: selectively enable only integer or only string encoding respectively.
  This variable is meaningful when spilling is enabled (see `enable_spill`) and is read from the session to decide how spilled data is encoded to reduce disk/IO cost and potentially trade CPU for IO. See `transmission_encode_level` for additional implementation details and rationale.
* **Introduced in**: v3.2.0

### spill_mode (3.0 and later)

The execution mode of intermediate result spilling. Valid values:

* `auto`: Spilling is automatically triggered when the memory usage threshold is reached.
* `force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.

This variable takes effect only when the variable `enable_spill` is set to `true`.

### spill_partitionwise_agg

* **Description**: Enable partition-wise aggregation spilling for aggregation operators. When set, the planner/runtime will partition aggregate state into multiple spill partitions and spill/restore per-partition, which can reduce peak memory usage for large GROUP BY aggregations and improve spill parallelism. This session flag is propagated into the engine via `TSpillOptions.setSpill_partitionwise_agg`. Use `spill_partitionwise_agg_partition_num` to control the number of partitions and `spill_partitionwise_agg_skew_elimination` to enable skew elimination behavior when partitioning.
* **Scope**: Session
* **Default Value**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.5.2

### spill_revocable_max_bytes

* **Scope**: Session
* **Description**: Per-session threshold (in bytes) that causes spillable operators to start spilling as soon as their revocable memory usage exceeds the configured value. The FE serializes this value into `TSpillOptions.spill_revocable_max_bytes` (see `toThrift()`), so it is carried to execution engines that act on the threshold. Set this to a positive byte value to force aggressive spilling for operators that allocate revocable memory (for example, spillable join/agg/sort operators). The session default is `0`; leave at `0` unless you want to enforce an early spill threshold.
* **Default**: `0`
* **Data Type**: long
* **Introduced in**: v3.2.0

### spill_storage_volume

* **Description**: The storage volume with which you want to store the intermediate results of queries that triggered spilling. For more information, see [Spill to object storage](../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage).
* **Default**: Empty string
* **Introduced in**: v3.3.0

### sql_dialect

* **Description**: The SQL dialect that is used. For example, you can run the `set sql_dialect = 'trino';` command to set the SQL dialect to Trino, so you can use Trino-specific SQL syntax and functions in your queries.

  > **NOTICE**
  >
  > After you configure StarRocks to use the Trino dialect, identifiers in queries are not case-sensitive by default. Therefore, you must specify names in lowercase for your databases and tables at database and table creation. If you specify database and table names in uppercase, queries against these databases and tables will fail.

* **Data type**: StarRocks
* **Introduced in**: v3.0

### sql_mode

Used to specify the SQL mode to accommodate certain SQL dialects. Valid values include:

* `PIPES_AS_CONCAT`: The pipe symbol `|` is used to concatenate strings, for example, `select 'hello ' || 'world'`.
* `ONLY_FULL_GROUP_BY` (Default): The SELECT LIST can only contain GROUP BY columns or aggregate functions.
* `ALLOW_THROW_EXCEPTION`: returns an error instead of NULL when type conversion fails.
* `FORBID_INVALID_DATE`: prohibits invalid dates.
* `MODE_DOUBLE_LITERAL`: interprets floating-point types as DOUBLE rather than DECIMAL.
* `SORT_NULLS_LAST`: places NULL values at the end after sorting.
* `ERROR_IF_OVERFLOW`: returns an error instead of NULL in the case of arithmetic overflow. Currently, only the DECIMAL data type supports this option.
* `GROUP_CONCAT_LEGACY`: uses the `group_concat` syntax of v2.5 and earlier. This option is supported from v3.0.9 and v3.1.6.

You can set only one SQL mode, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT';
```

Or, you can set multiple modes at a time, for example:

```SQL
set sql_mode = 'PIPES_AS_CONCAT,ERROR_IF_OVERFLOW,GROUP_CONCAT_LEGACY';
```

### sql_safe_updates

Used for MySQL client compatibility. No practical usage.

### sql_select_limit

* **Description**: Used to limit the maximum number of rows returned by a query, which can prevent issues such as insufficient memory or network congestion caused by the query returning too much data.
* **Default**: Unlimited
* **Data type**: Long

### storage_engine

The types of engines supported by StarRocks:

* olap (default): StarRocks system-owned engine.
* mysql: MySQL external tables.
* broker: Access external tables through a broker program.
* elasticsearch or es: Elasticsearch external tables.
* hive: Hive external tables.
* iceberg: Iceberg external tables, supported from v2.1.
* hudi: Hudi external tables, supported from v2.2.
* jdbc: external table for JDBC-compatible databases, supported from v2.3.

### streaming_preaggregation_mode

Used to specify the preaggregation mode for the first phase of GROUP BY. If the preaggregation effect in the first phase is not satisfactory, you can use the streaming mode, which performs simple data serialization before streaming data to the destination. Valid values:

* `auto`: The system first tries local preaggregation. If the effect is not satisfactory, it switches to the streaming mode. This is the default value.
* `force_preaggregation`: The system directly performs local preaggregation.
* `force_streaming`: The system directly performs streaming.

### system_time_zone

Used to display the time zone of the current system. Cannot be changed.

### thrift_plan_protocol

* **Scope**: Session
* **Description**: Session-level setting `thrift_plan_protocol` that controls which Thrift TProtocol is used when FE serializes execution plans and related Thrift structures sent to execution nodes (BE/CN). Valid values: `binary`, `compact`, `json`. The value is read from the session (`getThriftPlanProtocol`) and applied to JobSpec (JobSpec.Builder calls `setPlanProtocol` and stores a lower-cased string), so it affects all jobs created from the current ConnectContext. Use `binary` for the default/high-performance binary Thrift protocol, `compact` to use a more compact binary encoding, or `json` for human-readable debugging output. Choosing a different protocol can change serialized plan size, CPU cost for (de)serialization, and interoperability with components expecting a specific Thrift protocol.
* **Default**: `binary`
* **Data Type**: String
* **Introduced in**: v3.2.0

### time_zone

Used to set the time zone of the current session. The time zone can affect the results of certain time functions.

### transaction_isolation

* **Scope**: Session
* **Description**: Session-level string that records the transaction isolation level for MySQL-client compatibility. It exists to satisfy MySQL-compatible clients and connection pools (for example `c3p0`) and to be compatible with MySQL 5.8 client expectations. StarRocks stores this value in the session (the FE field is `transactionIsolation`) and exposes the same logical name as the legacy `tx_isolation` session variable (`tx_isolation`). Typical MySQL isolation level values are accepted and expected (e.g. `READ-UNCOMMITTED`, `READ-COMMITTED`, `REPEATABLE-READ`, `SERIALIZABLE`). The variable is informational for client compatibility; setting it updates the session value but does not, by itself, implement or change internal StarRocks storage/transaction enforcement beyond what the engine already provides.
* **Default Value**: `REPEATABLE-READ`
* **Data Type**: String
* **Introduced in**: v3.2.0

### transaction_read_only

* **Description**: Used for MySQL 5.8 compatibility. The alias is `tx_read_only`. This variable specifies the transaction access mode. `ON` indicates read only and `OFF` indicates readable and writable.
* **Default**: OFF
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7

### transmission_compression_type

* **Description**: Controls the compression algorithm used for transmitting data in a session (applied to query RPC payloads and stream-load requests). The session value is emitted into Thrift `TQueryOptions` by `SessionVariable.toThrift()` using `CompressionUtils.findTCompressionByName(transmissionCompressionType)`, so it maps to a `TCompressionType` on the receiving side. Stream load accepts the same setting via the HTTP header `transmission_compression_type` (see `StreamLoadHttpHeader.HTTP_TRANSMISSION_COMPRESSION_TYPE`) and `StreamLoadParams.getTransmissionCompressionType()`. Valid names are the strings recognized by `CompressionUtils.findTCompressionByName` (case-insensitive); common examples include `AUTO`, `NO_COMPRESSION`, `lz4_frame`, `zstd`, `snappy`. Use `AUTO` to let the system choose based on runtime conditions. For load-specific control, see `load_transmission_compression_type`.  
* **Default**: `AUTO`
* **Data Type**: String
* **Introduced in**: v3.2.0

### transmission_encode_level

* **Description**: Controls per-column transmission encoding used during data exchange (e.g., RPC/exchange). The variable is a bitmask:
  * bit 1 (`1`): enable adaptive encoding (choose encoding based on measured compression ratio),
  * bit 2 (`2`): encode integers and integer-compatible types using StreamVByte,
  * bit 4 (`4`): compress binary/string columns with LZ4.
  Examples: `7` (1|2|4) enables adaptive encoding for both numbers and strings and will choose encoding when the encoding ratio is &lt; 0.9; `6` (2|4) forces encoding of numbers and strings. JSON and object column types are not supported yet. This setting affects per-session transmission behavior and is exposed as `TRANSMISSION_ENCODE_LEVEL`.
* **Scope**: Session
* **Default**: `7`
* **Data Type**: int
* **Introduced in**: v3.2.0

### tx_isolation

Used for MySQL client compatibility. No practical usage. The alias is `transaction_isolation`.

### tx_visible_wait_timeout

* **Scope**: Session
* **Description**: Session-level timeout (in seconds) that controls how long the server waits for a committed transaction to become VISIBLE before returning a COMMITTED result. The value is multiplied by 1000 and passed to VisibleStateWaiter.await as milliseconds. When `Config.enable_sync_publish` is true, the system uses the job deadline instead of this session timeout. Typical use sites: commit paths in StmtExecutor and TransactionStmtExecutor (decide VISIBLE vs COMMITTED) and MV refresh logic in MVTaskRunProcessor (the MV refresh temporarily sets this session variable to `Long.MAX_VALUE / 1000` to wait indefinitely and restores it afterwards). The variable is exposed on the session (getter/setter in SessionVariable).
* **Default**: `10`
* **Data Type**: long
* **Introduced in**: v3.2.0

### use_compute_nodes

* **Description**: The maximum number of CN nodes that can be used. This variable is valid when `prefer_compute_node=true`. Valid values:

  * `-1`: indicates that all CN nodes are used.
  * `0`: indicates that no CN nodes are used.
* **Default**: -1
* **Data type**: Int
* **Introduced in**: v2.4

### use_page_cache

* **Scope**: Session
* **Description**: Session-level boolean that controls whether queries should use the backend file system page cache. When a session sets `use_page_cache`, the FE serializes it into query options (Thrift field `use_page_cache`) and explicitly instructs BEs to enable or disable page-cache-backed reads for that query. If the FE does not set this session variable, the query follows the BE-side page cache policy. The variable is commonly set to `false` for internal/statistics/maintenance queries (see usages in `StatisticsCollectJob`, `HyperQueryJob`, `OnlineOptimizeJobV2`) to avoid polluting the shared page cache.
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### version (global)

The MySQL server version returned to the client. The value is the same as FE parameter `mysql_server_version`.

### version_comment (global)

The StarRocks version. Cannot be changed.

### wait_timeout

* **Description**: The number of seconds the server waits for activity on a non-interactive connection before closing it. If a client does not interact with StarRocks for this length of time, StarRocks will actively close the connection.
* **Default**: 28800 (8 hours).
* **Unit**: Second
* **Data type**: Int


