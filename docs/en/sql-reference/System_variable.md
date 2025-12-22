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

### authentication_policy

* **Scope**: Session
* **Description**: Session-level variable that holds the raw authentication policy string (kept for MySQL 8.0 compatibility). It is declared and stored on the FE as part of the session state and exposed under the name `authentication_policy`. In the current codebase snapshot the value is stored as an opaque comma-separated string and there is no other usage or internal parsing of this field in FE (it is not referenced elsewhere). It is related to the `default_authentication_plugin` session variable: `authentication_policy` represents per-session authentication policy data while `default_authentication_plugin` indicates the default authentication plugin. Administrators or clients can set this variable per session (for example with `SET authentication_policy = '...'`) to preserve compatibility with MySQL clients or tooling.
* **Default**: `*,,`
* **Data Type**: String
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

### catalog

* **Description**: Used to specify the catalog to which the session belongs.
* **Default**: default_catalog
* **Data type**: String
* **Introduced in**: v3.2.4

### cbo_cte_force_reuse_node_count

* **Description**: Session-scoped threshold that controls an optimizer shortcut for Common Table Expressions (CTEs). In RelationTransformer.visitCTE the planner counts nodes in a CTE producer tree (cteContext.getCteNodeCount). If that count is greater than or equal to this threshold and the threshold is greater than 0, the transformer forces reuse of the CTE: it skips inlining/transforming the producer plan, builds a consume operator with precomputed expression mappings (no inputs) and uses generated column refs instead. This reduces optimizer time for very large CTE producer trees at the cost of potentially less optimal physical plans. Setting the value to `0` disables the force-reuse optimization. This variable has a getter/setter in SessionVariable and is applied per session.
* **Scope**: Session
* **Default**: `2000`
* **Data Type**: int
* **Introduced in**: v3.5.3

### cbo_cte_reuse

* **Description**: Controls whether the optimizer may rewrite multi-distinct aggregate queries by reusing a Common Table Expression (CTE) (the CBO CTE‑reuse rewrite). When enabled, the planner (RewriteMultiDistinctRule) may choose a CTE-based rewrite for multi-column distincts, skewed aggregations, or when statistics indicate the CTE rewrite is more efficient; it also respects the `prefer_cte_rewrite` hint. When disabled, CTE-based rewrite is not allowed and the planner will attempt the multi-function rewrite; if a query requires CTE (for example, multi-column DISTINCT or functions that cannot be handled by multi-function rewrite) the planner will raise a user error. Note: the effective setting checked by the optimizer is the logical AND of this flag and the pipeline engine flag — i.e. `isCboCteReuse()` returns this variable AND `enablePipelineEngine`, so CTE reuse is only effective when `enablePipelineEngine` is on.
* **Default**: `true`
* **Data Type**: Boolean
* **Introduced in**: `v3.2.0`

### cbo_disabled_rules

* **Description**: Comma-separated list of optimizer rule names to disable for the current session. Each name must match a `RuleType` enum value and only rules whose names start with `TF_` (transformation rules) or `GP_` (group-combination rules) may be disabled. The session variable is stored on `SessionVariable` (`getCboDisabledRules` / `setCboDisabledRules`) and is applied by the optimizer via `OptimizerOptions.applyDisableRuleFromSessionVariable()`, which parses the list and clears the corresponding rule switches so those rules are skipped during planning. When set through a SET statement, values are validated and the server will reject unknown names or names not starting with `TF_`/`GP_` with clear error messages (e.g. "Unknown rule name(s): ..." or "Only TF_ ... and GP_ ... can be disabled"). At planner runtime, unknown rule names are ignored with a warning (logged as "Ignoring unknown rule name: ... (may be from different version)"). Names must match enum identifiers exactly (case-sensitive). Whitespace around names is trimmed; empty entries are ignored.
* **Scope**: Session
* **Default**: `""` (no disabled rules)
* **Data Type**: String
* **Introduced in**: -

### cbo_enable_low_cardinality_optimize

* **Description**: Whether to enable low cardinality optimization. After this feature is enabled, the performance of querying STRING columns improves by about three times.
* **Default**: true

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

### character_set_database (global)

* **Data type**: StringThe character set supported by StarRocks. Only UTF8 (`utf8`) is supported.
* **Default**: utf8
* **Data type**: String

### collation_connection

* **Description**: Session-scoped variable that stores the connection collation name for the current client session. It is declared in `SessionVariable` as `collationConnection` and exposed with the show-name `collation_connection`. The variable is surfaced in metadata and SHOW outputs (for example, used when building `SHOW CREATE VIEW` rows in `ShowExecutor` and returned as a constant for `information_schema.views` via `ViewsSystemTable.CONSTANT_MAP`). It represents the collation reported by the server for the connection (paired with `character_set_client` and related character-set variables) but does not by itself indicate runtime collation enforcement beyond what other components implement.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_database

* **Description**: Session-level variable that holds the default database collation name for the current session. It is declared in SessionVariable (annotated with `@VariableMgr.VarAttr`) and lives alongside other charset/collation session variables such as `character_set_client`, `collation_connection` and `collation_server`. The value is serialized when session variables are exported (e.g., included in the JSON produced by SessionVariable#getJsonString and in the session variable machinery), and is used to report the session's database collation. Changing this variable updates the session's reported database collation name; engine-level or object-level collation settings (server/table/column) may still take precedence for actual comparison/ordering behavior depending on context.
* **Scope**: Session
* **Default**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: v3.2.0

### collation_server

* **Scope**: Session
* **Description**: Session-level server collation name used by the FE to present MySQL-compatible collation behavior for this session. This variable sets the default collation identifier (for example `utf8_general_ci`) that FE reports to clients and that is associated with `character_set_server` / `collation_connection` / `collation_database`. It is persisted in the session variable JSON (see SessionVariable#getJsonString / replayFromJson) and is exposed via the variable manager (`@VarAttr(name = COLLATION_SERVER)`), so it appears in SHOW VARIABLES and can be changed per-session. The value is stored as a plain String in SessionVariable and typically holds a standard MySQL collation name (e.g. `utf8_general_ci`, `utf8mb4_unicode_ci`); the code does not enforce a fixed enum or perform additional validation here, so the effective behavior depends on downstream components that interpret the collation name for comparisons, ordering and other collation-sensitive operations.
* **Default Value**: `utf8_general_ci`
* **Data Type**: String
* **Introduced in**: `v3.2.0`

### computation_fragment_scheduling_policy

* **Scope**: Session
* **Description**: Controls the scheduler policy used to choose execution instances for computation fragments. Valid values (case-insensitive) are:
  * `compute_nodes_only` — schedule fragments only on compute nodes (default).
  * `all_nodes` — allow scheduling on both compute nodes and traditional backend nodes.
  The variable is backed by the enum `SessionVariableConstants.ComputationFragmentSchedulingPolicy`. When set, the value is validated (upper-cased) against the enum; invalid values cause an error (`IllegalArgumentException` when set via API, `SemanticException` when used in a SET statement). The getter returns the corresponding enum value and falls back to `COMPUTE_NODES_ONLY` if unset or unrecognized. This setting affects how the FE chooses target nodes for fragment placement at planning/deployment time.
* **Default**: `COMPUTE_NODES_ONLY`
* **Data Type**: String
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
* **Description**: When enabled, the analyzer converts decimal arithmetic results that would overflow the maximum decimal precision into 64-bit floating point (`DOUBLE`) instead of widening to larger decimal types or failing. Concretely, in DecimalV3 arithmetic (see DecimalV3FunctionAnalyzer), if a multiplication's computed precision exceeds the engine's max decimal precision but its return scale is within the maximum, the session flag `decimal_overflow_to_double = true` causes the return type and operand target types to be set to `DOUBLE`. This yields an approximate (lossy) numeric result but avoids decimal precision overflow errors or forced use of larger decimal types. When false (default), the planner will keep decimal semantics (attempt decimal128/256 or throw on unrepresentable scale/precision).
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: -

### default_authentication_plugin

* **Scope**: Session
* **Description**: Session-scoped variable that specifies the default MySQL authentication plugin name for this session. It is stored as SessionVariable.defaultAuthenticationPlugin and is used by StarRocks' MySQL-protocol compatibility layers when the server needs to advertise or use a default authentication plugin (for example during handshake or when a plugin is not specified). Accepts standard MySQL authentication plugin identifiers (e.g. `mysql_native_password`, `caching_sha2_password`) supported by the server. This variable affects session behavior only; persistent user account authentication configuration is managed separately. See related session variable `authentication_policy`.
* **Default**: `mysql_native_password`
* **Data Type**: String
* **Introduced in**: -

### default_rowset_type (global)

Used to set the default storage format used by the storage engine of the computing node. The currently supported storage formats are `alpha` and `beta`.

### default_storage_engine

* **Scope**: Session
* **Description**: Session system variable exposed as `default_storage_engine` (see SessionVariable VarAttr). It exists for MySQL 8.0 compatibility and to satisfy MySQL clients/libraries that query the session default storage engine. The variable is stored per-session in the SessionVariable object and is returned to clients (e.g., via SHOW VARIABLES). It is informational for compatibility; changing it adjusts the session-reported value but does not imply StarRocks will change internal storage implementation.
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

### default_table_compression

* **Description**: The default compression algorithm for table storage. Supported compression algorithms are `snappy, lz4, zlib, zstd`.

  Note that if you specified the `compression` property in a CREATE TABLE statement, the compression algorithm specified by `compression` takes effect.

* **Default**: lz4_frame
* **Introduced in**: v3.0

### default_tmp_storage_engine

* **Description**: Session variable that controls the default storage engine used for temporary tables (both explicit `CREATE TEMPORARY TABLE` and internal/implicit temporary tables created by the engine). Declared in `SessionVariable.java` with a `@VariableMgr.VarAttr` annotation, it exists primarily for MySQL 8.0 compatibility so clients and tools expecting MySQL-like behavior can observe or change the temporary-table engine per session. Changing this value affects how temporary table data is stored/managed on storage layers that honor different engines (for example, choosing between memory-backed vs. disk-backed engines).
* **Scope**: Session
* **Default**: `InnoDB`
* **Data Type**: String
* **Introduced in**: v3.4.2, v3.5.0

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

### disable_spill_to_local_disk

* **Description**: When set to `true` for the session, FE will instruct BE to disable spilling to local disk and instead rely on remote storage spill (if remote spill is configured). This flag is only meaningful when `enable_spill` = `true`, `enable_spill_to_remote_storage` = `true`, and a valid `spill_storage_volume` is provided and found by FE. The value is serialized into TSpillToRemoteStorageOptions (sent to BE) as `disable_spill_to_local_disk`. If remote spill is not configured or the named storage volume cannot be resolved, this setting has no effect. Use with caution: disabling local-disk spill can increase network I/O and latency and requires reliable, performant remote storage.
* **Scope**: Session
* **Default**: false
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

* **Description**: When enabled, the optimizer will add the CBO table pruning rule (CboTablePruneRule) during memo optimization to perform cost-based table pruning for cardinality-preserving joins. The rule is conditionally added in the optimizer (see QueryOptimizer.memoOptimize and SPMOptimizer.memoOptimize) only when the join-node count in the join tree is small (fewer than 10 join nodes). This option complements the rule-based pruning toggle `enable_rbo_table_prune` and lets the Cost-Based Optimizer try to remove unnecessary tables or inputs from join processing to reduce planning and execution complexity. Default is off because pruning can change plan shape; enable it only after validating on representative workloads.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_color_explain_output

* **Scope**: Session
* **Description**: Controls whether ANSI color escape sequences are included in textual EXPLAIN / PROFILE outputs. When enabled (`true`), StmtExecutor passes the session setting into the explain/profile pipeline (via calls to ExplainAnalyzer) so explain, EXPLAIN ANALYZE and analyze-profile outputs contain colored highlighting for readability in ANSI-capable terminals. When disabled (`false`), the output is produced without ANSI sequences (plain text), which is appropriate for logging, clients that do not support ANSI, or when piping output to files. This is a per-session toggle and does not change execution semantics—only the presentation of explain/profile text.
* **Default**: `true`
* **Data type**: boolean
* **Introduced in**: v3.5.0

### enable_connector_adaptive_io_tasks

* **Description**: Whether to adaptively adjust the number of concurrent I/O tasks when querying external tables. Default value is `true`. If this feature is not enabled, you can manually set the number of concurrent I/O tasks using the variable `connector_io_tasks_per_scan_operator`.
* **Default**: true
* **Introduced in**: v2.5

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

### enable_eliminate_agg

* **Description**: Controls optimizer transformations that remove or simplify aggregation operators when it is safe to do so. When enabled, the planner applies rules (EliminateAggRule and EliminateAggFunctionRule) to replace a LogicalAggregationOperator with a LogicalProjectOperator (and optionally a LogicalFilterOperator) in two cases:
  - Whole-aggregation elimination (EliminateAggRule): when grouping keys form a unique key on the child (unique/UKFK constraints) and all aggregate calls are supported, non-distinct functions (SUM, COUNT, AVG, FIRST_VALUE, MAX, MIN, GROUP_CONCAT). COUNT is rewritten to an IF/CAST expression (COUNT(col) -> IF(col IS NULL, 0, 1); COUNT(*) -> 1).
  - Per-function elimination (EliminateAggFunctionRule): when individual non-distinct aggregate functions over a grouped column (FIRST_VALUE, LAST_VALUE, ANY_VALUE, MAX, MIN) can be replaced by the column itself while preserving other aggregations.
  The optimization requires non-empty group-by keys, supported function sets, and presence of relevant unique constraints or column relationships; it does not apply to DISTINCT aggregates.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.3.8, v3.4.0, v3.5.0

### enable_filter_unused_columns_in_scan_stage

* **Description**: Controls pruning of columns produced by Scan nodes so the scan stage only outputs columns that are actually needed downstream (either as outputs or for non-pushable predicates). When enabled, PlanFragmentBuilder.setUnUsedOutputColumns will mark scan output columns that are exclusively used in pushdownable predicates and not required later, allowing the scan to trim those columns and reduce I/O and network transfer. The pruning is guarded: it will not apply for aggregation-family indexes in the non-skip-aggregation (non-skip-aggr) scan stage (keys/value columns must be retained to merge/aggregate), and the planner always ensures at least one column is returned from a scan. See `isEnableFilterUnusedColumnsInScanStage()` and the enable/disable helpers `enableTrimOnlyFilteredColumnsInScanStage()` / `disableTrimOnlyFilteredColumnsInScanStage()` in SessionVariable.
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

* **Description**: Whether to enable Colocate Group Execution. Colocate Group Execution is an execution pattern that leverages physical data partitioning, where a fixed number of threads sequentially process their respective data ranges to enhance locality and throughput. Enabling this feature can reduce memory usage.
* **Default**: true
* **Introduced in**: v3.3

### enable_group_level_query_queue (global)

* **Description**: Whether to enable resource group-level [query queue](../administration/management/resource_management/query_queues.md).
* **Default**: false, which means this feature is disabled.
* **Introduced in**: v3.1.4

### enable_incremental_mv

* **Description**: Session flag that controls whether the server will plan and keep an in-memory plan for materialized views that use incremental refresh. When enabled, `MaterializedViewAnalyzer.planMVQuery` will proceed for create-MV statements whose refresh scheme is an `IncrementalRefreshSchemeDesc`: it builds the logical and physical plan for the view query and sets the session `enableMVPlanner` flag (`setMVPlanner(true)`). When disabled, planning for incremental-refresh MVs is skipped. Accessible via `isEnableIncrementalRefreshMV()` and `setEnableIncrementalRefreshMv(boolean)` in `SessionVariable`.
* **Scope**: Session (per-connection)
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

### enable_load_profile

* **Scope**: Session
* **Description**: When enabled, the FE requests collection of the runtime profile for load jobs and the load coordinator will collect/export the profile after a load completes. For stream load, FE sets `TQueryOptions.enable_profile = true` and passes `load_profile_collect_second` (from `stream_load_profile_collect_threshold_second`) to backends; the coordinator then conditionally calls profile collection (see StreamLoadTask.collectProfile()). The effective behavior is the logical OR of this session variable and the table-level property `enable_load_profile` on the destination table; collection is further gated by `load_profile_collect_interval_second` (FE-side sampling interval) to avoid frequent collection. The session flag is read via `SessionVariable.isEnableLoadProfile()` and can be set per-connection with `setEnableLoadProfile(...)`.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_local_shuffle_agg

* **Description**: Controls whether the planner and cost model may produce a one-phase local aggregation plan that uses a local shuffle (Scan -> LocalShuffle -> OnePhaseAgg) instead of a two-phase/global-shuffle aggregation. When enabled (the default), the optimizer and cost model will:
  - allow replacing a SHUFFLE exchange between Scan and Global Agg with a local shuffle + one-phase agg on single-backend-and-compute-node clusters (see `PruneShuffleDistributionNodeRule` and `EnforceAndCostTask`),
  - let the cost model ignore network cost for SHUFFLE in that single-node case to favor the one-phase plan (`CostModel`).
  The replacement is only considered when `enable_pipeline_engine` is enabled and the cluster is a single backend+compute node. The planner still rejects local-shuffle conversion in unsafe cases (e.g., DISTINCT aggregates, detected data skew, missing/unknown column statistics, multi-input operators like joins, or other semantic restrictions). Some code paths (INSERT/UPDATE/DELETE planners and MaterializedViewOptimizer) temporarily disable this session flag because non-query sinks or certain rewrites require per-driver scan assignment that local-shuffle cannot use.
* **Scope**: Session
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

### enable_parallel_merge

* **Description**: Whether to enable parallel merge for sorting. When this feature is enabled, the merge phase of sorting will utilize multiple threads for merge operations.
* **Default**: true
* **Introduced in**: v3.3

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

* **Description**: Whether to enable bucketed computation. When this feature is enabled, stage-one aggregation can be computed in bucketed order, reducing memory usage.
* **Default**: true
* **Introduced in**: v3.0

### enable_phased_scheduler

* **Description**: Whether to enable multi-phased scheduling. When multi-phased scheduling is enabled, it will schedule fragments according to their dependencies. For example, the system will first schedule the fragment on the build side of a Shuffle Join, and then the fragment on the probe side (Note that, unlike stage-by-stage scheduling, phased scheduling is still under the MPP execution mode). Enabling multi-phased scheduling can significantly reduce memory usage for a large number of UNION ALL queries.
* **Default**: false
* **Introduced in**: v3.3

### enable_pipeline_engine

* **Description**: Specifies whether to enable the pipeline execution engine. `true` indicates enabled and `false` indicates the opposite. Default value: `true`.
* **Default**: true

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

### enable_profile

* **Description**: Specifies whether to send the profile of a query for analysis. The default value is `false`, which means no profile is required.

  By default, a profile is sent to the FE only when a query error occurs in the BE. Profile sending causes network overhead and therefore affects high concurrency.

  If you need to analyze the profile of a query, you can set this variable to `true`. After the query is completed, the profile can be viewed on the web page of the currently connected FE (address: `fe_host:fe_http_port/query`). This page displays the profiles of the latest 100 queries with `enable_profile` turned on.

* **Default**: false

### enable_query_cache

* **Description**: Specifies whether to enable the Query Cache feature. Valid values: true and false. `true` specifies to enable this feature, and `false` specifies to disable this feature. When this feature is enabled, it works only for queries that meet the conditions specified in the application scenarios of [Query Cache](../using_starrocks/caching/query_cache.md#application-scenarios).
* **Default**: false
* **Introduced in**: v2.5

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

* **Description**: When enabled, the optimizer applies Rule-Based (RBO) table pruning for cardinality-preserving joins in the current session. The optimizer runs a sequence of rewrite and pruning steps (partition pruning, project merge/separate, `UniquenessBasedTablePruneRule`, join reorder, and `RboTablePruneRule`) to remove unnecessary table scan alternatives and reduce scanned partitions/rows in joins. Enabling this option also disables join-equivalence derivation (`context.setEnableJoinEquivalenceDerive(false)`) while logical rule rewrite is running to avoid conflicting transformations. The pruning flow may additionally run `PrimaryKeyUpdateTableRule` for update statements if `enable_table_prune_on_update` is set. The rule is only executed when a query contains prunable joins (checked via `Utils.hasPrunableJoin(tree)`).
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_runtime_adaptive_dop

* **Scope**: Session
* **Description**: When enabled for a session, the planner and fragment builder will mark pipeline-capable fragments that support runtime adaptive DOP to use adaptive degree-of-parallelism at runtime. This option only takes effect when `enable_pipeline_engine` is true. Enabling it causes fragments to call `enableAdaptiveDop()` during plan construction, and has runtime implications: join probes may wait for all build phases to complete (which conflicts with `group_execution` behavior), and enabling runtime adaptive DOP will disable pipeline-level multi-partitioned runtime filters (the setter clears `enablePipelineLevelMultiPartitionedRf`). The flag is recorded in the query profile and can be toggled per-session.
* **Default**: `false`
* **Data type**: boolean
* **Introduced in**: v3.2.0

### enable_scan_datacache

* **Description**: Specifies whether to enable the Data Cache feature. After this feature is enabled, StarRocks caches hot data read from external storage systems into blocks, which accelerates queries and analysis. For more information, see [Data Cache](../data_source/data_cache.md). In versions prior to 3.2, this variable was named as `enable_scan_block_cache`.
* **Default**: true 
* **Introduced in**: v2.5

### enable_shared_scan

* **Scope**: Session
* **Description**: Session-level boolean flag intended to request shared scan execution for pipeline queries. When pipeline execution is enabled, the FE will propagate this setting into the fragment execution parameters (`TExecPlanFragmentParams.enable_shared_scan`) so the BE can perform shared scanning (see `.../TFragmentInstanceFactory.java:153-159`). However, the FE currently does not honor user changes: `SessionVariable.isEnableSharedScan()` always returns `false` (see `.../SessionVariable.java:4176-4180`) and the feature has been disabled in FE since later versions due to incompatibility with event-based scheduling. As a result, setting this variable in a session has no effect in current releases.
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

* **Description**: Session-level boolean that controls whether the optimizer applies primary-key-specific table pruning rules for UPDATE statements. When enabled, QueryOptimizer (during the pruneTables stage) invokes `PrimaryKeyUpdateTableRule` to rewrite/prune update plans — potentially improving pruning for primary-key update patterns. This flag is only effective when rule-based/CBO table pruning is active (see `enable_rbo_table_prune`). It is disabled by default because the transformation can change data-layout/plan shape (e.g., bucket-shuffle layout for OlapTableSink) and may cause correctness or performance regressions for concurrent updates.
* **Default**: `false`
* **Data type**: boolean
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

### enable_ukfk_opt

* **Description**: Enables optimizer support for Unique-Key / Foreign-Key (UK/FK) based transformations and statistics enhancements. When set, the optimizer runs `UKFKConstraintsCollector` to collect unique and foreign-key constraints bottom‑up and attach them to plan nodes (OptExpressions). The collected constraints are consumed by transformation rules such as `PruneUKFKJoinRule` (which can prune the UK-side of joins, rewrite predicates from UK to FK columns and add IS NULL checks for outer-join cases) and `PruneUKFKGroupByKeysRule` (which can remove redundant GROUP BY keys derived from UK/FK relationships). The collected UK/FK information is also used in `StatisticsCalculator` to produce tighter join cardinality estimates for UK‑FK joins and may replace default estimates when more precise. Default is conservative (`false`) because these optimizations rely on declared schema constraints and can change plan shape and predicate placement.
* **Default**: `false`
* **Scope**: Session
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

### group_execution_max_groups

* **Description**: Maximum number of groups allowed for Group Execution. It is used to limit the granularity of splitting, preventing excessive scheduling overhead caused by an excessive number of groups.
* **Default**: 128
* **Introduced in**: v3.3

### group_execution_min_scan_rows

* **Description**: Minimum number of rows processed per group for Group Execution.
* **Default**: 5000000
* **Introduced in**: v3.3

### hash_join_push_down_right_table

* **Description**: Used to control whether the data of the left table can be filtered by using the filter condition against the right table in the Join query. If so, it can reduce the amount of data that needs to be processed during the query.
**Default**: `true` indicates the operation is allowed and the system decides whether the left table can be filtered. `false` indicates the operation is disabled. The default value is `true`.

### historical_nodes_min_update_interval

* **Description**: The minimum interval between two updates of historical node records. If the nodes of a cluster change frequently in a short period of time (that is, less than the value set in this variable), some intermediate states will not be recorded as valid historical node snapshots. The historical nodes are the main basis for the Cache Sharing feature to choose the right cache nodes during cluster scaling.
* **Default**: 600
* **Unit**: Seconds
* **Introduced in**: v3.5.1

### init_connect (global)

Used for MySQL client compatibility. No practical usage.

### innodb_read_only

* **Description**: Session-level flag (MySQL-compatible) that indicates the session's InnoDB read-only mode. The variable is declared and stored on the session as the Java field `innodbReadOnly` in `SessionVariable.java` and is accessible via `isInnodbReadOnly()` and `setInnodbReadOnly(boolean)`. The SessionVariable class only holds the flag; any enforcement (preventing write/DDL to InnoDB tables or altering transaction behavior) must be implemented by the transaction/storage/authorization layers which should read this session flag. Use this variable to convey client intent for read-only behavior within the current session for components that respect it.
* **Scope**: Session
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

### interpolate_passthrough

* **Description**: Whether to add local-exchange-passthrough for certain operators. Currently supported operators include streaming aggregates, etc. Adding local-exchange can mitigate the impact of data skew on computation, but will slightly increase memory usage. 
* **Default**: true
* **Introduced in**: v3.2

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

### join_late_materialization

* **Scope**: Session
* **Description**: Controls whether the planner enables "late materialization" on join operators. When set to `true`, PlanFragmentBuilder reads the session flag and calls `joinNode.setEnableLateMaterialization(...)` so join execution can defer full row/payload materialization until after join/key-based filtering. This reduces memory usage and I/O for joins with large payload columns or highly selective join predicates by carrying only join keys through the join and materializing payloads for matched rows. The flag is defined in `SessionVariable.java` as `JOIN_LATE_MATERIALIZATION` and defaults to `false`. Enabling this may interact with column-trimming and scan-stage pruning optimizations (for example `enable_filter_unused_columns_in_scan_stage`) and can change join runtime behavior; test queries for correctness and performance before enabling broadly.
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
* **Description**: Threshold (number of constants) at which the planner switches a regular IN-list predicate into the compact LargeInPredicate representation to avoid building very large ASTs and heavy FE analysis/planning overhead. When `enable_large_in_predicate` is true and an IN-list (string or integer) contains >= this many literals, the parser (AstBuilder) creates a LargeInPredicate containing the raw text and a compact value list (and keeps only a minimal representative Expr in the AST). For integer lists the parser additionally verifies all literals can be parsed as integral values and will fall back to a normal InPredicate if parsing fails. LargeInPredicate is later transformed into a left semi/anti join for execution, improving parse/analyze/deploy performance for queries with very large constant IN lists. The variable is exposed as a session-level `VariableMgr.VarAttr` and can be get/set per session.
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

### lower_case_table_names (global)

Used for MySQL client compatibility. No practical usage. Table names in StarRocks are case-sensitive.

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

### max_allowed_packet

* **Description**: Used for compatibility with the JDBC connection pool C3P0. This variable specifies the maximum size of packets that can be transmitted between the client and server.
* **Default**: 33554432 (32 MB). You can raise this value if the client reports "PacketTooBigException".
* **Unit**: Byte
* **Data type**: Int

### max_parallel_scan_instance_num

* **Scope**: Session
* **Description**: Session-level integer that caps the number of parallel scan instances the planner will produce for scan operators (applied to OLAP/Lake scan nodes). The value is propagated into the scan node Thrift message (`TOlapScanNode.max_parallel_scan_instance_num`) and is included in query/load runtime profiles. It is declared with `@VariableMgr.VarAttr` and can be read/set via the session variable APIs (`getMaxParallelScanInstanceNum` / `setMaxParallelScanInstanceNum`). Use this to limit scan parallelism per session for resource control or debugging. When left at the default `-1`, the planner/system default (resource- or configuration-derived) parallelism is used.
* **Default**: `-1`
* **Data Type**: int
* **Introduced in**: v3.2.0

### max_pipeline_dop

* **Scope**: Session
* **Description**: The per-session upper bound for the pipeline engine's degree-of-parallelism (DOP). Behavior:
  * Applies only when `enable_pipeline_engine` is enabled and `pipeline_dop` is not explicitly set (greater than 0). If `pipeline_dop` greater than 0 this variable is ignored and `pipeline_dop` is used directly.
  * When `pipeline_dop` less than or equal to 0 (adaptive/default mode), the effective DOP for execution is computed as min(`max_pipeline_dop`, the backend default DOP returned by BackendResourceStat). For pipeline sinks the same logic uses the sink default DOP.
  * If `max_pipeline_dop` less than or equal to 0 no additional cap is applied and the backend default DOP is used.
  * Purpose: avoid negative overhead from scheduling on machines with very large core counts by capping automatically computed parallelism.
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

* **Description**: The late materialization mode of parallel merge for sorting. Valid values:
  * `AUTO`
  * `ALWAYS`
  * `NEVER`
* **Default**: `AUTO`
* **Introduced in**: v3.3

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

* **Scope**: Session
* **Description**: Timeout (in seconds) for phase 1 of query execution — the delivery of all plan fragment instances from the coordinator (FE) to backend executors (BEs). StarRocks executes queries in two phases: (1) deliver fragments to BEs, and (2) pull results after fragments are prepared. `query_delivery_timeout` controls how long the coordinator waits for fragment delivery to complete before timing out. When building the query options sent to the backend, the value is assigned to the `query_delivery_timeout` field (subject to an internal cap to avoid integer overflow). If `enablePhasedScheduler` is enabled, the system uses the `query_timeout` value instead for delivery timeout.
* **Default**: `300`
* **Data Type**: int (seconds)
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

        * **Description**: The specified resource group of this session
        * **Default**: ""
        * **Data Type**: String
        * **Introduced in**: 3.2.0

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

* **Description**: Session flag that instructs the FE, when building scan ranges, to mark each tablet's internal scan range with `skip_disk_cache`. When set to `true`, `OlapScanNode.addScanRangeLocations()` sets `internalRange.setSkip_disk_cache(true)` on the created `TInternalScanRange` objects so downstream BE scan nodes are told to bypass the local disk cache for that scan. It is applied per-session and is evaluated at plan/scan-range construction time. Use this together with `skip_page_cache` (to control page cache skipping) and data-cache related variables (`enable_scan_datacache` / `enable_populate_datacache`) as appropriate.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### skip_page_cache

* **Scope**: Session
* **Description**: Session-level boolean flag that instructs the planner/frontend to mark scan ranges so backends should bypass the page cache when reading data. When enabled, `OlapScanNode.addScanRangeLocations` sets `TInternalScanRange.skip_page_cache` for each scan range sent to the backend, causing storage reads to skip the OS/page caching layer. Typical use cases: large one-time scans where avoiding page-cache pollution is desired or when a user prefers direct I/O semantics. Do not confuse with `skip_local_disk_cache`, which controls the storage-layer data cache; `fill_data_cache` can also influence caching behavior.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.3.9, v3.4.0, v3.5.0

### spill_encode_level

* **Scope**: Session
* **Description**: Controls the encoding/compression behaviour applied to operator spill files. The integer is a bit-flag level whose meanings mirror `transmission_encode_level`:
  * bit 1 (value `1`) — enable adaptive encoding;
  * bit 2 (value `2`) — encode integer-like columns with streamvbyte;
  * bit 4 (value `4`) — compress binary/string columns with LZ4.
  Example semantics from the related `transmission_encode_level` comment: `7` enables adaptive encoding for numbers and strings; `6` forces encoding of numbers and strings. Changing this value adjusts CPU vs. disk I/O trade-offs for spills (higher encoding levels increase CPU work but reduce spill size / I/O).
  Implemented as the session variable annotated `SPILL_ENCODE_LEVEL` in `SessionVariable.java` (getter `getSpillEncodeLevel()`), and documented adjacent to other spill tunables such as `spill_mem_table_size`.
* **Default**: `7`
* **Data Type**: int
* **Introduced in**: v3.2.0

### spill_mode (3.0 and later)

The execution mode of intermediate result spilling. Valid values:

* `auto`: Spilling is automatically triggered when the memory usage threshold is reached.
* `force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.

This variable takes effect only when the variable `enable_spill` is set to `true`.

### spill_partitionwise_agg

* **Description**: Session-level flag that enables partition-wise aggregation behavior when spill is used for aggregation operators. When `spill_partitionwise_agg` is true (and `enable_spill` is enabled), the execution engine will partition spilled aggregation data and perform per-partition spill/merge processing. The flag is propagated to execution via `TSpillOptions.setSpill_partitionwise_agg`. Related session settings that affect its behavior are `spill_partitionwise_agg_partition_num` (number of partitions created) and `spill_partitionwise_agg_skew_elimination` (skew handling). This option reduces peak memory usage for large-group aggregations by splitting work across partitions during spill, but may increase I/O and merge overhead.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.5.2

### spill_revocable_max_bytes

* **Scope**: Session
* **Description**: Experimental per-session threshold (in bytes) for operator revocable memory. If an operator's revocable memory exceeds this value, the operator will initiate spilling to disk "as soon as possible" to free revocable memory. Use this to tune aggressive spilling for memory‑intensive operators; the value is interpreted in bytes.
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

### time_zone

Used to set the time zone of the current session. The time zone can affect the results of certain time functions.

### transaction_isolation

* **Description**: Session-scoped compatibility variable that records the client-requested transaction isolation level using MySQL-style names. Declared in SessionVariable as `transactionIsolation` and annotated with `@VariableMgr.VarAttr(name = TRANSACTION_ISOLATION)`; its presence ensures compatibility with MySQL 5.8 clients. There is a related `tx_isolation` variable (kept for c3p0 client compatibility). The value is stored per-session to satisfy client/libraries, e.g., `REPEATABLE-READ`. Transaction isolation semantics in the engine are managed by StarRocks' transaction subsystem and may not be changed solely by modifying this session variable.
* **Scope**: Session
* **Default**: `REPEATABLE-READ`
* **Data Type**: String
* **Introduced in**: v3.2.0

### transaction_read_only

* **Description**: Used for MySQL 5.8 compatibility. The alias is `tx_read_only`. This variable specifies the transaction access mode. `ON` indicates read only and `OFF` indicates readable and writable.
* **Default**: OFF
* **Introduced in**: v2.5.18, v3.0.9, v3.1.7

### tx_isolation

Used for MySQL client compatibility. No practical usage. The alias is `transaction_isolation`.

### use_compute_nodes

* **Description**: The maximum number of CN nodes that can be used. This variable is valid when `prefer_compute_node=true`. Valid values:

  * `-1`: indicates that all CN nodes are used.
  * `0`: indicates that no CN nodes are used.
* **Default**: -1
* **Data type**: Int
* **Introduced in**: v2.4

### use_page_cache

* **Description**: Session-scoped boolean that controls whether a query should use the backend page cache. If not explicitly set by FE, the query follows the BE's page-cache policy; if set in the session, FE enforces the session value. The session variable is propagated to the execution layer (e.g., `tResult.setUse_page_cache`) so BE execution honors the decision. Commonly disabled (`false`) for internal/background jobs (statistics collection, hyper queries, online optimize) to avoid polluting the shared page cache with non-user data — see usages in `StatisticsCollectJob`, `HyperQueryJob`, and `OnlineOptimizeJobV2`.
* **Scope**: Session
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


