---
displayed_sidebar: docs
sidebar_label: "Execution Engine and Parallelism"
sidebar_position: 3
description: "Session variables for the pipeline engine, parallelism, runtime filters, and query cache."
---

# System Variables - Execution Engine and Parallelism

For how to view and set variables, see the [System variables overview](../System_variable.md).

### blacklist_backup_routing

* **Scope**: Session
* **Description**: In shared-data mode, if the compute node the plan prefers for a scan is not among the workers available to the current query (for example, the node is down or appears on the host blocklist), the planner must choose a backup compute node. This variable sets how that backup is chosen among eligible nodes (other than the primary). `RANDOM` samples uniformly at random from the eligible set. `CIRCULAR` walks the sorted compute node id ring from the primary and takes the first eligible node (deterministic). Which nodes are eligible for backup also depends on `skip_black_list`: by default, nodes on the host blocklist are excluded; if `skip_black_list` is `true`, a node that is on the blocklist may still be chosen as a backup when it is otherwise available (for example, alive and in the warehouse).
* **Default**: `CIRCULAR`
* **Data type**: String
* **Valid values**: `CIRCULAR`, `RANDOM`
* **Introduced in**: -

### computation_fragment_scheduling_policy

* **Scope**: Session
* **Description**: Controls the scheduler policy used to choose execution instances for computation fragments. Valid values (case-insensitive) are:
  * `compute_nodes_only` — schedule fragments only on compute nodes (default).
  * `all_nodes` — allow scheduling on both compute nodes and traditional backend nodes.
  The variable is backed by the enum `SessionVariableConstants.ComputationFragmentSchedulingPolicy`. When set, the value is validated (upper-cased) against the enum; invalid values cause an error (`IllegalArgumentException` when set via API, `SemanticException` when used in a SET statement). The getter returns the corresponding enum value and falls back to `COMPUTE_NODES_ONLY` if unset or unrecognized. This setting affects how the FE chooses target nodes for fragment placement at planning/deployment time.
* **Default**: `COMPUTE_NODES_ONLY`
* **Data Type**: String
* **Introduced in**: v3.2.7

### enable_adaptive_sink_dop

* **Description**: Specifies whether to enable adaptive parallelism for data loading. After this feature is enabled, the system automatically sets load parallelism for INSERT INTO and Broker Load jobs, which is equivalent to the mechanism of `pipeline_dop`. For a newly deployed v2.5 StarRocks cluster, the value is `true` by default. For a v2.5 cluster upgraded from v2.4, the value is `false`.
* **Default**: false
* **Introduced in**: v2.5

### enable_bucket_aware_execution_on_lake

* **Description**: Whether to enable bucket-aware execution for queries against data lakes (such as Iceberg tables). When this feature is enabled, the system optimizes query execution by leveraging bucketing information to reduce data shuffling and improve performance. This optimization is particularly effective for join operations and aggregations on bucketed tables.
* **Default**: true
* **Data type**: Boolean
* **Introduced in**: v4.0

### enable_cache_udaf

* **Description**: When set to `true`, enables in-memory caching of the class-level Java UDAF initialization (class loading, method introspection, and batch-update stub generation). The cache is populated on first use and reused across all aggregator/analytor instances within the same BE process, eliminating the repeated per-instance initialization overhead that is otherwise proportional to pipeline DOP. Caching only applies to UDAFs and window functions that were created with `"isolation" = "shared"`. Functions created with `"isolation" = "private"` always go through the uncached path regardless of this setting. Default is `false`; enable after verifying that shared-isolation UDAFs are safe to share their class-level state across concurrent queries. The runtime profile exposes `UdafCacheHitCount`, `UdafCachePopulateCount`, and `UdafLoadTime` counters to observe cache behavior.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.4.0

### enable_global_runtime_filter

Whether to enable global runtime filter (RF for short). RF filters data at runtime. Data filtering often occurs in the Join stage. During multi-table joins, optimizations such as predicate pushdown are used to filter data, in order to reduce the number of scanned rows for Join and the I/O in the Shuffle stage, thereby speeding up the query.

StarRocks offers two types of RF: Local RF and Global RF. Local RF is suitable for Broadcast Hash Join and Global RF is suitable for Shuffle Join.

Default value: `true`, which means global RF is enabled. If this feature is disabled, global RF does not take effect. Local RF can still work.

### enable_group_execution

* **Description**: Whether to enable Colocate Group Execution. Colocate Group Execution is an execution pattern that leverages physical data partitioning, where a fixed number of threads sequentially process their respective data ranges to enhance locality and throughput. Enabling this feature can reduce memory usage.
* **Default**: true
* **Introduced in**: v3.3

### enable_lake_tablet_internal_parallel

* **Description**: Whether to enable Parallel Scan for Cloud-native tables in a shared-data cluster.
* **Default**: true
* **Data type**: Boolean
* **Introduced in**: v3.3.0

### enable_local_shuffle_agg

* **Description**: Controls whether the planner and cost model may produce a one-phase local aggregation plan that uses a local shuffle (Scan -> LocalShuffle -> OnePhaseAgg) instead of a two-phase/global-shuffle aggregation. When enabled (the default), the optimizer and cost model will:
  - allow replacing a SHUFFLE exchange between Scan and Global Agg with a local shuffle + one-phase agg on single-backend-and-compute-node clusters (see `PruneShuffleDistributionNodeRule` and `EnforceAndCostTask`),
  - let the cost model ignore network cost for SHUFFLE in that single-node case to favor the one-phase plan (`CostModel`).
  The replacement is only considered when `enable_pipeline_engine` is enabled and the cluster is a single backend+compute node. The planner still rejects local-shuffle conversion in unsafe cases (e.g., DISTINCT aggregates, detected data skew, missing/unknown column statistics, multi-input operators like joins, or other semantic restrictions). Some code paths (INSERT/UPDATE/DELETE planners and MaterializedViewOptimizer) temporarily disable this session flag because non-query sinks or certain rewrites require per-driver scan assignment that local-shuffle cannot use.
* **Scope**: Session
* **Default**: `true`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### enable_multicolumn_global_runtime_filter

Whether to enable multi-column global runtime filter. Default value: `false`, which means multi-column global RF is disabled.

If a Join (other than Broadcast Join and Replicated Join) has multiple equi-join conditions:

* If this feature is disabled, only Local RF works.
* If this feature is enabled, multi-column Global RF takes effect and carries `multi-column` in the partition by clause.

### enable_parallel_merge

* **Description**: Whether to enable parallel merge for sorting. When this feature is enabled, the merge phase of sorting will utilize multiple threads for merge operations.
* **Default**: true
* **Introduced in**: v3.3

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

### enable_query_cache

* **Description**: Specifies whether to enable the Query Cache feature. Valid values: true and false. `true` specifies to enable this feature, and `false` specifies to disable this feature. When this feature is enabled, it works only for queries that meet the conditions specified in the application scenarios of [Query Cache](../using_starrocks/caching/query_cache.md#application-scenarios).
* **Default**: false
* **Introduced in**: v2.5

### enable_query_tablet_affinity

* **Description**: Boolean value to control whether to direct multiple queries against the same tablet to a fixed replica.

  In scenarios where the table to query has a large number of tablets, this feature significantly improves query performance because the meta information and data of the tablet can be cached in memory more quickly.

  However, if there are some hotspot tablets, this feature may degrade the query performance because it directs the queries to the same BE, making it unable to fully use the resources of multiple BEs in high concurrency scenarios.

* **Default**: false, which means the system selects a replica for each query.
* **Introduced in**: v2.5.6, v3.0.8, v3.1.4, and v3.2.0.

### enable_runtime_adaptive_dop

* **Scope**: Session
* **Description**: When enabled for a session, the planner and fragment builder will mark pipeline-capable fragments that support runtime adaptive DOP to use adaptive degree-of-parallelism at runtime. This option only takes effect when `enable_pipeline_engine` is true. Enabling it causes fragments to call `enableAdaptiveDop()` during plan construction, and has runtime implications: join probes may wait for all build phases to complete (which conflicts with `group_execution` behavior), and enabling runtime adaptive DOP will disable pipeline-level multi-partitioned runtime filters (the setter clears `enablePipelineLevelMultiPartitionedRf`). The flag is recorded in the query profile and can be toggled per-session.
* **Default**: `false`
* **Data type**: boolean
* **Introduced in**: v3.2.0

### enable_short_circuit

* **Description**: Whether to enable short circuiting for queries. Default: `false`. If it is set to `true`, when the query meets the criteria (to evaluate whether the query is a point query): the conditional columns in the WHERE clause include all primary key columns, and the operators in the WHERE clause are `=` or `IN`, the query takes the short circuit.
* **Default**: false
* **Introduced in**: v3.2.3

### enable_tablet_internal_parallel

* **Description**: Whether to enable adaptive parallel scanning of tablets. After this feature is enabled, multiple threads can be used to scan one tablet by segment, increasing the scan concurrency.
* **Default**: true
* **Introduced in**: v2.3

### enable_tablet_pre_split

* **Description**: Per-session opt-out for Sample-Based Tablet Pre-Split. Defaults to `true` so the FE Config gates (`enable_tablet_pre_split_for_*`) remain the primary on/off switch. Set this to `false` for a session whose load you want to leave undisturbed. Both the matching Config flag and this session variable must be `true` for pre-split to run.
* **Default**: true
* **Introduced in**: v4.1.0

### enable_topn_filter_back_pressure

* **Description**: Whether a scan self-enables TopN runtime-filter (RF) back-pressure. When a TopN/stream-build RF (from an `ORDER BY ... LIMIT` query, or an aggregate runtime in-filter) targets a scan, back-pressure clamps the scan's read-ahead to a small number of IO tasks until the RF actually arrives. This prevents a burst of concurrent readers from overshooting the (non-concurrency-aware) row budget and flooding the downstream aggregation before the RF can prune. Applies to both shared-nothing (OLAP) and shared-data (lake/connector) scans. When set to `false`, a scan only back-pressures if the FE `topn_filter_back_pressure_mode` is enabled.
* **Default**: true
* **Introduced in**: v4.1

The following variables tune the back-pressure behavior and only take effect when `enable_topn_filter_back_pressure` is `true`:

| Variable | Default | Description |
| --- | --- | --- |
| `topn_filter_back_pressure_io_tasks` | 1 | Read-ahead IO-task cap applied while the TopN RF is still pending. Set it to `<= 0` to disable the clamp (the scan uses the full `io_tasks_per_scan_operator`). |
| `topn_back_pressure_num_rows` | 1024 | Number of rows a scan may read in the first throttle round before back-pressure starts throttling. The allowance doubles each subsequent round. |
| `topn_back_pressure_throttle_time_ms` | 8 | Duration (in milliseconds) of the first throttle window. The window doubles each subsequent round. |
| `topn_back_pressure_throttle_time_upper_bound_ms` | 100 | Upper bound (in milliseconds) on the total time back-pressure throttles a scan before giving up and letting it proceed at full read-ahead, even if the RF never arrived. |
| `topn_back_pressure_max_rounds` | 8 | Maximum number of throttle rounds before back-pressure gives up. |

### enable_topn_runtime_filter

* **Description**: Whether to enable TopN Runtime Filter. If this feature is enabled, a runtime filter will be dynamically constructed for ORDER BY LIMIT queries and pushed down to the scan for filtering.
* **Default**: true
* **Introduced in**: v3.3

### enable_wait_dependent_event

* **Description**: Whether Pipeline waits for a dependent operator to finish execution before continuing within the same fragment. For example, in a left join query, when this feature is enabled, the probe-side operator waits for the build-side operator to finish before it starts executing. Enabling this feature can reduce memory usage, but may increase the query latency. However, for queries reused in CTE, enabling this feature may increase memory usage.
* **Default**: false
* **Introduced in**: v3.3

### force_schedule_local (Session)

* **Scope**: Session
* **Description**: When enabled, the query scheduler prefers assigning HDFS/file scan ranges to co‑located (local) compute backends that host the data blocks. `HDFSBackendSelector` receives this flag from the session and calls its computeForceScheduleLocalAssignment(...) path to: 1) select backends whose hostnames match scan-range locations, 2) choose among those local backends using reBalanceScanRangeForComputeNode(...) (which considers per-node assigned bytes and a max imbalance ratio), and 3) return remaining ranges for remote assignment via consistent hashing. `HiveConnectorScanRangeSource` also reads this session variable and, when set, emits block-level scan ranges (one per block) to enable local placement. Use this to improve data locality and reduce network I/O. Note it can increase skew (assignment imbalance) and may cause more rebalancing in heavy-locality scenarios.
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.2.0

### group_execution_max_groups

* **Description**: Maximum number of groups allowed for Group Execution. It is used to limit the granularity of splitting, preventing excessive scheduling overhead caused by an excessive number of groups.
* **Default**: 128
* **Introduced in**: v3.3

### group_execution_min_scan_rows

* **Description**: Minimum number of rows processed per group for Group Execution.
* **Default**: 5000000
* **Introduced in**: v3.3

### jit_level

* **Description**: The level at which JIT compilation for expressions is enabled. Valid values:
  * `1`: The system adaptively enables JIT compilation for compilable expressions.
  * `-1`: JIT compilation is enabled for all compilable, non-constant expressions.
  * `0`: JIT compilation is disabled. You can disable it manually if any error is returned for this feature.
* **Default**: 1
* **Data type**: Int
* **Introduced in**: -

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

### phased_scheduler_max_concurrency

* **Description**: The concurrency for phased scheduler scheduling leaf node fragments. For example, the default value means that, in a large number of UNION ALL Scan queries, at most two scan fragments are allowed to be scheduled at the same time.
* **Default**: 2
* **Introduced in**: v3.3

### pipeline_dop

* **Description**: The parallelism of a pipeline instance, which is used to adjust the query concurrency. Default value: 0, indicating the system automatically adjusts the parallelism of each pipeline instance. This variable also controls the parallelism of loading jobs on OLAP tables. You can also set this variable to a value greater than 0. Generally, set the value to half the number of physical CPU cores. From v3.0 onwards, StarRocks adaptively adjusts this variable based on query parallelism.

* **Default**: 0
* **Data type**: Int

### pipeline_sink_dop

* **Description**: The parallelism of sink for loading data into Iceberg tables and Hive tables, and unloading data using INSERT INTO FILES(). It is used to adjust the concurrency of these loading jobs. Default value: 0, indicating the system automatically adjusts the parallelism. You can also set this variable to a value greater than 0.
* **Default**: 0
* **Data type**: Int

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

### runtime_filter_on_exchange_node

* **Description**: Whether to place GRF on Exchange Node after GRF is pushed down across the Exchange operator to a lower-level operator. The default value is `false`, which means GRF will not be placed on Exchange Node after it is pushed down across the Exchange operator to a lower-level operator. This prevents repetitive use of GRF and reduces the computation time.

  However, GRF delivery is a "try-best" process. If the lower-level operator fails to receive the GRF but the GRF is not placed on Exchange Node, data cannot be filtered, which compromises filter performance. `true` means GRF will still be placed on Exchange Node even after it is pushed down across the Exchange operator to a lower-level operator.

* **Default**: false

### runtime_join_filter_push_down_limit

* **Description**: The maximum number of rows allowed for the Hash table based on which Bloom filter Local RF is generated. Local RF will not be generated if this value is exceeded. This variable prevents the generation of an excessively long Local RF.
* **Default**: 1024000
* **Data type**: Int

### transmission_compression_type

* **Description**: Controls the compression algorithm used for transmitting query-related data (RPC/exchange payloads). Use `AUTO` to let the system pick a suitable algorithm based on environment (trade CPU for network bandwidth when beneficial). Other valid values are names recognized by `CompressionUtils.findTCompressionByName()` (for example, codec identifiers exposed by the runtime). For load-specific transmission you can use the separate `load_transmission_compression_type` session variable or supply transmission compression in stream-load parameters (HTTP header `HTTP_TRANSMISSION_COMPRESSION_TYPE` / thrift field `transmission_compression_type`).
* **Scope**: Session
* **Default**: `AUTO`
* **Data Type**: String
* **Introduced in**: v3.2.0

