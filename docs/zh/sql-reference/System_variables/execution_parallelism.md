---
displayed_sidebar: docs
sidebar_label: "执行引擎与并行度"
sidebar_position: 3
description: "用于 Pipeline 引擎、并行度、运行时过滤器以及查询缓存的会话变量。"
---

# 系统变量 - 执行引擎与并行度

如需了解如何查看和设置变量，请参见 [系统变量概述](../System_variable.md)。

### blacklist_backup_routing

* **范围**：Session
* **描述**：在 shared-data（存算分离）模式下，若某次扫描在规划中偏好的计算节点不在本查询当前可使用的计算节点范围内（例如节点宕机或出现在主机黑名单上），则规划器需另选备份计算节点。本变量控制在该候选集合中如何挑选备份节点。`RANDOM` 在候选集中均匀随机选取。`CIRCULAR` 在按 id 排好序的节点环上、从主节点起向后寻找第一个可用候选（确定性、与旧版行为类似）。哪些节点可作为备份还受 `skip_black_list` 影响：默认会排除位于主机黑名单上的节点；`skip_black_list` 为 `true` 时，位于黑名单的节点在集群中仍属可用时，也可能被选为备份。

* **默认值**：`CIRCULAR`
* **类型**：String
* **合法取值**：`CIRCULAR`、`RANDOM`
* **引入版本**：-

### computation_fragment_scheduling_policy

* **作用域**: Session
* **描述**: 控制用于为 computation fragments 选择执行实例的调度策略。有效值（不区分大小写）有：
  * `compute_nodes_only` — 仅在 compute nodes 上调度 fragments（默认）。
  * `all_nodes` — 允许在 compute nodes 和传统 backend nodes 上调度。
  该变量由枚举 `SessionVariableConstants.ComputationFragmentSchedulingPolicy` 支持。设置时，会将值上调为大写并与枚举校验；无效值会导致错误（通过 API 设置时抛出 `IllegalArgumentException`，在 SET 语句中使用时抛出 `SemanticException`）。getter 返回相应的枚举值，如果未设置或无法识别则回退到 `COMPUTE_NODES_ONLY`。此设置影响 FE 在规划/部署时如何选择 fragment 放置的目标节点。
* **默认值**: `COMPUTE_NODES_ONLY`
* **数据类型**: String
* **引入版本**: v3.2.7

### enable_adaptive_sink_dop

* 描述：是否开启导入自适应并行度。开启后 INSERT INTO 和 Broker Load 自动设置导入并行度，保持和 `pipeline_dop` 一致。新部署的 2.5 版本默认值为 `true`，从 2.4 版本升级上来为 `false`。
* 默认值：false
* 引入版本：v2.5

### enable_bucket_aware_execution_on_lake

* 描述：是否针对数据湖（如 Iceberg 表）查询启用 Bucket-aware 执行。启用后，系统通过利用分桶信息来优化查询执行，减少数据 Shuffle 并提高性能。此优化对分桶表的 Join 和 Aggregation 特别有效。
* 默认值：true
* 数据类型：Boolean
* 引入版本：v4.0

### enable_cache_udaf

* **描述**: 设置为 `true` 时，启用 Java UDAF 类级初始化的内存缓存（包括类加载、方法内省和批量更新 stub 类生成）。缓存在首次使用时填充，并在同一 BE 进程内的所有 aggregator/analytor 实例之间复用，从而消除原本与 pipeline DOP 成线性比例的重复每实例初始化开销。缓存仅适用于创建时指定 `"isolation" = "shared"` 的 UDAF 和窗口函数；使用 `"isolation" = "private"` 创建的函数无论此设置如何，始终走非缓存路径。默认为 `false`；在确认 shared 隔离模式的 UDAF 可安全跨并发查询共享类级状态后再启用。运行时 Profile 中提供 `UdafCacheHitCount`、`UdafCachePopulateCount` 和 `UdafLoadTime` 计数器用于观测缓存行为。
* **范围**: Session
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.4.0

### enable_global_runtime_filter

* 描述：Global runtime filter 开关。Runtime Filter（简称 RF）在运行时对数据进行过滤，过滤通常发生在 Join 阶段。当多表进行 Join 时，往往伴随着谓词下推等优化手段进行数据过滤，以减少 Join 表的数据扫描以及 shuffle 等阶段产生的 IO，从而提升查询性能。StarRocks 中有两种 RF，分别是 Local RF 和 Global RF。Local RF 应用于 Broadcast Hash Join 场景。Global RF 应用于 Shuffle Join 场景。
* 默认值 `true`，表示打开 global runtime filter 开关。关闭该开关后, 不生成 Global RF, 但是依然会生成 Local RF。

### enable_group_execution

* 描述：Colocate Group Execution 是一种利用物理数据分区的执行模式，其中固定数量的线程依次处理各自的数据范围，以增强局部性和吞吐量。该模式可降低内存使用量。
* 默认值：true
* 引入版本：v3.3

### enable_lake_tablet_internal_parallel

* 描述：是否开启存算分离集群内云原生表的 Tablet 并行 Scan.
* 默认值：true
* 类型：Boolean
* 引入版本：v3.3.0

### enable_local_shuffle_agg

* **描述**: 控制 planner 和 cost model 是否可以生成使用本地 shuffle 的单阶段局部聚合计划（Scan -> LocalShuffle -> OnePhaseAgg），而不是两阶段/全局 shuffle 聚合。启用时（默认），优化器和成本模型会：
  - 允许在单 backend+compute-node 集群中将 Scan 与 Global Agg 之间的 SHUFFLE exchange 替换为本地 shuffle + 单阶段聚合（参见 `PruneShuffleDistributionNodeRule` 和 `EnforceAndCostTask`），
  - 在该单节点场景下让成本模型忽略 SHUFFLE 的网络成本以偏好单阶段计划（`CostModel`）。
  仅在 `enable_pipeline_engine` 启用且集群为单个 backend+compute 节点时考虑替换。规划器在不安全的情况下仍会拒绝本地 shuffle 转换（例如 DISTINCT 聚合、检测到的数据倾斜、缺失/未知的列统计、多输入算子如 joins 或其他语义限制）。某些代码路径（INSERT/UPDATE/DELETE 的 planner 和 MaterializedViewOptimizer）会临时禁用该会话标志，因为非查询的 sink 或某些重写需要按 driver 分配扫描，而本地 shuffle 无法使用这种分配。
* **范围**: Session
* **默认值**: `true`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_multicolumn_global_runtime_filter

* 描述：多列 Global runtime filter 开关。默认值为 false，表示关闭该开关。

  对于 Broadcast 和 Replicated Join 类型之外的其他 Join，当 Join 的等值条件有多个的情况下：
  * 如果该选项关闭: 则只会产生 Local RF。
  * 如果该选项打开, 则会生成 multi-part GRF, 并且该 GRF 需要携带 multi-column 作为 partition-by 表达式.

* 默认值：false

### enable_parallel_merge

* 描述：是否启用排序的 Parallel Merge。启用后，排序的合并阶段将使用多个线程进行合并操作。
* 默认值：true
* 引入版本：v3.3

### enable_per_bucket_optimize

* 描述：是否开启分桶计算。开启后对于一阶段聚合可以按照分桶顺序计算，降低内存使用。
* 默认值：true
* 引入版本：v3.0

### enable_phased_scheduler

* 描述: 是否启用多阶段调度。当启用多阶段调度时，系统将根据 Fragment 之间的依赖关系进行调度。例如，系统将首先调度 Shuffle Join 的 Build Side Fragment ，然后调度 Probe Side Fragment （注意，与分阶段调度不同，多阶段调度仍处于 MPP 执行模式下）。启用多阶段调度可显著降低大量 UNION ALL 查询的内存使用量。
* 默认值: false
* 引入版本: v3.3

### enable_pipeline_engine

* 描述：是否启用 Pipeline 执行引擎。`true`：启用（默认），`false`：不启用。
* 默认值：true

### enable_query_cache

* 描述：是否开启 Query Cache。取值范围：true 和 false。true 表示开启，false 表示关闭（默认值）。开启该功能后，只有当查询满足[Query Cache](../using_starrocks/caching/query_cache.md#应用场景) 所述条件时，才会启用 Query Cache。
* 默认值：false
* 引入版本：v2.5

### enable_query_tablet_affinity

* 描述：用于控制在多次查询同一个 tablet 时是否倾向于选择固定的同一个副本。

  如果待查询的表中存在大量 tablet，开启该特性会对性能有提升，因为会更快的将 tablet 的元信息以及数据缓存在内存中。但是，如果查询存在一些热点 tablet，开启该特性可能会导致性能有所退化，因为该特性倾向于将一个热点 tablet 的查询调度到相同的 BE 上，在高并发的场景下无法充分利用多台 BE 的资源。
* 默认值：`false`，表示使用原来的机制，即每次查询会从多个副本中选择一个。
* 类型：Boolean
* 引入版本：v2.5.6、v3.0.8、v3.1.4、v3.2.0

### enable_runtime_adaptive_dop

* **范围**: Session
* **描述**: 当为某个 Session 启用此功能时，Planner 和 Fragment Builder 会标记支持运行时自适应 DOP 的 Pipeline-capable Fragment，使其在运行时使用自适应并行度。此选项仅在 `enable_pipeline_engine` 为 true 时生效。启用后，Fragment 在计划构建期间会使用自适应并行度：Join 的 Probe 可能会等待所有 Build 阶段完成（这与 `group_execution` 行为冲突），启用运行时自适应 DOP 会禁用 Pipeline 级别的 Multi-Partitioned Runtime Filter。该参数会记录在 Query Profile 中，并且可以在每个会话中切换。
* **默认值**: `false`
* **数据类型**: boolean
* **引入版本**: v3.2.0

### enable_short_circuit

* 描述：是否启用短路径查询。默认值：`false`。如果将其设置为 `true`，当[查询满足条件](../table_design/hybrid_table.md#查询数据)（用于评估是否为点查）：WHERE 子句的条件列必须包含所有主键列，并且运算符为 `=` 或者 `IN`，则该查询才会走短路径。
* 默认值：false
* 引入版本：v3.2.3

### enable_tablet_internal_parallel

* 描述：是否开启自适应 Tablet 并行扫描，使用多个线程并行分段扫描一个 Tablet，可以减少 Tablet 数量对查询能力的限制。
* 默认值：true
* 引入版本：v2.3

### enable_tablet_pre_split

* 描述：基于采样的 Tablet 预分裂（Sample-Based Tablet Pre-Split）的会话级开关。默认 `true`，因此主开关由 FE 配置 `enable_tablet_pre_split_for_*` 控制。对于不希望被预分裂干扰的会话，可将其设为 `false`。预分裂需同时满足对应 FE 配置和该会话变量都为 `true`。
* 默认值：true
* 引入版本：v4.1.0

### enable_topn_filter_back_pressure

* 描述: Scan 是否自动启用 TopN Runtime Filter（RF）背压。当一个 TopN/流式构建的 RF（来自 `ORDER BY ... LIMIT` 查询，或聚合 in-filter）作用于某个 Scan 时,背压会在该 RF 真正到达之前,将 Scan 的预读 IO 任务数钳制到较小的值,避免大量并发读取超出(非并发感知的)行预算、在 RF 生效前就淹没下游聚合。该机制对 shared-nothing（OLAP）和 shared-data（湖仓/connector）Scan 均生效。设为 `false` 时,Scan 仅在 FE 的 `topn_filter_back_pressure_mode` 开启时才启用背压。
* 默认值: true
* 引入版本: v4.1

以下变量用于调节背压行为,仅在 `enable_topn_filter_back_pressure` 为 `true` 时生效:

| 变量 | 默认值 | 描述 |
| --- | --- | --- |
| `topn_filter_back_pressure_io_tasks` | 1 | TopN RF 尚未到达期间,Scan 预读的 IO 任务数上限。设为 `<= 0` 可关闭钳制（Scan 使用完整的 `io_tasks_per_scan_operator`）。 |
| `topn_back_pressure_num_rows` | 1024 | 第一个节流轮次中,背压开始节流前 Scan 可读取的行数。每个后续轮次翻倍。 |
| `topn_back_pressure_throttle_time_ms` | 8 | 第一个节流窗口的时长（毫秒）。每个后续轮次翻倍。 |
| `topn_back_pressure_throttle_time_upper_bound_ms` | 100 | 背压节流某个 Scan 的总时长上限（毫秒）；达到上限后即使 RF 仍未到达,也会放行 Scan 以完整预读运行。 |
| `topn_back_pressure_max_rounds` | 8 | 背压放弃前的最大节流轮次数。 |

### enable_topn_runtime_filter

* 描述: 是否启用 TopN Runtime Filter。如果启用此功能，对于 ORDER BY LIMIT 查询，将动态构建一个 Runtime Filter 并将其下推到 Scan 阶段进行过滤。
* 默认值: true
* 引入版本: v3.3

### enable_wait_dependent_event

* 描述: 在同一 Fragment 内，Pipeline 是否等待依赖的 Operator 完成执行后再继续执行。例如，在 Left Join 查询中，当启用此功能时，Probe Side Operator 会在 Build Side Operator 完成执行后再开始执行。启用此功能可减少内存使用，但可能增加查询延迟。然而，对于在CTE中重用的查询，启用此功能可能增加内存使用。
* 默认值: false
* 引入版本: v3.3

### group_execution_max_groups

* 描述：Group Execution 允许的最大组数。用于限制拆分粒度，防止因组数过多导致过度的调度开销。
* 默认值：128
* 引入版本：v3.3

### group_execution_min_scan_rows

* 描述：Group Execution 每组处理的最小行数。
* 默认值：5000000
* 引入版本：v3.3

### jit_level

* 描述：表达式 JIT 编译的启用级别。有效值：
  * `1`：系统为可编译表达式自适应启用 JIT 编译。
  * `-1`：对所有可编译的非常量表达式启用 JIT 编译。
  * `0`：禁用 JIT 编译。如果该功能返回任何错误，您可以手动禁用。
* 默认值：1
* 数据类型：Int
* 引入版本：-

### max_pipeline_dop

* **范围**: Session
* **描述**: 每会话的 pipeline 引擎并行度（DOP）上限。行为：
  * 仅在 `enable_pipeline_engine` 启用且未显式设置 `pipeline_dop`（大于 0）时生效。如果 `pipeline_dop` 大于 0 则忽略此变量并直接使用 `pipeline_dop`。
  * 当 `pipeline_dop` 小于或等于 0（自适应/默认模式）时，执行的实际 DOP 计算为 min(`max_pipeline_dop`, BackendResourceStat 返回的后端默认 DOP)。对于 pipeline sinks，同样的逻辑使用 sink 的默认 DOP。
  * 如果 `max_pipeline_dop` 小于或等于 0，则不施加额外上限，使用后端默认 DOP。
  * 目的：通过对自动计算的并行度设置上限，避免在核数非常大的机器上调度带来的负面开销。
* **默认值**: `64`
* **数据类型**: int
* **引入版本**: v3.2.0

### parallel_exchange_instance_num

用于设置执行计划中，一个上层节点接收下层节点数据所使用的 exchange node 数量。默认为 -1，即表示 exchange node 数量等于下层节点执行实例的个数（默认行为）。当设置大于 0，并且小于下层节点执行实例的个数，则 exchange node 数量等于设置值。

在一个分布式的查询执行计划中，上层节点通常有一个或多个 exchange node 用于接收来自下层节点在不同 BE 上的执行实例的数据。通常 exchange node 数量等于下层节点执行实例数量。

在一些聚合查询场景下，如果底层需要扫描的数据量较大，但聚合之后的数据量很小，则可以尝试修改此变量为一个较小的值，可以降低此类查询的资源开销。如在 DUPLICATE KEY 明细表上进行聚合查询的场景。

### parallel_fragment_exec_instance_num

针对扫描节点，设置其在每个 BE 节点上执行实例的个数。默认为 1。

一个查询计划通常会产生一组 scan range，即需要扫描的数据范围。这些数据分布在多个 BE 节点上。一个 BE 节点会有一个或多个 scan range。默认情况下，每个 BE 节点的一组 scan range 只由一个执行实例处理。当机器资源比较充裕时，可以将增加该变量，让更多的执行实例同时处理一组 scan range，从而提升查询效率。

而 scan 实例的数量决定了上层其他执行节点，如聚合节点，join 节点的数量。因此相当于增加了整个查询计划执行的并发度。修改该参数会对大查询效率提升有帮助，但较大数值会消耗更多的机器资源，如CPU、内存、磁盘I/O。

### parallel_merge_late_materialization_mode

* 描述：Parallel Merge 的延迟物化模式。有效值：
  * `AUTO`
  * `ALWAYS`
  * `NEVER`
* 默认值：`AUTO`
* 引入版本：v3.3

### phased_scheduler_max_concurrency

* 描述: 多阶段调度器调度 Leaf Node Fragment 的并发度。例如，默认值表示在大量 UNION ALL Scan 查询中，最多允许同时调度两个 Scan Fragment。
* 默认值: 2
* 引入版本: v3.3

### pipeline_dop

* 描述：一个 Pipeline 实例的并行数量。可通过设置实例的并行数量调整查询并发度。默认值为 0，即系统自适应调整每个 pipeline 的并行度。该变量还控制着 OLAP 表上导入作业的并行度。您也可以设置为大于 0 的数值，通常为 BE 节点 CPU 物理核数的一半。从 3.0 版本开始，支持根据查询并发度自适应调节 `pipeline_dop`。
* 默认值：0
* 类型：Int

### pipeline_sink_dop

* 描述：用于向 Iceberg 表、Hive 表导入数据以及通过 INSERT INTO FILES() 导出数据的 Sink 并行数量。该参数用于调整这些导入作业的并发性。默认值为 0，即系统自适应调整并行度。您也可以将此变量设置为大于 0 的值。
* 默认值：0
* 类型：Int

### query_cache_agg_cardinality_limit

* 描述：GROUP BY 聚合的高基数上限。GROUP BY 聚合的输出预估超过该行数, 则不启用 cache。
* 默认值：5000000
* 类型：Long
* 引入版本：v2.5

### query_cache_entry_max_bytes

* 描述：Cache 项的字节数上限，触发 Passthrough 模式的阈值。当一个 Tablet 上产生的计算结果的字节数或者行数超过 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 指定的阈值时，则查询采用 Passthrough 模式执行。当 `query_cache_entry_max_bytes` 或 `query_cache_entry_max_rows` 取值为 0 时, 即便 Tablet 产生结果为空，也采用 Passthrough 模式。
* 取值范围：0 ~ 9223372036854775807
* 默认值：4194304
* 单位：字节
* 类型：Long
* 引入版本：v2.5

### query_cache_entry_max_rows

* 描述：Cache 项的行数上限，见 `query_cache_entry_max_bytes` 描述。
* 默认值：409600
* 类型：Long
* 引入版本：v2.5

### query_cache_size (global)

用于兼容 MySQL 客户端。无实际作用。

### query_cache_type

 用于兼容 JDBC 连接池 C3P0。无实际作用。

### runtime_filter_on_exchange_node

* 描述：GRF 成功下推跨过 Exchange 算子后，是否在 Exchange Node 上放置 GRF。当一个 GRF 下推跨越 Exchange 算子，最终安装在 Exchange 算子更下层的算子上时，Exchange 算子本身是不放置 GRF 的，这样可以避免重复性使用 GRF 过滤数据而增加计算时间。但是 GRF 的投递本身是 try-best 模式，如果 query 执行时，Exchange 下层的算子接收 GRF 失败，而 Exchange 本身又没有安装 GRF，数据无法被过滤，会造成性能衰退.

  该选项打开（设置为 `true`）时，GRF 即使下推跨过了 Exchange 算子, 依然会在 Exchange 算子上放置 GRF 并生效。
* 默认值：false

### runtime_join_filter_push_down_limit

* 描述：生成 Bloomfilter 类型的 Local RF 的 Hash Table 的行数阈值。超过该阈值, 则不产生 Local RF。该变量避免产生过大 Local RF。取值为整数，表示行数。
* 默认值：1024000
* 类型：Long

