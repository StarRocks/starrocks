---
displayed_sidebar: docs
description: "FE configuration parameters for authentication, query execution, and data loading."
sidebar_label: "用户管理、查询引擎和导入导出"
---

# FE 配置 - 认证、查询和加载

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端上运行 ADMIN SHOW FRONTEND CONFIG 命令来查看参数配置。如果您想查询某个特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅[`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用以下方式配置或修改 FE 动态参数的设置[`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md)。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

***

本文介绍以下类型的 FE 配置：

- [用户、角色和权限](#user-role-and-privilege)
- [查询](#query-engine)
- [导入和导出](#loading-and-unloading)

## 用户、角色和权限

### `enable_task_info_mask_credential`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可动态修改：是
- 描述：当值为 true 时，StarRocks 在通过 `information_schema.tasks` 和 `information_schema.task_runs` 返回任务 SQL 定义之前，会通过 SqlCredentialRedactor.redact 对 DEFINITION 列应用脱敏处理，以隐藏凭据信息。在 `information_schema.task_runs` 中，无论定义来自任务运行状态，还是在为空时来自任务定义查找，都会应用相同的脱敏处理。当值为 false 时，将返回原始任务定义（可能暴露凭据）。脱敏操作涉及 CPU 和字符串处理，当任务数量或 `task_runs` 较大时可能耗时较长；仅在需要获取未脱敏定义且接受安全风险的情况下才禁用此功能。
- 引入版本：v3.5.6

### `privilege_max_role_depth`

- 默认值：16
- 类型：Int
- 单位：
- 是否可动态修改：是
- 描述：角色的最大角色深度（继承层级）。
- 引入版本：v3.0.0

### `privilege_max_total_roles_per_user`

- 默认值：64
- 类型：Int
- 单位：
- 是否可动态修改：是
- 描述：用户可拥有的最大角色数量。
- 引入版本：v3.0.0

## 查询引擎

### `brpc_send_plan_fragment_timeout_ms`

- 默认值：60000
- 类型：Int
- 单位：毫秒
- 是否可动态修改：是
- 描述：在发送计划片段之前，应用于 BRPC TalkTimeoutController 的超时时间（毫秒）。`BackendServiceClient.sendPlanFragmentAsync` 在调用后端 `execPlanFragmentAsync` 之前设置此值。它控制 BRPC 在从连接池借用空闲连接以及执行发送操作时的等待时长；若超时，RPC 将失败并可能触发该方法的重试逻辑。将此值设置得较小可在竞争情况下快速失败，设置得较大则可容忍连接池短暂耗尽或网络缓慢的情况。请谨慎设置：过大的值可能延迟故障检测并阻塞请求线程。
- 引入版本：v3.3.11、v3.4.1、v3.5.0

### `connector_row_size_estimate_bytes`

- 默认值: 256
- 类型: Long
- 单位: Bytes
- 是否可变: Yes
- 描述: 优化器在存储格式未知或列 Schema 不可用时，用于估算外部文件表（FILES() 和 ENGINE=file 表）行数的平均行大小（字节）。行数估算公式为 `总文件字节数 / connector_row_size_estimate_bytes`。值越小，估算行数越大，可能影响 Join 顺序决策。
- 引入版本: v3.4

### `connector_table_query_trigger_analyze_large_table_interval`

- 默认值：12 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：大表的查询触发 ANALYZE 任务的间隔。
- 引入版本：v3.4.0

### `connector_table_query_trigger_analyze_max_pending_task_num`

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

### `connector_table_query_trigger_analyze_max_running_task_num`

- 默认值：2
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本：v3.4.0

### `connector_table_query_trigger_analyze_small_table_interval`

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：小表的查询触发 ANALYZE 任务的间隔。
- 引入版本：v3.4.0

### `connector_table_query_trigger_analyze_small_table_rows`

- 默认值：10000000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：用于判断表是否为查询触发 ANALYZE 任务中小表的阈值。
- 引入版本：v3.4.0

### `connector_table_query_trigger_task_schedule_interval`

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Scheduler 线程调度查询触发后台任务的间隔。该参数用于替代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。其中，后台任务在 v3.4 中指 `ANALYZE` 任务，在 v3.4 之后的版本中指低基数列字典的采集任务。
- 引入版本：v3.4.2

### `create_table_max_serial_replicas`

- 默认值：128
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：串行创建副本的最大数量。如果实际副本数超过此值，副本将并发创建。如果表创建耗时较长，请尝试减小此值。
- 引入版本：-

### `default_mv_partition_refresh_number`

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：当物化视图刷新涉及多个分区时，此参数控制默认情况下单次批量刷新的分区数量。
从 3.3.0 版本开始，系统默认每次刷新一个分区，以避免潜在的内存溢出（OOM）问题。在早期版本中，默认一次刷新所有分区，可能导致内存耗尽和任务失败。但请注意，当物化视图刷新涉及大量分区时，每次仅刷新一个分区可能导致调度开销过大、整体刷新时间过长以及刷新记录数量过多。在这种情况下，建议适当调整此参数以提高刷新效率并降低调度成本。
- 引入版本：v3.3.0

### `default_mv_refresh_immediate`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否在创建后立即刷新异步物化视图。当此项设置为 `true` 时，新创建的物化视图将立即刷新。
- 引入版本：v3.2.3

### `dynamic_partition_check_interval_seconds`

- 默认值：600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：检查新数据的时间间隔。如果检测到新数据，StarRocks 将自动为该数据创建分区。
- 引入版本：-

### `dynamic_partition_enable`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用动态分区功能。启用此功能后，StarRocks 将为新数据动态创建分区，并自动删除过期分区，以确保数据的新鲜度。
- 引入版本：-

### `enable_active_materialized_view_schema_strict_check`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：激活非活跃物化视图时，是否严格检查数据类型的长度一致性。当此项设置为 `false` 时，若基表中数据类型的长度发生变化，不影响物化视图的激活。
- 引入版本：v3.3.4

### `mv_fast_schema_change_mode`

- 默认值：strict
- 类型：String
- 单位：-
- 是否可变：是
- 描述：控制物化视图（MV）快速 Schema 演进（FSE）的行为。有效值为：`strict`（默认）- 仅在 `isSupportFastSchemaEvolutionInDanger` 为 true 时允许 FSE，并从版本映射中清除受影响的分区条目；`force` - 即使 `isSupportFastSchemaEvolutionInDanger` 为 false 也允许 FSE，并清除受影响的分区条目以在刷新时触发重新计算；`force_no_clear` - 即使 `isSupportFastSchemaEvolutionInDanger` 为 false 也允许 FSE，但不清除分区条目。
- 引入版本：v4.1.0

### `enable_auto_collect_array_ndv`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 ARRAY 类型的 NDV 信息启用自动收集。
- 引入版本：v4.0

### `enable_backup_materialized_view`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在备份或恢复特定数据库时，是否启用异步物化视图的 BACKUP 和 RESTORE。如果此项设置为 `false`，StarRocks 将跳过备份异步物化视图。
- 引入版本：v3.2.0

### `enable_collect_full_statistic`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用自动全量统计信息收集。该功能默认启用。
- 引入版本：-

### `enable_colocate_mv_index`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：在创建同步物化视图时，是否支持将同步物化视图索引与基表进行 Colocate。如果此项设置为 `true`，tablet sink 将加速同步物化视图的写入性能。
- 引入版本：v3.2.0

### `enable_decimal_v3`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否支持 DECIMAL V3 数据类型。
- 引入版本：-

### `enable_experimental_mv`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用异步物化视图功能。TRUE 表示启用该功能。从 v2.5.2 起，该功能默认启用。v2.5.2 之前的版本，该功能默认禁用。
- 引入版本：v2.4

### `enable_local_replica_selection`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为查询选择本地副本。本地副本可降低网络传输开销。如果此参数设置为 TRUE，CBO 将优先选择与当前 FE IP 地址相同的 BE 上的 tablet 副本。如果此参数设置为 `FALSE`，则本地副本和非本地副本均可被选择。
- 引入版本：-

### `enable_manual_collect_array_ndv`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 ARRAY 类型的 NDV 信息启用手动采集。
- 引入版本：v4.0

### `enable_materialized_view`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用物化视图的创建。
- 引入版本：-

### `enable_materialized_view_external_table_precise_refresh`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：将此项设置为 `true`，可在基表为外部（非云原生）表时启用物化视图刷新的内部优化。启用后，物化视图刷新处理器会计算候选分区，仅刷新受影响的基表分区而非所有分区，从而降低 I/O 和刷新开销。将其设置为 `false` 可强制对外部表执行全分区刷新。
- 引入版本：v3.2.9

### `enable_materialized_view_metrics_collect`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否默认为异步物化视图采集监控指标。
- 引入版本：v3.1.11、v3.2.5

### `enable_materialized_view_spill`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为物化视图刷新任务启用中间结果落盘。
- 引入版本：v3.1.1

### `enable_materialized_view_text_based_rewrite`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否默认启用基于文本的查询改写。若将此项设置为 `true`，系统将在创建异步物化视图时构建抽象语法树。
- 引入版本：v3.2.5

### `enable_mv_automatic_active_check`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用系统自动检查并重新激活因基表（视图）发生 Schema Change 或被删除并重新创建而被设置为非活跃状态的异步物化视图。请注意，此功能不会重新激活由用户手动设置为非活跃状态的物化视图。
- 引入版本：v3.1.6

### `enable_mv_automatic_repairing_for_broken_base_tables`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：当此项设置为 `true` 时，StarRocks 将尝试在基础外部表被删除并重新创建或其表标识符发生变化时，自动修复物化视图基表元数据。修复流程可以更新物化视图的基表信息，收集外部表分区的分区级修复信息，并在遵循 `autoRefreshPartitionsLimit` 的前提下，驱动异步自动刷新物化视图的分区刷新决策。目前自动修复支持 Hive 外部表；不支持的表类型将导致物化视图被设置为非活跃状态并抛出修复异常。分区信息收集为非阻塞操作，失败信息将被记录到日志中。
- 引入版本：v3.3.19、v3.4.8、v3.5.6

### `enable_predicate_columns_collection`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否启用谓词列收集。如果禁用，在查询优化期间将不会记录谓词列。
- 引入版本：-

### `enable_query_queue_v2`

- 默认值：true
- 类型：boolean
- 单位：-
- 是否可变：否
- 描述：当为 true 时，将 FE 基于槽的查询调度器切换为 Query Queue V2。该标志由槽管理器和追踪器（例如 `BaseSlotManager.isEnableQueryQueueV2` 和 `SlotTracker#createSlotSelectionStrategy`）读取，以选择 `SlotSelectionStrategyV2` 而非旧版策略。`query_queue_v2_xxx` 配置选项和 `QueryQueueOptions` 仅在启用此标志时生效。从 v4.1 起，默认值从 `false` 更改为 `true`。
- 引入版本：v3.3.4、v3.4.0、v3.5.0

### `enable_sql_blacklist`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 SQL 查询启用黑名单检查。启用此功能后，黑名单中的查询将无法执行。
- 引入版本：-

### `enable_statistic_collect`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为 CBO 收集统计信息。此功能默认启用。
- 引入版本：-

### `enable_statistic_collect_on_first_load`

- 默认值：true

- 类型：Boolean

- 单位：-

- 是否可变：是

- 描述：控制由数据加载操作触发的自动统计信息收集与维护。包括：

  - 首次将数据加载到分区时的统计信息收集（分区版本等于 2）。
  - 将数据加载到多分区表的空分区时的统计信息收集。
  - INSERT OVERWRITE 操作的统计信息复制和更新。

  **统计信息收集类型的决策策略：**

  - 对于 INSERT OVERWRITE：`deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load`（默认值：0.1），则不执行统计信息收集，仅复制现有统计信息。
    - 否则，如果 `targetRows > statistic_sample_collect_rows`（默认值：200000），则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。

  - 对于首次加载：`deltaRatio = loadRows / (totalRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load`（默认值：0.1），则不执行统计信息收集。
    - 否则，如果 `loadRows > statistic_sample_collect_rows`（默认值：200000），则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。

  **同步行为：**

  - 对于 DML 语句（INSERT INTO/INSERT OVERWRITE）：带表锁的同步模式。加载操作等待统计信息收集完成（最长等待 `semi_sync_collect_statistic_await_seconds`）。
  - 对于 Stream Load 和 Broker Load：无锁的异步模式。统计信息收集在后台运行，不阻塞加载操作。

  :::note
  禁用此配置将阻止所有由加载触发的统计信息操作，包括 INSERT OVERWRITE 的统计信息维护，这可能导致表缺少统计信息。如果频繁创建新表并频繁加载数据，启用此功能将增加内存和 CPU 开销。
  :::

- 引入版本：v3.1

### `enable_statistic_collect_on_update`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：控制 UPDATE 语句是否可以触发自动统计信息收集。启用后，修改表数据的 UPDATE 操作可能会通过由 `enable_statistic_collect_on_first_load` 控制的基于摄取的统计信息框架来调度统计信息收集。禁用此配置将跳过 UPDATE 语句的统计信息收集，同时保持加载触发的统计信息收集行为不变。
- 引入版本：v3.5.11、v4.0.4

### `enable_sync_statistics_load`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：控制查询在执行前是否等待所有统计信息加载完成。启用后，如果查询引用的表/列的统计信息尚未完全加载，查询将等待统计信息加载完成后再继续执行。这确保查询优化器能够访问最新的统计信息以生成更优的执行计划，但如果统计信息加载耗时较长，可能会增加查询延迟。禁用后，查询将不等待统计信息加载而直接执行，若统计信息缺失或过期，可能导致次优执行计划。
- 引入版本：-

### `sync_statistics_load_timeout_ms`

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：同步等待统计信息的超时时间（当 `enable_sync_statistics_load` 启用时）。若在此时间内统计信息不可用，查询将在没有统计信息的情况下继续执行，这可能导致次优执行计划。请根据集群性能和工作负载特性将此值设置为合理的时间。
- 引入版本：-

### `enable_udf`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：否
- 描述：是否启用 UDF。
- 引入版本：-

### `expr_children_limit`

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：表达式中允许的最大子表达式数量。
- 引入版本：-

### `histogram_buckets_size`

- 默认值：64
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：直方图的默认桶数。
- 引入版本：-

### `histogram_max_sample_row_count`

- 默认值：10000000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：为直方图收集的最大行数。
- 引入版本：-

### `histogram_mcv_size`

- 默认值：100
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：直方图的最高频值（MCV）数量。
- 引入版本：-

### `histogram_sample_ratio`

- 默认值：0.1
- 类型：Double
- 单位：-
- 是否可变：是
- 描述：直方图的采样比例。
- 引入版本：-

### `http_slow_request_threshold_ms`

- 默认值：5000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：如果 HTTP 请求的响应时间超过该参数指定的值，则会生成日志以追踪该请求。
- 引入版本：v2.5.15, v3.1.5

### `lock_checker_enable_deadlock_check`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：启用后，LockChecker 线程将使用 ThreadMXBean.findDeadlockedThreads() 执行 JVM 级别的死锁检测，并记录相关线程的堆栈跟踪信息。该检查在 LockChecker 守护线程中运行（其频率由 `lock_checker_interval_second` 控制），并将详细的堆栈信息写入日志，可能会消耗较多 CPU 和 I/O 资源。请仅在排查实时或可复现的死锁问题时启用此选项；在正常运行中保持启用可能会增加系统开销和日志量。
- 引入版本：v3.2.0

### `low_cardinality_threshold`

- 默认值：255
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：低基数字典的阈值。
- 引入版本：v3.5.0

### `materialized_view_min_refresh_interval`

- 默认值：60
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：ASYNC 物化视图调度允许的最小刷新间隔（秒）。当物化视图以基于时间的间隔创建时，该间隔会被转换为秒，且不得小于此值；否则 CREATE/ALTER 操作将因 DDL 错误而失败。若此值大于 0，则强制执行该检查；将其设置为 0 或负值可禁用该限制，从而防止因刷新过于频繁导致 TaskManager 调度过载以及 FE 内存/CPU 使用率过高。此配置项不适用于 `EVENT_TRIGGERED` 刷新。
- 引入版本：v3.3.0、v3.4.0、v3.5.0

### `materialized_view_refresh_ascending`

- 默认值：false
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：当此配置项设置为 `true` 时，物化视图分区刷新将按分区键升序（从最旧到最新）迭代分区。当设置为 `false`（默认值）时，系统按降序（从最新到最旧）迭代。StarRocks 在列表分区和范围分区物化视图的刷新逻辑中均使用此配置项，以在分区刷新限制适用时选择要处理的分区，并为后续 TaskRun 执行计算下一个起始/结束分区边界。更改此配置项会影响哪些分区优先刷新以及如何推导下一个分区范围；对于范围分区物化视图，调度器会验证新的起始/结束值，若更改会导致重复边界（死循环），则会报错，因此请谨慎设置此配置项。
- 引入版本：v3.3.1、v3.4.0、v3.5.0

### `max_allowed_in_element_num_of_delete`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: DELETE 语句中 IN 谓词允许的最大元素数量。
- 引入版本: -

### `enable_non_primary_key_delete_warning`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当设置为 `true` 时，对非主键 OLAP 表（明细表、聚合表、更新表）执行成功的 `DELETE` 后，StarRocks 会在 MySQL OK 包的 `info` 字段中返回一条提示，提醒用户 `DELETE` 会写入删除谓词、在 base compaction 完成前每次查询都需要 merge-on-read，并建议批量按分区删除时改用 `ALTER TABLE ... TRUNCATE PARTITION`。设置为 `false` 可关闭此提示。该提示不会改变 DELETE 的语义或执行流程，只是额外向客户端返回一段 info 字符串。详见 [DELETE](../../../sql-reference/sql-statements/table_bucket_part_index/DELETE.md)。
- 引入版本: -

### `max_create_table_timeout_second`

- 默认值：600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：创建表的最大超时时长。
- 引入版本：-

### `max_distribution_pruner_recursion_depth`

- 默认值：100
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：分区裁剪器允许的最大递归深度。增加递归深度可以裁剪更多元素，但也会增加 CPU 消耗。
- 引入版本：-

### `max_partitions_in_one_batch`

- 默认值：4096
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：批量创建分区时可创建的最大分区数。
- 引入版本：-

### `max_planner_scalar_rewrite_num`

- 默认值：100000
- 类型：Long
- 单位：-
- 是否可变：是
- 描述：优化器可以重写标量运算符的最大次数。
- 引入版本：-

### `max_query_queue_history_slots_number`

- 默认值：0
- 类型：Int
- 单位：Slots
- 是否可变：是
- 描述：控制每个查询队列中保留多少个最近释放（历史）的已分配槽位，用于监控和可观测性。当 `max_query_queue_history_slots_number` 设置为大于 `> 0` 的值时，BaseSlotTracker 会在内存队列中保留最多该数量的最近释放的 LogicalSlot 条目，超出限制时淘汰最旧的条目。启用此功能后，getSlots() 将包含这些历史条目（最新的在前），允许 BaseSlotTracker 尝试向 ConnectContext 注册槽位以获取更丰富的 ExtraMessage 数据，并允许 LogicalSlot.ConnectContextListener 将查询完成元数据附加到历史槽位。当 `max_query_queue_history_slots_number` `<= 0` 时，历史机制被禁用（不使用额外内存）。请使用合理的值以平衡可观测性和内存开销。
- 引入版本：v3.5.0

### `max_query_retry_time`

- 默认值：2
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 上查询重试的最大次数。
- 引入版本：-

### `max_running_rollup_job_num_per_table`

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：一张表可以并行运行的最大 rollup 作业数。
- 引入版本：-

### `max_scalar_operator_flat_children`

- 默认值：10000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：ScalarOperator 的最大扁平子节点数。您可以设置此限制以防止优化器使用过多内存。
- 引入版本：-

### `max_scalar_operator_optimize_depth`

- 默认值：256
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：ScalarOperator 优化可应用的最大深度。
- 引入版本：-

### `mv_active_checker_interval_seconds`

- 默认值：60
- 类型：长
- 单位：秒
- 是否可变：是
- 描述：当后台 `active_checker` 线程启用时，系统将定期检测并自动重新激活因基础表（或视图）的 schema 变更或重建而变为非活跃状态的物化视图。此参数控制检查线程的调度间隔，单位为秒。默认值由系统定义。
- 引入版本：v3.1.6

### `mv_rewrite_consider_data_layout_mode`

- 默认值：`enable`
- 类型：字符串
- 单位：-
- 是否可变：是
- 描述：控制物化视图重写在选择最佳物化视图时是否应考虑基表数据布局。有效值：
  - `disable`：在候选物化视图之间进行选择时，从不使用数据布局标准。
  - `enable`：仅当查询被识别为布局敏感时，才使用数据布局标准。
  - `force`：在选择最佳物化视图时，始终应用数据布局标准。
更改此项会影响 `BestMvSelector` 的行为，并可能根据物理布局是否对计划正确性或性能有影响，来改善或扩大重写的适用范围。
- 引入版本：-

### `publish_version_interval_ms`

- 默认值：10
- 类型：Int
- 单位：毫秒
- 是否可变：否
- 描述：发布验证任务的发布时间间隔。
- 引入版本：-

### `query_queue_slots_estimator_strategy`

- 默认值：MAX
- 类型：字符串
- 单位：-
- 是否可变：是
- 描述：当 `enable_query_queue_v2` 为 true 时，选择用于基于队列的查询的槽位估算策略。有效值：MBE（基于内存）、PBE（基于并行度）、MAX（取 MBE 和 PBE 的最大值）和 MIN（取 MBE 和 PBE 的最小值）。MBE 根据预测的内存或计划成本除以每槽位内存目标来估算槽位数，并受 `totalSlots` 的上限约束。PBE 从片段并行度（扫描范围数量或基数/每槽位行数）以及基于 CPU 成本的计算（使用每槽位 CPU 成本）中推导槽位数，然后将结果限定在 [numSlots/2, numSlots] 范围内。MAX 和 MIN 分别通过取 MBE 和 PBE 的最大值或最小值来组合两者。如果配置的值无效，则使用默认值（`MAX`）。
- 引入版本：v3.5.0

### `query_queue_v2_concurrency_level`

- 默认值：4
- 类型：Int
- 单位：-
- 是否可变：是
- Description: Controls how many logical concurrency "layers" are used when computing the system's total query slots. In shared-nothing mode the total slots = `query_queue_v2_concurrency_level` * number_of_BEs * cores_per_BE (derived from BackendResourceStat). In multi-warehouse mode the effective concurrency is scaled down to max(1, `query_queue_v2_concurrency_level` / 4). If the configured value is non-positive it is treated as `4`. Changing this value increases or decreases totalSlots (and therefore concurrent query capacity) and affects per-slot resources: memBytesPerSlot is derived by dividing per-worker memory by (cores_per_worker * concurrency), and CPU accounting uses `query_queue_v2_cpu_costs_per_slot`. Set it proportional to cluster size; very large values may reduce per-slot memory and cause resource fragmentation.
- 引入版本：v3.3.4、v3.4.0、v3.5.0

### `query_queue_v2_cpu_costs_per_slot`

- 默认值：1000000000
- 类型：Long
- 单位：规划器 CPU 成本单位
- 是否可变：是
- 描述：每槽 CPU 成本阈值，用于根据查询规划器的 CPU 成本估算查询所需的槽数。调度器将槽数计算为 integer(`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`)，然后将结果限制在 [1, totalSlots] 范围内（totalSlots 由查询队列 V2 的 `V2` 参数派生）。V2 代码将非正数设置规范化为 1（Math.max(1, value)），因此非正数值实际上等同于 `1`。增大此值会减少每个查询分配的槽数（有利于槽数较少但较大的查询）；减小此值则会增加每个查询的槽数。请与 `query_queue_v2_num_rows_per_slot` 及并发设置一起调整，以控制并行度与资源粒度之间的平衡。
- 引入版本：v3.3.4、v3.4.0、v3.5.0

### `query_queue_v2_num_rows_per_slot`

- 默认值：4096
- 类型：Int
- 单位：行
- 是否可变：是
- 描述：在估算每个查询的槽位数量时，分配给单个调度槽位的源行记录的目标数量。StarRocks 计算 `estimated_slots` = （Source Node 的基数）/ `query_queue_v2_num_rows_per_slot`，然后将结果限制在 [1, totalSlots] 范围内，并在计算值为非正数时强制最小值为 1。totalSlots 由可用资源（大致为 DOP * `query_queue_v2_concurrency_level` * number_of_workers/BE）推导得出，因此取决于集群/核心数量。增大此值可减少槽位数量（每个槽位处理更多行），降低调度开销；减小此值可提高并行度（更多、更小的槽位），直至资源上限。
- 引入版本：v3.3.4、v3.4.0、v3.5.0

### `query_queue_v2_schedule_strategy`

- 默认值：SWRR
- 类型：String
- 单位：-
- 是否可变：是
- 描述：选择 Query Queue V2 用于对待处理查询排序的调度策略。支持的值（不区分大小写）为 `SWRR`（平滑加权轮询）——默认值，适用于需要公平加权共享的混合工作负载——以及 `SJF`（短作业优先 + 老化）——在使用老化机制避免饥饿的同时优先处理短作业。该值通过不区分大小写的枚举查找进行解析；无法识别的值将被记录为错误，并使用默认策略。此配置仅在启用 Query Queue V2 时生效，并与 V2 大小设置（如 `query_queue_v2_concurrency_level`）相互作用。
- 引入版本：v3.3.12、v3.4.2、v3.5.0

### `semi_sync_collect_statistic_await_seconds`

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：DML 操作（INSERT INTO 和 INSERT OVERWRITE 语句）期间半同步统计信息收集的最大等待时间。Stream Load 和 Broker Load 使用异步模式，不受此配置影响。如果统计信息收集时间超过此值，加载操作将继续执行而不等待收集完成。此配置与 `enable_statistic_collect_on_first_load` 配合使用。
- 引入版本：v3.1

### `slow_query_analyze_threshold`

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：触发 Query Feedback 分析的查询执行时间阈值。
- 引入版本：v3.4.0

### `statistic_analyze_status_keep_second`

- 默认值：3 * 24 * 3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：保留采集任务历史记录的时长。默认值为 3 天。
- 引入版本：-

### `statistic_auto_analyze_end_time`

- 默认值：23:59:59
- 类型：String
- 单位：-
- 是否可变：是
- 描述：自动采集的结束时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本：-

### `statistic_auto_analyze_start_time`

- 默认值：00:00:00
- 类型：String
- 单位：-
- 是否可变：是
- 描述：自动采集的开始时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本：-

### `statistic_auto_collect_ratio`

- 默认值: 0.8
- 类型: Double
- 单位: -
- 是否可变: 是
- 描述: 用于判断自动采集统计信息是否健康的阈值。如果统计信息健康度低于此阈值，则触发自动采集。
- 引入版本: -

### `statistic_auto_collect_small_table_rows`

- 默认值: 10000000
- 类型: Long
- 单位: -
- 是否可变: 是
- 描述: 在自动采集期间，用于判断外部数据源（Hive、Iceberg、Hudi）中的表是否为小表的阈值。如果表的行数小于此值，则该表被视为小表。
- 引入版本: v3.2

### `statistic_cache_columns`

- 默认值: 100000
- 类型: Long
- 单位: -
- 是否可变: 否
- 描述: 统计信息表可缓存的行数。
- 引入版本: -

### `statistic_cache_thread_pool_size`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: 否
- 描述: 用于刷新统计信息缓存的线程池大小。
- 引入版本: -

### `statistic_collect_interval_sec`

- 默认值: 5 * 60
- 类型: Long
- 单位: 秒
- 是否可变: 是
- 描述: 自动采集期间检查数据更新的时间间隔。
- 引入版本: -

### `statistic_max_full_collect_data_size`

- 默认值: 100 * 1024 * 1024 * 1024
- 类型: Long
- 单位: 字节
- 是否可变: 是
- 描述: 自动采集统计信息的数据大小阈值。如果总大小超过此值，则执行抽样采集而非全量采集。
- 引入版本: -

### `statistic_sample_collect_rows`

- 默认值: 200000
- 类型: Long
- 单位: -
- 是否可变: 是
- 描述：在加载触发的统计信息操作期间，用于决定使用 SAMPLE 还是 FULL 统计信息收集的行数阈值。如果加载或更改的行数超过此阈值（默认 200,000），则使用 SAMPLE 统计信息收集；否则使用 FULL 统计信息收集。此设置与 `enable_statistic_collect_on_first_load` 和 `statistic_sample_collect_ratio_threshold_of_first_load` 配合使用。
- 引入版本：-

### `statistic_update_interval_sec`

- 默认值: 24 * 60 * 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 统计信息缓存的更新间隔。
- 引入版本: -


### `enable_external_stats_lazy_refresh_on_replay`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 Follower（以及重启恢复）在回放统计信息日志时如何刷新 Connector（外部表）统计信息缓存。设为 `true` 时，按日志中持久化的表 UUID 失效缓存，并在下次查询时惰性重新加载，从而避免在回放期间解析外部表元数据（`MetadataMgr.getTable`）——该解析可能因 Hive Metastore 或对象存储变慢而阻塞日志回放线程。设为 `false`（默认）时使用原有的主动刷新方式，保持现有行为。在该 UUID 被持久化之前写入的统计信息日志，无论该配置如何都会回退到主动刷新。

### `statistics_large_string_column_merge_threshold`

- 默认值: 0
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 默认关闭（`0`）。设为正值后，在统计信息收集的过程中，会单独生成一条 SQL 来收集声明长度超过该阈值的字符串列（`VARCHAR` / `CHAR`）的统计信息，不与其他列合并起来收集。抽样统计和全量统计都遵循该策略。这样做是为了限制单条统计 SQL 在 Exchange 阶段的内存峰值，避免长字符串列与其他列合并后进一步放大聚合算子的状态。保持为 `0` 时，所有列走原先的合并批量收集路径。注意，`STRING` 在内部会表示为最大长度的 `VARCHAR`，因此启用该配置并设置正值阈值后，`STRING` 列也可能被单独拆分出来收集。
- 引入版本: -

### `task_check_interval_second`

- 默认值：60
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：任务后台作业的执行间隔。GlobalStateMgr 使用此值调度 TaskCleaner FrontendDaemon，该守护进程会调用 `doTaskBackgroundJob()`；该值乘以 1000 后设置为守护进程的毫秒间隔。减小该值会使后台维护（任务清理、检查）运行更频繁、响应更快，但会增加 CPU/IO 开销；增大该值可降低开销，但会延迟清理和检测过期任务。请调整此值以平衡维护响应性和资源使用。
- 引入版本：v3.2.0

### `task_min_schedule_interval_s`

- 默认值：10
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：SQL 层检查的任务调度所允许的最小调度间隔（秒）。提交任务时，TaskAnalyzer 会将调度周期转换为秒，如果周期小于 `task_min_schedule_interval_s`，则以 `ERR_INVALID_PARAMETER` 拒绝提交。这可防止创建运行过于频繁的任务，保护调度器免受高频任务影响。如果调度没有明确的开始时间，TaskAnalyzer 会将开始时间设置为当前的 epoch 秒数。
- 引入版本：v3.3.0、v3.4.0、v3.5.0

### `task_runs_timeout_second`

- 默认值：4 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：TaskRun 的默认执行超时时间（秒）。此项由 TaskRun 执行作为基准超时时间使用。如果任务运行的属性中包含会话变量 `query_timeout` 或 `insert_timeout` 且值为正整数，则运行时将使用该会话超时与 `task_runs_timeout_second` 中的较大值。有效超时时间随后被限制为不超过已配置的 `task_runs_ttl_second` 和 `task_ttl_second`。设置此项可限制任务运行的最长执行时间。非常大的值可能会被任务/任务运行的 TTL 设置截断。
- 引入版本：-

## 导入与导出

### `broker_load_default_timeout_second`

- 默认值：14400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Broker Load 作业的超时时长。
- 引入版本：-

### `desired_max_waiting_jobs`

- 默认值：1024
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：FE 中待处理作业的最大数量。该数量涵盖所有作业类型，例如建表、导入和 Schema Change 作业。如果 FE 中待处理作业数量达到此值，FE 将拒绝新的导入请求。此参数仅对异步导入生效。从 v2.5 起，默认值由 100 更改为 1024。
- 引入版本：-

### `disable_load_job`

- 默认值：false
- 类型：布尔值
- 单位：-
- 是否可变：是
- 描述：当集群遇到错误时是否禁用加载。这可以防止因集群错误造成的任何损失。默认值为 `FALSE`，表示不禁用加载。`TRUE` 表示禁用加载，集群处于只读状态。
- 引入版本：-

### `empty_load_as_error`

- 默认值：true
- 类型：布尔值
- 单位：-
- Is mutable: Yes
- Description: Whether to return an error message "all partitions have no load data" if no data is loaded. Valid values:
  - `true`: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data".
  - `false`：如果没有加载数据，系统将显示成功消息并返回 OK，而不是错误。
- 引入版本：-

### `enable_file_bundling`

- 默认值：true
- 类型：Boolean
- 单位：-
- 是否可变：是
- 描述：是否为云原生表启用文件捆绑优化。当此功能启用时（设置为 `true`），系统会自动捆绑由加载、Compaction 或 Publish 操作生成的数据文件，从而降低因高频访问外部存储系统而产生的 API 成本。您也可以使用 CREATE TABLE 属性 `file_bundling` 在表级别控制此行为。有关详细说明，请参阅 CREATE TABLE。
- 引入版本：v4.0

### `enable_routine_load_lag_metrics`

- 默认值：false
- 类型：布尔值
- 单位：-
- 是否可变：是
- 描述：是否收集 Routine Load Kafka 分区偏移量滞后指标。请注意，将此项设置为 `true` 将调用 Kafka API 以获取分区的最新偏移量。
- 引入版本：-

### `enable_sync_publish`

- 默认值：true
- 类型：布尔值
- 单位：-
- 是否可变：是
- 描述：是否在加载事务的发布阶段同步执行应用任务。此参数仅适用于主键表。有效值：
  - `TRUE`（默认）：apply 任务在加载事务的发布阶段同步执行。这意味着只有在 apply 任务完成后，加载事务才会被报告为成功，并且加载的数据才能真正被查询到。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `true` 可以提高查询性能和稳定性，但可能会增加加载延迟。
  - `FALSE`：apply 任务在加载事务的发布阶段异步执行。这意味着在 apply 任务提交后，加载事务即被报告为成功，但已加载的数据无法立即查询。在这种情况下，并发查询需要等待 apply 任务完成或超时后才能继续。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `false` 可能会影响查询性能和稳定性。
- 引入版本：v3.2.0

### `export_checker_interval_second`

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：调度加载作业的时间间隔。
- 引入版本：-

### `export_max_bytes_per_be_per_task`

- 默认值：268435456
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：单个数据卸载任务从单个 BE 导出的最大数据量。
- 引入版本：-

### `export_running_job_num_limit`

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：可并行运行的最大数据导出任务数。
- 引入版本：-

### `export_task_default_timeout_second`

- 默认值：2 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：数据导出任务的超时时长。
- 引入版本：-

### `export_task_pool_size`

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：否
- 描述：卸载任务线程池的大小。
- 引入版本：-

### `external_table_commit_timeout_ms`

- 默认值：10000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：向 StarRocks 外部表提交（发布）写事务的超时时长。默认值 `10000` 表示超时时长为 10 秒。
- 引入版本：-

### `finish_transaction_default_lock_timeout_ms`

- 默认值：1000
- 类型：Int
- 单位：毫秒
- 是否可变：是
- 描述：在完成事务期间获取数据库和表锁的默认超时时长。
- 引入版本：v4.0.0, v3.5.8

### `history_job_keep_max_second`

- 默认值：7 * 24 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：历史作业（如 Schema Change 作业）可保留的最长时长。
- 引入版本：-

### `insert_load_default_timeout_second`

- 默认值：3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：用于加载数据的 INSERT INTO 语句的超时时长。
- 引入版本：-

### `label_clean_interval_second`

- 默认值：4 * 3600
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：清理标签的时间间隔。单位：秒。建议指定较短的时间间隔，以确保历史标签能够及时清理。
- 引入版本：-

### `label_keep_max_num`

- 默认值：1000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：在一段时间内可保留的最大加载作业数量。超过此数量后，历史作业的信息将被删除。
- 引入版本：-

### `label_keep_max_second`

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：保留已完成且处于 FINISHED 或 CANCELLED 状态的加载作业标签的最大时长（秒）。默认值为 3 天。超过此时长后，标签将被删除。该参数适用于所有类型的加载作业。值过大会消耗大量内存。
- 引入版本：-

### `load_checker_interval_second`

- 默认值：5
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：滚动处理加载作业的时间间隔。
- 引入版本：-

### `load_parallel_instance_num`

- 默认值：1
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：控制在单个主机上为 broker 和 stream load 创建的并行加载 fragment 实例数量。LoadPlanner 将此值用作每主机并行度，除非会话启用了自适应 sink DOP；若会话变量 `enable_adaptive_sink_dop` 为 true，则会话 `s `sink_degree_of_parallelism` 将覆盖此配置。当需要 shuffle 时，此值应用于 fragment 并行执行（scan fragment 和 sink fragment 并行执行实例）。当不需要 shuffle 时，此值用作 sink pipeline DOP。注意：来自本地文件的加载强制使用单实例（pipeline DOP = 1，parallel exec = 1），以避免本地磁盘竞争。增大此值会提高每主机并发度和吞吐量，但可能增加 CPU、内存和 I/O 竞争。
- 引入版本：v3.2.0

### `load_straggler_wait_second`

- 默认值：300
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：BE 副本可容忍的最大加载滞后时间。如果超过此值，将执行克隆操作以从其他副本克隆数据。
- 引入版本：-

### `loads_history_retained_days`

- 默认值：30
- 类型：Int
- 单位：天
- 是否可变：是
- 描述：在内部 `_statistics_.loads_history` 表中保留加载历史记录的天数。该值用于表创建时设置表属性 `partition_live_number`，并传递给 `TableKeeper`（最小值限制为 1）以确定保留多少个每日分区。增大或减小此值会调整已完成加载作业在每日分区中的保留时长；它会影响新表的创建和 keeper 的清理行为，但不会自动重新创建过去的分区。`LoadsHistorySyncer` 在管理加载历史生命周期时依赖此保留设置；其同步节奏由 `loads_history_sync_interval_second` 控制。
- 引入版本：v3.3.6、v3.4.0、v3.5.0

### `loads_history_sync_interval_second`

- 默认值：60
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：LoadsHistorySyncer 用于调度将已完成加载作业从 `information_schema.loads` 定期同步到内部 `_statistics_.loads_history` 表的时间间隔（单位：秒）。该值在构造函数中乘以 1000 以设置 FrontendDaemon 的间隔。同步器会跳过第一次运行（以允许表创建），并且只导入一分钟前已完成的加载；较小的值会增加 DML 和执行器负载，而较大的值会延迟历史加载记录的可用性。目标表的保留/分区行为请参见 `loads_history_retained_days`。
- 引入版本：v3.3.6、v3.4.0、v3.5.0

### `max_broker_load_job_concurrency`

- 默认值: 5
- 别名: `async_load_task_pool_size`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StarRocks 集群中允许的最大并发 Broker Load 作业数。此参数仅对 Broker Load 有效。此参数的值必须小于 `max_running_txn_num_per_db` 的值。从 v2.5 开始，默认值从 `10` 更改为 `5`。
- 引入版本: -

### `max_load_initial_open_partition_number`

- 默认值: 4096
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 单次导入开始时最多预先打开的分区数。该值在以下两种场景作为上限生效:(1) LIST 分区表(默认全部打开);(2) RANGE 分区表通过 INSERT / Broker Load / Spark Load 导入(默认全部打开)。Stream Load 与 Routine Load 写入 RANGE 分区表时不受该上限限制,保留更保守的「最新 32」默认行为。表属性 `load_initial_open_partition_number` 优先级最高,可覆盖该值并突破此上限。从 v4.0 起,默认值从 32 调整为 4096。
- 引入版本: -

### `max_load_timeout_second`

- 默认值：259200
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：加载作业允许的最大超时时长。超过此限制后，加载作业将失败。此限制适用于所有类型的加载作业。
- 引入版本：-

### `max_routine_load_batch_size`

- 默认值：4294967296
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：Routine Load 任务可加载的最大数据量。
- 引入版本：-

### `max_routine_load_task_concurrent_num`

- 默认值：5
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：每个 Routine Load 作业的最大并发任务数。
- 引入版本：-

### `max_routine_load_task_num_per_be`

- 默认值：16
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：每个 BE 上并发 Routine Load 任务的最大数量。自 v3.1.0 起，该参数的默认值从 5 增加到 16，且不再需要小于或等于 BE 静态参数 `routine_load_thread_pool_size` 的值（已废弃）。
- 引入版本：-

### `max_running_txn_num_per_db`

- 默认值：1000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：StarRocks 集群中每个数据库允许同时运行的最大导入事务数。默认值为 `1000`。从 v3.1 起，默认值从 `100` 更改为 `1000`。当某个数据库实际运行的导入事务数超过该参数值时，新的导入请求将不会被处理。同步导入作业的新请求将被拒绝，异步导入作业的新请求将被放入队列。不建议增大该参数的值，因为这会增加系统负载。
- 引入版本：-

### `max_stream_load_timeout_second`

- 默认值：259200
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：Stream Load 作业允许的最大超时时长。
- 引入版本：-

### `max_tolerable_backend_down_num`

- 默认值：0
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：允许的最大故障 BE 节点数。若超过该数量，Routine Load 作业将无法自动恢复。
- 引入版本：-

### `min_bytes_per_broker_scanner`

- 默认值：67108864
- 类型：Long
- 单位：字节
- 是否可变：是
- 描述：Broker Load 实例可处理的最小数据量。
- 引入版本：-

### `min_load_timeout_second`

- 默认值：1
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：导入作业允许的最小超时时长。该限制适用于所有类型的导入作业。
- 引入版本：-

### `min_routine_load_lag_for_metrics`

- 默认值：10000
- 类型：INT
- 单位：-
- 是否可变：是
- 描述：在监控指标中显示的 Routine Load 作业的最小偏移滞后量。偏移滞后量大于此值的 Routine Load 作业将显示在指标中。
- 引入版本：-

### `period_of_auto_resume_min`

- 默认值：5
- 类型：Int
- 单位：分钟
- 是否可变：是
- 描述：Routine Load 作业自动恢复的时间间隔。
- 引入版本：-

### `prepared_transaction_default_timeout_second`

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：预处理事务的默认超时时长。
- 引入版本：-

### `rejected_records_retained_days`

- 默认值：7
- 类型：Int
- 单位：天
- 是否可变：是
- 描述：内部系统表 `_statistics_.rejected_records` 中保留的每日分区数量。该值会传递给 `TableKeeper`（最小值限制为 1），并在每次 keeper 触发时与目标表的 `partition_live_number` 属性进行同步。当您需要超出默认一周的被拒绝行历史记录（用于审计或更长的重放窗口），或存储预算紧张时，可调整此值。该值仅影响新的每日分区和 keeper 的 TTL 同步，不会追溯恢复已删除的分区。
- 引入版本：-

### `routine_load_task_consume_second`

- 默认值：15
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：集群中每个 Routine Load 任务消费数据的最大时长。自 v3.1.0 起，Routine Load 作业在 `job_properties` 中支持新参数 `task_consume_second`。该参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本：-

### `routine_load_task_timeout_second`

- 默认值：60
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：集群中每个 Routine Load 任务的超时时长。自 v3.1.0 起，Routine Load 作业在 `job_properties` 中支持新参数 `task_timeout_second`。该参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本：-

### `routine_load_unstable_threshold_second`

- 默认值：3600
- 类型：Long
- 单位：秒
- 是否可变：是
- 描述：如果 Routine Load 作业中的任何任务出现滞后，该作业将被设置为 UNSTABLE 状态。具体而言，当正在消费的消息的时间戳与当前时间之差超过此阈值，且数据源中存在未消费的消息时，将触发该状态。
- 引入版本：-

### `spark_dpp_version`

- 默认值：1.0.0
- 类型：String
- 单位：-
- 是否可变：否
- 描述：所使用的 Spark 动态分区裁剪（DPP）版本。
- 引入版本：-

### `spark_home_default_dir`

- 默认值：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Spark 客户端的根目录。
- 引入版本：-

### `spark_launcher_log_dir`

- 默认值：`sys_log_dir` + "/spark_launcher_log"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：存储 Spark 日志文件的目录。
- 引入版本：-

### `spark_load_default_timeout_second`

- 默认值：86400
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：每个 Spark Load 作业的超时时长。
- 引入版本：-

### `spark_load_submit_timeout_second`

- 默认值：300
- 类型：long
- 单位：秒
- 是否可变：否
- 描述：提交 Spark 应用程序后等待 YARN 响应的最长时间（秒）。`SparkLauncherMonitor.LogMonitor` 会将此值转换为毫秒，若作业在 UNKNOWN/CONNECTED/SUBMITTED 状态下停留时间超过该超时时长，则停止监控并强制终止 Spark Launcher 进程。`SparkLoadJob` 将此配置作为默认值读取，并允许通过 `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` 属性对每个 Load 作业进行单独覆盖。请将其设置得足够大以适应 YARN 排队延迟；设置过低可能会中止合法排队的作业，而设置过高则可能延迟故障处理和资源清理。
- 引入版本：v3.2.0

### `spark_resource_path`

- 默认值：空字符串
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Spark 依赖包的根目录。
- 引入版本：-

### `stream_load_default_timeout_second`

- 默认值：600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：每个 Stream Load 作业的默认超时时长。
- 引入版本：-

### `stream_load_max_txn_num_per_be`

- 默认值：-1
- 类型：Int
- 单位：事务
- 是否可变：是
- 描述：限制从单个 BE（后端）主机接受的并发 stream-load 事务数量。当设置为非负整数时，FrontendServiceImpl 会检查该 BE（按客户端 IP）的当前事务数，如果数量 `>=` 此限制，则拒绝新的 stream-load 开始请求。值为 `< 0` 时禁用限制（无限制）。此检查在 stream load 开始时发生，超出限制时可能导致 `streamload txn num per be exceeds limit` 错误。相关运行时行为使用 `stream_load_default_timeout_second` 作为请求超时回退。
- 引入版本：v3.3.0、v3.4.0、v3.5.0

### `stream_load_task_keep_max_num`

- 默认值：1000
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：StreamLoadMgr 在内存中保留的 Stream Load 任务最大数量（跨所有数据库的全局限制）。当已跟踪的任务数量（`idToStreamLoadTask`）超过此阈值时，StreamLoadMgr 首先调用 `cleanSyncStreamLoadTasks()` 移除已完成的同步 stream-load 任务；如果数量仍大于该阈值的一半，则调用 `cleanOldStreamLoadTasks(true)` 强制移除较旧或已完成的任务。增大此值可在内存中保留更多任务历史；减小此值可降低内存使用并使清理更为积极。此值仅控制内存中的保留，不影响已持久化/重放的任务。
- 引入版本：v3.2.0

### `stream_load_task_keep_max_second`

- 默认值：3 * 24 * 3600
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：已完成或已取消的 Stream Load 任务的保留窗口。当任务达到最终状态且其结束时间戳早于此阈值（`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`）时，该任务将有资格被 `StreamLoadMgr.cleanOldStreamLoadTasks` 移除，并在加载持久化状态时被丢弃。适用于 `StreamLoadTask` 和 `StreamLoadMultiStmtTask`。如果任务总数超过 `stream_load_task_keep_max_num`，清理可能会提前触发（同步任务由 `cleanSyncStreamLoadTasks` 优先处理）。设置此值以在历史记录/可调试性与内存使用之间取得平衡。
- 引入版本：v3.2.0

### `transaction_clean_interval_second`

- 默认值：30
- 类型：Int
- 单位：秒
- 是否可变：否
- 描述：清理已完成事务的时间间隔。单位：秒。建议指定较短的时间间隔，以确保已完成的事务能够及时清理。
- 引入版本：-

### `transaction_stream_load_coordinator_cache_capacity`

- 默认值：4096
- 类型：Int
- 单位：-
- 是否可变：是
- 描述：存储事务标签到协调节点映射关系的缓存容量。
- 引入版本：-

### `transaction_stream_load_coordinator_cache_expire_seconds`

- 默认值：900
- 类型：Int
- 单位：秒
- 是否可变：是
- 描述：协调节点映射在缓存中被驱逐前的保留时间（TTL）。
- 引入版本：-

### `yarn_client_path`

- 默认值：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- 类型：String
- 单位：-
- 是否可变：否
- 描述：Yarn 客户端包的根目录。
- 引入版本：-

### `yarn_config_dir`

- 默认值：`StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- 类型：字符串
- 单位：-
- 是否可变：否
- 描述：存储 Yarn 配置文件的目录。
- 引入版本：-
