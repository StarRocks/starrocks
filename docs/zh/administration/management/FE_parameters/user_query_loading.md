---
displayed_sidebar: docs
sidebar_label: "用户管理、查询引擎和导入导出"
---

# FE 配置 - 用户管理、查询引擎和导入导出

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令配置或修改 FE 动态参数。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

---

当前主题包含以下类型的 FE 配置：
- [用户、角色和权限](#用户角色和权限)
- [查询引擎](#查询引擎)
- [导入导出](#导入和导出)

## 用户、角色和权限

### `enable_task_info_mask_credential`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，StarRocks 会在将凭据从任务 SQL 定义返回到 `information_schema.tasks` 和 `information_schema.task_runs` 之前，通过对 DEFINITION 列应用 SqlCredentialRedactor.redact 来屏蔽凭据。在 `information_schema.task_runs` 中，无论定义是来自任务运行状态还是在为空时来自任务定义查找，都应用相同的屏蔽。当为 false 时，返回原始任务定义（可能会暴露凭据）。屏蔽是 CPU/字符串处理工作，当任务或 `task_runs` 数量很大时可能非常耗时；仅当您需要未屏蔽的定义并接受安全风险时才禁用。
- 引入版本: v3.5.6

### `privilege_max_role_depth`

- 默认值: 16
- 类型: Int
- 单位:
- 是否可变: Yes
- 描述: 角色的最大角色深度（继承级别）。
- 引入版本: v3.0.0

### `privilege_max_total_roles_per_user`

- 默认值: 64
- 类型: Int
- 单位:
- 是否可变: Yes
- 描述: 用户可以拥有的最大角色数量。
- 引入版本: v3.0.0

## 查询引擎

### `brpc_send_plan_fragment_timeout_ms`

- 默认值: 60000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在发送计划片段之前应用于 BRPC TalkTimeoutController 的超时（毫秒）。`BackendServiceClient.sendPlanFragmentAsync` 在调用后端 `execPlanFragmentAsync` 之前设置此值。它控制 BRPC 在从连接池借用空闲连接以及执行发送时将等待多长时间；如果超过，RPC 将失败并可能触发该方法的重试逻辑。在争用情况下，将此值设置得更低以快速失败，或提高它以容忍瞬时池耗尽或慢速网络。请谨慎：非常大的值可能会延迟故障检测并阻塞请求线程。
- 引入版本: v3.3.11, v3.4.1, v3.5.0

### `connector_table_query_trigger_analyze_large_table_interval`

- 默认值: 12 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 大型表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

### `connector_table_query_trigger_analyze_max_pending_task_num`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

### `connector_table_query_trigger_analyze_max_running_task_num`

- 默认值: 2
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

### `connector_table_query_trigger_analyze_small_table_interval`

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 小型表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

### `connector_table_query_trigger_analyze_small_table_rows`

- 默认值: 10000000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 用于确定查询触发 ANALYZE 任务的表是否为小型表的阈值。
- 引入版本: v3.4.0

### `connector_table_query_trigger_task_schedule_interval`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 调度器线程调度查询触发后台任务的间隔。此项旨在取代 v3.4.0 中引入的 `connector_table_query_trigger_analyze_schedule_interval`。在此处，后台任务指 v3.4 中的 `ANALYZE` 任务，以及 v3.4 之后版本中低基数列字典的收集任务。
- 引入版本: v3.4.2

### `create_table_max_serial_replicas`

- 默认值: 128
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 串行创建副本的最大数量。如果实际副本数量超过此值，则将并发创建副本。如果表创建时间过长，请尝试减小此值。
- 引入版本: -

### `default_mv_partition_refresh_number`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 当物化视图刷新涉及多个分区时，此参数控制默认情况下单个批次中刷新多少个分区。
从 3.3.0 版本开始，系统默认一次刷新一个分区，以避免潜在的内存溢出 (OOM) 问题。在早期版本中，所有分区默认一次刷新，这可能导致内存耗尽和任务失败。但是，请注意，当物化视图刷新涉及大量分区时，一次只刷新一个分区可能会导致过多的调度开销、更长的整体刷新时间以及大量的刷新记录。在这种情况下，建议适当调整此参数以提高刷新效率并降低调度成本。
- 引入版本: v3.3.0

### `default_mv_refresh_immediate`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在创建后立即刷新异步物化视图。当此项设置为 `true` 时，新创建的物化视图将立即刷新。
- 引入版本: v3.2.3

### `dynamic_partition_check_interval_seconds`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 检查新数据的间隔。如果检测到新数据，StarRocks 会自动为数据创建分区。
- 引入版本: -

### `dynamic_partition_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用动态分区功能。启用此功能后，StarRocks 会为新数据动态创建分区，并自动删除过期分区以确保数据的及时性。
- 引入版本: -

### `enable_active_materialized_view_schema_strict_check`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 激活非活动物化视图时是否严格检查数据类型的长度一致性。当此项设置为 `false` 时，如果基表中的数据类型长度发生变化，物化视图的激活不受影响。
- 引入版本: v3.3.4

### `mv_fast_schema_change_mode`

- 默认值: strict
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 控制物化视图快速模式变更（FSE）的行为。有效值为：`strict`（默认）- 仅在 `isSupportFastSchemaEvolutionInDanger` 为 true 时允许 FSE，并清除版本映射中受影响的分区条目；`force` - 即使 `isSupportFastSchemaEvolutionInDanger` 为 false 也允许 FSE，并清除受影响的分区条目以在刷新时触发重新计算；`force_no_clear` - 即使 `isSupportFastSchemaEvolutionInDanger` 为 false 也允许 FSE，但不清除分区条目。
- 引入版本: v4.1.0

### `enable_auto_collect_array_ndv`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 ARRAY 类型 NDV 信息的自动收集。
- 引入版本: v4.0

### `enable_backup_materialized_view`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在备份或恢复特定数据库时，是否启用异步物化视图的 BACKUP 和 RESTORE。如果此项设置为 `false`，StarRocks 将跳过备份异步物化视图。
- 引入版本: v3.2.0

### `enable_collect_full_statistic`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用自动全量统计信息收集。此功能默认启用。
- 引入版本: -

### `enable_colocate_mv_index`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 创建同步物化视图时是否支持将同步物化视图索引与基表进行 Colocate。如果此项设置为 `true`，tablet sink 将加速同步物化视图的写入性能。
- 引入版本: v3.2.0

### `enable_decimal_v3`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否支持 DECIMAL V3 数据类型。
- 引入版本: -

### `enable_experimental_mv`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用异步物化视图功能。TRUE 表示启用此功能。从 v2.5.2 开始，此功能默认启用。对于 v2.5.2 之前的版本，此功能默认禁用。
- 引入版本: v2.4

### `enable_local_replica_selection`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为查询选择本地副本。本地副本可降低网络传输成本。如果此参数设置为 TRUE，CBO 优先选择与当前 FE 具有相同 IP 地址的 BE 上的 tablet 副本。如果此参数设置为 `FALSE`，则可以选择本地副本和非本地副本。
- 引入版本: -

### `enable_manual_collect_array_ndv`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 ARRAY 类型 NDV 信息的手动收集。
- 引入版本: v4.0

### `enable_materialized_view`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许创建物化视图。
- 引入版本: -

### `enable_materialized_view_external_table_precise_refresh`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 将此项设置为 `true` 可为物化视图刷新启用内部优化，当基表是外部（非云原生）表时。启用后，物化视图刷新处理器会计算候选分区并仅刷新受影响的基表分区，而不是所有分区，从而减少 I/O 和刷新成本。将其设置为 `false` 以强制对外部表进行全分区刷新。
- 引入版本: v3.2.9

### `enable_materialized_view_metrics_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认收集异步物化视图的监控指标。
- 引入版本: v3.1.11, v3.2.5

### `enable_materialized_view_spill`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为物化视图刷新任务启用中间结果 spilling。
- 引入版本: v3.1.1

### `enable_materialized_view_text_based_rewrite`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认启用基于文本的查询重写。如果此项设置为 `true`，系统将在创建异步物化视图时构建抽象语法树。
- 引入版本: v3.2.5

### `enable_mv_automatic_active_check`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用系统自动检查和重新激活那些因基表（视图）进行 Schema Change 或被删除和重新创建而变为 Inactive 的异步物化视图。请注意，此功能不会重新激活用户手动设置为 Inactive 的物化视图。
- 引入版本: v3.1.6

### `enable_mv_automatic_repairing_for_broken_base_tables`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，StarRocks 将尝试在基外部表被删除并重新创建或其表标识符更改时自动修复物化视图基表元数据。修复流程可以更新物化视图的基表信息，收集外部表分区的分区级修复信息，并推动异步自动刷新物化视图的分区刷新决策，同时遵守 `autoRefreshPartitionsLimit`。目前自动修复支持 Hive 外部表；不支持的表类型将导致物化视图设置为非活动状态并引发修复异常。分区信息收集是非阻塞的，失败将被记录。
- 引入版本: v3.3.19, v3.4.8, v3.5.6

### `enable_predicate_columns_collection`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用谓词列收集。如果禁用，谓词列在查询优化期间将不会被记录。
- 引入版本: -

### `enable_query_queue_v2`

- 默认值: true
- 类型: boolean
- 单位: -
- 是否可变: No
- 描述: 当为 true 时，将 FE 基于槽位的查询调度器切换到查询队列 V2。槽位管理器和跟踪器（例如 `BaseSlotManager.isEnableQueryQueueV2` 和 `SlotTracker#createSlotSelectionStrategy`）会读取此标志以选择 `SlotSelectionStrategyV2` 而不是旧版策略。`query_queue_v2_xxx` 配置选项和 `QueryQueueOptions` 仅在此标志启用时生效。从 v4.1 开始，默认值从 `false` 更改为 `true`。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

### `enable_sql_blacklist`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用 SQL 查询的黑名单检查。启用此功能后，黑名单中的查询无法执行。
- 引入版本: -

### `enable_statistic_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集 CBO 的统计信息。此功能默认启用。
- 引入版本: -

### `enable_statistic_collect_on_first_load`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制由数据加载操作触发的自动统计信息收集和维护。这包括：
  - 当数据首次加载到分区时（分区版本等于 2）的统计信息收集。
  - 当数据加载到多分区表的空分区时（分区版本等于 2）的统计信息收集。
  - INSERT OVERWRITE 操作的统计信息复制和更新。

  **统计信息收集类型决策策略：**
  
  - 对于 INSERT OVERWRITE：`deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。仅复制现有统计信息。
    - 否则，如果 `targetRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  - 对于首次加载：`deltaRatio = loadRows / (totalRows + 1)`
    - 如果 `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (默认值: 0.1)，则不执行统计信息收集。
    - 否则，如果 `loadRows > statistic_sample_collect_rows` (默认值: 200000)，则使用 SAMPLE 统计信息收集。
    - 否则，使用 FULL 统计信息收集。
  
  **同步行为：**
  
  - 对于 DML 语句（INSERT INTO/INSERT OVERWRITE）：同步模式，带表锁。加载操作等待统计信息收集完成（最长 `semi_sync_collect_statistic_await_seconds`）。
  - 对于 Stream Load 和 Broker Load：异步模式，无锁。统计信息收集在后台运行，不阻塞加载操作。
  
  :::note
  禁用此配置将阻止所有加载触发的统计信息操作，包括 INSERT OVERWRITE 的统计信息维护，这可能导致表缺少统计信息。如果经常创建新表并频繁加载数据，启用此功能将增加内存和 CPU 开销。
  :::

- 引入版本: v3.1

### `enable_statistic_collect_on_update`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 UPDATE 语句是否可以触发自动统计信息收集。启用后，修改表数据的 UPDATE 操作可能会通过与 `enable_statistic_collect_on_first_load` 控制的基于摄取统计信息框架调度统计信息收集。禁用此配置会跳过 UPDATE 语句的统计信息收集，同时保持加载触发的统计信息收集行为不变。
- 引入版本: v3.5.11, v4.0.4

### `enable_udf`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否启用 UDF。
- 引入版本: -

### `expr_children_limit`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 表达式中允许的最大子表达式数量。
- 引入版本: -

### `histogram_buckets_size`

- 默认值: 64
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图的默认 bucket 数量。
- 引入版本: -

### `histogram_max_sample_row_count`

- 默认值: 10000000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图收集的最大行数。
- 引入版本: -

### `histogram_mcv_size`

- 默认值: 100
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 直方图的最常见值 (MCV) 数量。
- 引入版本: -

### `histogram_sample_ratio`

- 默认值: 0.1
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 直方图的采样率。
- 引入版本: -

### `http_slow_request_threshold_ms`

- 默认值: 5000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 如果 HTTP 请求的响应时间超过此参数指定的值，则生成日志以跟踪此请求。
- 引入版本: v2.5.15, v3.1.5

### `lock_checker_enable_deadlock_check`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，LockChecker 线程使用 ThreadMXBean.findDeadlockedThreads() 执行 JVM 级死锁检测，并记录违规线程的堆栈跟踪。检查在 LockChecker 守护程序（其频率由 `lock_checker_interval_second` 控制）内部运行，并将详细的堆栈信息写入日志，这可能耗费 CPU 和 I/O。仅在调试实时或可重现的死锁问题时才启用此选项；在正常操作中保持启用状态可能会增加开销和日志量。
- 引入版本: v3.2.0

### `low_cardinality_threshold`

- 默认值: 255
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 低基数字典的阈值。
- 引入版本: v3.5.0

### `materialized_view_min_refresh_interval`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: ASYNC 物化视图调度的最小允许刷新间隔（以秒为单位）。当以基于时间的间隔创建物化视图时，该间隔将转换为秒，并且不得小于此值；否则 CREATE/ALTER 操作将因 DDL 错误而失败。如果此值大于 0，则强制执行检查；将其设置为 0 或负值以禁用限制，这可以防止TaskManager 过度调度和因刷新过于频繁而导致 FE 内存/CPU 使用率过高。此项不适用于 `EVENT_TRIGGERED` 刷新。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `materialized_view_refresh_ascending`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，物化视图分区刷新将以升序分区键顺序（从最旧到最新）迭代分区。当设置为 `false`（默认）时，系统以降序（从最新到最旧）迭代。StarRocks 在列表分区和范围分区物化视图刷新逻辑中都使用此项来选择在应用分区刷新限制时要处理的分区，并计算后续 TaskRun 执行的下一个开始/结束分区边界。更改此项会改变首先刷新哪些分区以及如何导出下一个分区范围；对于范围分区物化视图，调度器会验证新的开始/结束，如果更改会创建重复边界（死循环），则会引发错误，因此请谨慎设置此项。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

### `max_allowed_in_element_num_of_delete`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: DELETE 语句中 IN 谓词允许的最大元素数量。
- 引入版本: -

### `max_create_table_timeout_second`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 创建表的超时时长上限。
- 引入版本: -

### `max_distribution_pruner_recursion_depth`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 分区剪枝器允许的最大递归深度。增加递归深度可以剪枝更多元素，但也会增加 CPU 消耗。
- 引入版本: -

### `max_partitions_in_one_batch`

- 默认值: 4096
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 批量创建分区时可创建的最大分区数。
- 引入版本: -

### `max_planner_scalar_rewrite_num`

- 默认值: 100000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 优化器可以重写标量操作符的最大次数。
- 引入版本: -

### `max_query_queue_history_slots_number`

- 默认值: 0
- 类型: Int
- 单位: 槽位
- 是否可变: Yes
- 描述: 控制每个查询队列保留多少最近释放的（历史）已分配槽位用于监控和可观察性。当 `max_query_queue_history_slots_number` 设置为 `> 0` 的值时，BaseSlotTracker 在内存队列中保留最多指定数量的最新释放的 LogicalSlot 条目，当超出限制时驱逐最旧的条目。启用此功能会导致 getSlots() 包含这些历史条目（最新的在前），允许 BaseSlotTracker 尝试使用 ConnectContext 注册槽位以获取更丰富的 ExtraMessage 数据，并允许 LogicalSlot.ConnectContextListener 将查询完成元数据附加到历史槽位。当 `max_query_queue_history_slots_number` `<= 0` 时，历史机制被禁用（不使用额外的内存）。使用合理的值来平衡可观察性和内存开销。
- 引入版本: v3.5.0

### `max_query_retry_time`

- 默认值: 2
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 上查询重试的最大次数。
- 引入版本: -

### `max_running_rollup_job_num_per_table`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每张表可以并行运行的 rollup 作业的最大数量。
- 引入版本: -

### `max_scalar_operator_flat_children`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: ScalarOperator 的最大扁平子节点数。您可以设置此限制以防止优化器使用过多内存。
- 引入版本: -

### `max_scalar_operator_optimize_depth`

- 默认值: 256
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: ScalarOperator 优化可以应用的最大深度。
- 引入版本: -

### `mv_active_checker_interval_seconds`

- 默认值: 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 当后台 `active_checker` 线程启用时，系统会定期检测并自动重新激活因其基表（或视图）的 schema 变更或重建而变为 Inactive 的物化视图。此参数以秒为单位控制检查器线程的调度间隔。默认值由系统定义。
- 引入版本: v3.1.6

### `mv_rewrite_consider_data_layout_mode`

- 默认值: `enable`
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 控制物化视图重写在选择最佳物化视图时是否应考虑基表数据布局。有效值：
  - `disable`: 在选择候选物化视图时，从不使用数据布局标准。
  - `enable`: 仅当查询被识别为布局敏感时才使用数据布局标准。
  - `force`: 在选择最佳物化视图时始终应用数据布局标准。
  更改此项会影响 `BestMvSelector` 的行为，并可以根据物理布局是否影响计划正确性或性能来改进或扩大重写的适用性。
- 引入版本: -

### `publish_version_interval_ms`

- 默认值: 10
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 发布验证任务发出的时间间隔。
- 引入版本: -

### `query_queue_slots_estimator_strategy`

- 默认值: MAX
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 当 `enable_query_queue_v2` 为 true 时，选择用于基于队列的查询的槽位估算策略。有效值：MBE（基于内存）、PBE（基于并行度）、MAX（取 MBE 和 PBE 的最大值）和 MIN（取 MBE 和 PBE 的最小值）。MBE 根据预测内存或计划成本除以每个槽位内存目标来估算槽位，并受 `totalSlots` 限制。PBE 根据片段并行度（扫描范围计数或基数/每槽位行数）和基于 CPU 成本的计算（使用每槽位 CPU 成本）推导出槽位，然后将结果限制在 [numSlots/2, numSlots] 范围内。MAX 和 MIN 通过取其最大值或最小值来组合 MBE 和 PBE。如果配置值无效，则使用默认值 (`MAX`)。
- 引入版本: v3.5.0

### `query_queue_v2_concurrency_level`

- 默认值: 4
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制计算系统总查询槽位时使用的逻辑并发“层数”。在 shared-nothing 模式下，总槽位 = `query_queue_v2_concurrency_level` * BE 数量 * 每个 BE 的核心数（来自 BackendResourceStat）。在多仓库模式下，有效并发会缩减为 max(1, `query_queue_v2_concurrency_level` / 4)。如果配置值为非正数，则视为 `4`。更改此值会增加或减少 totalSlots（以及因此的并发查询容量），并影响每个槽位的资源：memBytesPerSlot 通过将每个 worker 内存除以（每个 worker 的核心数 * 并发）得出，并且 CPU 记账使用 `query_queue_v2_cpu_costs_per_slot`。将其设置为与集群大小成比例；非常大的值可能会减少每个槽位的内存并导致资源碎片。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_cpu_costs_per_slot`

- 默认值: 1000000000
- 类型: Long
- 单位: 规划器 CPU 成本单位
- 是否可变: Yes
- 描述: 每个槽位的 CPU 成本阈值，用于根据查询的规划器 CPU 成本估算查询所需的槽位数量。调度器计算槽位为整数（`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`），然后将结果限制在 [1, totalSlots] 范围内（totalSlots 来自查询队列 V2 `V2` 参数）。V2 代码将非正值规范化为 1 (Math.max(1, value))，因此非正值实际上变为 `1`。增加此值会减少每个查询分配的槽位（有利于更少、更大槽位的查询）；减少此值会增加每个查询的槽位。与 `query_queue_v2_num_rows_per_slot` 和并发设置一起调整，以控制并行度与资源粒度。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_num_rows_per_slot`

- 默认值: 4096
- 类型: Int
- 单位: 行
- 是否可变: Yes
- 描述: 当估算每个查询的槽位计数时，分配给单个调度槽位的目标源行记录数。StarRocks 计算 `estimated_slots` = (源节点的基数) / `query_queue_v2_num_rows_per_slot`，然后将结果限制在 [1, totalSlots] 范围内，如果计算值为非正数，则强制最小值为 1。totalSlots 来自可用资源（大致为 DOP * `query_queue_v2_concurrency_level` * worker/BE 数量），因此取决于集群/核心计数。增加此值以减少槽位计数（每个槽位处理更多行）并降低调度开销；减少此值以增加并行度（更多、更小的槽位），直至达到资源限制。
- 引入版本: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_schedule_strategy`

- 默认值: SWRR
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 选择 Query Queue V2 用于对待处理查询进行排序的调度策略。支持的值（不区分大小写）为 `SWRR` (Smooth Weighted Round Robin) - 默认值，适用于需要公平加权共享的混合/混合工作负载 - 和 `SJF` (Short Job First + Aging) - 优先处理短作业，同时使用老化机制避免饥饿。该值通过不区分大小写的枚举查找进行解析；无法识别的值将记录为错误并使用默认策略。此配置仅在 Query Queue V2 启用时影响行为，并与 V2 大小设置（如 `query_queue_v2_concurrency_level`）交互。
- 引入版本: v3.3.12, v3.4.2, v3.5.0

### `semi_sync_collect_statistic_await_seconds`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: DML 操作（INSERT INTO 和 INSERT OVERWRITE 语句）期间半同步统计信息收集的最大等待时间。Stream Load 和 Broker Load 使用异步模式，不受此配置影响。如果统计信息收集时间超过此值，加载操作将继续，而不等待收集完成。此配置与 `enable_statistic_collect_on_first_load` 协同工作。
- 引入版本: v3.1

### `slow_query_analyze_threshold`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 查询执行时间阈值，用于触发查询反馈分析。
- 引入版本: v3.4.0

### `statistic_analyze_status_keep_second`

- 默认值: 3 * 24 * 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 保留收集任务历史记录的持续时间。默认值为 3 天。
- 引入版本: -

### `statistic_auto_analyze_end_time`

- 默认值: 23:59:59
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 自动收集的结束时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

### `statistic_auto_analyze_start_time`

- 默认值: 00:00:00
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 自动收集的开始时间。取值范围：`00:00:00` - `23:59:59`。
- 引入版本: -

### `statistic_auto_collect_ratio`

- 默认值: 0.8
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断自动收集统计信息是否健康的阈值。如果统计信息健康度低于此阈值，将触发自动收集。
- 引入版本: -

### `statistic_auto_collect_small_table_rows`

- 默认值: 10000000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 自动收集期间，判断外部数据源（Hive、Iceberg、Hudi）中的表是否为小型表的阈值。如果表的行数小于此值，则认为该表为小型表。
- 引入版本: v3.2

### `statistic_cache_columns`

- 默认值: 100000
- 类型: Long
- 单位: -
- 是否可变: No
- 描述: 统计信息表可缓存的行数。
- 引入版本: -

### `statistic_cache_thread_pool_size`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 用于刷新统计信息缓存的线程池大小。
- 引入版本: -

### `statistic_collect_interval_sec`

- 默认值: 5 * 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 自动收集期间检查数据更新的间隔。
- 引入版本: -

### `statistic_max_full_collect_data_size`

- 默认值: 100 * 1024 * 1024 * 1024
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 统计信息自动收集的数据大小阈值。如果总大小超过此值，则执行采样收集而不是全量收集。
- 引入版本: -

### `statistic_sample_collect_rows`

- 默认值: 200000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 在加载触发的统计信息操作期间，用于决定 SAMPLE 和 FULL 统计信息收集之间的行数阈值。如果加载或更改的行数超过此阈值（默认 200,000），则使用 SAMPLE 统计信息收集；否则，使用 FULL 统计信息收集。此设置与 `enable_statistic_collect_on_first_load` 和 `statistic_sample_collect_ratio_threshold_of_first_load` 协同工作。
- 引入版本: -

### `statistic_update_interval_sec`

- 默认值: 24 * 60 * 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 统计信息缓存的更新间隔。
- 引入版本: -

### `task_check_interval_second`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 任务后台作业执行之间的间隔。GlobalStateMgr 使用此值调度 TaskCleaner FrontendDaemon，该守护程序调用 `doTaskBackgroundJob()`；该值乘以 1000 以设置守护程序间隔（毫秒）。减小此值可使后台维护（任务清理、检查）运行更频繁并更快响应，但会增加 CPU/IO 开销；增加此值可减少开销，但会延迟清理和陈旧任务的检测。调整此值以平衡维护响应性和资源使用。
- 引入版本: v3.2.0

### `task_min_schedule_interval_s`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: SQL 层检查的任务调度的最小允许调度间隔（以秒为单位）。提交任务时，TaskAnalyzer 将调度周期转换为秒，如果周期小于 `task_min_schedule_interval_s`，则以 `ERR_INVALID_PARAMETER` 拒绝提交。这可以防止创建运行过于频繁的任务，并保护调度器免受高频任务的影响。如果调度没有显式开始时间，TaskAnalyzer 会将开始时间设置为当前纪元秒。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `task_runs_timeout_second`

- 默认值: 4 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: TaskRun 的默认执行超时（以秒为单位）。此项用作 TaskRun 执行的基线超时。如果任务运行的属性包含会话变量 `query_timeout` 或 `insert_timeout` 且具有正整数值，则运行时使用会话超时和 `task_runs_timeout_second` 之间的较大值。有效超时随后被限制为不超过配置的 `task_runs_ttl_second` 和 `task_ttl_second`。设置此项以限制任务运行的执行时间。非常大的值可能会被任务/任务运行 TTL 设置截断。
- 引入版本: -

## 导入和导出

### `broker_load_default_timeout_second`

- 默认值: 14400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Broker Load 作业的超时时长。
- 引入版本: -

### `desired_max_waiting_jobs`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE 中待处理作业的最大数量。该数量指的是所有作业，例如表创建、加载和 schema 变更作业。如果 FE 中待处理作业的数量达到此值，FE 将拒绝新的加载请求。此参数仅对异步加载生效。从 v2.5 开始，默认值从 100 更改为 1024。
- 引入版本: -

### `disable_load_job`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 集群发生错误时是否禁用加载。这可以防止因集群错误造成的任何损失。默认值为 `FALSE`，表示不禁用加载。`TRUE` 表示禁用加载，集群处于只读状态。
- 引入版本: -

### `empty_load_as_error`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果没有数据加载，是否返回错误消息 "all partitions have no load data"。有效值：
  - `true`: 如果没有数据加载，系统会显示失败消息并返回错误 "all partitions have no load data"。
  - `false`: 如果没有数据加载，系统会显示成功消息并返回 OK，而不是错误。
- 引入版本: -

### `enable_file_bundling`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为云原生表启用 File Bundling 优化。启用此功能 (设置为 `true`) 后，系统会自动捆绑加载、Compaction 或 Publish 操作生成的数据文件，从而降低因频繁访问外部存储系统而产生的 API 成本。您还可以使用 CREATE TABLE 属性 `file_bundling` 在表级别控制此行为。
- 引入版本: v4.0

### `enable_routine_load_lag_metrics`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否收集 Routine Load Kafka 分区偏移量滞后指标。请注意，将此项设置为 `true` 将调用 Kafka API 以获取分区的最新偏移量。
- 引入版本: -

### `enable_sync_publish`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在加载事务的发布阶段同步执行应用任务。此参数仅适用于 Primary Key 表。有效值：
  - `TRUE`（默认）：应用任务在加载事务的发布阶段同步执行。这意味着只有在应用任务完成后，加载事务才会被报告为成功，并且加载的数据才能真正被查询。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `true` 可以提高查询性能和稳定性，但可能会增加加载延迟。
  - `FALSE`: 应用任务在加载事务的发布阶段异步执行。这意味着在应用任务提交后，加载事务被报告为成功，但加载的数据不能立即被查询。在这种情况下，并发查询需要等待应用任务完成或超时才能继续。当任务一次加载大量数据或频繁加载数据时，将此参数设置为 `false` 可能会影响查询性能和稳定性。
- 引入版本: v3.2.0

### `export_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 调度加载作业的时间间隔。
- 引入版本: -

### `export_max_bytes_per_be_per_task`

- 默认值: 268435456
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: 单个数据卸载任务从单个 BE 导出的最大数据量。
- 引入版本: -

### `export_running_job_num_limit`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可并行运行的数据导出任务的最大数量。
- 引入版本: -

### `export_task_default_timeout_second`

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 数据导出任务的超时时长。
- 引入版本: -

### `export_task_pool_size`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 卸载任务线程池的大小。
- 引入版本: -

### `external_table_commit_timeout_ms`

- 默认值: 10000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 提交（发布）写入事务到 StarRocks 外部表的超时时长。默认值 `10000` 表示 10 秒超时时长。
- 引入版本: -

### `finish_transaction_default_lock_timeout_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 完成事务期间获取数据库和表锁的默认超时。
- 引入版本: v4.0.0, v3.5.8

### `history_job_keep_max_second`

- 默认值: 7 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 历史作业（例如 schema 变更作业）可保留的最长时间。
- 引入版本: -

### `insert_load_default_timeout_second`

- 默认值: 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 用于加载数据的 INSERT INTO 语句的超时时长。
- 引入版本: -

### `label_clean_interval_second`

- 默认值: 4 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 标签清理的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保可以及时清理历史标签。
- 引入版本: -

### `label_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在一段时间内可以保留的最大加载作业数。如果超过此数量，将删除历史作业信息。
- 引入版本: -

### `label_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 已完成且处于 FINISHED 或 CANCELLED 状态的加载作业标签的最长保留时间（秒）。默认值为 3 天。此持续时间过后，标签将被删除。此参数适用于所有类型的加载作业。值过大会消耗大量内存。
- 引入版本: -

### `load_checker_interval_second`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 加载作业滚动处理的时间间隔。
- 引入版本: -

### `load_parallel_instance_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制为单个主机上的 broker 和 stream loads 创建的并行加载片段实例的数量。LoadPlanner 将此值用作每个主机的并行度，除非会话启用自适应 sink DOP；如果会话变量 `enable_adaptive_sink_dop` 为 true，则会话的 `sink_degree_of_parallelism` 将覆盖此配置。当需要 shuffle 时，此值应用于片段并行执行（扫描片段和 sink 片段并行执行实例）。当不需要 shuffle 时，它用作 sink 管道 DOP。注意：从本地文件加载被强制为单个实例（管道 DOP = 1，并行执行 = 1）以避免本地磁盘争用。增加此数字会提高每个主机的并发性和吞吐量，但可能会增加 CPU、内存和 I/O 争用。
- 引入版本: v3.2.0

### `load_straggler_wait_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: BE 副本可容忍的最大加载延迟。如果超过此值，将执行克隆以从其他副本克隆数据。
- 引入版本: -

### `loads_history_retained_days`

- 默认值: 30
- 类型: Int
- 单位: 天
- 是否可变: Yes
- 描述: 内部 `_statistics_.loads_history` 表中加载历史记录的保留天数。此值用于表创建以设置表属性 `partition_live_number`，并传递给 `TableKeeper`（最小值为 1）以确定要保留多少个每日分区。增加或减少此值可调整完成的加载作业在每日分区中保留的时间；它会影响新表创建和 keeper 的清理行为，但不会自动重新创建过去的分区。`LoadsHistorySyncer` 在管理加载历史生命周期时依赖此保留；其同步节奏由 `loads_history_sync_interval_second` 控制。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

### `loads_history_sync_interval_second`

- 默认值: 60
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: LoadsHistorySyncer 用于调度从 `information_schema.loads` 到内部 `_statistics_.loads_history` 表的周期性同步的间隔（以秒为单位）。该值在构造函数中乘以 1000 以设置 FrontendDaemon 间隔。同步器跳过第一次运行（以允许表创建），并且仅导入一分钟前完成的加载；较小的值会增加 DML 和执行器负载，而较大的值会延迟历史加载记录的可用性。有关目标表的保留/分区行为，请参阅 `loads_history_retained_days`。
- 引入版本: v3.3.6, v3.4.0, v3.5.0

### `max_broker_load_job_concurrency`

- 默认值: 5
- 别名: `async_load_task_pool_size`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StarRocks 集群中允许的最大并发 Broker Load 作业数。此参数仅对 Broker Load 有效。此参数的值必须小于 `max_running_txn_num_per_db` 的值。从 v2.5 开始，默认值从 `10` 更改为 `5`。
- 引入版本: -

### `max_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 加载作业允许的最大超时时长。如果超过此限制，加载作业将失败。此限制适用于所有类型的加载作业。
- 引入版本: -

### `max_routine_load_batch_size`

- 默认值: 4294967296
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: Routine Load 任务可加载的最大数据量。
- 引入版本: -

### `max_routine_load_task_concurrent_num`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个 Routine Load 作业的最大并发任务数。
- 引入版本: -

### `max_routine_load_task_num_per_be`

- 默认值: 16
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个 BE 上的最大并发 Routine Load 任务数。从 v3.1.0 开始，此参数的默认值从 5 增加到 16，并且不再需要小于或等于 BE 静态参数 `routine_load_thread_pool_size`（已弃用）的值。
- 引入版本: -

### `max_running_txn_num_per_db`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StarRocks 集群中每个数据库允许运行的最大加载事务数。默认值为 `1000`。从 v3.1 开始，默认值从 `100` 更改为 `1000`。当数据库的实际运行加载事务数超过此参数的值时，新的加载请求将不会被处理。同步加载作业的新请求将被拒绝，异步加载作业的新请求将排队。不建议您增加此参数的值，因为这会增加系统负载。
- 引入版本: -

### `max_stream_load_timeout_second`

- 默认值: 259200
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: Stream Load 作业允许的最大超时时长。
- 引入版本: -

### `max_tolerable_backend_down_num`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许的最大故障 BE 节点数。如果超过此数量，Routine Load 作业无法自动恢复。
- 引入版本: -

### `min_bytes_per_broker_scanner`

- 默认值: 67108864
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: Broker Load 实例可处理的最小数据量。
- 引入版本: -

### `min_load_timeout_second`

- 默认值: 1
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 加载作业允许的最小超时时长。此限制适用于所有类型的加载作业。
- 引入版本: -

### `min_routine_load_lag_for_metrics`

- 默认值: 10000
- 类型: INT
- 单位: -
- 是否可变: Yes
- 描述: 在监控指标中显示的 Routine Load 作业的最小偏移量滞后。偏移量滞后大于此值的 Routine Load 作业将显示在指标中。
- 引入版本: -

### `period_of_auto_resume_min`

- 默认值: 5
- 类型: Int
- 单位: 分钟
- 是否可变: Yes
- 描述: Routine Load 作业自动恢复的间隔。
- 引入版本: -

### `prepared_transaction_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 准备事务的默认超时时长。
- 引入版本: -

### `routine_load_task_consume_second`

- 默认值: 15
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 集群中每个 Routine Load 任务消耗数据的最大时间。从 v3.1.0 开始，Routine Load 作业在 `job_properties` 中支持一个新的参数 `task_consume_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本: -

### `routine_load_task_timeout_second`

- 默认值: 60
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 集群中每个 Routine Load 任务的超时时长。从 v3.1.0 开始，Routine Load 作业在 `job_properties` 中支持一个新的参数 `task_timeout_second`。此参数适用于 Routine Load 作业中的单个加载任务，更加灵活。
- 引入版本: -

### `routine_load_unstable_threshold_second`

- 默认值: 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 如果 Routine Load 作业中的任何任务滞后，则将其设置为 UNSTABLE 状态。具体来说，消耗的消息时间戳与当前时间的差值超过此阈值，并且数据源中存在未消耗的消息。
- 引入版本: -

### `spark_dpp_version`

- 默认值: 1.0.0
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 使用的 Spark 动态分区剪枝 (DPP) 版本。
- 引入版本: -

### `spark_home_default_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Spark 客户端的根目录。
- 引入版本: -

### `spark_launcher_log_dir`

- 默认值: `sys_log_dir` + "/spark_launcher_log"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 Spark 日志文件的目录。
- 引入版本: -

### `spark_load_default_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 每个 Spark Load 作业的超时时长。
- 引入版本: -

### `spark_load_submit_timeout_second`

- 默认值: 300
- 类型: long
- 单位: 秒
- 是否可变: No
- 描述: 提交 Spark 应用程序后等待 YARN 响应的最大时间（秒）。`SparkLauncherMonitor.LogMonitor` 将此值转换为毫秒，如果作业在 UNKNOWN/CONNECTED/SUBMITTED 状态停留时间超过此超时，它将停止监控并强制杀死 Spark 启动器进程。`SparkLoadJob` 将此配置作为默认值读取，并允许通过 `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` 属性进行按加载覆盖。将其设置得足够高以适应 YARN 排队延迟；设置得过低可能会中止合法排队的作业，而设置得过高可能会延迟故障处理和资源清理。
- 引入版本: v3.2.0

### `spark_resource_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Spark 依赖包的根目录。
- 引入版本: -

### `stream_load_default_timeout_second`

- 默认值: 600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 每个 Stream Load 作业的默认超时时长。
- 引入版本: -

### `stream_load_max_txn_num_per_be`

- 默认值: -1
- 类型: Int
- 单位: 事务数
- 是否可变: Yes
- 描述: 限制从单个 BE（后端）主机接受的并发 Stream Load 事务数。当设置为非负整数时，FrontendServiceImpl 检查 BE（按客户端 IP）的当前事务计数，如果计数 `>=` 此限制，则拒绝新的 Stream Load 开始请求。值 `< 0` 禁用限制（无限制）。此检查发生在 Stream Load 开始期间，当超出限制时可能会导致 `streamload txn num per be exceeds limit` 错误。相关的运行时行为使用 `stream_load_default_timeout_second` 作为请求超时回退。
- 引入版本: v3.3.0, v3.4.0, v3.5.0

### `stream_load_task_keep_max_num`

- 默认值: 1000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: StreamLoadMgr 在内存中保留的 Stream Load 任务的最大数量（全局范围，跨所有数据库）。当跟踪的任务数量 (`idToStreamLoadTask`) 超过此阈值时，StreamLoadMgr 首先调用 `cleanSyncStreamLoadTasks()` 删除已完成的同步 Stream Load 任务；如果大小仍然大于此阈值的一半，它会调用 `cleanOldStreamLoadTasks(true)` 强制删除较旧或已完成的任务。增加此值可在内存中保留更多任务历史记录；减少此值可减少内存使用并使清理更具侵略性。此值仅控制内存中的保留，不影响持久化/重放的任务。
- 引入版本: v3.2.0

### `stream_load_task_keep_max_second`

- 默认值: 3 * 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 已完成或取消的 Stream Load 任务的保留窗口。当任务达到最终状态且其结束时间戳早于此阈值（`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`）时，它就有资格由 `StreamLoadMgr.cleanOldStreamLoadTasks` 删除，并在加载持久化状态时被丢弃。适用于 `StreamLoadTask` 和 `StreamLoadMultiStmtTask`。如果总任务计数超过 `stream_load_task_keep_max_num`，清理可能会更早触发（同步任务由 `cleanSyncStreamLoadTasks` 优先处理）。设置此值以平衡历史/可调试性与内存使用。
- 引入版本: v3.2.0

### `transaction_clean_interval_second`

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: 已完成事务清理的时间间隔。单位：秒。建议您指定较短的时间间隔，以确保可以及时清理已完成的事务。
- 引入版本: -

### `transaction_stream_load_coordinator_cache_capacity`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 存储事务标签到协调器节点映射的缓存容量。
- 引入版本: -

### `transaction_stream_load_coordinator_cache_expire_seconds`

- 默认值: 900
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 在协调器映射被驱逐（TTL）之前，在缓存中保留的时间。
- 引入版本: -

### `yarn_client_path`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Yarn 客户端包的根目录。
- 引入版本: -

### `yarn_config_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储 Yarn 配置文件的目录。
- 引入版本: -
