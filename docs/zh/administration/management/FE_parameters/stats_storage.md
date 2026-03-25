---
displayed_sidebar: docs
sidebar_label: "统计报告和存储"
---

# FE 配置 - 统计报告和存储

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
- [统计报告](#统计报告)
- [存储](#存储)

## 统计报告

### `enable_collect_warehouse_metrics`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，系统将收集并导出每个仓库的指标。启用它会将仓库级别的指标（槽位/使用率/可用性）添加到指标输出中，并增加指标基数和收集开销。禁用它可省略仓库特定指标，并减少 CPU/网络和监控存储成本。
- 引入版本: v3.5.0

### `enable_http_detail_metrics`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 当为 true 时，HTTP 服务器计算并暴露详细的 HTTP worker 指标（特别是 `HTTP_WORKER_PENDING_TASKS_NUM` gauge）。启用此功能会导致服务器迭代 Netty worker 执行器并调用每个 `NioEventLoop` 上的 `pendingTasks()` 以汇总待处理任务计数；禁用时，gauge 返回 0 以避免此成本。这种额外的收集可能会耗费 CPU 和延迟 — 仅在调试或详细调查时启用。
- 引入版本: v3.2.3

### `proc_profile_collect_time_s`

- 默认值: 120
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 单次进程 profile 收集的持续时间（秒）。当 `proc_profile_cpu_enable` 或 `proc_profile_mem_enable` 设置为 `true` 时，AsyncProfiler 启动，收集器线程休眠此持续时间，然后 profiler 停止并写入 profile。较大的值会增加样本覆盖率和文件大小，但会延长 profiler 运行时并延迟后续收集；较小的值会减少开销，但可能会产生不足的样本。确保此值与 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 等保留设置对齐。
- 引入版本: v3.2.12

## 存储

### `alter_table_timeout_second`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: schema 变更操作 (ALTER TABLE) 的超时时长。
- 引入版本: -

### `capacity_used_percent_high_water`

- 默认值: 0.75
- 类型: double
- 单位: 分数 (0.0–1.0)
- 是否可变: Yes
- 描述: 计算后端负载分数时使用的磁盘容量使用百分比（总容量的百分比）的高水位阈值。`BackendLoadStatistic.calcSore` 使用 `capacity_used_percent_high_water` 设置 `LoadScore.capacityCoefficient`：如果后端的使用百分比小于 0.5，则系数等于 0.5；如果使用百分比 `>` `capacity_used_percent_high_water`，则系数 = 1.0；否则，系数通过 (2 * usedPercent - 0.5) 随使用百分比线性变化。当系数为 1.0 时，负载分数完全由容量比例决定；较低的值会增加副本计数的权重。调整此值会改变平衡器惩罚磁盘利用率高的后端的积极程度。
- 引入版本: v3.2.0

### `catalog_trash_expire_second`

- 默认值: 86400
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 数据库、表或分区删除后，元数据可保留的最长时间。如果此持续时间过期，数据将被删除，并且无法通过 RECOVER 命令恢复。
- 引入版本: -

### `catalog_recycle_bin_erase_min_latency_ms`

- 默认值: 600000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 数据库、表或分区被删除后，擦除元数据前的最小延迟时间（毫秒）。用于避免擦除日志先于删除日志写入。
- 引入版本: -

### `catalog_recycle_bin_erase_max_operations_per_cycle`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 每个周期内从回收站中实际删除数据库、表或分区的最大擦除操作数。擦除操作会持有锁，因此单批次不宜过大。
- 引入版本: -

### `catalog_recycle_bin_erase_fail_retry_interval_ms`

- 默认值: 60000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 回收站擦除操作失败后的重试间隔时间（毫秒）。
- 引入版本: -

### `check_consistency_default_timeout_second`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 副本一致性检查的超时时长。您可以根据 tablet 的大小设置此参数。
- 引入版本: -

### `consistency_check_cooldown_time_second`

- 默认值: 24 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 控制同一 tablet 两次一致性检查之间的最小间隔（以秒为单位）。在 tablet 选择期间，只有当 `tablet.getLastCheckTime()` 小于 `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)` 时，tablet 才被视为符合条件。默认值 (24 * 3600) 强制每个 tablet 大约每天检查一次，以减少后端磁盘 I/O。降低此值会增加检查频率和资源使用；提高此值会以更慢地检测不一致为代价减少 I/O。该值在从索引的 tablet 列表中过滤冷却的 tablet 时全局应用。
- 引入版本: v3.5.5

### `consistency_check_end_time`

- 默认值: "4"
- 类型: String
- 单位: 一天中的小时 (0-23)
- 是否可变: No
- 描述: 指定 ConsistencyChecker 工作窗口的结束小时（一天中的小时）。该值使用 SimpleDateFormat("HH") 在系统时区中解析，并接受 0-23（一位或两位数）。StarRocks 将其与 `consistency_check_start_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口跨越午夜（例如，默认 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器从不运行。解析失败将导致 FE 启动记录错误并退出，因此请提供有效的小时字符串。
- 引入版本: v3.2.0

### `consistency_check_start_time`

- 默认值: "23"
- 类型: String
- 单位: 一天中的小时 (00-23)
- 是否可变: No
- 描述: 指定 ConsistencyChecker 工作窗口的开始小时（一天中的小时）。该值使用 SimpleDateFormat("HH") 在系统时区中解析，并接受 0-23（一位或两位数）。StarRocks 将其与 `consistency_check_end_time` 一起使用，以决定何时调度和添加一致性检查作业。当 `consistency_check_start_time` 大于 `consistency_check_end_time` 时，窗口跨越午夜（例如，默认 `consistency_check_start_time` = "23" 到 `consistency_check_end_time` = "4"）。当 `consistency_check_start_time` 等于 `consistency_check_end_time` 时，检查器从不运行。解析失败将导致 FE 启动记录错误并退出，因此请提供有效的小时字符串。
- 引入版本: v3.2.0

### `consistency_tablet_meta_check_interval_ms`

- 默认值: 2 * 3600 * 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: ConsistencyChecker 用于在 `TabletInvertedIndex` 和 `LocalMetastore` 之间运行完整 tablet 元数据一致性扫描的间隔。`runAfterCatalogReady` 中的守护程序在 `current time - lastTabletMetaCheckTime` 超过此值时触发 checkTabletMetaConsistency。当首次检测到无效 tablet 时，其 `toBeCleanedTime` 设置为 `now + (consistency_tablet_meta_check_interval_ms / 2)`，因此实际删除会延迟到后续扫描。增加此值可减少扫描频率和负载（清理较慢）；减少此值可更快检测和删除陈旧 tablet（开销较高）。
- 引入版本: v3.2.0

### `default_replication_num`

- 默认值: 3
- 类型: Short
- 单位: -
- 是否可变: Yes
- 描述: 设置在 StarRocks 中创建表时每个数据分区的默认副本数。此设置可以在创建表时通过在 CREATE TABLE DDL 中指定 `replication_num=x` 来覆盖。
- 引入版本: -

### `enable_auto_tablet_distribution`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否自动设置 bucket 数量。
  - 如果此参数设置为 `TRUE`，您在创建表或添加分区时无需指定 bucket 数量。StarRocks 会自动确定 bucket 数量。
  - 如果此参数设置为 `FALSE`，您在创建表或添加分区时需要手动指定 bucket 数量。如果您在向表添加新分区时不指定 bucket 数量，新分区将继承表创建时设置的 bucket 数量。但是，您也可以手动为新分区指定 bucket 数量。
- 引入版本: v2.5.7

### `enable_experimental_rowstore`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用混合行存和列存功能。
- 引入版本: v3.2.3

### `enable_fast_schema_evolution`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 StarRocks 集群中的所有表启用 Fast Schema Evolution。有效值为 `TRUE` 和 `FALSE`（默认）。启用 Fast Schema Evolution 可以提高 schema 变更的速度，并在添加或删除列时减少资源使用。
- 引入版本: v3.2.0

> **NOTE**
>
> - StarRocks 存算分离集群从 v3.3.0 开始支持此参数。
> - 如果您需要为特定表配置 Fast Schema Evolution，例如禁用特定表的 Fast Schema Evolution，您可以在表创建时设置表属性 `fast_schema_evolution`。

### `enable_online_optimize_table`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 在创建优化作业时是否使用非阻塞在线优化路径。当 `enable_online_optimize_table` 为 true 且目标表满足兼容性检查（无分区/键/排序规范，分布不是 `RandomDistributionDesc`，存储类型不是 `COLUMN_WITH_ROW`，已启用复制存储，且该表不是云原生表或物化视图）时，规划器会创建一个 `OnlineOptimizeJobV2` 以在不阻塞写入的情况下执行优化。如果为 false 或任何兼容性条件失败，StarRocks 将回退到 `OptimizeJobV2`，这可能会在优化期间阻塞写入操作。
- 引入版本: v3.3.3, v3.4.0, v3.5.0

### `enable_strict_storage_medium_check`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 在用户创建表时是否严格检查 BE 的存储介质。如果此参数设置为 `TRUE`，FE 会在用户创建表时检查 BE 的存储介质，如果 BE 的存储介质与 CREATE TABLE 语句中指定的 `storage_medium` 参数不同，则返回错误。例如，CREATE TABLE 语句中指定的存储介质是 SSD，但 BE 的实际存储介质是 HDD。因此，表创建失败。如果此参数为 `FALSE`，FE 在用户创建表时不会检查 BE 的存储介质。
- 引入版本: -

### `max_bucket_number_per_partition`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个分区中可以创建的最大 bucket 数量。
- 引入版本: v3.3.2

### `max_column_number_per_table`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个表中可以创建的最大列数。
- 引入版本: v3.3.2

### `max_dynamic_partition_num`

- 默认值: 500
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 限制分析或创建动态分区表时可一次性创建的最大分区数。在动态分区属性验证期间，`systemtask_runs_max_history_number` 计算预期分区（结束偏移量 + 历史分区号），如果总数超过 `max_dynamic_partition_num`，则抛出 DDL 错误。仅当您预期合法的大分区范围时才增加此值；增加它允许创建更多分区，但会增加元数据大小、调度工作和操作复杂性。
- 引入版本: v3.2.0

### `max_partition_number_per_table`

- 默认值: 100000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 一个表中可以创建的最大分区数。
- 引入版本: v3.3.2

### `max_task_consecutive_fail_count`

- 默认值: 10
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 任务在调度器自动暂停它之前可能连续失败的最大次数。当 `TaskSource.MV.equals(task.getSource())` 且 `max_task_consecutive_fail_count` 大于 0 时，如果任务的连续失败计数达到或超过 `max_task_consecutive_fail_count`，任务将通过 TaskManager 暂停，并且对于物化视图任务，物化视图将失效。抛出异常，指示暂停以及如何重新激活（例如，`ALTER MATERIALIZED VIEW <mv_name> ACTIVE`）。将此项设置为 0 或负值以禁用自动暂停。
- 引入版本: -

### `partition_recycle_retention_period_secs`

- 默认值: 1800
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: INSERT OVERWRITE 或物化视图刷新操作删除的分区的元数据保留时间。请注意，此类元数据无法通过执行 RECOVER 恢复。
- 引入版本: v3.5.9

### `recover_with_empty_tablet`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否用空的 tablet 替换丢失或损坏的 tablet 副本。如果 tablet 副本丢失或损坏，对此 tablet 或其他健康 tablet 的数据查询可能会失败。用空的 tablet 替换丢失或损坏的 tablet 副本可确保查询仍能执行。但是，由于数据丢失，结果可能不正确。默认值为 `FALSE`，这意味着丢失或损坏的 tablet 副本不会被空的副本替换，并且查询失败。
- 引入版本: -

### `storage_usage_hard_limit_percent`

- 默认值: 95
- 别名: `storage_flood_stage_usage_percent`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: BE 目录中存储使用率的硬限制（百分比）。如果 BE 存储目录的存储使用率（百分比）超过此值，并且剩余存储空间小于 `storage_usage_hard_limit_reserve_bytes`，则加载和恢复作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_usage_percent` 一起设置，以使配置生效。
- 引入版本: -

### `storage_usage_hard_limit_reserve_bytes`

- 默认值: 100 * 1024 * 1024 * 1024
- 别名: `storage_flood_stage_left_capacity_bytes`
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: BE 目录中剩余存储空间的硬限制。如果 BE 存储目录中剩余存储空间小于此值，并且存储使用率（百分比）超过 `storage_usage_hard_limit_percent`，则加载和恢复作业将被拒绝。您需要将此项与 BE 配置项 `storage_flood_stage_left_capacity_bytes` 一起设置，以使配置生效。
- 引入版本: -

### `storage_usage_soft_limit_percent`

- 默认值: 90
- 别名: `storage_high_watermark_usage_percent`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: BE 目录中存储使用率的软限制（百分比）。如果 BE 存储目录的存储使用率（百分比）超过此值，并且剩余存储空间小于 `storage_usage_soft_limit_reserve_bytes`，则 tablet 无法克隆到此目录。
- 引入版本: -

### `storage_usage_soft_limit_reserve_bytes`

- 默认值: 200 * 1024 * 1024 * 1024
- 别名: `storage_min_left_capacity_bytes`
- 类型: Long
- 单位: 字节
- 是否可变: Yes
- 描述: BE 目录中剩余存储空间的软限制。如果 BE 存储目录中剩余存储空间小于此值，并且存储使用率（百分比）超过 `storage_usage_soft_limit_percent`，则 tablet 无法克隆到此目录。
- 引入版本: -

### `tablet_checker_lock_time_per_cycle_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: tablet 检查器在释放并重新获取表锁之前，每个周期最大锁持有时间。小于 100 的值将被视为 100。
- 引入版本: v3.5.9, v4.0.2

### `tablet_create_timeout_second`

- 默认值: 10
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 创建 tablet 的超时时长。从 v3.1 开始，默认值从 1 更改为 10。
- 引入版本: -

### `tablet_delete_timeout_second`

- 默认值: 2
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 删除 tablet 的超时时长。
- 引入版本: -

### `tablet_sched_balance_load_disk_safe_threshold`

- 默认值: 0.5
- 别名: `balance_load_disk_safe_threshold`
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断 BE 磁盘使用率是否平衡的百分比阈值。如果所有 BE 的磁盘使用率都低于此值，则认为平衡。如果磁盘使用率大于此值，并且最高和最低 BE 磁盘使用率之间的差异大于 10%，则认为磁盘使用率不平衡，并触发 tablet 重新平衡。
- 引入版本: -

### `tablet_sched_balance_load_score_threshold`

- 默认值: 0.1
- 别名: `balance_load_score_threshold`
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 用于判断 BE 负载是否平衡的百分比阈值。如果 BE 的负载低于所有 BE 的平均负载，并且差异大于此值，则此 BE 处于低负载状态。相反，如果 BE 的负载高于平均负载，并且差异大于此值，则此 BE 处于高负载状态。
- 引入版本: -

### `tablet_sched_be_down_tolerate_time_s`

- 默认值: 900
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 调度器允许 BE 节点保持非活动状态的最大持续时间。达到时间阈值后，该 BE 节点上的 tablet 将迁移到其他活动 BE 节点。
- 引入版本: v2.5.7

### `tablet_sched_disable_balance`

- 默认值: false
- 别名: `disable_balance`
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否禁用 tablet 平衡。`TRUE` 表示禁用 tablet 平衡。`FALSE` 表示启用 tablet 平衡。
- 引入版本: -

### `tablet_sched_disable_colocate_balance`

- 默认值: false
- 别名: `disable_colocate_balance`
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否禁用 Colocate 表的副本平衡。`TRUE` 表示禁用副本平衡。`FALSE` 表示启用副本平衡。
- 引入版本: -

### `tablet_sched_max_balancing_tablets`

- 默认值: 500
- 别名: `max_balancing_tablets`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可同时平衡的最大 tablet 数量。如果超过此值，将跳过 tablet 重新平衡。
- 引入版本: -

### `tablet_sched_max_clone_task_timeout_sec`

- 默认值: 2 * 60 * 60
- 别名: `max_clone_task_timeout_sec`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 克隆 tablet 的最大超时时长。
- 引入版本: -

### `tablet_sched_max_not_being_scheduled_interval_ms`

- 默认值: 15 * 60 * 1000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: 当 tablet 克隆任务正在调度时，如果 tablet 在此参数指定的时间内未被调度，StarRocks 会给予其更高的优先级，以便尽快调度。
- 引入版本: -

### `tablet_sched_max_scheduling_tablets`

- 默认值: 10000
- 别名: `max_scheduling_tablets`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可同时调度的最大 tablet 数量。如果超过此值，将跳过 tablet 平衡和修复检查。
- 引入版本: -

### `tablet_sched_min_clone_task_timeout_sec`

- 默认值: 3 * 60
- 别名: `min_clone_task_timeout_sec`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 克隆 tablet 的最小超时时长。
- 引入版本: -

### `tablet_sched_num_based_balance_threshold_ratio`

- 默认值: 0.5
- 别名: -
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 基于数量的平衡可能会破坏磁盘大小平衡，但磁盘之间的最大差距不能超过 `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold`。如果集群中有 tablet 不断从 A 平衡到 B，又从 B 平衡到 A，请减小此值。如果您希望 tablet 分布更平衡，请增加此值。
- 引入版本: - 3.1

### `tablet_sched_repair_delay_factor_second`

- 默认值: 60
- 别名: `tablet_repair_delay_factor_second`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 副本修复的间隔，单位为秒。
- 引入版本: -

### `tablet_sched_slot_num_per_path`

- 默认值: 8
- 别名: `schedule_slot_num_per_path`
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 单个 BE 存储目录中可并发运行的 tablet 相关任务的最大数量。从 v2.5 开始，此参数的默认值从 `4` 更改为 `8`。
- 引入版本: -

### `tablet_sched_storage_cooldown_second`

- 默认值: -1
- 别名: `storage_cooldown_second`
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 从表创建时间开始的自动冷却延迟。默认值 `-1` 表示禁用自动冷却。如果需要启用自动冷却，请将此参数设置为大于 `-1` 的值。
- 引入版本: -

### `tablet_stat_update_interval_second`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: No
- 描述: FE 从每个 BE 获取 tablet 统计信息的时间间隔。
- 引入版本: -

### `enable_range_distribution`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为建表启用 Range-based Distribution 语意。
- 引入版本: v4.1.0

### `tablet_reshard_max_parallel_tablets`

- 默认值: 10240
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可并行拆分或合并的 Tablet 最大数量。
- 引入版本: v4.1.0

### `tablet_reshard_target_size`

- 默认值: 1073741824 (1 GB)
- 类型: Int
- 单位: Bytes
- 是否可变: Yes
- 描述: 执行 SPLIT 或 MERGE 操作后，Tablet 的目标大小。
- 引入版本: v4.1.0

### `tablet_reshard_max_split_count`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 旧 Tablet 最多可分割成多少个新 Tablet。
- 引入版本: v4.1.0

### `tablet_reshard_history_job_max_keep_ms`

- 默认值: 259200000 (72 hours)
- 类型: Int
- 单位: Milliseconds
- 是否可变: Yes
- 描述: SPLIT/MERGE 批处理作业历史的最大保留时间。
- 引入版本: v4.1.0
