# 理解物化视图Task Run

物化视图刷新 Task Run 的信息可以帮助理解物化视图刷新行为、排查问题以及监控性能。

## 概述

当系统刷新物化视图时，会创建一个 Task Run，其中包含有关刷新操作的详细信息。这些信息存储在 `MVTaskRunExtraMessage` 对象中，可以通过查询系统视图 [`information_schema.task_runs`](../../sql-reference/information_schema/task_runs.md) 中的 `EXTRA_MESSAGE` 字段获取。

## Task Run 的 EXTRA MESSAGE

本节描述了 `MVTaskRunExtraMessage` 中提供的字段。

#### `forceRefresh`

- **类型**: Boolean
- **描述**: 指示是否为绕过正常刷新条件的强制刷新。当通过运行 `REFRESH MATERIALIZED VIEW ... FORCE` 手动触发强制全量刷新时，返回 `true`。

#### `partitionStart`

- **类型**: String
- **描述**: 此刷新操作的起始分区边界。定义要刷新的分区范围的下限。
- **格式**: 分区键值（例如，日期分区为 `"2024-01-01"`）
- **示例**: 当刷新从 2024 年 1 月开始的数据时，此字段为 `"2024-01-01"`。

#### `partitionEnd`

- **类型**: String
- **描述**: 此刷新操作的结束分区边界。定义要刷新的分区范围的上限。
- **格式**: 分区键值（例如，日期分区为 `"2024-01-31"`）
- **示例**: 当刷新到 2024 年 1 月的数据时，此字段为 `"2024-01-31"`。

#### `mvPartitionsToRefresh`

- **类型**: Set of Strings
- **描述**: 在Task Run中计划刷新的物化视图分区列表。此项帮助您跟踪哪些物化视图分区将被更新。
- **大小限制**: 如果实际数量超过该值，系统将自动将分区数量截断为 `max_mv_task_run_meta_message_values_length`（默认值：100），以防止元数据存储过多。
- **示例**: `["p20240101", "p20240102", "p20240103"]`
- **注意**: 这表示物化视图自身的分区，而不是基表分区。

#### `refBasePartitionsToRefreshMap`

- **类型**: Map of String to Set of Strings
- **描述**: 参考基表与其应刷新的分区集合之间的映射。
- **用法**: 该值在物化视图计划调度阶段设置。可用于跟踪需要扫描的基表分区。
- **格式**: `{tableName -> Set<partitionName>}`
- **大小限制**: 如果实际数量超过该值，系统将自动将分区数量截断为 `max_mv_task_run_meta_message_values_length`（默认值：100），以防止元数据存储过多。
- **示例**:

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```

- **注意**: 这是优化前的**计划**分区集。

#### `basePartitionsToRefreshMap`

- **类型**: Map of String to Set of Strings
- **描述**: 参考基表与在执行期间刷新的分区集合之间的映射。
- **用法**: 该值在物化视图版本映射提交后设置。它反映了优化器使用的实际分区。
- **格式**: `{tableName -> Set<partitionName>}`
- **大小限制**: 如果实际数量超过该值，系统将自动将分区数量截断为 `max_mv_task_run_meta_message_values_length`（默认值：100），以防止元数据存储过多。
- **示例**: 

  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```

- **注意**: 这是查询优化和执行后的**实际**分区集。

:::note

**refBasePartitionsToRefreshMap 和 basePartitionsToRefreshMap 的区别:**
- `refBasePartitionsToRefreshMap`: 优化前的计划分区（通常用于主要参考表）
- `basePartitionsToRefreshMap`: 优化后的实际分区（包括所有表和优化的分区集）

:::

#### `nextPartitionStart`

- **类型**: String
- **描述**: 下次增量刷新操作的起始分区边界。当由于资源限制或数据量大而将刷新操作分成多个Task Run时，此值定义要刷新的分区范围的下限。
- **示例**: 如果当前Task Run刷新到 `"2024-01-15"` 的数据，此字段的值可能为 `"2024-01-16"`。

#### `nextPartitionEnd`

- **类型**: String
- **描述**: 下次增量刷新操作的结束分区边界。当由于资源限制或数据量大而将刷新操作分成多个Task Run时，此值定义要刷新的分区范围的上限。
- **示例**: `"2024-01-31"` 为下一批要处理的分区。

#### `nextPartitionValues`

- **类型**: String
- **描述**: 下次刷新操作的序列化分区值（用于 list 分区或复杂分区方案）。当简单的起始/结束范围不足时，存储特定的分区值。
- **示例**: `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"` 用于多列 list 分区。

#### `processStartTime`

- **类型**: Integer (timestamp in milliseconds)
- **描述**: Task Run实际开始处理的时间戳（不包括等待时间）。可以使用公式 `Processing Time = Finish Time - Process Start Time` 计算实际处理时间（不包括队列等待时间）。
- **示例**: `1704067200000` (2024-01-01 00:00:00 UTC)

#### `executeOption`

- **类型**: ExecuteOption object
- **描述**: 任务执行的配置选项。
- **默认值**: `Priority` = `LOWEST`, `isMergeRedundant` = `false`
- **字段**:
  - `priority`: 任务执行优先级（值来自 `Constants.TaskRunPriority`）
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: 是否合并冗余的刷新操作。
  - `properties`: 额外的执行属性，格式为 `Map<String, String>`。

#### `planBuilderMessage`

- **类型**: Map of String to String
- **描述**: 查询计划构建器的诊断消息和元数据。包含有关查询计划、优化决策和潜在问题的信息。
- **大小限制**: 如果实际数量超过该值，系统将自动将分区数量截断为 `max_mv_task_run_meta_message_values_length`（默认值：100），以防止元数据存储过多。

#### `refreshMode`

- **类型**: String
- **描述**: 此Task Run的刷新模式。指示物化视图是如何刷新的。
- **有效值**:
  - `"COMPLETE"`: 全量刷新所有分区
  - `"PARTIAL"`: 增量刷新特定分区
  - `"FORCE"`: 强制刷新，绕过陈旧性检查
  - `""` (空): 默认或未指定

#### `adaptivePartitionRefreshNumber`

- **类型**: Integer
- **描述**: 使用自适应分区刷新时，每次迭代应刷新的分区数量。此值根据系统资源和数据量自动确定，以优化刷新性能。
- **默认值**: `-1`（表示未设置或未使用自适应刷新）
- **示例**: `10`（表示一次刷新 10 个分区）

## 查询Task Run详情

您可以通过系统视图 `information_schema.task_runs` 查询物化视图Task Run信息。

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    FINISH_TIME,
    STATE,
    EXTRA_MESSAGE
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%'
ORDER BY CREATE_TIME DESC
LIMIT 10;
```

`EXTRA_MESSAGE` 列包含 `MVTaskRunExtraMessage` 的 JSON 表示。

您可以进一步解析 JSON 字符串 `EXTRA_MESSAGE` 以提高可读性。

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS refresh_mode,
    get_json_string(EXTRA_MESSAGE, '$.forceRefresh') AS force_refresh,
    get_json_string(EXTRA_MESSAGE, '$.mvPartitionsToRefresh') AS mv_partitions,
    get_json_int(EXTRA_MESSAGE, '$.processStartTime') AS process_start_ms,
    get_json_int(EXTRA_MESSAGE, '$.adaptivePartitionRefreshNumber') AS adaptive_batch_size
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## 理解刷新性能

### 计算处理时间

```sql
SELECT 
    TASK_NAME,
    FINISH_TIME,
    get_json_bigint(EXTRA_MESSAGE, '$.processStartTime') AS process_start_time,
    (unix_timestamp(FINISH_TIME) * 1000 - 
     get_json_bigint(EXTRA_MESSAGE, '$.processStartTime')) / 1000 AS processing_seconds
FROM information_schema.task_runs
WHERE TASK_NAME LIKE 'mv-%' AND STATE = 'SUCCESS';
```

### 分析分区刷新模式

```sql
SELECT 
    TASK_NAME,
    CREATE_TIME,
    get_json_string(EXTRA_MESSAGE, '$.partitionStart') AS start_partition,
    get_json_string(EXTRA_MESSAGE, '$.partitionEnd') AS end_partition,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionStart') AS next_start,
    get_json_string(EXTRA_MESSAGE, '$.nextPartitionEnd') AS next_end
FROM information_schema.task_runs
WHERE TASK_NAME = 'mv-12345'
ORDER BY CREATE_TIME DESC;
```

## 配置项

### max_mv_task_run_meta_message_values_length

- **类型**: Integer
- **默认值**: 100
- **范围**: FE 配置
- **描述**: 限制集合或 MAP 字段中存储的项目数量，以防止元数据过度增长。限制 `mvPartitionsToRefresh`、`refBasePartitionsToRefreshMap`、`basePartitionsToRefreshMap` 和 `planBuilderMessage` 的大小。

## 最佳实践

### 监控刷新性能

- 比较 `processStartTime` 和实际完成时间以识别排队问题。
- 使用 `adaptivePartitionRefreshNumber` 优化批处理大小。

### 调试刷新失败

- 检查 `planBuilderMessage` 以查找优化器问题。
- 比较 `refBasePartitionsToRefreshMap` 和 `basePartitionsToRefreshMap` 以解决分区裁剪问题。

### 优化增量刷新

- 监控 `nextPartitionStart` 和 `nextPartitionEnd` 以了解多次迭代刷新模式。
- 如果刷新频繁跨越多次运行，请调整分区粒度。

### 理解分区覆盖

- 比较 `mvPartitionsToRefresh` 和 `basePartitionsToRefreshMap` 以检查物化视图到基表的分区映射。
- 验证基表分区是否与预期的刷新范围对齐。

## 故障排除

### 问题: 刷新耗时过长

**检查:**

1. `processStartTime` - 与创建时间的显著差异表明Task Run在队列中。
2. `basePartitionsToRefreshMap` - 显著值表明扫描的分区过多。
3. `adaptivePartitionRefreshNumber` - 可能需要调整工作负载。

### 问题: 刷新了意外的分区

**检查:**

1. `forceRefresh` - 如果返回 `true`，表示执行了强制全量刷新。
2. `refBasePartitionsToRefreshMap` - 此字段显示计划的分区。
3. `basePartitionsToRefreshMap` - 此字段显示优化后的实际分区。
4. 比较上述两个映射以查看优化器是否更改了计划。

### 问题: 刷新卡在多次迭代中

**检查:**

1. `nextPartitionStart` 和 `nextPartitionEnd` - 此字段显示未完成的刷新状态。
2. `adaptivePartitionRefreshNumber` - 可能需要调整工作负载。
3. 考虑增加批处理大小或减少分区粒度。

## 另请参阅

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema.task_runs](../../sql-reference/information_schema/task_runs.md)
- [Materialized View Management](./Materialized_view.md)