# 物化视图任务运行详情

本文档说明物化视图（MV）刷新任务运行的内部详情，这有助于理解 MV 刷新行为、排查问题和监控 MV 性能。

## 概述

当 StarRocks 刷新物化视图时，它会创建一个包含刷新操作详细信息的任务运行。这些信息存储在 `MVTaskRunExtraMessage` 对象中，可以通过系统视图查询。

## 任务运行额外消息字段

#### forceRefresh
- **类型**: Boolean
- **描述**: 指示这是否是绕过正常刷新条件的强制刷新
- **用法**: 使用 `REFRESH MATERIALIZED VIEW ... FORCE` 手动触发完全刷新时设置为 `true`
- **默认值**: `false`

#### partitionStart
- **类型**: String
- **描述**: 此刷新操作的起始分区边界
- **用法**: 定义要刷新的分区范围的下限
- **格式**: 分区键值（例如，日期分区为 `"2024-01-01"`）
- **示例**: 当从 2024 年 1 月开始刷新数据时，这将是 `"2024-01-01"`

#### partitionEnd
- **类型**: String
- **描述**: 此刷新操作的结束分区边界
- **用法**: 定义要刷新的分区范围的上限
- **格式**: 分区键值（例如，日期分区为 `"2024-01-31"`）
- **示例**: 当刷新截止到 2024 年 1 月的数据时，这将是 `"2024-01-31"`

#### mvPartitionsToRefresh
- **类型**: 字符串集合
- **描述**: 此任务运行中计划刷新的物化视图分区集合
- **用法**: 跟踪将要更新的 MV 分区
- **大小限制**: 自动截断到 `max_mv_task_run_meta_message_values_length`（默认：100）以防止过多的元数据存储
- **示例**: `["p20240101", "p20240102", "p20240103"]`
- **注意**: 这表示 MV 自身的分区，而不是基表分区

#### refBasePartitionsToRefreshMap
- **类型**: 字符串到字符串集合的映射
- **描述**: 将参考基表名称映射到应该刷新的分区集合
- **用法**: 在 MV 计划调度阶段设置；跟踪需要扫描的基表分区
- **格式**: `{tableName -> Set<partitionName>}`
- **大小限制**: 自动截断到 `max_mv_task_run_meta_message_values_length`
- **示例**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "customers": ["p202401"]
  }
  ```
- **注意**: 这是优化前的**计划**分区集合

#### basePartitionsToRefreshMap
- **类型**: 字符串到字符串集合的映射
- **描述**: 将所有基表名称映射到执行期间实际刷新的分区集合
- **用法**: 在 MV 版本映射提交后设置；反映优化器使用的实际分区
- **格式**: `{tableName -> Set<partitionName>}`
- **大小限制**: 自动截断到 `max_mv_task_run_meta_message_values_length`
- **示例**: 
  ```json
  {
    "orders": ["p20240101", "p20240102"],
    "line_items": ["p20240101_batch1", "p20240101_batch2"]
  }
  ```
- **注意**: 这是查询优化和执行后的**实际**分区集合

**refBasePartitionsToRefreshMap 和 basePartitionsToRefreshMap 的区别：**
- `refBasePartitionsToRefreshMap`: 优化前的计划分区（通常用于主参考表）
- `basePartitionsToRefreshMap`: 优化后的实际分区（包括所有表和优化的分区集）

#### nextPartitionStart
- **类型**: String
- **描述**: 下一次增量刷新的起始分区边界
- **用法**: 当刷新操作由于资源限制或大数据量而分割成多个任务运行时使用
- **示例**: 如果当前运行刷新到 `"2024-01-15"`，这可能是 `"2024-01-16"`

#### nextPartitionEnd
- **类型**: String
- **描述**: 下一次增量刷新的结束分区边界
- **用法**: 与 `nextPartitionStart` 配对，定义下一次刷新迭代的范围
- **示例**: 下一批要处理的分区为 `"2024-01-31"`

#### nextPartitionValues
- **类型**: String
- **描述**: 下一次刷新的序列化分区值（用于列表分区或复杂分区方案）
- **用法**: 当简单的开始/结束范围不足时，存储特定的分区值
- **示例**: 多列列表分区的 `"('US', 'ACTIVE'), ('UK', 'ACTIVE')"`

#### processStartTime
- **类型**: Long（毫秒级时间戳）
- **描述**: 任务运行实际开始处理的时间戳（不包括挂起时间）
- **用法**: 用于计算实际处理时间：`finishTime - processStartTime`
- **公式**: `处理时间 = 完成时间 - 处理开始时间`（不包括队列等待时间）
- **示例**: `1704067200000`（2024-01-01 00:00:00 UTC）

#### executeOption
- **类型**: ExecuteOption 对象
- **描述**: 任务执行的配置选项
- **默认值**: Priority = LOWEST, isMergeRedundant = false
- **字段**:
  - `priority`: 任务执行优先级（来自 `Constants.TaskRunPriority` 的值）
    - `HIGHEST`: 0
    - `HIGH`: 32
    - `NORMAL`: 64
    - `LOW`: 96
    - `LOWEST`: 127
  - `isMergeRedundant`: 是否合并冗余刷新操作
  - `properties`: 额外的执行属性（Map<String, String>）

#### planBuilderMessage
- **类型**: 字符串到字符串的映射
- **描述**: 来自查询计划构建器的诊断消息和元数据
- **用法**: 包含有关查询计划、优化决策和潜在问题的信息
- **大小限制**: 自动截断到 `max_mv_task_run_meta_message_values_length`

#### refreshMode
- **类型**: String
- **描述**: 此任务运行使用的刷新模式
- **用法**: 指示 MV 如何刷新
- **可能的值**:
  - `"COMPLETE"`: 所有分区的完全刷新
  - `"PARTIAL"`: 特定分区的增量刷新
  - `"FORCE"`: 绕过过期检查的强制刷新
  - `""`（空）: 默认或未指定模式

#### adaptivePartitionRefreshNumber
- **类型**: Integer
- **描述**: 使用自适应分区刷新时，每次迭代应刷新的分区数
- **用法**: 根据系统资源和数据量自动确定，以优化刷新性能
- **默认值**: -1（未设置或未使用自适应刷新）
- **示例**: `10` 表示一次刷新 10 个分区

## 查询任务运行详情

您可以通过系统视图查询物化视图任务运行信息：

### 使用 information_schema.task_runs

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

### 解析额外消息 JSON

`EXTRA_MESSAGE` 列包含 `MVTaskRunExtraMessage` 的 JSON 表示：

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

## 配置参数

### max_mv_task_run_meta_message_values_length

- **类型**: Integer
- **默认值**: 100
- **描述**: 要在 set/map 字段中存储的最大项数，以防止过多的元数据增长
- **范围**: FE 配置参数
- **用法**: 限制以下大小：
  - `mvPartitionsToRefresh`
  - `refBasePartitionsToRefreshMap`
  - `basePartitionsToRefreshMap`
  - `planBuilderMessage`

## 最佳实践

### 1. 监控刷新性能
- 跟踪 `processStartTime` 与实际完成时间以识别排队问题
- 使用 `adaptivePartitionRefreshNumber` 优化批次大小

### 2. 调试失败的刷新
- 检查 `planBuilderMessage` 以了解优化器问题
- 查看 `refBasePartitionsToRefreshMap` 与 `basePartitionsToRefreshMap` 以了解分区修剪问题

### 3. 优化增量刷新
- 监控 `nextPartitionStart` 和 `nextPartitionEnd` 以理解多迭代刷新模式
- 如果刷新经常跨越多次运行，调整分区粒度

### 4. 理解分区覆盖
- 比较 `mvPartitionsToRefresh` 与 `basePartitionsToRefreshMap` 以查看 MV 到基表分区映射
- 验证基表分区是否与预期的刷新范围一致

## 故障排除

### 问题：刷新时间过长

**检查：**
1. `processStartTime` - 与创建时间的差异很大表示排队
2. `basePartitionsToRefreshMap` - 扫描的分区过多
3. `adaptivePartitionRefreshNumber` - 可能需要针对您的工作负载进行调整

### 问题：刷新了意外的分区

**检查：**
1. `forceRefresh` - 可能设置为 true 导致完全刷新
2. `refBasePartitionsToRefreshMap` - 显示计划的分区
3. `basePartitionsToRefreshMap` - 显示优化后的实际分区
4. 比较两个映射以查看优化器是否更改了计划

### 问题：刷新在多次迭代中停滞

**检查：**
1. `nextPartitionStart` 和 `nextPartitionEnd` - 显示未完成的刷新状态
2. `adaptivePartitionRefreshNumber` - 对于数据量可能太小
3. 考虑增加批次大小或减少分区粒度

## 另请参阅

- [`CREATE MATERIALIZED VIEW`](../sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [`REFRESH MATERIALIZED VIEW`](../sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [Information Schema: task_runs](task_runs.md)
- [物化视图管理](../../using_starrocks/async_mv/Materialized_view.md)

