---
displayed_sidebar: docs
---

# Warehouse 监控指标

从 v3.5 开始，StarRocks 提供了多种指标用于管理 Warehouse 和监控查询队列，当为 Warehouse 启用 Query Queue 功能时，可以使用这些指标。

您可以通过执行以下 SQL 命令为Warehouse启用 Query Queue 功能：

```SQL
ALTER WAREHOUSE <warehouse_name> SET("enable_query_queue" = "true");
```

## 指标项

### warehouse_query_queue

- 类型: Gauge
- 描述: Warehouse 查询队列指标，具有不同的字段标签，用于监控 Warehouse 查询处理的各个方面。

#### 字段标签

##### query_pending_length

- 类型: Gauge
- 描述: 当前在 Warehouse 查询队列中等待的查询数量。

##### query_running_length

- 类型: Gauge
- 描述: 当前在 Warehouse 中运行的查询数量。

##### max_query_queue_length

- 类型: Gauge
- 描述: Warehouse 查询队列的最大长度。

##### earliest_query_wait_time

- 类型: Gauge
- 描述: 队列中最早查询的等待时间（以秒为单位）。如果未设置，返回 `0.0`。

##### max_query_pending_time_second

- 类型: Gauge
- 描述: 查询在 Warehouse 查询队列中处于等待状态的最长时间（以秒为单位）。

##### max_required_slots

- 类型: Gauge
- 描述: 尚未分配 Slot 的查询所需的最大 Slot 数量。

##### sum_required_slots

- 类型: Gauge
- 描述: 尚未分配 Slot 的查询所需的 Slot 总数。

##### remain_slots

- 类型: Gauge
- 描述: Warehouse中剩余的可用 Slot 数量。

##### max_slots

- 类型: Gauge
- 描述: Warehouse中可用的最大 Slot 数量。

## 使用示例

### 监控 Warehouse 查询队列的状态

您可以使用这些指标来监控 Warehouse  的健康状况和性能：

```promql
# 检查所有Warehouse中等待的查询
warehouse_query_queue{field="query_pending_length"}

# 检查所有Warehouse中正在运行的查询
warehouse_query_queue{field="query_running_length"}

# 监控 Slot 利用率
warehouse_query_queue{field="remain_slots"} / warehouse_query_queue{field="max_slots"}
```

## 指标标签

所有 Warehouse 指标包括以下标签：

- `warehouse_id`: Warehouse 的唯一标识符
- `warehouse_name`: Warehouse 的名称
- `field`: 被测量的具体字段（如上所列）

这些标签允许您按特定 Warehouse 过滤和分组指标，并监控其各自的性能特征。

## 指标示例

示例 1: Warehouse 查询队列指标演示：

```Plain
{"tags":{"metric":"warehouse_query_queue","field":"query_pending_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"query_running_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_queue_length","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":1024},
{"tags":{"metric":"warehouse_query_queue","field":"earliest_query_wait_time","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0.0},
{"tags":{"metric":"warehouse_query_queue","field":"max_query_pending_time_second","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":600},
{"tags":{"metric":"warehouse_query_queue","field":"max_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"sum_required_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_query_queue","field":"remain_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
{"tags":{"metric":"warehouse_query_queue","field":"max_slots","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":208},
```
