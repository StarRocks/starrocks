---
displayed_sidebar: docs
description: "用于管理 Warehouse 和监控各 Warehouse 查询队列的指标。"
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

### 按 Warehouse 统计的查询指标

以下指标是集群级 FE 查询指标（`request_total`、`query_total`、`query_err`、`query_timeout`、`query_analysis_err`、`query_internal_err`、`slow_query`、`qps`、`rps`、`query_latency` 和 `query_latency_ms`）的按 Warehouse 版本。它们由每个 FE 节点针对其处理的请求分别维护，带有 `warehouse_id` 和 `warehouse_name` 标签，由 FE 配置项 `enable_per_warehouse_query_metrics` 控制。某个 Warehouse 处理第一个请求时，它的所有指标即被创建（初始值为 0），Warehouse 被删除时这些指标随之移除。通过 `SET_VAR(warehouse='...')` 提示切换 Warehouse 的查询会归入实际执行它的 Warehouse，而 `warehouse_request_total` 始终归入会话所在的 Warehouse。

#### warehouse_request_total

- 类型: Counter
- 描述: 当前 FE 节点上为该 Warehouse 处理的请求总数。

#### warehouse_query_total

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中已结束的查询总数（包含成功和失败的查询）。

#### warehouse_query_err

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中失败的查询总数。

#### warehouse_query_timeout

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中因超时而失败的查询总数。

#### warehouse_query_analysis_err

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中在分析阶段失败的查询总数（例如语法错误或引用了不存在的对象）。

#### warehouse_query_internal_err

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中因内部错误失败的查询总数（既不是分析错误也不是超时的所有失败）。

#### warehouse_slow_query

- 类型: Counter
- 描述: 当前 FE 节点上该 Warehouse 中延迟超过 FE 配置项 `qe_slow_log_ms` 的成功查询总数。

#### warehouse_qps

- 类型: Gauge
- 描述: 当前 FE 节点上该 Warehouse 的每秒查询数，根据 `warehouse_query_total` 在最近一个计算周期（15 秒）内的增量计算。

#### warehouse_rps

- 类型: Gauge
- 描述: 当前 FE 节点上该 Warehouse 的每秒请求数，根据 `warehouse_request_total` 在最近一个计算周期（15 秒）内的增量计算。

#### warehouse_query_latency

- 类型: Gauge
- 描述: 当前 FE 节点上该 Warehouse 在最近一个计算周期（15 秒）内结束的成功查询的延迟（以毫秒为单位）。`type` 标签取值与集群级 `query_latency` 指标一致：
  - `mean`: 平均延迟。
  - `50_quantile`: 中位数延迟。
  - `75_quantile`: 75 分位延迟。
  - `90_quantile`: 90 分位延迟。
  - `95_quantile`: 95 分位延迟。
  - `99_quantile`: 99 分位延迟。
  - `999_quantile`: 99.9 分位延迟。

#### warehouse_query_latency_ms

- 类型: Summary
- 描述: 当前 FE 节点自启动以来该 Warehouse 所有成功查询的延迟（以毫秒为单位）。带有 `quantile` 标签（`0.75`、`0.95`、`0.98`、`0.99`、`0.999`），以及 `_sum` 和 `_count` 两个序列，可配合 `rate()` 计算任意时间窗口内的平均延迟。

## 使用示例

### 监控 Warehouse 查询队列的状态

您可以使用这些指标来监控 Warehouse 的健康状况和性能。

在 FE HTTP 端口的 Prometheus 端点上，该指标暴露的名称是 `starrocks_fe_warehouse_query_queue`。不带前缀的 `warehouse_query_queue` 是内部指标名，仅出现在 [指标示例](#指标示例) 中的 JSON 输出里。

```promql
# 检查所有 Warehouse 中等待的查询
starrocks_fe_warehouse_query_queue{field="query_pending_length"}

# 检查所有 Warehouse 中正在运行的查询
starrocks_fe_warehouse_query_queue{field="query_running_length"}

# 监控 Slot 利用率
starrocks_fe_warehouse_query_queue{field="remain_slots"} / starrocks_fe_warehouse_query_queue{field="max_slots"}
```

### 监控 Warehouse 的查询吞吐和延迟

在 Prometheus 端点上，按 Warehouse 统计的查询指标以 `starrocks_fe_` 为前缀暴露，例如 `starrocks_fe_warehouse_query_total`。计数器按 FE 节点分别统计，按 FE 节点求和即可得到 Warehouse 在整个集群的值。

```promql
# 各 Warehouse 的 QPS，由计数器计算（汇总所有 FE 节点）
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# 各 Warehouse 的 QPS，使用预先计算的 Gauge（每个 FE 节点一条序列）
starrocks_fe_warehouse_qps

# 各 Warehouse 的查询失败率
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_err[1m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_total[1m]))

# 各 Warehouse 最近一个计算周期内的 P95 查询延迟
starrocks_fe_warehouse_query_latency{type="95_quantile"}

# 各 Warehouse 最近 5 分钟的平均查询延迟，由 Summary 计算
sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_sum[5m]))
  / sum by (warehouse_name) (rate(starrocks_fe_warehouse_query_latency_ms_count[5m]))
```

## 采集范围

本页的指标有两种不同的采集方式，不要把它们混在一起 `sum()`：

- `warehouse_query_queue` 和 [`warehouse_cngroup`](./metrics-warehouse_cngroup.md) **仅在 Leader FE 上采集**，由 `enable_collect_warehouse_metrics` 控制。它们来自查询调度器，描述的是整个 Warehouse，不能跨 FE 节点求和。
- 按 Warehouse 统计的查询指标（`warehouse_request_total`、`warehouse_query_*`、`warehouse_slow_query`、`warehouse_qps`、`warehouse_rps`）**在每个 FE 节点上采集**，只统计该节点执行的语句，由 `enable_per_warehouse_query_metrics` 控制。跨 FE 节点求和即为 Warehouse 的总量。

`warehouse_cngroup` 中的查询计数和延迟（`success_queries_count`、`failed_queries_count`、`query_avg_latency_ms` 等）与按 Warehouse 统计的查询指标度量的对象不同，数值不会一致。`warehouse_cngroup` 由调度器在分配到某个 CN Group 的查询结束时记录，并按 `cngroup_name` 拆分。按 Warehouse 统计的查询指标在 FE 语句层为每条语句记录，包括从未到达 CN Group 的语句（分析错误、仅访问元数据的查询、SHOW 语句，这些语句的 `cngroup_name` 为空）。观察调度器视角的 CN Group 状态（节点数、运行中查询、CPU）时使用 `warehouse_cngroup`，观察客户端感受到的吞吐、错误率和延迟时使用按 Warehouse 统计的查询指标，可按 Warehouse 或按 CN Group 查看。

## 指标标签

所有 Warehouse 指标包括以下标签：

- `warehouse_id`: Warehouse 的唯一标识符
- `warehouse_name`: Warehouse 的名称

按 Warehouse 统计的查询指标（`warehouse_query_*`、`warehouse_slow_query`、`warehouse_qps`）额外包含 `cngroup_name` 标签；`warehouse_query_queue` 额外包含 `field` 标签，`warehouse_query_latency` 额外包含 `type` 标签，`warehouse_query_latency_ms` 额外包含 `quantile` 标签：

- `cngroup_name`: 执行该语句的 CN Group。语句没有经过 CN Group 时为空（例如仅访问元数据的语句，或没有计算节点的 Warehouse）。`warehouse_request_total` 和 `warehouse_rps` 不带此标签，因为请求在语句解析前计数；对查询指标按 `cngroup_name` 求和即为 Warehouse 级别的值。
- `field`: 被测量的具体字段（如上所列）
- `type`: 所报告的延迟统计量（如上所列）
- `quantile`: Summary 的分位数（`0.75`、`0.95`、`0.98`、`0.99`、`0.999`）

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

示例 2: 按 Warehouse 统计的查询指标演示：

```Plain
{"tags":{"metric":"warehouse_request_total","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"requests","value":1610},
{"tags":{"metric":"warehouse_query_total","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1532},
{"tags":{"metric":"warehouse_query_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":7},
{"tags":{"metric":"warehouse_query_timeout","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_query_analysis_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":5},
{"tags":{"metric":"warehouse_query_internal_err","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":1},
{"tags":{"metric":"warehouse_slow_query","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"requests","value":3},
{"tags":{"metric":"warehouse_qps","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":12.6},
{"tags":{"metric":"warehouse_rps","warehouse_id":"0","warehouse_name":"default_warehouse"},"unit":"nounit","value":13.2},
{"tags":{"metric":"warehouse_query_latency","type":"mean","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":42.5},
{"tags":{"metric":"warehouse_query_latency","type":"50_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":31.0},
{"tags":{"metric":"warehouse_query_latency","type":"75_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency","type":"90_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":95.0},
{"tags":{"metric":"warehouse_query_latency","type":"95_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency","type":"99_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency","type":"999_quantile","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.75"},"unit":"milliseconds","value":58.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.95"},"unit":"milliseconds","value":120.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.98"},"unit":"milliseconds","value":180.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.99"},"unit":"milliseconds","value":250.0},
{"tags":{"metric":"warehouse_query_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_","quantile":"0.999"},"unit":"milliseconds","value":900.0},
{"tags":{"metric":"warehouse_query_latency_ms_sum","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"milliseconds","value":64812.5},
{"tags":{"metric":"warehouse_query_latency_ms_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1525},
```
