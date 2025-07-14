---
displayed_sidebar: docs
---

# 监控 Warehouse CN Group 的指标

从 v4.0 开始，StarRocks 提供了多种指标用于监控和管理 Warehouse 中的计算节点组（CN Group）。

## 指标项

### warehouse_cngroup

- 类型: Gauge/Counter
- 描述: Warehouse CN Group 的指标，具有不同的字段标签，用于监控 CN Group 性能和健康的各个方面。

#### 字段标签

##### cngroup_nodes_count

- 类型: Gauge
- 描述: CN Group 中计算节点的总数。

##### cngroup_alive_nodes_count

- 类型: Gauge
- 描述: CN Group 中存活的计算节点数量。

##### running_queries_count

- 类型: Gauge
- 描述: 在当前 FE 上,CN Group 中正在运行的查询数量。

##### cngroup_status

- 类型: Gauge
- 描述: CN Group 的状态。有效值：`0`（禁用）和 `1`（启用）。

##### scheduled_queries_count

- 类型: Counter
- 描述: 调度到 CN Group 的查询总数。

##### success_queries_count

- 类型: Counter
- 描述: 在 CN Group 中成功执行的查询总数。

##### failed_queries_count

- 类型: Counter
- 描述: 在 CN Group 中失败的查询总数。

##### query_max_latency_ms

- 类型: Gauge
- 描述: CN Group 中查询的最大延迟（以毫秒为单位）。

##### query_avg_latency_ms

- 类型: Gauge
- 描述: CN Group 中查询的平均延迟（以毫秒为单位）。

##### avg_cpu_used_permille

- 类型: Gauge
- 描述: CN Group 中所有计算节点的平均 CPU 使用率（千分比）。如果值无效或不可用，则返回 `-1.0`。

##### max_compute_node_running_queries_count

- 类型: Gauge
- 描述: CN Group 中所有计算节点的最大运行查询数。如果值无效或不可用，则返回 `-1`。

## 使用示例

### 监控 CN Group 的健康和性能

您可以使用这些指标来监控您的 Warehouse CN Group 的健康和性能：

```promql
# 检查 CN Group中节点的可用性
warehouse_cngroup{field="cngroup_alive_nodes_count"} / warehouse_cngroup{field="cngroup_nodes_count"}

# 监控 CN Group状态
warehouse_cngroup{field="cngroup_status"}

# 检查查询成功率
warehouse_cngroup{field="success_queries_count"} / warehouse_cngroup{field="scheduled_queries_count"}

# 监控查询延迟
warehouse_cngroup{field="query_avg_latency_ms"}

# 检查 CPU 利用率
warehouse_cngroup{field="avg_cpu_used_permille"} / 10
```

## 指标标签

所有 Warehouse CN Group 指标都包含以下标签：

- `warehouse_id`: Warehouse 的唯一标识符
- `warehouse_name`: Warehouse 的名称
- `cngroup_name`: CN Group 的名称
- `field`: 被测量的特定字段（如上所列）

这些标签允许您按特定 Warehouse 和 CN Group 过滤和分组指标，以监控它们的个体性能特征。

## 性能注意事项

- CN Group 资源使用指标缓存 1 秒，以避免过多计算
- 当值无效、为空或 NaN 时，CPU 使用率指标返回 `-1.0`
- 当值无效或不可用时，最大运行查询数返回 `-1`
- 查询延迟指标以原子方式更新以确保线程安全

## 指标示例

示例 1: Warehouse CN Group 指标演示：

```Plain
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_alive_nodes_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":2},
{"tags":{"metric":"warehouse_cngroup","field":"running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
{"tags":{"metric":"warehouse_cngroup","field":"cngroup_status","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1},
{"tags":{"metric":"warehouse_cngroup","field":"scheduled_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":98},
{"tags":{"metric":"warehouse_cngroup","field":"success_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":83},
{"tags":{"metric":"warehouse_cngroup","field":"failed_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":15},
{"tags":{"metric":"warehouse_cngroup","field":"query_max_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":1485.0},
{"tags":{"metric":"warehouse_cngroup","field":"query_avg_latency_ms","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"avg_cpu_used_permille","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":54.255102040816325},
{"tags":{"metric":"warehouse_cngroup","field":"max_compute_node_running_queries_count","warehouse_id":"0","warehouse_name":"default_warehouse","cngroup_name":"_builtin_cngroup_0_"},"unit":"nounit","value":0},
```
