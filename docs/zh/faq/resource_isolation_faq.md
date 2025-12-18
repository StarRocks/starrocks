---
displayed_sidebar: docs
---

# 资源隔离故障排查

本主题提供了一些关于资源隔离的常见问题解答。

## 资源组

### 在资源组中必须配置哪些资源？

必须配置 CPU 资源限制。您必须设置 `cpu_weight` 或 `exclusive_cpu_core`，且值必须大于 0。

### StarRocks 是否支持硬资源限制？

是的。StarRocks 支持内存的硬限制。从 v3.3.5 开始，StarRocks 支持通过 `exclusive_cpu_cores` 对 CPU 进行硬限制。

### CPU 在资源组之间如何分配？

当多个资源组同时运行查询时，CPU 使用率与每个组的 `cpu_core_limit` 成比例。如果普通组在一个调度周期内超过了 `BE vCPU cores - short_query.cpu_core_limit`，则在该周期内不会进一步调度。

### `short_query` 资源组的资源如何计算？

当 `short_query` 资源组有正在运行的查询时，所有普通组的 CPU 限制变为 `BE vCPU cores − short_query.cpu_core_limit`。如果 `short_query` 资源组空闲，其资源可以被普通组使用。

### 没有匹配资源组的查询如何处理？

它们使用默认资源组 `default_wg`，其资源限制和属性如下：

- `cpu_core_limit` = vCPU cores
- `mem_limit` = 100%
- `type` = `normal`

### 如果资源组 `rg3` 没有查询，所有资源都分配给资源组 `rg1` 和 `rg2`，当 `rg3` 接收到一个大查询时，这些资源会重新分配吗？

会的。回收逐渐发生，并在几十毫秒到几秒内稳定。

### 分类器的作用是什么？如果没有分类器匹配，或者两个分类器重叠怎么办？

如果没有分类器匹配，查询将回退到默认资源组 `default_wg`。分类器有权重；如果一个查询匹配多个分类器，将选择权重最高的。

### 为什么资源组需要 `mem_limit`，如果 BE 默认使用 90% 的内存？

`mem_limit` 在资源组级别限制内存。它仅适用于匹配该资源组的查询。

### 如果资源组的 `query_type` 设置为 `insert`，INSERT INTO SELECT 仅限制 INSERT 还是也限制 SELECT？

仅 SELECT 部分受资源组限制。INSERT 操作不受限制。

### 如果资源组的 `mem_limit` 设置为 `20%`，可用内存是否计算为 `BE_memory * 90% * 20%`？如果有多个资源组，总 `mem_limit` 超过 100% 怎么办？

各组的总 `mem_limit` 可以超过 100%。但如果查询超过其资源组限制，将会失败。

### 如何验证资源组是否应用于查询？

检查 `fe.audit.log` 或运行 `EXPLAIN VERBOSE <SQL>` 查看匹配的资源组。

### 资源组是按集群定义还是按 BE 节点定义？

资源在每个 BE 节点上划分，资源组配置适用于集群中的所有 BE 节点。

### 如何检查资源组使用情况或监控指标？

使用 FE/BE 指标端点查看特定的资源组相关指标。

- 对于 FE，从 `fe_host:8030/metrics?type=json` 收集以下指标：
  - `starrocks_fe_query_resource_group`：在此资源组中历史运行的查询数量（包括当前正在运行的）。
  - `starrocks_fe_query_resource_group_latency`：此资源组的查询延迟百分位数。标签类型指示特定百分位数，包括 `mean`、`75_quantile`、`95_quantile`、`98_quantile`、`99_quantile`、`999_quantile`。
  - `starrocks_fe_query_resource_group_err`：在此资源组中遇到错误的查询数量。

- 对于 BE，从 `be_host:8040/metrics?type=json` 收集以下指标：
  - `starrocks_be_resource_group_cpu_limit_ratio`：此资源组的 `cpu_core_limit` 与所有资源组的总 `cpu_core_limit` 的比率。
  - `starrocks_be_resource_group_mem_limit_bytes`：此资源组的内存限制。

### `short_query` 和普通资源组有什么区别？我可以创建多个 `short_query` 资源组吗？

只允许一个 `short_query` 资源组。当 `short_query` 资源组中有查询运行时，它使用实际的 BE 核心，而普通组按比例共享剩余资源。

### StarRocks 是否提供查询优先级或基于大查询的优先级？

没有优先级系统。当查询超过任何配置的资源阈值时，它就成为“大查询”。

### 资源组属于特定的 BE 节点还是运行查询的 BE？

资源组在集群中的所有 BEs 上统一应用。

### 我应该如何配置 `concurrency_limit`？

这取决于查询的复杂性、集群大小和工作负载模式。

### 基于分类器的匹配如何工作？它是否与用户和/或数据库相关？

匹配取决于分类器属性，如 IP、用户、数据库、角色或 `query_type`。

### 如何通过会话变量指定资源组？

您可以将其设置为变量：

```SQL
SET resource_group = '<resource_group_name>';
```

或通过提示在查询中指定：

```SQL
SELECT /*+ SET_VAR(SET resource_group = '<resource_group_name>') */ * FROM tbl;
```

### 并发控制是全局生效、按用户生效还是按 BE 生效？

`concurrency_limit` 限制每个资源组的并发性，而 `pipeline_dop` 控制单个 Pipeline Instance 的并行度。

### 内存限制是全局生效、按用户生效还是按 BE 生效？

`mem_limit` 应用于每个 BE 的资源组。每个 Instance 的内存由 `exec_mem_limit` 控制。

### 并发和内存限制仅在启用资源组时有效吗？

并发仅由资源组控制。查询并行度由会话变量如 `pipeline_dop` 控制。

### 如果 `query_type` 设置为 `INSERT`，CTAS 任务会匹配资源组吗？

会的。资源组将限制 CTAS 任务的 SELECT 部分的资源。如果 SELECT 部分超过阈值，大查询限制也将生效。

### 为什么 `short_query` 资源组中的查询不能消耗所有 CPU？

`short_query` 资源组必须至少留出 1 个 CPU 核心给普通组。

### 没有查询队列和资源组，是否限制并发查询？

不限制。过载会导致查询超时或内存限制错误。

### 启用资源组但禁用查询队列时，并发是否由资源组限制？

是的。新查询超过资源组的 `concurrency_limit` 将会失败。

### 何时查询被识别为“大查询”？

当查询超过以下任一项时，被视为大查询：

- `big_query_cpu_second_limit`
- `big_query_scan_rows_limit`
- `big_query_mem_limit`

### 可以更改 `default_wg` 资源限制吗？

不可以。作为一种变通方法，创建一个可以匹配所有查询的一般资源组。

示例：

```SQL
CREATE RESOURCE GROUP general_group TO (
    query_type IN ('select', 'insert') 
)
WITH (
    'cpu_core_limit' = '6', 
    'mem_limit' = '0.0000000000001'
);
```

### 为什么“query_resource_group”指标不显示新创建的组？

该指标是惰性初始化的；只有在查询命中该组后才会出现。

### 如果许多查询在一个 BE 上运行，其中一个达到 CPU 限制，是否所有查询都会失败？

只有达到限制的查询会失败。

### 如果 BE 节点有不同的内存/CPU 大小，`mem_limit` 或 `cpu_core_limit` 是否影响结果？

内存限制是硬性的，可能会导致内存较小的 BEs 先失败。CPU 是软性的，不会导致错误。

### `big_query_` 参数是按节点应用还是全局应用？

它们按 BE 节点应用。

### 如何为 Broker Load 配置资源组？

示例分类器：`query_type="insert", user="alice"`。

### 与 `exec_mem_limit` 相关的问题

问：一个查询会生成多少个 Instance ？

答：这是不可预测的，因为不同的查询可以生成不同数量的片段。

问：如何检查 Instance 的数量？

答：无法检查 Instance 的数量。

问：如果一个查询总共消耗 128 GB 内存并生成 60 个 Instance ，而 `query_mem_limit=0` 和 `exec_mem_limit=2G`。查询会失败吗？

答：只要任何 Instance 消耗超过 2 GB (`exec_mem_limit`) 内存，查询就会失败。

### 如何仅为一个资源组禁用全局查询队列？

1. 启用资源组级别的查询队列：

   ```SQL
   SET GLOBAL enable_show_all_variables = true;
   SET enable_group_level_query_queue = true;
   ```

2. 在当前会话或用户级别禁用查询队列：

   ```SQL
   -- 为当前会话禁用查询队列
   SET enable_query_queue = false;
   -- 在用户级别禁用查询队列
   ALTER USER 'xxx' SET PROPERTIES ("session.enable_query_queue" = "false");
   ```

### 可以让未匹配任何资源组的查询强制失败吗？

不可以。它们最终将会回退到默认资源组 `default_wg`。

## 查询队列

### 查询队列内存触发如何计算？

查询队列触发 = BE 可用内存大小 * `query_queue_mem_used_pct_limit`。

### 哪个优先：资源组并发性还是查询队列并发性？

如果 `enable_group_level_query_queue` 设置为：

- `false`：全局或组限制可能会先触发。
- `true`：两者都适用；较小的限制触发排队。

### 如果达到队列大小或超时，查询会立即失败吗？

- 当达到 `query_queue_max_queued_queries` 时，查询立即失败。
- 当达到 `query_queue_concurrency_limit` 时，查询在队列中等待。

### 资源组限制和查询队列限制有什么区别？

资源组限制每个 BE 节点的每组资源使用。

查询队列使用 BE 级别的限制来限制所有查询。

当启用资源组级查询队列时，`concurrency_limit` 和 `max_cpu_cores` 都适用。

### `pipeline_dop`、`exec_mem_limit` 和资源组并发限制有什么区别？

`pipeline_dop` 控制查询内的并行度。

资源组/查询队列控制集群范围内的并发查询。

`query_mem_limit` 控制每个 BE 的每个查询的内存。