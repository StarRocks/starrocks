---
displayed_sidebar: docs
description: "StarRocks supports query queues from v2.5 to automatically queue queries when concurrent count or resource usage reaches thresholds, preventing cluster overload."
---

# 查询队列

本文档介绍如何在 StarRocks 中管理查询队列。

自 v2.5 版本起，StarRocks 支持查询队列功能。启用查询队列后，StarRocks 会在并发查询数量或资源使用率达到一定阈值时自动对查询进行排队，从而避免过载加剧。待执行查询将在队列中等待直至有足够的计算资源时开始执行。

查询队列分为两个版本：

- [**Query Queue v1**](#query-queue-v1)：基于并发查询数量、BE 内存使用率和 BE CPU 使用率触发排队。本文档中原有的查询队列配置和行为均属于 v1。自 v3.1.4 版本起，v1 支持设置资源组粒度的查询队列。
- [**Query Queue v2**](#query-queue-v2)：自 v3.3 版本起支持。v2 会估算每个 Query 消耗的 BE 资源，并将 BE 资源抽象为逻辑 slot，根据 Query 需要的 slot 数量进行排队和调度。

## Query Queue v1

Query Queue v1 支持为 CPU 使用率、内存使用率和查询并发度设置阈值以触发查询队列。

**Roadmap**:

| 版本   | 全局查询队列 | 资源组粒度查询队列 | 并发数量集中管理 | 并发度动态调整 |
| ------ | --------- | --------------- | ------------- | ------------ |
| v2.5   | ✅        | ❌               | ❌            | ❌           |
| v3.1.4 | ✅        | ✅               | ✅            | ✅           |

### 启用 Query Queue v1

StarRocks 默认关闭查询队列。您可以通过设置相应的全局会话变量（Global session variable）来为 INSERT 导入、SELECT 查询和统计信息查询启用全局或资源组粒度的查询队列。

#### 启用全局查询队列

设置以下全局会话变量来为导入任务、SELECT 查询或统计信息查询启用全局查询队列管理。

- 为导入任务启用查询队列：

```SQL
SET GLOBAL enable_query_queue_load = true;
```

- 为 SELECT 查询启用查询队列：

```SQL
SET GLOBAL enable_query_queue_select = true;
```

- 为统计信息查询启用查询队列：

```SQL
SET GLOBAL enable_query_queue_statistic = true;
```

#### 启用资源组粒度查询队列

从 v3.1.4 开始，StarRocks 支持资源组粒度查询队列。

如需启用资源组粒度查询队列，除上述全局会话变量之外，您还需要额外设置 `enable_group_level_query_queue`。

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

:::note

在存算分离集群中，上述全局会话变量无法启用查询队列。查询队列需要按 Warehouse 分别启用，且只有在 Warehouse 属性 `enable_query_queue` 设置为 `true` 之后，`enable_group_level_query_queue` 才会生效。详细信息，参见 [存算分离集群中的 Query Queue v2](#存算分离集群中的-query-queue-v2)。

:::

### 指定资源阈值

#### 全局粒度的资源阈值

您可以通过以下全局会话变量设置触发查询队列的阈值：

| **变量**                            | **默认值** | **描述**                                                     |
| ----------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0          | 单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。      |
| query_queue_mem_used_pct_limit      | 0          | 单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。取值范围：[0, 1] |
| query_queue_cpu_used_permille_limit | 0          | 单个 BE 节点中 CPU 使用千分比上限（即 CPU 使用率 * 1000）。仅在设置为大于 `0` 后生效。设置为 `0` 表示没有限制。取值范围：[0, 1000] |

:::note

- 启用 Query Queue v2 后，不再支持通过 `query_queue_concurrency_limit`、`query_queue_mem_used_pct_limit` 和 `query_queue_cpu_used_permille_limit` 触发排队。
- 默认设置下，BE 每隔一秒向 FE 报告资源使用情况。您可以通过设置 BE 配置项 `report_resource_usage_interval_ms` 来更改此间隔时间。

:::

#### 资源组粒度的资源阈值

从 v3.1.4 开始，您可以在创建资源组时为其设置各自的并发查询上限 `concurrency_limit` 和 CPU 核数上限 `max_cpu_cores`。您还可以为资源组设置内存使用率阈值 `mem_used_pct_limit`。当发起一个查询时，如果任意一项资源占用超过了全局粒度或资源组粒度的资源阈值，那么查询会进行排队，直到所有资源都没有超过阈值，再执行该查询。

| **属性**           | **默认值** | **描述**                                                                            |
|--------------------|------------|--------------------------------------------------------------------------------------|
| concurrency_limit  | 0          | 该资源组在单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。                     |
| max_cpu_cores      | 0          | 该资源组在单个 BE 节点中使用的 CPU 核数上限。仅在设置为大于 `0` 后生效。取值范围：[0, `avg_be_cpu_cores`]，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。 |
| mem_used_pct_limit | 0          | 该资源组在单个 BE 节点中的内存使用率上限。仅在设置为大于 `0` 后生效。取值范围：[0, 1] |

`mem_used_pct_limit` 仅适用于 Query Queue v1。启用 Query Queue v2（将 `enable_query_queue_v2` 设置为 `true`）后，该参数不再生效。

您可以通过 SHOW USAGE RESOURCE GROUPS 来查看每个资源组在每个 BE 上的资源使用信息，参见[查看资源组的使用信息](./resource_group.md#查看资源组的使用信息)。

#### 管理查询并发数量

当正在运行的查询数量 `num_running_queries` 超过全局粒度或资源组粒度的 `concurrency_limit`  时，新到来的查询会进行排队。在 `<` v3.1.4  和 `>=` v3.1.4 版本中，获取 `num_running_queries` 的方式不同。

- `<` v3.1.4 版本，`num_running_queries` 由 BE 周期性汇报得出正在运行的查询数量，汇报周期为 `report_resource_usage_interval_ms`。所以，系统对于 `num_running_queries` 的变化感知会有一定的延迟。例如，如果当下 BE 汇报的 `num_running_queries` 没有超过全局粒度和资源组粒度的 `concurrency_limit`，但是在下次汇报前如果发起了大量查询，超过了 `concurrency_limit` 的限制，那么这些新查询也都会执行，而不会进行排队。

- `>=` v3.1.4 版本，所有 FE 正在运行的查询数量 `num_running_queries` 由 Leader FE 集中管理。每个 Follower FE 在发起和结束一个查询时，会通知 Leader FE，从而可以应对短时间内查询激增超过了 `concurrency_limit` 的场景。

### 配置 Query Queue v1

您可以通过以下全局会话变量设置查询队列的容量和队列中查询的最大超时时间：

| **变量**                           | **默认值** | **描述**                                                     |
| ---------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024       | 队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。 |
| query_queue_pending_timeout_second | 300        | 队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。单位：秒。 |

### 根据查询并发数量动态调整查询并发度

从 v3.1.4 版本起，对于被查询队列管理的由 Pipeline Engine 运行的查询，StarRocks 可以根据当前正在运行的查询数量 `num_running_queries`、Fragment 数量 `num_fragments`、查询并发度 `pipeline_dop` 动态调整新到来查询的并发度 `pipeline_dop`。您可以通过这种方式动态控制查询并发数量，在保证 BE 资源充分利用的基础上，降低调度的开销。关于 Fragment 和查询并发度 `pipeline_dop`，参见[查询管理-调整查询并发度](./Query_management.md#调整查询并发度)。

针对每个查询队列下的查询，StarRocks 会维护一个逻辑 Driver 数量 `num_drivers`，用以表示该查询的所有 Fragment 在一个 BE 上总的并发数量。`num_drivers` 的值等于 `num_fragments*pipeline_dop`。当新到来一个查询时，StarRocks 会根据以下逻辑调整查询并发度 `pipeline_dop`：

- 正在运行的 Driver 数量 `num_drivers` 超过查询并发 Driver 低位上限 `query_queue_driver_low_water` 越多，查询并发度 `pipeline_dop` 越小。
- StarRocks 尽量保证正在运行的 Driver 数量 `num_drivers` 不超过查询并发 Driver 高位上限 `query_queue_driver_high_water`。

您可以通过以下全局会话变量控制查询并发度 `pipeline_dop` 的动态调整：

| **变量**                      | **默认值** | **描述**                                                     |
| ----------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_driver_high_water | -1         | 查询并发 Driver 高位上限。仅在设置为大于 `0` 后生效。等于 `0` 时，会设置为 `avg_be_cpu_cores*16`，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。大于 `0` 时，会直接使用该值。 |
| query_queue_driver_low_water  | -1         | 查询并发 Driver 低位上限。仅在设置为大于 `0` 后生效。等于 `0` 时，会设置为 `avg_be_cpu_cores*8`。大于 `0` 时，会直接使用该值。 |

## Query Queue v2

自 v3.3 版本起，StarRocks 支持 Query Queue v2。v2 不再基于并发查询数量、BE 内存使用率或 BE CPU 使用率的固定阈值触发排队，而是估算每个 Query 需要消耗的 BE 资源，并基于逻辑 slot 进行排队和调度。如果可用 Slot 不足，该查询将在队列中等待，直到释放出足够 Slot 为止。

### 配置 Query Queue v2

在存算一体集群中，Query Queue v2 通过 FE 配置项启用和调整。其中，修改 `enable_query_queue_v2` 后，需要重启 FE 节点才能生效。

:::note

在存算分离集群中，`enable_query_queue_v2` 不生效。Query Queue v2 需要按 Warehouse 分别启用和调整。详细信息，参见 [存算分离集群中的 Query Queue v2](#存算分离集群中的-query-queue-v2)。

:::

| 配置项 | 默认值 | 含义 |
| ------ | ------ | ---- |
| `enable_query_queue_v2` | `false`（v3.3 至 v4.0）<br />`true`（自 v4.1 起） | 是否启用 Query Queue v2。设置为 `true` 后，StarRocks 使用 v2 基于 slot 的查询调度机制。该配置项仅适用于存算一体集群。 |
| `query_queue_v2_concurrency_level` | `4` | Query Queue v2 计算集群总 slot 数量时使用的逻辑并发层数。值越大，系统可放行的 Query 越多，是一个相对调节参数。 |
| `query_queue_slots_estimator_strategy` | `PBE` | 队列查询使用的 Slot 估算策略。支持的取值包括：`PBE`（基于并行度，默认值）、`MBE`（基于内存成本）和 `CBE`（基于 CPU 成本）。PBE 根据扫描并行度估算查询所需的 Slot 数，并以 Worker 数量为上限。对于 OLAP 表，它使用裁剪（Pruning）后剩余的 Scan Range 数量进行估算，因此只有极小型查询的 Slot 数才会低于 Worker 数量。对于 Connector 或外部表扫描，则会按全并行扫描处理（即 Worker 数量），而不是作为单 Slot 查询。MBE 根据查询的内存成本除以 `query_queue_v2_mem_bytes_per_slot` 来估算 Slot 数。CBE 根据执行计划的 CPU 成本除以 `query_queue_v2_cpu_costs_per_slot` 来估算 Slot 数。MBE 和 CBE 计算出的每个查询的 Slot 数还会受到 `number_of_workers * max(1, pipeline_dop / 2)` 的限制。为了保证向前兼容，历史取值 `MAX` 和 `MIN` 仍然可以使用，但都会被视为默认估算策略；其他任何取值都会在配置校验时被拒绝。 |
| `query_queue_v2_schedule_strategy` | `SWRR` | Query Queue V2 对等待中的查询进行排序时使用的调度策略。支持的取值（不区分大小写）包括：`SWRR`（Smooth Weighted Round Robin，默认值），适用于需要公平加权调度的混合工作负载；以及 `SJF`（Short Job First + Aging），优先调度短任务，同时通过 Aging 机制避免任务饥饿。该配置项通过大小写不敏感的枚举解析；如果指定了无法识别的值，系统会记录错误日志并回退到默认调度策略。该配置仅在启用 Query Queue V2 时生效，并与 `query_queue_v2_concurrency_level` 等 V2 容量配置共同影响调度行为。 |
| `query_queue_v2_mem_bytes_per_slot` | `0` | 基于内存成本估算策略（MBE）使用的每 Slot 内存目标值。当 `query_queue_slots_estimator_strategy` 设置为 `MBE` 时，总 Slot 数由 Warehouse 的内存预算计算得到，而单个查询所需的 Slot 数由其总内存成本除以该值计算，并限制在 `number_of_workers * max(1, pipeline_dop / 2)` 以内。如果该值小于等于 0，则 Query Queue V2 会使用每个 Worker 每个 CPU Core 的平均可用内存作为默认值。 |
| `query_queue_v2_cpu_costs_per_slot` | `1000000000` | 基于 CPU 成本估算策略（CBE）使用的每 Slot CPU 成本阈值，用于根据查询执行计划的 CPU 成本估算所需的 Slot 数。调度器按照 `ceil(plan_cpu_costs / query_queue_v2_cpu_costs_per_slot)` 计算 Slot 数，并将结果限制在 `[1, min(totalSlots, number_of_workers * max(1, pipeline_dop / 2))]` 范围内。如果该值小于等于 0，则会自动规范化为 `1`。增大该值会减少每个查询分配的 Slot 数，从而提高整体并发能力；减小该值则会增加每个查询分配的 Slot 数，从而降低并发能力。 |
| `query_queue_concurrency_limit` | `0` | 单个 BE 上允许同时运行的查询数上限。仅当该值大于 `0` 时才生效。设置为 `0` 表示不限制并发查询数量。 |

:::note

`query_queue_mem_used_pct_limit` 和 `query_queue_cpu_used_permille_limit` 仅适用于 Query Queue v1。启用 Query Queue v2 后，上述参数不再生效。

:::

### 资源 Slot

Query Queue v2 将 BE 资源表示为逻辑 Slot：

- **集群总 Slot 数量**：StarRocks 会为整个集群设置一个逻辑上的 Slot 总量。该总量与 BE 数量和 BE CPU Core 数量成正相关，也会受 `query_queue_v2_concurrency_level` 影响。
- **Query 需要的 Slot 数量**：StarRocks 会为每个 Query 估算需要消耗的 Slot 数量。估算依据包括统计信息、查询复杂度、Fragment 数量、复杂算子的输入和输出数据量估计，以及 DOP 等因素。

### 排队逻辑

当一个 Query 需要的 Slot 数量超过当前剩余的 Slot 数量时，该 Query 会进入队列等待。Query Queue v2 会优先满足 Slot 需求量较小的 Query，使小查询可以先获得资源，避免大查询长期占用队首导致后续小查询被阻塞，即队头阻塞（Head-of-line blocking）问题。

整个排队逻辑都在 FE 上完成，包括设置集群总 Slot 数量、估算 Query 需要的 Slot 数量，以及决定优先满足哪个 Query 的 Slot 需求。Query Queue v2 不会根据 BE 的实际资源使用情况进行调度。

### 选择估算策略

#### PBE

基于并行度估算（PBE）适用于以下场景：

- 常规报表查询
- 点查与大查询混合的工作负载
- 不希望深入了解成本模型细节的用户
- 希望优先获得稳定、简单且易于解释的排队行为的 DBA

使用 PBE 时，通常具有以下特点：

- 点查或经过裁剪后扫描数据量较少的查询会使用较少的 Slot。
- 扫描范围较大的查询会使用更多的 Slot。
- 在业务高峰期间，小查询更容易获得执行资源。

以下示例将 PBE 设置为估算策略：

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "PBE");
```

#### MBE

基于内存成本估算（MBE）适用于存在内存压力的场景，例如大规模 Join、大规模聚合或高基数聚合。

以下示例将 MBE 设置为估算策略，并为每个 Slot 分配 2 GB 内存：

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "MBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_mem_bytes_per_slot" = "2147483648");
```

MBE 使用查询总内存成本除以该值计算查询所需的 Slot 数，并使用 Warehouse 的总内存预算除以该值计算总 Slot 数。

可以根据以下方向对 MBE 进行调优：

**现象：内存仍然很容易耗尽**

- **调整方式**：降低 `query_queue_v2_concurrency_level`
- **效果**：直接降低 MBE 使用的总内存预算。

**现象：查询排队严重，但 BE 内存仍有余量**

- **调整方式**：提高 `query_queue_v2_concurrency_level`
- **效果**：直接提高 MBE 使用的总内存预算。

**现象：`max_slots` 很小，整数取整带来的误差较明显**

- **调整方式**：减小 `query_queue_v2_mem_bytes_per_slot`
- **效果**：使用更细粒度的内存 Slot，降低整数取整带来的误差。

#### CBE

基于 CPU 成本估算（CBE）适用于 CPU 压力较大的场景，例如计算密集型 SQL、复杂表达式，或扫描完成后仍需大量 CPU 计算的查询。

以下示例将 CBE 设置为估算策略，并将每个 Slot 的 CPU 成本阈值设置为 `1000000000`：

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_slots_estimator_strategy" = "CBE");
ADMIN SET FRONTEND CONFIG ("query_queue_v2_cpu_costs_per_slot" = "1000000000");
```

**现象：CPU 经常达到饱和**

- **调整方式**：减小 `query_queue_v2_cpu_costs_per_slot`
- **效果**：相同 CPU 成本对应更多 Slot，使系统采用更保守的并发策略。

**现象：查询排队明显，但 CPU 仍有余量**

- **调整方式**：增大 `query_queue_v2_cpu_costs_per_slot`
- **效果**：相同 CPU 成本对应更少 Slot，提高系统并发能力。

### 调整并发容量

如果只是希望提高或降低整体并发能力，不建议首先在 PBE、MBE 和 CBE 之间切换，而应优先调整总 Slot 容量：

```SQL
ADMIN SET FRONTEND CONFIG ("query_queue_v2_concurrency_level" = "<value>");
```

推荐按照以下步骤进行调优：

1. 从默认值 `4` 开始。
2. 观察 `remain_slots`、`max_slots`、`query_pending_length`、CPU、内存以及查询延迟等指标。
3. 如果资源仍有余量，但查询排队明显，则逐步提高 `query_queue_v2_concurrency_level`。
4. 如果资源经常饱和，或者查询之间资源竞争严重，则逐步降低 `query_queue_v2_concurrency_level`。
5. 每次调整幅度建议控制在 10%～25%，并至少观察一个业务高峰周期后，再进行下一次调整。

**调优优先级**：应优先使用 `query_queue_v2_concurrency_level` 调整整体容量。只有在完成整体容量调优之后，再考虑是否切换到 MBE 或 CBE 等不同估算策略。调优初期不要同时修改多个参数，否则很难判断具体是哪一个参数产生了效果。

#### 回退并发上限

`query_queue_concurrency_limit` 是一个回退并发上限，适用于 PBE、MBE 和 CBE。Query Queue V2 首先使用当前估算策略计算查询所需的 Slot 数，并检查是否还有足够的 Slot 可用；随后再检查当前正在运行的查询数是否已经达到 `query_queue_concurrency_limit`。

默认值 `0` 表示不限制。只有在需要为同时运行的查询数设置绝对上限时，才建议配置该参数：

```SQL
ALTER WAREHOUSE default_warehouse SET ("query_queue_concurrency_limit" = "8");
```

建议优先使用 `query_queue_v2_concurrency_level` 调整资源容量。只有在需要显式限制同时运行的查询数量时，才使用 `query_queue_concurrency_limit`。

## 存算分离集群中的 Query Queue v2

在存算分离集群中，计算资源以 Warehouse 的形式组织，每个 Warehouse 拥有独立的查询队列。因此，Query Queue v2 需要按 Warehouse 分别启用和调整，而不是通过 FE 配置项。

:::warning

Warehouse 的查询队列在其 `enable_query_queue` 属性设置为 `true` 之前始终关闭。该属性为 `false`（默认值）时，该 Warehouse 中的查询不会排队；此时超出资源组 `concurrency_limit` 的查询会直接以 `Exceed concurrency limit` 报错被拒绝，而不会进入队列等待。

:::

### 为 Warehouse 启用查询队列

Warehouse 的查询队列默认关闭。您可以通过设置 Warehouse 属性 `enable_query_queue` 启用：

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue" = "true");
```

该修改立即生效，无需重启 FE 节点。

只要 `enable_query_queue` 为 `true`，SELECT 查询就会排队，没有单独的属性控制。如需同时为导入任务和统计信息查询启用队列，请设置对应的属性：

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue_load" = "true");
ALTER WAREHOUSE <warehouse_name> SET ("enable_query_queue_statistic" = "true");
```

如需在 Warehouse 粒度之外，再按资源组粒度排队，请设置全局会话变量 `enable_group_level_query_queue`。该变量只有在 `enable_query_queue` 为 `true` 之后才起作用：

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

### 查询队列相关的 Warehouse 属性

| 属性 | 默认值 | 含义 |
| ---- | ------ | ---- |
| `enable_query_queue` | `false` | 是否为该 Warehouse 启用查询队列。该属性是总开关，为 `false` 时，该 Warehouse 中的任何查询都不会排队。 |
| `enable_query_queue_load` | `false` | 是否为该 Warehouse 中的导入任务启用队列。仅当 `enable_query_queue` 为 `true` 时生效。 |
| `enable_query_queue_statistic` | `false` | 是否为该 Warehouse 中的统计信息查询启用队列。仅当 `enable_query_queue` 为 `true` 时生效。 |
| `query_queue_concurrency_limit` | `-1` | 该 Warehouse 中允许同时运行的查询数上限。仅当该值大于 `0` 时才生效。小于等于 `0` 表示不限制。 |
| `query_queue_max_queued_queries` | `1024` | 该 Warehouse 队列中允许等待的查询数上限。超出该数量后到达的查询会被直接拒绝，而不是进入队列。 |
| `query_queue_pending_timeout_second` | `600` | 查询在该 Warehouse 队列中等待的最长时间，单位为秒。超时后查询失败。 |
| `query_queue_slots_estimator_strategy` | 跟随同名 FE 配置项 | 自 v4.1 起支持。该 Warehouse 使用的 Slot 估算策略。有效值：`PBE`、`MBE` 和 `CBE`。详细信息，参见 [选择估算策略](#选择估算策略)。 |
| `query_queue_v2_concurrency_level` | 跟随同名 FE 配置项 | 自 v4.1 起支持。计算该 Warehouse 总 Slot 数量时使用的逻辑并发层数。详细信息，参见 [调整并发容量](#调整并发容量)。 |
| `query_queue_v2_mem_bytes_per_slot` | 跟随同名 FE 配置项 | 自 v4.1 起支持。该 Warehouse 中 MBE 估算策略使用的每 Slot 内存目标值。 |
| `query_queue_v2_cpu_costs_per_slot` | 跟随同名 FE 配置项 | 自 v4.1 起支持。该 Warehouse 中 CBE 估算策略使用的每 Slot CPU 成本阈值。 |
| `query_queue_v2_schedule_strategy` | 跟随同名 FE 配置项 | 自 v4.1 起支持。该 Warehouse 对等待中的查询进行排序时使用的调度策略。有效值：`SWRR` 和 `SJF`。 |

最后五个属性仅对当前 Warehouse 覆盖同名 FE 配置项。未设置时，使用 FE 级别的取值。其中两个字符串类型的属性一旦设置便无法恢复为未设置状态，如需取消覆盖，请将其显式设置回 FE 级别的取值。

您可以在一条语句中设置多个属性：

```SQL
ALTER WAREHOUSE <warehouse_name> SET (
    "enable_query_queue" = "true",
    "query_queue_v2_concurrency_level" = "8",
    "query_queue_v2_schedule_strategy" = "SJF"
);
```

### 确认查询队列已生效

为 Warehouse 启用查询队列后，您可以通过以下方式确认查询确实进入了队列：

- 执行 [SHOW RUNNING QUERIES](#show-running-queries)。排队中的查询状态为 `PENDING`，`Slots` 列显示每个查询预计需要的 Slot 数量。
- 执行 [SHOW PROCESSLIST](#show-processlist)。排队中的查询 `IsPending` 列为 `true`。
- 执行 `SHOW WAREHOUSES`。`Property` 列展示该 Warehouse 当前生效的查询队列属性。其 `RunningSql` 和 `QueuedSql` 列尚未实现，始终返回 `0`，请通过下面的 `warehouse_metrics` 查看当前负载。
- 查询 `information_schema.warehouse_metrics`。`QUEUE_PENDING_LENGTH` 为当前等待中的查询数，`REMAIN_SLOTS` 和 `MAX_SLOTS` 分别为该 Warehouse 的剩余 Slot 数和总 Slot 数。只有查询队列处于启用状态的 Warehouse 才会出现在该视图中，因此查询结果为空本身就说明 `enable_query_queue` 仍为 `false`：

  ```SQL
  SELECT WAREHOUSE_NAME, QUEUE_PENDING_LENGTH, QUEUE_RUNNING_LENGTH, REMAIN_SLOTS, MAX_SLOTS
  FROM information_schema.warehouse_metrics;
  ```

- 查看 **fe.audit.log** 中的 `PendingTimeMs` 字段。该值大于 `0` 表示查询曾在队列中等待。
- 监控 FE HTTP 端口暴露的 `starrocks_fe_warehouse_query_queue` 指标，例如 `starrocks_fe_warehouse_query_queue{field="query_pending_length"}`。详细信息，参见 [Warehouse 监控指标](../monitoring/metrics-warehouse_queue.md)。

如果查询始终不排队，请确认查询所在的 Warehouse 已将 `enable_query_queue` 设置为 `true`，并确认测试查询确实扫描了表。不含 Scan 节点的查询不会进入队列，既不会等待，也不会占用 Slot。`SELECT sleep(10)`、`SELECT 1` 以及只读取 `information_schema` 的查询都属于这一类，不适合用来验证查询队列是否生效。

### 调整单个 Warehouse

[选择估算策略](#选择估算策略) 和 [调整并发容量](#调整并发容量) 中的调优建议同样适用于存算分离集群。区别在于，需要使用 `ALTER WAREHOUSE` 而不是 `ADMIN SET FRONTEND CONFIG`，使改动只作用于单个 Warehouse，而不是整个集群。

例如，提升单个 Warehouse 的 Slot 容量：

```SQL
ALTER WAREHOUSE <warehouse_name> SET ("query_queue_v2_concurrency_level" = "8");
```

将单个 Warehouse 切换为基于内存成本的估算策略，并为每个 Slot 分配 2 GB 内存：

```SQL
ALTER WAREHOUSE <warehouse_name> SET (
    "query_queue_slots_estimator_strategy" = "MBE",
    "query_queue_v2_mem_bytes_per_slot" = "2147483648"
);
```

## 观测查询队列

您可以通过以下方式查看查询队列相关的信息。

### SHOW PROC

通过 [SHOW PROC](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md) 查看 BE 节点运行查询的数量、内存和 CPU 使用情况：

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

通过 [SHOW PROCESSLIST](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST.md) 查看查询是否在队列中（即 `IsPending` 为 `true` 时）。在存算分离集群中，`Warehouse` 列展示该查询所在的 Warehouse：

```Plain
MySQL [(none)]> SHOW PROCESSLIST;
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
| ServerName                      | Id       | User | Host                | Db   | Command | ConnectionStartTime | Time | State | Info             | IsPending | Warehouse         | CNGroup             | Catalog         | QueryId                              |
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
| 127.00.00.01_9010_1787542926940 | 33554554 | root | xxx.xx.xxx.xx:xxxxx |      | Query   | 2026-08-24 15:08:08 |    0 | OK    | SHOW PROCESSLIST | false     | default_warehouse | _builtin_cngroup_0_ | default_catalog | 01a03299-1521-77ee-ab7e-ec1387a3beb6 |
+---------------------------------+----------+------+---------------------+------+---------+---------------------+------+-------+------------------+-----------+-------------------+---------------------+-----------------+--------------------------------------+
```

### FE 审计日志

查看 FE 审计日志文件 **fe.audit.log**。 其中 `PendingTimeMs` 字段表示查询在队列中等待的时间，单位为毫秒。

### 监控指标

您可以通过[监控报警](../monitoring/monitoring.md)功能获取相应监控指标观测查询队列。下列 FE 指标为各 FE 节点基于自身的统计数据得出。

| 指标                                            | 单位 | 类型   | 描述                                                         |
| ----------------------------------------------- | ---- | ------ | --------------------------------------------------------- |
| starrocks_fe_query_queue_pending                | 个   | 瞬时值 | 当前正在队列中的查询数量。                                      |
| starrocks_fe_query_queue_total                  | 个   | 瞬时值 | 历史排队过的查询数量（包括正在运行的查询）。                       |
| starrocks_fe_query_queue_timeout                | 个   | 瞬时值 | 排队超时的查询总数量。                                         |
| starrocks_fe_resource_group_query_queue_total   | 个   | 瞬时值 | 该资源组历史排队的查询数量（包括正在运行的查询）。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |
| starrocks_fe_resource_group_query_queue_pending | 个   | 瞬时值 | 该资源组正在排队的查询数量。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |
| starrocks_fe_resource_group_query_queue_timeout | 个   | 瞬时值 | 该资源组排队超时的查询数量。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |

### SHOW RUNNING QUERIES

从 v3.1.4 版本开始，StarRocks 支持 SQL 语句 SHOW RUNNING QUERIES，用于展示每个查询的队列信息。各字段的含义如下：

- `QueryId`：该查询的 Query ID。
- `WarehouseId`：该查询所在 Warehouse 的 ID。默认 Warehouse 显示为 “-”。
- `ResourceGroupId`：该查询命中的资源组 ID。当没有命中用户定义的资源组时，会显示为 “-”。
- `StartTime`：该查询开始时间。
- `PendingTimeout`：PENDING 状态下查询在队列中超时的时间。
- `QueryTimeout`：该查询超时的时间。
- `State`：该查询的排队状态。其中，PENDING 表示在队列中；RUNNING 表示正在执行。
- `Slots`：该查询申请的逻辑资源数量。在 Query Queue v1 中通常为 `1`；在 Query Queue v2 中为该查询估算出的 slot 数量。
- `Fragments`：该查询执行计划中的 Fragment 数量。
- `DOP`：该查询的并发度（`pipeline_dop`）。为 `0` 表示并发度自适应，在执行时确定。
- `Frontend`：发起该查询的 FE 节点。
- `FeStartTime`：发起该查询的 FE 节点的启动时间。

示例：

```Plain
MySQL [(none)]> SHOW RUNNING QUERIES;
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
| QueryId                              | WarehouseId | ResourceGroupId | StartTime           | PendingTimeout      | QueryTimeout        | State   | Slots | Fragments | DOP  | Frontend                        | FeStartTime         |
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
| a46f68c6-3b49-11ee-8b43-00163e10863a | -           | 12003           | 2023-08-15 16:56:37 | 2023-08-15 17:01:37 | 2023-08-15 17:01:37 | RUNNING | 3     | 2         | 0    | 127.00.00.01_9010_1692069711535 | 2023-08-15 16:37:03 |
| a6935989-3b49-11ee-935a-00163e13bca3 | -           | 12003           | 2023-08-15 16:56:40 | 2023-08-15 17:01:40 | 2023-08-15 17:01:40 | PENDING | 3     | 2         | 0    | 127.00.00.02_9010_1692069658426 | 2023-08-15 16:37:03 |
| a7b5e137-3b49-11ee-8b43-00163e10863a | -           | 12003           | 2023-08-15 16:56:42 | 2023-08-15 17:01:42 | 2023-08-15 17:01:42 | PENDING | 3     | 2         | 0    | 127.00.00.03_9010_1692069711535 | 2023-08-15 16:37:03 |
+--------------------------------------+-------------+-----------------+---------------------+---------------------+---------------------+---------+-------+-----------+------+---------------------------------+---------------------+
```
