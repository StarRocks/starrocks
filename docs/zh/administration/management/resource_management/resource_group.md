---
displayed_sidebar: docs
keywords: ['fuzai', 'ziyuan', 'ziyuanzu'] 
---

# 资源隔离

本文介绍如何使用资源隔离功能。

自 2.2 版本起，StarRocks 支持资源组管理，集群可以通过设置资源组（Resource Group）的方式限制查询对资源的消耗，实现多租户之间的资源隔离与合理利用。在 2.3 版本中，StarRocks 支持限制大查询，集群可以进一步控制大查询对资源的消耗，避免少数的大查询耗尽系统资源，进而影响系统稳定性。StarRocks 2.5 版本支持通过资源组对导入计算进行资源隔离，从而间接控制导入任务对集群资源的消耗。自 v3.3.5 起，StarRocks 支持对 CPU 资源进行硬隔离。

通过资源隔离功能，您可以将 BE 节点的计算资源划分成若干个资源组，并且为每个资源组关联一个或多个分类器（Classifier）。根据在分类器中设置的条件，系统将匹配查询任务的对应信息。当您发起查询任务时，分类器会根据查询任务的相关信息进行匹配。其中匹配度最高的分类器才会生效，系统则会根据生效的分类器所属的资源组为查询任务分配资源。

在后续版本中，我们将会持续强化资源隔离功能。

资源隔离功能支持计划

|             | 内部表 | 外部表 | 大查询熔断 | INSERT 计算资源隔离 | BROKER LOAD 计算资源隔离 | Routine Load、Stream Load、Schema Change 资源隔离 | CPU 硬隔离 |
| ----------- | ----- | ----- | -------- | ----------------- | ---------------------- | ----------------------------------------------- | --------- |
| 2.2         | √     | ×     | ×        | ×                 | ×                      | ×                                               | x         |
| 2.3         | √     | √     | √        | ×                 | ×                      | ×                                               | x         |
| 2.5         | √     | √     | √        | √                 | ×                      | ×                                               | x         |
| 3.1 & 3.2   | √     | √     | √        | √                 | √                      | ×                                               | x         |
| 3.3.5 及以后 | √     | √     | √        | √                 | √                      | ×                                               | √         |

## 基本概念

本小节介绍资源隔离功能相关的基本概念。

### 资源组

通过将 BE 节点划分为若干个资源组 (resource group)，系统在执行相应资源组的查询任务时，会按照为该资源组划分的资源配额（CPU 及内存）分配查询资源。

您可以为资源组设置如下的资源限制。

| 配置名称                    | 描述                                                       | 取值范围                                     | 默认值  |
| -------------------------- | --------------------------------------------------------- | ------------------------------------------- | ------ |
| cpu_weight                 | 该资源组在一个 BE 节点上调度 CPU 的权重。                           | (0, `avg_be_cpu_cores`] (大于 0 时生效)       | 0      |
| exclusive_cpu_cores        | 该资源组的 CPU 硬隔离参数。                                  | (0, `min_be_cpu_cores - 1`] (大于 0 时生效)   | 0      |
| mem_limit                  | 该资源组在当前 BE 节点可使用于查询的内存的比例。                 | (0, 1] (必填项)                              | -      |
| spill_mem_limit_threshold  | 该资源组触发落盘的内存占用阈值。                               | (0, 1]                                      | 1.0    |
| concurrency_limit          | 该资源组中并发查询数的上限。                                  | 整数 (大于 0 才生效)                           | 0      |
| big_query_cpu_second_limit | 该资源组的大查询任务在每个 BE 上可以使用 CPU 的时间上限。         | 整数 (大于 0 才生效)                          | 0      |
| big_query_scan_rows_limit  | 该资源组的大查询任务在每个 BE 上可以扫描的行数上限。              | 整数 (大于 0 才生效)                          | 0      |
| big_query_mem_limit        | 该资源组的大查询任务在每个 BE 上可以使用的内存上限。              | 整数 (大于 0 才生效)                          | 0      |

#### CPU 资源相关配置项

##### `cpu_weight`

该资源组在单个 BE 节点上调度 CPU 的权重。该值指定了该资源组的任务可用的 CPU 时间的相对份额。在 v3.3.5 以前，该配置名称为 `cpu_core_limit`。

取值范围为 (0, `avg_be_cpu_cores`]，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。只有大于 0 时才生效。`cpu_weight` 和 `exclusive_cpu_cores` 有且只能有一个为正数。

> **说明**
>
> 例如，假设设置了三个资源组 rg1、rg2、rg3，`cpu_weight` 分别设置为 `2`、`6`、`8`。如果当前 BE 节点满载，那么资源组 rg1、rg2、rg3 能分配到的 CPU 时间分别为 12.5%、37.5%、50%。如果当前 BE 节点资源非满载，rg1、rg2 有负载，rg3 无负载，那么资源组 rg1、rg2 分配到的 CPU 时间分别为 25% 和 75%。

##### `exclusive_cpu_cores`

该项为资源组的 CPU 硬隔离参数，有如下双重含义：

- **专属**：为该资源组预留 `exclusive_cpu_cores` 个 CPU Core，其余资源组不可以使用，即使这些 CPU 处于空闲状态。
- **限额**：该资源组只能使用这 `exclusive_cpu_cores` 个 CPU Core。即使其他资源组有空闲的 CPU 资源，该资源组也不能使用。

该项取值范围为 (0, `min_be_cpu_cores - 1`]，其中 `min_be_cpu_cores` 表示所有 BE 的 CPU 核数的最小值。只有大于 0 时才生效。`cpu_weight` 和 `exclusive_cpu_cores` 有且只能有一个为正数。

- `exclusive_cpu_cores` 大于 0 的资源组称为 Exclusive 资源组，分配给它的 CPU Core 称为 Exclusive Core。其余资源组称为 Shared 资源组，他们运行在非 Exclusive Core 上，称为 Shared Core。
- 所有资源组的 `exclusive_cpu_cores` 之和不能超过 `min_be_cpu_cores - 1`。之所以最大值为 `min_be_cpu_cores - 1` 而非 `min_be_cpu_cores`，是为了让 Shared Core 至少为 1。

该项与 `cpu_weight` 的关系：

`cpu_weight`  和 `exclusive_cpu_cores` 有且仅有一个为正数，即有且仅有一个生效。因为 Exclusive 资源组可以在自己完全拥有的为其预留的 `exclusive_cpu_cores` 个 CPU Core 上运行，无须通过 `cpu_weight` 分配到相对份额的 CPU 时间片。
    
此外，您可以使用 BE 配置项 `enable_resource_group_cpu_borrowing` 来指定是否允许 Shared 资源组借用 Exclusive 资源组的 Exclusive Core。将该配置项设置为 `true` 表示允许借用。默认为 `true`。具体来讲，当开启该功能时：

- 在一个 BE 上，当一个 Exclusive 资源组没有任务运行时，Shared 资源组可以暂时借用该 Exclusive 资源组的 Exclusive Core。
- 在一个 BE 上，当该 Exclusive 有任务到来后，Shared 资源组不可以再借用该 Exclusive 资源组的 Exclusive Core，需要尽快让出使用的 Exclusive Core。这里可能会有一些调度的延迟和开销，所以如果对隔离性要求极强并且允许浪费一定的 CPU，那么可以选择关闭借用功能。
  
您可以通过以下命令动态修改该配置：

```SQL
UPDATE information_schema.be_configs SET VALUE = "false" WHERE NAME = "enable_resource_group_cpu_borrowing";
```

#### 内存资源相关配置项

##### `mem_limit`

该资源组在当前 BE 节点可使用于查询的内存（query_pool）占总内存的百分比（%）。取值范围为 (0,1]。 有关 `query_pool` 的查看方式，参见 [内存管理](Memory_management.md)。

##### `spill_mem_limit_threshold`

该资源组触发落盘的内存占用阈值（百分比）。取值范围：(0,1)，默认值为 1，即不生效。该参数自 v3.1.7 版本引入。

- 如果开启自动落盘功能（即系统变量 `spill_mode` 设置为 `auto`），但未开启资源组功能，系统将在查询的内存占用超过 `query_mem_limit` 的 80% 时触发中间结果落盘。其中 `query_mem_limit` 为单个查询可使用的内存上限，由系统变量 `query_mem_limit` 控制，默认值为 0，代表不设限制。
- 如果开启自动落盘功能且查询命中资源组（包括所有系统内建资源组）后，该查询满足以下任意情况时，都将触发中间结果落盘：
  - 当前资源组内所有查询使用的内存超过 `当前 BE 节点内存上限 * mem_limit * spill_mem_limit_threshold` 时
  - 当前查询占用超过 `query_mem_limit` 的 80% 时。

#### 查询并发相关配置项

##### `concurrency_limit`

该资源组中并发查询数的上限，用以防止并发查询提交过多而导致的过载。只有大于 0 时才生效，默认值为 0。

#### 大查询资源相关配置项

在以上资源限制的基础上，您可以通过以下大查询限制进一步对资源组进行如下的配置。

##### `big_query_cpu_second_limit`

大查询任务在每个 BE 上可以使用 CPU 的时间上限，其中的并行任务将累加 CPU 实际使用时间。单位为秒。只有大于 0 时才生效，默认值为 0。

##### `big_query_scan_rows_limit`

大查询任务在每个 BE 上可以扫描的行数上限。只有大于 0 时才生效，默认值为 0。

##### `big_query_mem_limit`

大查询任务在每个 BE 上可以使用的内存上限。单位为 Byte。只有大于 0 时才生效，默认值为 0。

> **说明**
>
> 当资源组中运行的查询超过以上大查询限制时，查询将会终止，并返回错误。您也可以在 FE 节点 **fe.audit.log** 的 `ErrorCode` 列中查看错误信息。

#### type (自 v3.3.5 起弃用)

在 v3.3.5 之前，StarRocks 支持设置 `type` 为 `short_query` 类型的资源组。目前，该参数已经被废弃，由 `exclusive_cpu_cores` 所代替。对于已有的该类型的资源组，在集群升级至 v3.3.5 后，系统会将其替换为 `exclusive_cpu_cores` 值等于 `cpu_weight` 的 Exclusive 资源组。

#### 系统定义资源组

每个 StarRocks 示例中有两个系统定义资源组：`default_wg` 和 `default_mv_wg`。您可以通过 `ALTER RESOURCE GROUP` 来修改系统定义资源组的配置，但不能为其定义分类器，也不能删除系统定义资源组。

##### default_wg

如果普通查询受资源组管理，但是没有匹配到分类器，系统将默认为其分配 `default_wg`。该资源组的默认资源配置如下：

- `cpu_core_limit`：1 (&le;2.3.7 版本) 或 BE 的 CPU 核数（&gt;2.3.7版本）。
- `mem_limit`：100%。
- `concurrency_limit`：0。
- `big_query_cpu_second_limit`：0。
- `big_query_scan_rows_limit`：0。
- `big_query_mem_limit`：0。
- `spill_mem_limit_threshold`: 1.0。

##### default_mv_wg

如果创建异步物化视图时没有通过 `resource_group` 属性指定资源组，该物化视图刷新时，系统将默认为其分配 `default_mv_wg`。该资源组的默认资源配置如下：

- `cpu_core_limit`：1。
- `mem_limit`：80%。
- `concurrency_limit`: 0。
- `spill_mem_limit_threshold`: 80%。

### 分类器

您可以为每个资源组关联一个或多个分类器。系统将会根据所有分类器中设置的条件，为每个查询任务选择一个匹配度最高的分类器，并根据生效的分类器所属的资源组为该查询任务分配资源。

分类器可以包含以下条件：

- `user`：用户名。
- `role`：用户所属的 Role。
- `query_type`: 查询类型，目前支持 `SELECT` 与 `INSERT` (2.5及以后)。当 `query_type` 为 `insert` 的资源组有 INSERT INTO 或 Broker Load 导入任务正在运行时，当前 BE 节点会为其预留相应的计算资源。
- `source_ip`：发起查询的 IP 地址，类型为 CIDR。
- `db`：查询所访问的 Database，可以为 `,` 分割的字符串。
- `plan_cpu_cost_range`：系统估计的查询 CPU 开销范围。格式为 `(DOUBLE, DOUBLE]`。默认为 NULL，表示没有该限制。fe.audit.log 的 `PlanCpuCost` 列为系统估计的该查询的 CPU 开销。自 v3.1.4 版本起，StarRocks 支持该参数。
- `plan_mem_cost_range`：系统估计的查询内存开销范围。格式为 `(DOUBLE, DOUBLE]`。默认为 NULL，表示没有该限制。fe.audit.log 的 `PlanMemCost` 列为系统估计的该查询的内存开销。自 v3.1.4 版本起，StarRocks 支持该参数。

系统在为查询任务匹配分类器时，查询任务的信息与分类器的条件完全相同，才能视为匹配。如果存在多个分类器的条件与查询任务完全匹配，则需要计算不同分类器的匹配度。其中只有匹配度最高的分类器才会生效。

> **说明**
>
> 您可以在 FE 节点 **fe.audit.log** 的 `ResourceGroup` 列或 `EXPLAIN VERBOSE <query>` 的 `RESOURCE GROUP` 列中查看特定查询任务最终所匹配的资源组，参见[查看查询命中的资源组](#查看查询命中的资源组)。

匹配度的计算方式如下：

- 如果 `user` 一致，则该分类器匹配度增加 1。
- 如果 `role` 一致，则该分类器匹配度增加 1。
- 如果 `query_type` 一致，则该分类器匹配度增加 1 + 1/分类器的 `query_type` 数量。
- 如果 `source_ip` 一致，则该分类器匹配度增加 1 + (32 - cidr_prefix)/64。
- 如果查询的 `db` 匹配，则匹配度加 10。
- 如果查询的 CPU 开销在 `plan_cpu_cost_range` 范围内，则该分类器匹配度增加 1。
- 如果查询的内存开销在 `plan_mem_cost_range` 范围内，则该分类器匹配度增加 1。

例如，多个与查询任务匹配的分类器中，分类器的条件数量越多，则其匹配度越高。

```Plain
-- 因为分类器 B 的条件数量比 A 多，所以 B 的匹配度比 A 高。
classifier A (user='Alice')
classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

如果分类器的条件数量相等，则分类器的条件描述越精确，其匹配度越高。

```Plain
-- 因为分类器 B 限定的 `source_ip` 地址范围更小，所以 B 的匹配度比 A 高。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- 因为分类器 C 限定的查询类型数量更少，所以 C 的匹配度比 D 高。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select')）
```

如果多个分类器的匹配度相同，那么会随机选择其中一个分类器。

```Plain
-- 如果一个查询同时查询了 db1 和 db2，并且命中的分类器中 E 和 F 的匹配度最高，那么会从 E 和 F 中随机选择一个。
classifier E (db='db1')
classifier F (db='db2')
```

## 隔离计算资源

您可以通过创建资源组并设置相应分类器为不同查询任务隔离计算资源。

### 开启资源组

要使用资源组，需通过设置相应会话变量启用 Pipeline 引擎。

```sql
-- 在当前 Session 启用 Pipeline 引擎。
SET enable_pipeline_engine = true;
-- 全局启用 Pipeline 引擎。
SET GLOBAL enable_pipeline_engine = true;
```

对于导入任务，还需要开启 FE 配置项 `enable_pipeline_load` 来为导入任务启用 Pipeline 引擎。该参数自 v2.5.0 起支持。

```sql
ADMIN SET FRONTEND CONFIG ("enable_pipeline_load" = "true");
```

> **说明**
>
> 自 v3.1.0 起，默认启用资源组功能。会话变量 `enable_resource_group` 弃用。

### 创建资源组和分类器

创建资源组，关联分类器，并分配资源。

```SQL
CREATE RESOURCE GROUP group_name 
TO (
    user='string', 
    role='string', 
    query_type in ('select'), 
    source_ip='cidr'
) -- 创建分类器，多个分类器间用英文逗号（,）分隔。
WITH (
    "{ cpu_weight | exclusive_cpu_cores }" = "INT",
    "mem_limit" = "m%",
    "concurrency_limit" = "INT"
);
```

示例：

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH (
    'exclusive_cpu_cores' = '10',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '100000',
    'big_query_mem_limit' = '1073741824'
);
```

### 指定资源组（可选）

除通过分类器自动指定资源组外，您也可以通过会话变量直接指定资源组，包括 `default_wg` 和 `default_mv_wg`。

```sql
SET resource_group = 'group_name';
```

### 查看资源组和分类器

查询所有的资源组和分类器。

```SQL
SHOW RESOURCE GROUPS ALL;
```

查询和当前用户匹配的资源组和分类器。

```SQL
SHOW RESOURCE GROUPS;
```

查询指定的资源组和分类器。

```SQL
SHOW RESOURCE GROUP group_name;
```

示例：

```Plain_Text
mysql> SHOW RESOURCE GROUPS ALL;

+---------------+-------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+----------------------------------------+
| name          | id    | cpu_weight | exclusive_cpu_cores | mem_limit | big_query_cpu_second_limit | big_query_scan_rows_limit | big_query_mem_limit | concurrency_limit | spill_mem_limit_threshold | classifiers                            |
+---------------+-------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+----------------------------------------+
| default_mv_wg | 3     | 1          | 0                   | 80.0%     | 0                          | 0                         | 0                   | null              | 80%                       | (id=0, weight=0.0)                     |
| default_wg    | 2     | 1          | 0                   | 100.0%    | 0                          | 0                         | 0                   | null              | 100%                      | (id=0, weight=0.0)                     |
| rge1          | 15015 | 0          | 6                   | 90.0%     | 0                          | 0                         | 0                   | null              | 100%                      | (id=15016, weight=1.0, user=rg1_user)  |
| rgs1          | 15017 | 8          | 0                   | 90.0%     | 0                          | 0                         | 0                   | null              | 100%                      | (id=15018, weight=1.0, user=rgs1_user) |
| rgs2          | 15019 | 8          | 0                   | 90.0%     | 0                          | 0                         | 0                   | null              | 100%                      | (id=15020, weight=1.0, user=rgs2_user) |
+---------------+-------+------------+---------------------+-----------+----------------------------+---------------------------+---------------------+-------------------+---------------------------+----------------------------------------+
```

> **说明**
>
> `weight` 代表分类器的匹配度。

查询资源组的所有字段（包括被废弃的字段）。

为上述三个命令增加关键字 `VERBOSE`，可以显示资源组的所有字段，包括被废弃的字段，主要包括 `type` 和 `max_cpu_cores`。

```sql
SHOW VERBOSE RESOURCE GROUPS ALL;
SHOW VERBOSE RESOURCE GROUPS;
SHOW VERBOSE RESOURCE GROUP group_name;
```

### 管理资源组配额和分类器

您可以修改资源组的配额，以及增加或删除资源组的分类器。

为已有的资源组修改资源配额。

```SQL
ALTER RESOURCE GROUP group_name WITH (
    'cpu_core_limit' = 'INT',
    'mem_limit' = 'm%'
);
```

删除指定资源组。

```SQL
DROP RESOURCE GROUP <group_name>;
```

添加新的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

删除指定的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFIER_ID_1, CLASSIFIER_ID_2, ...);
```

删除所有的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

## 观测资源组

### 查看查询命中的资源组

对于尚未被执行的查询，您可以通过 `EXPLAIN VERBOSE <query>` 的 `RESOURCE GROUP` 字段查看该查询所匹配的资源组。

在查询正在运行时，您可以通过 `SHOW PROC '/current_queries'` 和 `SHOW PROC '/global_current_queries'`  的 `ResourceGroup` 字段来查看正在运行的查询命中的资源组。

在查询运行结束后，您可以通过 FE 节点 **fe.audit.log** 的 `ResourceGroup` 字段查看特定查询任务最终所匹配的资源组。

对于上述资源组信息：

- 如果该查询不受资源组管理，那么该列值为空字符串 `""`。

- 如果该查询受资源组管理，但是没有匹配到分类器，那么该列值为空字符串 `""`，表示使用默认资源组 `default_wg`。

### 监控资源组

您可以为资源组设置[监控与报警](../monitoring/Monitor_and_Alert.md)。

可监控的资源组相关的 FE 与 BE 指标 如下所示。下面所有指标都带有 label `name`，表示其对应的资源组。

#### FE 指标

下列 FE Metrics 是每个 FE 上各自统计该 FE 上的查询数量。

| 指标                                            | 单位 | 类型   | 描述                                                         |
| ----------------------------------------------- | ---- | ------ | ------------------------------------------------------------ |
| starrocks_fe_query_resource_group               | 个   | 瞬时值 | 该资源组历史运行过的查询数量（包括正在运行的查询）。         |
| starrocks_fe_query_resource_group_latency       | ms   | 瞬时值 | 该资源组的查询延迟百分位数，label type 表示特定的分位数，包括 `mean`、`75_quantile`、`95_quantile`、`98_quantile`、`99_quantile`、`999_quantile`。 |
| starrocks_fe_query_resource_group_err           | 个   | 瞬时值 | 该资源组报错的查询任务的数量。                               |
| starrocks_fe_resource_group_query_queue_total   | 个   | 瞬时值 | 该资源组历史排队的查询数量（包括正在运行的查询）。该指标自 v3.1.4 起支持。仅在开启查询队列时，该指标有意义，参见[查询队列](query_queues.md)。 |
| starrocks_fe_resource_group_query_queue_pending | 个   | 瞬时值 | 该资源组正在排队的查询数量。该指标自 v3.1.4 起支持。仅在开启查询队列时，该指标有意义，参见[查询队列](query_queues.md)。 |
| starrocks_fe_resource_group_query_queue_timeout | 个   | 瞬时值 | 该资源组排队超时的查询数量。该指标自 v3.1.4 起支持。仅在开启查询队列时，该指标有意义，参见[查询队列](query_queues.md)。 |

#### BE 指标

| 指标                                      | 单位   | 类型   | 描述                                                         |
| ----------------------------------------- | ------ | ------ | ------------------------------------------------------------ |
| resource_group_running_queries            | 个     | 瞬时值 | 该资源组当前正在运行的查询数量。                             |
| resource_group_total_queries              | 个     | 瞬时值 | 该资源组历史运行过的查询数量（包括正在运行的查询）。         |
| resource_group_bigquery_count             | 个     | 瞬时值 | 该资源组触发大查询限制的查询数量。                           |
| resource_group_concurrency_overflow_count | 个     | 瞬时值 | 该资源组触发 `concurrency_limit` 限制的查询数量。              |
| resource_group_mem_limit_bytes            | Bytes  | 瞬时值 | 该资源组的内存上限。                                         |
| resource_group_mem_inuse_bytes            | Bytes  | 瞬时值 | 该资源组正在使用的内存。                                     |
| resource_group_cpu_limit_ratio            | 百分比 | 瞬时值 | 该资源组的 `cpu_core_limit` 占所有资源组 `cpu_core_limit` 的比例。 |
| resource_group_inuse_cpu_cores            | 个     | 平均值 | 该资源组正在使用的 CPU 核数，该值为一个估计值。统计的是两次获取 Metric 时间间隔内的平均值。该指标自 v3.1.4 起支持。 |
| resource_group_cpu_use_ratio              | 百分比 | 平均值 | **Deprecated** 该资源组使用的 Pipeline 线程时间片占所有资源组 Pipeline 线程时间片的比例。统计的是两次获取指标时间间隔内的平均值。 |
| resource_group_connector_scan_use_ratio   | 百分比 | 平均值 | **Deprecated** 该资源组使用的外表 Scan 线程时间片占所有资源组 Pipeline 线程时间片的比例。统计的是两次获取指标时间间隔内的平均值。 |
| resource_group_scan_use_ratio             | 百分比 | 平均值 | **Deprecated** 该资源组使用的内表 Scan 线程时间片占所有资源组 Pipeline 线程时间片的比例。统计的是两次获取指标时间间隔内的平均值。 |

### 查看资源组的使用信息

从 v3.1.4 版本开始，StarRocks 支持 SQL 语句 [SHOW USAGE RESOURCE GROUPS](../../../sql-reference/sql-statements/cluster-management/resource_group/SHOW_USAGE_RESOURCE_GROUPS.md)，用以展示每个资源组在各个 BE 上的使用信息。各个字段的含义如下：

- `Name`：资源组的名称。
- `Id`：资源组的 ID。
- `Backend`：BE 的 IP 或 FQDN。
- `BEInUseCpuCores`：该资源组在该 BE 上正在使用的 CPU 核数，该值为一个估计近似值。
- `BEInUseMemBytes`：该资源组在该 BE 上正在使用的内存字节数。
- `BERunningQueries`：该资源组在该 BE 上还未结束的查询数量。

注意：

- 这些资源使用信息由 BE 周期性汇报给 Leader FE，汇报周期为 `report_resource_usage_interval_ms`，默认 1s。
- 结果中只会展示 BEInUseCpuCores/BEInUseMemBytes/BERunningQueries 至少一个为正数的行，即只有一个资源组在一个 BE 上使用了某种资源时，才会在结果中进行展示。

示例：

```Plain
MySQL [(none)]> SHOW USAGE RESOURCE GROUPS;
+------------+----+-----------+-----------------+-----------------+------------------+
| Name       | Id | Backend   | BEInUseCpuCores | BEInUseMemBytes | BERunningQueries |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.1 | 0.100           | 1               | 5                |
+------------+----+-----------+-----------------+-----------------+------------------+
| default_wg | 0  | 127.0.0.2 | 0.200           | 2               | 6                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg1        | 0  | 127.0.0.1 | 0.300           | 3               | 7                |
+------------+----+-----------+-----------------+-----------------+------------------+
| wg2        | 0  | 127.0.0.1 | 0.400           | 4               | 8                |
+------------+----+-----------+-----------------+-----------------+------------------+
```

### 查看 Exclusive 和 Shared 资源组的线程信息

查询执行主要涉及三个线程池（`pip_exec`、`pip_scan` 以及 `pip_con_scan`）。

- Exclusive 资源组创建并且运行在自己独占的上述三个线程池，并绑定到分配给它的 Exclusive CPU Core 上。
- Shared 资源组运行在共享的线程池上，并绑定到剩余的 Shared CPU Core 上。

上述三个线程池中的线程命名规则为 `{ pip_exec | pip_scan | pip_con_scan }_{ com | <resource_group_id> }`。其中，`com` 表示共享的线程池，`<resource_group_id>` 为 Exclusive 资源组的 ID。

您可以通过系统视图 `information_schema.be_threads` 可以查看每个 BE 线程和其绑定的 CPU 信息。其中，`BE_ID`, `NAME`, `BOUND_CPUS` 分别表示 BE 的 ID、线程的名称、该线程绑定的 CPU Core 数量。

```sql
select * from information_schema.be_threads where name like '%pip_exec%';
select * from information_schema.be_threads where name like '%pip_scan%';
select * from information_schema.be_threads where name like '%pip_con_scan%';
```

一个具体的示例如下所示。

```sql
select BE_ID, NAME, FINISHED_TASKS, BOUND_CPUS from information_schema.be_threads where name like '%pip_exec_com%' and be_id = 10223;
+-------+--------------+----------------+------------+
| BE_ID | NAME         | FINISHED_TASKS | BOUND_CPUS |
+-------+--------------+----------------+------------+
| 10223 | pip_exec_com | 2091295        | 10         |
| 10223 | pip_exec_com | 2088025        | 10         |
| 10223 | pip_exec_com | 1637603        | 6          |
| 10223 | pip_exec_com | 1641260        | 6          |
| 10223 | pip_exec_com | 1634197        | 6          |
| 10223 | pip_exec_com | 1633804        | 6          |
| 10223 | pip_exec_com | 1638184        | 6          |
| 10223 | pip_exec_com | 1636374        | 6          |
| 10223 | pip_exec_com | 2095951        | 10         |
| 10223 | pip_exec_com | 2095248        | 10         |
| 10223 | pip_exec_com | 2098745        | 10         |
| 10223 | pip_exec_com | 2085338        | 10         |
| 10223 | pip_exec_com | 2101221        | 10         |
| 10223 | pip_exec_com | 2093901        | 10         |
| 10223 | pip_exec_com | 2092364        | 10         |
| 10223 | pip_exec_com | 2091366        | 10         |
+-------+--------------+----------------+------------+
```

## 下一步

成功设置资源组后，您可以：

- [管理内存](Memory_management.md)
- [管理查询](Query_management.md)
