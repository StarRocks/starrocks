# 资源隔离

本文介绍如何使用资源隔离功能。

自 2.2 版本起，StarRocks 支持资源组管理，集群可以通过设置资源组（Resource Group）的方式限制查询对资源的消耗，实现多租户之间的资源隔离与合理利用。在 2.3 版本中，StarRocks 支持限制大查询，集群可以进一步控制大查询对资源的消耗，避免少数的大查询耗尽系统资源，进而影响系统稳定性。StarRocks 2.5 版本支持通过资源组对导入计算进行资源隔离，从而间接控制导入任务对集群资源的消耗。

通过资源隔离功能，您可以将 BE 节点的计算资源划分成若干个资源组，并且为每个资源组关联一个或多个分类器（Classifier）。根据在分类器中设置的条件，系统将匹配查询任务的对应信息。当您发起查询任务时，分类器会根据查询任务的相关信息进行匹配。其中匹配度最高的分类器才会生效，系统则会根据生效的分类器所属的资源组为查询任务分配资源。

在后续版本中，我们将会持续强化资源隔离功能。

资源隔离功能支持计划

|  | 内部表 | 外部表 | 大小查询隔离 | Short query 资源组 | 导入计算资源隔离 | Schema Change 资源隔离 | INSERT 计算资源隔离 |
|---|---|---|---|---|---|---|---|
| 2.2 | √ | × | × | × | × | × | × |
| 2.3 | √ | √ | √ |√ | × | × | × |
| 2.4 | √ | √ | √ |√ | × | × | × |
| 2.5 | √ | √ | √ |√ | √ | × | √ |

## 基本概念

本小节介绍资源隔离功能相关的基本概念。

### 资源组

通过将 BE 节点划分为若干个资源组 (resource group)，系统在执行相应资源组的查询任务时，会按照为该资源组划分的资源配额（CPU 及内存）分配查询资源。

您可以为资源组设置以下资源限制：

- `cpu_core_limit`：该资源组在当前 BE 节点可使用的 CPU 核数软上限，实际使用的 CPU 核数会根据节点资源空闲程度按比例弹性伸缩。取值为正整数。
  > 说明：例如，在 16 核的 BE 节点中设置三个资源组 rg1、rg2、rg3，`cpu_core_limit` 分别设置为 `2`、`6`、`8`。当在该 BE 节点满载时，资源组 rg1、rg2、rg3 能分配到的 CPU 核数分别为 BE 节点总 CPU 核数 ×（2/16）= 2、 BE 节点总 CPU 核数 ×（6/16）= 6、BE 节点总 CPU 核数 ×（8/16）= 8。如果当前 BE 节点资源非满载，rg1、rg2 有负载，rg3 无负载，则 rg1、rg2 分配到的 CPU 核数分别为 BE 节点总 CPU 核数 ×（2/8）= 4、 BE 节点总 CPU 核数 ×（6/8）= 12。
- `mem_limit`：该资源组在当前 BE 节点可使用于查询的内存（query_pool）占总内存的百分比（%）。取值范围为 (0,1)。
  > 说明：query_pool 的查看方式，参见 [内存管理](Memory_management.md)。
- `concurrency_limit`：资源组中并发查询数的上限，用以防止并发查询提交过多而导致的过载。只有大于 0 时才生效，默认值为 0。

在以上资源限制的基础上，您可以通过以下大查询限制进一步对资源组进行如下的配置：

- `big_query_cpu_second_limit`：大查询任务可以使用 CPU 的时间上限，其中的并行任务将累加 CPU 使用时间。单位为秒。只有大于 0 时才生效，默认值为 0。
- `big_query_scan_rows_limit`：大查询任务可以扫描的行数上限。只有大于 0 时才生效，默认值为 0。
- `big_query_mem_limit`：大查询任务可以使用的内存上限。单位为 Byte。只有大于 0 时才生效，默认值为 0。

> **说明**
>
> 当资源组中运行的查询超过以上大查询限制时，查询将会终止，并返回错误。您也可以在 FE 节点 **fe.audit.log** 的 `ErrorCode` 列中查看错误信息。

资源组的类型 `type` 支持 `short_query` 与 `normal`。

- 默认为 `normal` 资源组，无需通过 `type` 参数指定。
- 当 `short_query` 资源组有查询正在运行时，当前 BE 节点会为其预留 `short_query.cpu_core_limit` 的 CPU 资源，即所有 `normal` 资源组的总 CPU 核数使用上限会被硬限制为 BE 节点核数 - `short_query.cpu_core_limit`。
- 当 `short_query` 资源组没有查询正在运行时，所有 `normal` 资源组的 CPU 核数没有硬限制。

> **注意**
>
> - 您最多只能创建一个 `short_query` 资源组。
> - StarRocks 不会硬限制 `short_query` 资源组的 CPU 资源。

### 分类器

您可以为每个资源组关联一个或多个分类器。系统将会根据所有分类器中设置的条件，为每个查询任务选择一个匹配度最高的分类器，并根据生效的分类器所属的资源组为该查询任务分配资源。

分类器可以包含以下条件：

- `user`：用户名。
- `role`：用户所属的 Role。
- `query_type`: 查询类型，目前支持 `SELECT` 与 `INSERT` (2.5及以后)。当 `query_type` 为 `insert` 的资源组有导入任务正在运行时，当前 BE 节点会为其预留相应的计算资源。
- `source_ip`：发起查询的 IP 地址，类型为 CIDR。
- `db`：查询所访问的 Database，可以为 `,` 分割的字符串。

系统在为查询任务匹配分类器时，查询任务的信息与分类器的条件完全相同，才能视为匹配。如果存在多个分类器的条件与查询任务完全匹配，则需要计算不同分类器的匹配度。其中只有匹配度最高的分类器才会生效。

> **说明**
>
> 您可以在 FE 节点 **fe.audit.log** 的 `ResourceGroup` 列中查看特定查询任务最终所匹配的资源组。
>
> 如果没有匹配到分类器，那么会使用默认资源组 `default_wg`，它的资源配置如下：
>
> - `cpu_core_limit`：1 (&le;2.3.7 版本) 或 BE 的 CPU 核数（&gt;2.3.7版本）。
> - `mem_limit`：100%。
> - `concurrency_limit`：0。
> - `big_query_cpu_second_limit`：0。
> - `big_query_scan_rows_limit`：0。
> - `big_query_mem_limit`：0。

匹配度的计算方式如下：

- 如果 `user` 一致，则该分类器匹配度增加 1。
- 如果 `role` 一致，则该分类器匹配度增加 1。
- 如果 `query_type` 一致，则该分类器匹配度增加 1 + 1/分类器的 `query_type` 数量。
- 如果 `source_ip` 一致，则该分类器匹配度增加 1 + (32 - cidr_prefix)/64。
- 如果查询的 `db` 匹配，则匹配度加 10。

例如，多个与查询任务匹配的分类器中，分类器的条件数量越多，则其匹配度越高。

```Plain_Text
-- 因为分类器 B 的条件数量比 A 多，所以 B 的匹配度比 A 高。
classifier A (user='Alice')
classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

如果分类器的条件数量相等，则分类器的条件描述越精确，其匹配度越高。

```Plain_Text
-- 因为分类器 B 限定的 `source_ip` 地址范围更小，所以 B 的匹配度比 A 高。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- 因为分类器 C 限定的查询类型数量更少，所以 C 的匹配度比 D 高。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select')）
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
    "cpu_core_limit" = "INT",
    "mem_limit" = "m%",
    "concurrency_limit" = "INT",
    "type" = "str" -- 资源组的类型，取值为 normal 或 short_query。
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
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'big_query_cpu_second_limit' = '100',
    'big_query_scan_rows_limit' = '100000',
    'big_query_mem_limit' = '1073741824'
);
```

### 指定资源组（可选）

除通过分类器自动指定资源组外，您也可以通过会话变量直接指定资源组。

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

+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> **说明**
>
> `weight` 代表分类器的匹配度。

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

## 监控资源组

您可以为资源组设置[监控与报警](Monitor_and_Alert.md)。

可监控的资源组相关 Metrics 包括：

- FE 节点
  - starrocks_fe_query_resource_group：该资源组中查询任务的数量。
  - starrocks_fe_query_resource_group_latency：该资源组的查询延迟百分位数。
  - starrocks_fe_query_resource_group_err：该资源组中报错的查询任务的数量。
- BE 节点
  - starrocks_be_resource_group_cpu_limit_ratio：该资源组 CPU 配额比率的瞬时值。
  - starrocks_be_resource_group_cpu_use_ratio：该资源组 CPU 使用率瞬时值。
  - starrocks_be_resource_group_mem_limit_bytes：该资源组内存配额比率的瞬时值。
  - starrocks_be_resource_group_mem_allocated_bytes：该资源组内存使用率瞬时值。

## 下一步

成功设置资源组后，您可以：

- [管理内存](Memory_management.md)
- [管理查询](Query_management.md)
