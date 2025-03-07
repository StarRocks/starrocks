---
displayed_sidebar: docs
keywords: ['ziyuanzu'] 
---

# CREATE RESOURCE GROUP

## 功能

创建资源组。关于资源组的更多信息，参见[资源隔离](../../../../administration/management/resource_management/resource_group.md)。

:::tip

该操作需要 SYSTEM 级 CREATE RESOURCE GROUP 权限。请参考 [GRANT](../../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```SQL
CREATE RESOURCE GROUP resource_group_name
TO CLASSIFIER1, CLASSIFIER2, ...
WITH resource_limit
```

## 参数说明

- `resource_group_name`：需要创建的资源组名称。

- `CLASSIFIER`：用于区分需要被施加资源限制的查询的分类器，采用 `"key"="value"` 对形式。您可以为同一个资源组设置多个分类器。

  - 分类器可以包含以下参数：

    | **参数**   | **必选** | **说明**                                            |
    | ---------- | -------- | --------------------------------------------------- |
    | user       | 否       | 用户名。                                            |
    | role       | 否       | 用户所属角色名。                                    |
    | query_type | 否       | 需要被施加资源限制的查询类型，目前支持 `SELECT` 与 `INSERT` (2.5 及以后)。当 `query_type` 为 `insert` 的资源组有导入任务正在运行时，当前 BE 节点会为其预留相应的计算资源。 |
    | source_ip  | 否       | 发起查询的 IP 地址，类型为 CIDR。                   |
    | db         | 否       | 查询所访问的数据库，可以为逗号（,）分割的字符串。   |
    | plan_cpu_cost_range | 否     | 估计的查询 CPU 开销范围。该值与 **fe.audit.log** 中的 `PlanCpuCost` 字段含义相同，无单位。格式为 `[DOUBLE, DOUBLE)`。默认为 NULL，表示没有该限制。自 v3.1.4 起支持。                   |
    | plan_mem_cost_range | 否     | 估计的查询内存开销范围。该值与 **fe.audit.log** 中的 `PlanMemCost` 字段含义相同，无单位。格式为 `[DOUBLE, DOUBLE)`。默认为 NULL，表示没有该限制。自 v3.1.4 起支持。                |

- `resource_limit`：为当前资源组设置的资源限制，采用 `"key"="value"` 对形式。您可以为同一个资源组设置多个资源限制。

  - 资源限制可以包含以下参数：

    | **参数**                   | **必选** | **说明**                                                     |
    | -------------------------- | -------- | ------------------------------------------------------------ |
    | cpu_weight                 | 否       | 创建 Shared 资源组所需参数，表示该资源组在单个 BE 节点上 CPU 调度的权重。该值指定了该资源组的任务可用的 CPU 时间的相对份额。实际使用的 CPU 核数会根据节点资源空闲程度按比例弹性伸缩。取值范围为 (0, `avg_be_cpu_cores`]，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。只有大于 0 时才生效。`cpu_weight` 和 `exclusive_cpu_cores` 有且只能有一个为正数。|
    | exclusive_cpu_cores        | 否       | 创建 Exclusive 资源组（CPU 硬隔离）所需参数，表示为该资源组预留 `exclusive_cpu_cores` 个 CPU Core，其余资源组不可以使用，即使这些 CPU 处于空闲状态，并且该资源组只能使用这 `exclusive_cpu_cores` 个 CPU Core。即使其他资源组有空闲的 CPU 资源，该资源组也不能使用。只有大于 0 时才生效。`cpu_weight` 和 `exclusive_cpu_cores` 有且只能有一个为正数。|
    | mem_limit                  | 否       | 该资源组在当前 BE 节点可使用于查询的内存（query_pool）占总内存的百分比（%）。取值范围为 (0,1)。 |
    | concurrency_limit          | 否       | 资源组中并发查询数的上限，用以防止并发查询提交过多而导致的过载。 |
    | max_cpu_cores              | 否       | 资源组在单个 BE 节点中使用的 CPU 核数上限。仅在设置为大于 `0` 后生效。取值范围：[0, `avg_be_cpu_cores`]，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。默认值为 0。自 v3.1.4 起支持。 |
    | big_query_cpu_second_limit | 否       | 大查询任务可以使用 CPU 的时间上限，其中的并行任务将累加 CPU 使用时间。单位为秒。 |
    | big_query_scan_rows_limit  | 否       | 大查询任务可以扫描的行数上限。                               |
    | big_query_mem_limit        | 否       | 大查询任务可以使用的内存上限。单位为 Byte。                  |

    > **NOTE**
    >
    > 在 v3.3.5 之前，StarRocks 支持设置 `type` 为 `short_query` 类型的资源组。目前，该参数已经被废弃，由 `exclusive_cpu_cores` 所代替。对于已有的该类型的资源组，在集群升级至 v3.3.5 后，系统会将其替换为 `exclusive_cpu_cores` 值等于 `cpu_weight` 的 Exclusive 资源组。

## 示例

示例一：基于多个分类器创建 Shared 资源组 `rg1`。

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_weight' = '10',
      'mem_limit' = '20%',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```

示例二：基于多个分类器创建 Exclusive 资源组 `rg2`。

```SQL
CREATE RESOURCE GROUP rg2
TO 
    (user='rg1_user5', role='rg1_role5', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user6', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user7', source_ip='192.168.x.x/24'),
    (user='rg1_user8'),
    (db='db2')
WITH ('exclusive_cpu_cores' = '10',
      'mem_limit' = '20%',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```
