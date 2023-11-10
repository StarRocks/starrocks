# CREATE RESOURCE GROUP

## 功能

创建资源组。关于资源组的更多信息，参见[资源隔离](../../../administration/resource_group.md)。

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

- `resource_limit`：为当前资源组设置的资源限制，采用 `"key"="value"` 对形式。您可以为同一个资源组设置多个资源限制。

  - 资源限制可以包含以下参数：

    | **参数**                   | **必选** | **说明**                                                     |
    | -------------------------- | -------- | ------------------------------------------------------------ |
    | cpu_core_limit             | 否       | 该资源组在当前 BE 节点可使用的 CPU 核数软上限，实际使用的 CPU 核数会根据节点资源空闲程度按比例弹性伸缩。取值为正整数。 |
    | mem_limit                  | 否       | 该资源组在当前 BE 节点可使用于查询的内存（query_pool）占总内存的百分比（%）。取值范围为 (0,1)。 |
    | concurrency_limit          | 否       | 资源组中并发查询数的上限，用以防止并发查询提交过多而导致的过载。 |
    | big_query_cpu_second_limit | 否       | 大查询任务可以使用 CPU 的时间上限，其中的并行任务将累加 CPU 使用时间。单位为秒。 |
    | big_query_scan_rows_limit  | 否       | 大查询任务可以扫描的行数上限。                               |
    | big_query_mem_limit        | 否       | 大查询任务可以使用的内存上限。单位为 Byte。                  |
    | type                       | 否       | 资源组类型。有效取值包括：<br />`short_query`：当 `short_query` 资源组有查询正在运行时，当前 BE 节点会为其预留 `short_query.cpu_core_limit` 的 CPU 资源，所有 `normal` 资源组的总 CPU 核数使用上限会被硬限制为 BE 节点核数减 `short_query.cpu_core_limit`。<br />`normal`：当 `short_query` 资源组没有查询正在运行时，所有 `normal` 资源组的 CPU 核数没有硬限制。<br />注意：同一集群中，您最多只能创建一个 `short_query` 资源组。 |

## 示例

示例一：基于多个分类器创建资源组 `rg1`。

```SQL
CREATE RESOURCE GROUP rg1
TO 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.x.x/24'),
    (user='rg1_user3', source_ip='192.168.x.x/24'),
    (user='rg1_user4'),
    (db='db1')
WITH ('cpu_core_limit' = '10',
      'mem_limit' = '20%',
      'type' = 'normal',
      'big_query_cpu_second_limit' = '100',
      'big_query_scan_rows_limit' = '100000',
      'big_query_mem_limit' = '1073741824'
);
```
