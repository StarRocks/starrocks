# 资源隔离【公测中】

本文介绍如何使用资源隔离功能。

自 2.2 版本起，StarRocks 支持资源组管理，集群可以通过设置资源组（Resource Group）的方式限制查询对资源的消耗，实现多租户之间的资源隔离与合理利用。

通过资源隔离功能，您可以将 BE 节点的计算资源划分成若干个资源组，并且为每个资源组关联一个或多个分类器（Classifier）。根据在分类器中设置的条件，系统将匹配查询任务的对应信息。当您发起查询任务时，分类器会根据查询任务的相关信息进行匹配。其中匹配度最高的分类器才会生效，系统则会根据生效的分类器所属的资源组为查询任务分配资源。

## 基本概念

本小节介绍资源隔离功能相关的基本概念。

### 资源组

通过将 BE 节点划分为若干个资源组，系统在执行相应资源组的查询任务时，会按照为该资源组划分的资源配额（CPU 及内存）分配查询资源。

您可以为资源组设置以下资源限制：

- `cpu_core_limit`：该资源组在当前 BE 节点可使用的 CPU 核数软上限，实际使用的 CPU 核数会根据节点资源空闲程度按比例弹性伸缩。取值为正整数。
  > 说明：例如，在 16 核的 BE 节点中设置三个资源组 rg1、rg2、rg3，`cpu_core_limit` 分别设置为 `2`、`6`、`8`。当在该 BE 节点满载时，资源组 rg1、rg2、rg3 能分配到的 CPU 核数分别为 BE 节点总 CPU 核数 ×（2/16）= 2、 BE 节点总 CPU 核数 ×（6/16）= 6、BE 节点总 CPU 核数 ×（8/16）= 8。如果当前 BE 节点资源非满载，rg1、rg2 有负载，rg3 无负载，则 rg1、rg2 分配到的 CPU 核数分别为 BE 节点总 CPU 核数 ×（2/8）= 4、 BE 节点总 CPU 核数 ×（6/8）= 12。
- `mem_limit`：该资源组在当前 BE 节点可使用于查询的内存（query_pool）占总内存的百分比（%）。取值范围为 (0,1)。
  > 说明：query_pool 的查看方式，参见 [内存管理](Memory_management.md)。

### 分类器

您可以为每个资源组关联一个或多个分类器。系统将会根据所有分类器中设置的条件，为每个查询任务选择一个匹配度最高的分类器，并根据生效的分类器所属的资源组为该查询任务分配资源。

分类器可以包含以下四个条件：

- `user`：用户名。
- `role`：用户所属的 Role。
- `query_type`: 查询类型，目前仅支持 `SELECT`。
- `source_ip`：发起查询的 IP 地址，类型为 CIDR。

系统在为查询任务匹配分类器时，查询任务的信息与分类器的条件完全相同，才能视为匹配。如果存在多个分类器的条件与查询任务完全匹配，则需要计算不同分类器的匹配度。其中只有匹配度最高的分类器才会生效。

匹配度的计算方式如下：

- 如果 `user` 一致，则该分类器匹配度增加 1。
- 如果 `role` 一致，则该分类器匹配度增加 1。
- 如果 `query_type` 一致，则该分类器匹配度增加 1 + 1/分类器的 `query_type` 数量。
- 如果 `source_ip` 一致，则该分类器匹配度增加 1 + (32 - cidr_prefix)/64。

例如，多个与查询任务匹配的分类器中，分类器的条件数量越多，则其匹配度越高。

```Plain%20Text
-- 因为分类器 B 的条件数量比 A 多，所以 B 的匹配度比 A 高。
classifier A (user='Alice')
classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

如果分类器的条件数量相等，则分类器的条件描述越精确，其匹配度越高。

```Plain%20Text
-- 因为分类器 B 限定的 `source_ip` 地址范围更小，所以 B 的匹配度比 A 高。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

-- 因为分类器 C 限定的查询类型数量更少，所以 C 的匹配度比 D 高。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select', 'ctas')）
```

## 隔离计算资源

您可以通过创建资源组并设置相应分类器为不同查询任务隔离计算资源。

### 开启资源组

通过设置相应会话变量开启资源组功能。

```sql
SET enable_resource_group = true;
```

> 说明：如果需要设置全局变量，需要运行 `SET GLOBAL enable_resource_group = true;`。

### 创建资源组和分类器

创建资源组，关联分类器，并分配资源。

```SQL
CREATE RESOURCE GROUP <group_name> 
TO (user='string', role='string', query_type in ('select'), source_ip='cidr') --创建分类器，多个分类器间用英文逗号（,）分隔。
WITH (
    "cpu_core_limit" = "INT",
    "mem_limit" = "m%",
    "type" = "normal" --资源组的类型，固定取值为 normal。
);
```

示例：

```SQL
CREATE RESOURCE GROUP rg1
to 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.3.1/24'),
    (user='rg1_user3', source_ip='192.168.4.1/24'),
    (user='rg1_user4')
with (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'type' = 'normal'
);
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
SHOW RESOURCE GROUP <group_name>；
```

示例：

```Plain%20Text
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

> 说明：`weight`：代表分类器的匹配度。

### 管理资源组配额和分类器

您可以修改资源组的配额，以及增加或删除资源组的分类器。

为已有的资源组修改资源配额。

```SQL
ALTER RESOURCE GROUP <group_name> WITH (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'type' = 'normal'
);
```

示例：

```SQL
-- 修改资源组rg1的CPU核数上限为20;
ALTER RESOURCE GROUP rg1 WITH (
    'cpu_core_limit' = '20'
);
```

添加新的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> ADD (user='string', role='string', query_type in ('select'), source_ip='cidr');
```

删除指定的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> DROP (CLASSIFER_ID_1, CLASSIFIER_ID_2, ...);
```

删除所有的分类器。

```SQL
ALTER RESOURCE GROUP <group_name> DROP ALL;
```

示例：

```SQL
-- 在资源组 rg1 中, 添加新的分类器。
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));

-- 删除资源组 rg1 中 id 为 300040, 300041, 300041 的分类器。
ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);

-- 删除资源组 rg1 的所有分类器。
ALTER RESOURCE GROUP rg1 DROP ALL;
```

删除指定资源组。

```SQL
DROP RESOURCE GROUP <group_name>;
```

示例：

```SQL
-- 删除资源组rg1。
DROP RESOURCE GROUP rg1;
```

## 下一步

成功设置资源组后，您可以：

- [管理内存](Memory_management.md)
- [管理查询](Query_management.md)
