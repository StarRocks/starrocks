# 资源隔离

自 2.2.0 版本起，StarRocks 支持资源组管理【公测中】，通过限制查询任务对计算资源的消耗，实现资源隔离和合理利用。本文介绍资源隔离的功能、基本概念和使用方式。

## 功能简介

资源隔离功能可以限制查询任务对计算资源的消耗，让不同租户的查询任务在同一集群执行时，既能实现资源隔离，又能合理使用资源。

您需要创建资源组（Resource Group），将 BE 节点的计算资源（CPU、内存）划分成若干个资源组，并且每个资源组会关联一个或多个分类器（Classifier），您需要在分类器中设置条件（用户名、用户所属的 Role、IP 地址、 查询类型），用于匹配查询任务的对应信息。

当您发起查询任务时，分类器会根据查询任务的信息（用户名、用户所属的 Role、IP 地址、 查询类型），进行匹配。匹配度最高的分类器才会生效，最终该分类器所属资源组为查询任务的资源组。

## 基本概念

- 资源组（Resource Group）

  资源组包括 BE 节点计算资源（CPU、内存）的配额。并且，一个资源组可以绑定一个或多个分类器。
  资源配额的计算方式如下：
  - `cpu_core_limit`：该资源组在每台 BE 节点所在机器上占用的 CPU 核数，取值为正整数。该限制为软限制，资源组实际占用的 CPU 核数会根据机器资源空闲程度按比例弹性伸缩，当机器资源充足时，可能会占用更多 CPU。例如 16 核的机器，设置三个资源组 rg1、rg2、rg3，`cpu_core_limit`为 2、6、8，则资源组 rg1、rg2、rg3 分别能分配到的 CPU 核数分别为 BE 节点 CPU 核数×（2/16）、 BE 节点 CPU 核数×（6/16）、BE 节点 CPU 核数×（8/16）。如果资源空闲，rg1、rg2 有负载，但是 rg3 没有请求，则 rg1、rg2 分配到的 CPU 核数分别为 BE 节点 CPU 核数×（2/8）、 BE 节点 CPU 核数×（6/8）。
  - `mem_limit`：使用 query_pool（BE 节点中用于查询的内存）的上限，取值范围为 0.0 ~ 1.0。
    > query_pool 的查看方式，请参见 [内存管理](Memory_management.md)。

- 分类器（Classifier）
  
  分类器包含四个条件（用户名，用户所属的 Role，用户发起查询的 IP 地址，查询类型），用于匹配查询任务的信息。 匹配度最高的分类器才会生效，最终该分类器所属资源组为查询任务的资源组。

  > 一个资源组可以绑定一个或多个分类器。

- 分类器包含的四个条件
  - `user`：用户名。
  - `role`：用户所属的 Role。
  - `query_type`: 查询类型，目前仅支持 SELECT。
  - `source_ip`：发起查询的 IP 地址，类型为 CIDR。

- 分类器与查询任务的匹配方式

1. 分类器与查询任务匹配时， 分类器的条件需要与查询任务的信息完全匹配。
2. 如果存在多个分类器的条件与查询任务完全匹配，则需要计算不同分类器的匹配度。其中只有匹配度最高的分类器才会生效。匹配度的计算方式如下：
   - 如果用户名一致，则匹配度加 1。
   - 如果用户所属 Role 一致，则匹配度加 1。
   - 如果查询类型一致，则该部分匹配度为 1 + 1/分类器的 `query_type` 数量。
   - 如果发起查询的 IP 地址一致，则该部分匹配度为 1 +  (32-cidr_prefix)/64。

例如，多个与查询任务匹配的分类器中，分类器的条件数量越多，则其匹配度越高。

```Plain_Text
-- B的匹配度比A高，因为条件数量越多。
classifier A (user='Alice')
classifier B (user='Alice', source_ip = '192.168.1.0/24')
```

如果分类器的条件数量相等，则分类器的条件描述越精确，其匹配度越高。

```Plain_Text
-- B的匹配度比A高, 因为192.168.1.0/24相对于192.168.1.0/16限定的source_ip地址范围更小。
classifier A (user='Alice', source_ip = '192.168.1.0/16')
classifier B (user='Alice', source_ip = '192.168.1.0/24')

--C的匹配度比D高, 因为('select')比 ('insert','select', 'ctas')限定的查询类型数量少。
classifier C (user='Alice', query_type in ('select'))
classifier D (user='Alice', query_type in ('insert','select', 'ctas')）
```

## 操作步骤

### 开启资源组

设置会话变量 `SET enable_resource_group = true`，开启资源组。

> 如果需要设置全局变量，则需要设置`SET GLOBAL enable_resource_group = true`。

### 创建资源组和分类器

语法如下：

```SQL
CREAE RESOURCE GROUP <name> 
TO CLASSIFIER[,...] --创建分类器，语法见下方。多个分类器间用英文逗号（,）分隔。
WITH (
    "cpu_core_limit" = "n",
    "mem_limit" = "m%",
    "type" = "normal" --资源组的类型，固定取值为normal。
);

-- 创建分类器的语法如下：
CLASSIFIER: 
   (user='string', role='string', query_type in ('select'), source_ip='cidr')
```

示例：

```SQL
create resource group rg1
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

语法如下：

```SQL
-- 查询所有的资源组和分类器。
SHOW RESOURCE GROUPS ALL;

-- 查询和当前用户匹配的资源组和分类器。
SHOW RESOURCE GROUPS;

-- 查询指定的资源组和分类器。
SHOW RESOURCE GROUP <NAME>
```

示例：

```Plain_Text
SHOW RESOURCE GROUPS ALL;

+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| Name | Id     | CPUCoreLimit | MemLimit | ConcurrencyLimit | Type   | Classifiers                                                                                                            |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300040, weight=4.409375, user=rg1_user1, role=rg1_role1, query_type in (SELECT), source_ip=192.168.2.1/24)         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300041, weight=3.459375, user=rg1_user2, query_type in (SELECT), source_ip=192.168.3.1/24)                         |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300042, weight=2.359375, user=rg1_user3, source_ip=192.168.4.1/24)                                                 |
| rg1  | 300039 | 10           | 20.0%    | 11               | NORMAL | (id=300043, weight=1.0, user=rg1_user4)                                                                                |
+------+--------+--------------+----------+------------------+--------+------------------------------------------------------------------------------------------------------------------------+
```

> `weight`：代表分类器的匹配度。

### 管理资源组配额和分类器

您可以执行命令，修改资源组的配额，以及增加或删除资源组的分类器。

#### 修改资源组的配额

语法如下：

```SQL
ALTER RESOURCE GROUP <NAME> WITH (
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

#### 增加或删除分类器

语法如下：

```SQL
-- 添加新的分类器
ALTER RESOURCE GROUP <NAME> ADD CLASSIFIER[,...];

-- 删除指定的分类器
ALTER RESOURCE GROUP <NAME> DROP (CLASSIFER_ID_1, CLASSIFIER_ID_2, ...);

-- 删除所有的分类器
ALTER RESOURCE GROUP <NAME> DROP ALL;
```

示例：

```SQL
-- 在资源组rg1中, 添加新的分类器。
ALTER RESOURCE GROUP rg1 ADD (user='root', query_type in ('select'));

-- 删除资源组rg1的id为(300040, 300041, 300041)的分类器。
ALTER RESOURCE GROUP rg1 DROP (300040, 300041, 300041);

-- 删除资源组rg1的所有分类器。
ALTER RESOURCE GROUP rg1 DROP ALL;
```

### 删除资源组

语法如下：

```Nginx
DROP RESOURCE GROUP <NAME>;
```

示例：

```SQL
-- 删除资源组rg1。
DROP RESOURCE GROUP rg1;
```
