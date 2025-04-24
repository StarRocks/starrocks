---
displayed_sidebar: docs
sidebar_position: 60
---

import Beta from '../../_assets/commonMarkdown/_beta.mdx'

# 向量索引

<Beta />

本文介绍了StarRocks的向量索引功能及如何使用它进行近似最近邻搜索（ANNS）。

向量索引功能仅在v3.4或更高版本的存算一体集群中支持。

## 概述

目前，StarRocks支持在Segment文件级别进行向量索引。索引将每个搜索项映射到Segment文件中的行ID，通过直接定位相应的数据行实现快速数据检索，而无需进行暴力的向量距离计算。系统目前提供两种类型的向量索引：倒排文件与产品量化（IVFPQ）和分层可导航小世界（HNSW），每种都有其独特的组织结构。

### 倒排文件与产品量化（IVFPQ）

倒排文件与产品量化（IVFPQ）是一种用于大规模高维向量近似最近邻搜索的方法，常用于深度学习和机器学习中的向量检索任务。IVFPQ由两个主要组件组成：倒排文件和产品量化。

- **倒排文件**：这是一个索引方法。它将数据集划分为多个簇（或Voronoi单元），每个簇都有一个质心（种子点），并将每个数据点（向量）分配到其最近的簇中心。对于向量搜索，IVFPQ只需搜索最近的簇，大大减少了搜索范围和复杂性。
- **产品量化**：这是一种数据压缩技术。它将高维向量分割为子向量，并将每个子向量量化以映射到预定义集合中的最近点，从而在保持高精度的同时降低存储和计算成本。

通过结合倒排文件和产品量化，IVFPQ能够在大规模高维数据集中实现高效的近似最近邻搜索。

### 分层可导航小世界（HNSW）

分层可导航小世界（HNSW）是一种基于图的高维最近邻搜索算法，也广泛用于向量检索任务。

HNSW构建了一个分层图结构，其中每一层都是一个可导航的小世界（NSW）图。在图中，每个顶点代表一个数据点，边表示顶点之间的相似性。图的高层包含较少的顶点和稀疏的连接用于快速全局搜索，而低层包含所有顶点和密集的连接用于精确的局部搜索。

对于向量搜索，HNSW首先在顶层进行搜索，快速识别一个近似的最近邻区域，然后逐层向下移动，在底层找到精确的最近邻。

HNSW提供了效率和精度的平衡，使其适应各种数据和查询分布。

### IVFPQ与HNSW的比较

- **数据压缩比**：IVFPQ具有较高的压缩比（约为1:0.15）。由于PQ会压缩向量，索引计算仅提供粗略排序后的初步排序结果，需要额外的精细排序以获得最终排序结果，导致更高的计算和延迟。HNSW具有较低的压缩比（约为1:0.8），无需额外处理即可提供精确排序，计算成本和延迟较低，但存储成本较高。
- **召回率调整**：两种索引都支持通过参数调整召回率，但在相似的召回率下，IVFPQ的计算成本更高。
- **缓存策略**：IVFPQ允许通过调整索引块的缓存比例来平衡内存成本和计算延迟，而HNSW目前仅支持全文件缓存。

## 使用方法

每个表仅支持一个向量索引。

### 前提条件

在创建向量索引之前，必须通过设置FE配置项`enable_experimental_vector`为`true`来启用它。

执行以下语句以动态启用：

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_vector" = "true");
```

要永久启用它，必须将`enable_experimental_vector = true`添加到FE配置文件`fe.conf`中并重启FE。

### 创建向量索引

本教程在创建表时创建向量索引。您也可以将向量索引附加到现有表中。有关详细说明，请参见[附加向量索引](#append-vector-index)。

- 以下示例在表`hnsw`的列`vector`上创建一个HNSW向量索引`hnsw_vector`。

    ```SQL
    CREATE TABLE hnsw (
        id     BIGINT(20)   NOT NULL COMMENT "",
        vector ARRAY<FLOAT> NOT NULL COMMENT "",
        INDEX hnsw_vector (vector) USING VECTOR (
            "index_type" = "hnsw", 
            "dim"="5", 
            "metric_type" = "l2_distance", 
            "is_vector_normed" = "false", 
            "M" = "16", 
            "efconstruction" = "40"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1;
    ```
- 以下示例在表`ivfpq`的列`vector`上创建一个IVFPQ向量索引`ivfpq_vector`。

    ```SQL
    CREATE TABLE ivfpq (
        id     BIGINT(20)   NOT NULL COMMENT "",
        vector ARRAY<FLOAT> NOT NULL COMMENT "",
        INDEX ivfpq_vector (vector) USING VECTOR (
            "index_type" = "ivfpq", 
            "dim"="5", 
            "metric_type" = "l2_distance", 
            "is_vector_normed" = "false", 
            "nbits" = "16", 
            "nlist" = "40"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1;
    ```

#### 索引构建参数

##### USING VECTOR

- **默认值**: N/A
- **必需**: 是
- **描述**: 创建一个向量索引。

##### index_type

- **默认值**: N/A
- **必需**: 是
- **描述**: 向量索引类型。有效值：`hnsw` 和 `ivfpq`。

##### dim

- **默认值**: N/A
- **必需**: 是
- **描述**: 索引的维度。索引构建完成后，不符合维度要求的向量将被拒绝加载到基础列中。必须是大于或等于`1`的整数。

##### metric_type

- **默认值**: N/A
- **必需**: 是
- **描述**: 向量索引的度量类型（测量函数）。有效值：
  - `l2_distance`: 欧氏距离。值越小，相似度越高。
  - `cosine_similarity`: 余弦相似度。值越大，相似度越高。

##### is_vector_normed

- **默认值**: false
- **必需**: 否
- **描述**: 向量是否已归一化。有效值为`true`和`false`。仅当`metric_type`为`cosine_similarity`时生效。如果向量已归一化，计算出的距离值将在[-1, 1]之间。向量必须满足平方和为`1`，否则返回错误。

##### M

- **默认值**: 16
- **必需**: 否
- **描述**: HNSW特定参数。图构建过程中为每个新元素创建的双向连接数。必须是大于或等于`2`的整数。`M`的值直接影响图构建和搜索的效率和准确性。在图构建过程中，每个顶点将尝试与其最近的`M`个顶点建立连接。如果一个顶点已经有`M`个连接，但发现了一个更近的顶点，最远的连接将被删除，并与更近的顶点建立新连接。向量搜索将从一个入口点开始，并沿着与其连接的顶点找到最近的邻居。因此，`M`的值越大，每个顶点的搜索范围越大，搜索效率越高，但图构建和存储的成本也越高。

##### efconstruction

- **默认值**: 40
- **必需**: 否
- **描述**: HNSW特定参数。包含最近邻居的候选列表的大小。必须是大于或等于`1`的整数。用于控制图构建过程中的搜索深度。具体来说，`efconstruction`定义了图构建过程中每个顶点的搜索列表（也称为候选列表）的大小。这个候选列表用于存储当前顶点的邻居候选者，列表的大小为`efconstruction`。`efconstruction`的值越大，在图构建过程中被视为顶点邻居的候选者越多，因此图的质量（如更好的连通性）越好，但图构建的时间消耗和计算复杂度也越高。

##### nbits

- **默认值**: 16
- **必需**: 否
- **描述**: IVFPQ特定参数。产品量化（PQ）的精度。必须是`8`的倍数。在IVFPQ中，每个向量被分割为多个子向量，然后每个子向量被量化。`Nbits`定义了量化的精度，即每个子向量被量化为多少个二进制位。`nbits`的值越大，量化精度越高，但存储和计算成本也越高。

##### nlist

- **默认值**: 16
- **必需**: 否
- **描述**: IVFPQ特定参数。簇的数量或倒排列表。必须是大于或等于`1`的整数。在IVFPQ中，数据集被划分为簇，每个簇的质心对应一个倒排列表。向量搜索将首先找到与数据点最近的簇质心，然后在相应的倒排列表中检索最近邻居。因此，`nlist`的值将影响搜索的准确性和效率。`nlist`的值越大，聚类的粒度越细，因此搜索的准确性越高，但搜索的复杂性也越高。

##### M_IVFPQ

- **必需**: 是
- **描述**: IVFPQ特定参数。原始向量将被分割成的子向量数量。IVFPQ索引将一个`dim`维向量分割成`M_IVFPQ`个等长的子向量。因此，它必须是`dim`值的因数。

#### 附加向量索引

您还可以使用[CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md)或[ALTER TABLE ADD INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md)将向量索引添加到现有表中。

示例：

```SQL
CREATE INDEX ivfpq_vector 
ON ivfpq (vector) 
USING VECTOR (
    "index_type" = "ivfpq",
    "metric_type" = "l2_distance", 
    "is_vector_normed" = "false",  
    "dim"="5", 
    "nlist" = "256", 
    "nbits"="10"
);

ALTER TABLE ivfpq 
ADD INDEX ivfpq_vector (vector) 
USING VECTOR (
    "index_type" = "ivfpq",
    "metric_type" = "l2_distance", 
    "is_vector_normed" = "false", 
    "dim"="5", 
    "nlist" = "256", 
    "nbits"="10"
);
```

### 管理向量索引

#### 查看向量索引

您可以使用[SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md)语句查看向量索引的定义：

示例：

```SQL
mysql> SHOW CREATE TABLE hnsw \G
*************************** 1. row ***************************
       Table: hnsw
Create Table: CREATE TABLE hnsw (
  id bigint(20) NOT NULL COMMENT "",
  vector array<float> NOT NULL COMMENT "",
  INDEX index_vector (vector) USING VECTOR("dim" = "5", "efconstruction" = "40", "index_type" = "hnsw", "is_vector_normed" = "false", "M" = "512", "metric_type" = "l2_distance") COMMENT ''
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "false",
"replication_num" = "3"
);
1 row in set (0.00 sec)
```

#### 删除向量索引

您可以使用[DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md)或[ALTER TABLE DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md)删除向量索引。

```SQL
DROP INDEX ivfpq_vector ON ivfpq;
ALTER TABLE ivfpq DROP INDEX ivfpq_vector;
```

### 使用向量索引执行ANNS

在运行向量搜索之前，请确保FE配置项`enable_experimental_vector`设置为`true`。

#### 基于向量索引的查询要求

```SQL
SELECT *, <vector_index_distance_func>(v1, [1,2,3]) as dis
FROM table_name
WHERE <vector_index_distance_func>(v1, [1,2,3]) <= 10
ORDER BY <vector_index_distance_func>(v1, [1,2,3]) 
LIMIT 10
```

要在查询中使用向量索引，必须满足以下所有要求：

- **ORDER BY要求：**
  - ORDER BY子句格式：`ORDER BY`子句必须遵循格式`ORDER BY <vector_index_distance_func>(vector_column, constant_array)`，且不包括其他ORDER BY列。
    - `<vector_index_distance_func>`的函数名要求：
      - 如果`metric_type`是`l2_distance`，函数名必须是`approx_l2_distance`。
      - 如果`metric_type`是`cosine_similarity`，函数名必须是`approx_cosine_similarity`。
    - `<vector_index_distance_func>`的参数要求：
      - `constant_array`必须是一个与向量索引`dim`匹配的常量`ARRAY<FLOAT>`。
      - `vector_column`必须是与向量索引对应的列。
  - ORDER方向要求：
    - 如果`metric_type`是`l2_distance`，顺序必须是`ASC`。
    - 如果`metric_type`是`cosine_similarity`，顺序必须是`DESC`。
  - 必须有`LIMIT N`子句。
- **谓词要求：**
  - 所有谓词必须是`<vector_index_distance_func>`表达式，通过`AND`和比较运算符（`>`或`<`）组合。比较运算符的方向必须与`ASC`/`DESC`顺序一致。具体来说：
  - 要求1：
    - 如果`metric_type`是`l2_distance`：`col_ref <= constant`。
    - 如果`metric_type`是`cosine_similarity`：`col_ref >= constant`。
    - 这里，`col_ref`指的是`<vector_index_distance_func>(vector_column, constant_array)`的结果，可以转换为`FLOAT`或`DOUBLE`类型，例如：
      - `approx_l2_distance(v1, [1,2,3])`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS FLOAT)`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS DOUBLE)`
  - 要求2：
    - 谓词必须使用`AND`，每个子谓词满足要求1。

#### 准备

创建带有向量索引的表并插入向量数据：

```SQL
CREATE TABLE test_hnsw (
    id     BIGINT(20)   NOT NULL COMMENT "",
    vector ARRAY<FLOAT> NOT NULL COMMENT "",
    INDEX index_vector (vector) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "l2_distance", 
        "is_vector_normed" = "false", 
        "M" = "512", 
        "dim"="5")
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO t_test_vector_table VALUES
    (1, [1,2,3,4,5]),
    (2, [4,5,6,7,8]);
    
CREATE TABLE test_ivfpq (
    id     BIGINT(20)   NOT NULL COMMENT "",
    vector ARRAY<FLOAT> NOT NULL COMMENT "",
    INDEX index_vector (vector) USING VECTOR (
        "index_type" = "hnsw",
        "metric_type" = "l2_distance", 
        "is_vector_normed" = "false", 
        "nlist" = "256", 
        "nbits"="10")
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1;

INSERT INTO test_ivfpq VALUES
    (1, [1,2,3,4,5]),
    (2, [4,5,6,7,8]);
```

#### 执行向量搜索

##### 近似搜索

近似搜索将命中向量索引，从而加速搜索过程。

以下示例搜索向量`[1,1,1,1,1]`的前1个近似最近邻。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### 标量-向量联合搜索

您可以将标量搜索与向量搜索结合。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### 范围搜索

您可以对向量数据执行范围搜索。

以下示例将`score < 40`条件下推到索引中，并通过`score`范围过滤向量。

```SQL
SELECT * FROM (
    SELECT id, approx_l2_distance([1,1,1,1,1], vector) score 
    FROM test_hnsw
) a 
WHERE score < 40 
ORDER BY score 
LIMIT 1;
```

##### 精确计算

精确计算将忽略向量索引，直接计算向量之间的距离以获得精确结果。

```SQL
SELECT id, l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw WHERE id = 1 
ORDER BY l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

> **注意**
>
> 不同的距离度量函数对“相似性”的定义不同。对于`l2_distance`，值越小表示相似性越高；对于`cosine_similarity`，值越大表示相似性越高。因此，在计算`topN`时，排序（ORDER BY）方向应与度量的相似性方向一致。对于`l2_distance`使用`ORDER BY ASC LIMIT x`，对于`cosine_similarity`使用`ORDER BY DESC LIMIT x`。

#### 为搜索微调索引参数

参数调优在向量搜索中至关重要，因为它影响性能和准确性。建议在小数据集上调优搜索参数，并在达到预期的召回率和延迟后再转向大数据集。

搜索参数通过SQL语句中的提示传递。

##### 对于HNSW索引

在继续之前，请确保向量列已使用HNSW索引构建。

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{efsearch=256}') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**参数**：

###### efsearch

- **默认值**: 16
- **必需**: 否
- **描述**: 控制精度-速度权衡的参数。在分层图结构搜索中，此参数控制搜索期间候选列表的大小。`efsearch`的值越大，准确性越高，但速度越慢。

##### 对于IVFPQ索引

在继续之前，请确保向量列已使用IVFPQ索引构建。

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{
        nprobe=256,
        max_codes=0,
        scan_table_threshold=0,
        polysemous_ht=0,
        range_search_confidence=0.1
    }') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_ivfpq 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**参数**：

###### nprobe

- **默认值**: 1
- **必需**: 否
- **描述**: 搜索期间检查的倒排列表数量。`nprobe`的值越大，准确性越高，但速度越慢。

###### max_codes

- **默认值**: 0
- **必需**: 否
- **描述**: 每个倒排列表检查的最大代码数。此参数也会影响准确性和速度。

###### scan_table_threshold

- **默认值**: 0
- **必需**: 否
- **描述**: 控制多义哈希的参数。当元素的哈希与要搜索的向量的哈希之间的汉明距离低于此阈值时，该元素将被添加到候选列表中。

###### polysemous_ht

- **默认值**: 0
- **必需**: 否
- **描述**: 控制多义哈希的参数。当元素的哈希与要搜索的向量的哈希之间的汉明距离低于此阈值时，该元素将直接添加到结果中。

###### range_search_confidence

- **默认值**: 0.1
- **必需**: 否
- **描述**: 近似范围搜索的置信度。值范围：[0, 1]。将其设置为`1`可产生最准确的结果。

#### 计算近似召回率

您可以通过将暴力检索的`topK`元素与近似检索的元素相交来计算近似召回率：`Recall = TP / (TP + FN)`。

```SQL
-- 近似检索
SELECT id 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector)
LIMIT 5;
8
9
7
5
1

-- 暴力检索
SELECT id 
FROM test_hnsw
ORDER BY l2_distance([1,1,1,1,1], vector)
LIMIT 5;
8
9
5
7
10
```

在上述示例中，近似检索返回8, 9, 7, 和5。然而，正确的结果是8, 9, 5, 7, 和10。在这种情况下，召回率为4/5=80%。

#### 检查向量索引是否生效

对查询语句执行[EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md)。如果`OlapScanNode`属性显示`VECTORINDEX: ON`，则表示向量索引已应用于近似向量搜索。

示例：

```SQL
> EXPLAIN SELECT id FROM t_test_vector_table ORDER BY approx_l2_distance([1,1,1,1,1], vector) LIMIT 5;

+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                                                      |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                                                                                                     |
|  OUTPUT EXPRS:1: id                                                                                                                                 |
|   PARTITION: UNPARTITIONED                                                                                                                          |
|                                                                                                                                                     |
|   RESULT SINK                                                                                                                                       |
|                                                                                                                                                     |
|   4:Project                                                                                                                                         |
|   |  <slot 1> : 1: id                                                                                                                               |
|   |  limit: 5                                                                                                                                       |
|   |                                                                                                                                                 |
|   3:MERGING-EXCHANGE                                                                                                                                |
|      limit: 5                                                                                                                                       |
|                                                                                                                                                     |
| PLAN FRAGMENT 1                                                                                                                                     |
|  OUTPUT EXPRS:                                                                                                                                      |
|   PARTITION: RANDOM                                                                                                                                 |
|                                                                                                                                                     |
|   STREAM DATA SINK                                                                                                                                  |
|     EXCHANGE ID: 03                                                                                                                                 |
|     UNPARTITIONED                                                                                                                                   |
|                                                                                                                                                     |
|   2:TOP-N                                                                                                                                           |
|   |  order by: <slot 3> 3: approx_l2_distance ASC                                                                                                   |
|   |  offset: 0                                                                                                                                      |
|   |  limit: 5                                                                                                                                       |
|   |                                                                                                                                                 |
|   1:Project                                                                                                                                         |
|   |  <slot 1> : 1: id                                                                                                                               |
|   |  <slot 3> : 4: __vector_approx_l2_distance                                                                                                      |
|   |                                                                                                                                                 |
|   0:OlapScanNode                                                                                                                                    |
|      TABLE: t_test_vector_table                                                                                                                     |
|      VECTORINDEX: ON                                                                                                                                |
|           IVFPQ: OFF, Distance Column: <4:__vector_approx_l2_distance>, LimitK: 5, Order: ASC, Query Vector: [1, 1, 1, 1, 1], Predicate Range: -1.0 |
|      PREAGGREGATION: ON                                                                                                                             |
|      partitions=1/1                                                                                                                                 |
|      rollup: t_test_vector_table                                                                                                                    |
|      tabletRatio=1/1                                                                                                                                |
|      tabletList=11302                                                                                                                               |
|      cardinality=2                                                                                                                                  |
|      avgRowSize=4.0                                                                                                                                 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------+
```

## 限制

- 每个表仅支持一个向量索引。
- 不支持在单个查询语句中对多个向量索引进行搜索。
- 向量搜索不能用作子查询或在连接中使用。