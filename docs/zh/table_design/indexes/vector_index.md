---
displayed_sidebar: docs
sidebar_position: 60
---

# 向量索引

本文介绍 StarRocks 的向量索引功能，以及如何使用该功能进行近似最近邻搜索（ANNS）。

仅 v3.4 及以上版本的存算一体集群支持向量索引功能。

## 概述

StarRocks 目前支持的向量索引是基于 Segment 文件的索引。索引记录了搜索项与 Segment 文件中数据行号的映射关系，通过快速查找索引文件，可以直接定位到相应数据行，避免了暴力的向量距离计算。StarRocks 当前支持两种类型的向量索引，分别为倒排乘积量化（IVFPQ）和分层小世界图（HNSW），它们各自具有不同的组织方式。

### 倒排乘积量化（IVFPQ）

倒排乘积量化（Inverted File with Product Quantization，简称 IVFPQ）是一种用于大规模高维向量的近似最近邻搜索方法，广泛应用于深度学习和机器学习中的向量检索任务。IVFPQ 主要包括两个部分：倒排文件和乘积量化。

- **倒排文件（Inverted File）**：这是一种索引方法，将数据集分为多个聚类（或称为 Voronoi 单元），每个聚类有一个中心，所有数据点根据距离分配到最近的聚类中心。查询时，只需在与被查询向量最接近的聚类中查找，从而显著缩小搜索范围和计算复杂度。
- **乘积量化****（****Product Quantization****）**：这是一种压缩数据的方法，它将高维向量分割为多个子向量，并对每个子向量进行量化（即将其映射到预定义的有限集合中的最近点），从而压缩数据，减少存储和计算开销，同时保持一定的精度。

IVFPQ 结合了倒排文件和乘积量化，能够在大规模高维数据集上实现高效的近似最近邻搜索。

### 分层小世界图（HNSW）

分层小世界图（Hierarchical Navigable Small World，简称 HNSW）是一种基于图的高维向量最近邻搜索算法，也广泛应用于向量检索任务。

HNSW 构建了一个层次化的图结构，每层都是一个小世界网络（navigable small world graph，简称 NSW）。在 NSW 图中，每个节点代表一个数据点，节点之间的边表示它们的相似性。高层图仅包含部分节点且连接较稀疏，用于快速全局搜索；低层图包含所有节点，连接更密集，用于精确的局部搜索。查询时，HNSW 先在高层进行搜索，定位到近似邻居区域，再逐层向下查找，直至最底层找到最近邻。

HNSW 具有高效率和高精度的特点，适用于多种数据和查询分布情况。

### IVFPQ 与 HNSW 的对比

- **数据压缩****比**：IVFPQ 压缩比较高（约为 1:0.15），但需进行精排计算以获得最终排序结果，因此计算开销和延迟较大；HNSW 压缩比较低（约为 1:0.8），但无需额外精排，因此计算开销和延迟更低。
- **召回率调整**：两种索引均可通过参数调整召回率（Recall），但 IVFPQ 在相同召回率下的计算成本更高。
- **缓存策略**：IVFPQ 可通过调整索引数据块的缓存占比来平衡内存成本和计算延迟，但当前 HNSW 仅支持全量文件缓存。

## 使用方法

每张表只支持创建一个向量索引。

### 前提条件

在创建向量索引之前，需要通过设置FE配置项 `enable_experimental_vector` 为 `true` 来启用向量索引功能。

您可以通过以下命令动态启用此功能：

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_vector" = "true");
```

要永久启用此功能，需在 FE 配置文件 **fe.conf** 中添加 `enable_experimental_vector = true` 并重启 FE。

### 创建向量索引

以下示例在建表时创建向量索引。您也可以向现有表中添加向量索引，详见[添加向量索引](#添加向量索引)。

- 以下示例基于 `hnsw` 表中的 `vector` 列创建  HNSW 向量索引 `hnsw_vector`。
  - ```SQL
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
- 以下示例基于 `ivfpq` 表中的 `vector` 列创建  IVFPQ 向量索引 `ivfpq_vector`。
  - ```SQL
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

- **默认值**：N/A
- **是否必填：**是
- **描述**：创建向量索引。

##### index_type

- **默认值**：N/A
- **是否必填：**是
- **描述**：向量索引类型。有效值为 `hnsw` 和 `ivfpq`。

##### dim

- **默认值**：N/A
- **是否必填：**是
- **描述**：索引的维度。在索引构建完成后，不符合维度要求的向量将被拒绝写入至对应列。必须为大于等于 `1` 的整数。

##### metric_type

- **默认值**：N/A
- **是否必填**：是
- **描述**：向量索引的度量类型（测量函数）。有效值：
  - `l2_distance`：欧几里得距离。值越小，相似度越高。
  - `cosine_similarity`：余弦相似度。值越大，相似度越高。

##### is_vector_normed

- **默认值**：false
- **是否必填**：否
- **描述**：是否已对向量进行归一化。有效值为 `true` 和 `false`。仅在 `metric_type` 为 `cosine_similarity` 时生效。若向量已归一化，计算所得距离值将介于 [-1, 1] 范围内。向量必须满足平方和为 `1` 的条件，否则将返回错误。

##### M

- **默认值**：16
- **是否必填**：否
- **描述**：HNSW 专用参数。每个新元素在图构建过程中所创建的双向连接数量。必须为大于等于 `2` 的整数。`M` 值直接影响图构建和搜索的效率与精度。在图构建过程中，每个顶点会尝试与其最近的 `M` 个顶点建立连接。如果顶点已有 `M` 个连接，但发现一个更近的顶点，则会删除最远的连接并与更近的顶点建立新连接。向量搜索会从一个入口点开始，沿着已连接的顶点找到最近邻。因此，`M` 值越大，每个顶点的搜索范围越大，搜索效率越高，但图构建和存储的成本也会增加。

##### efconstruction

- **默认值**：40
- **是否必填**：否
- **描述**：HNSW 专用参数。用于控制图构建过程中的搜索深度。必须为大于等于 `1` 的整数。具体来说，`efconstruction` 定义了图构建过程中每个顶点的候选列表大小，用于存储当前顶点的最近邻候选项。`efconstruction` 值越大，在图构建过程中考虑的最近邻候选项越多，图的质量（如连通性）越好，但同时图构建的时间和计算复杂度也会增加。

##### nbits

- **默认值**：16
- **否必填**：否
- **描述**：IVFPQ 专用参数。表示乘积量化（PQ）的精度。必须为 `8` 的倍数。在 IVFPQ 中，每个向量被分成多个子向量，然后每个子向量被量化。`nbits` 定义了量化的精度，即每个子向量被量化为多少位的二进制数。`nbits` 值越大，量化精度越高，但存储和计算成本也会增加。

##### nlist

- **默认值**：16
- **是否必填**：否
- **描述**：IVFPQ 专用参数。表示聚类数量，或倒排列表数量。必须为大于等于 `1` 的整数。在 IVFPQ 中，数据集被分成多个聚类，每个聚类的中心对应一个倒排列表。向量搜索首先找到距离数据点最近的聚类中心，然后在对应的倒排列表中检索最近邻。因此，`nlist` 值会影响搜索的准确性和效率。`nlist` 值越大，聚类的粒度越精细，搜索准确性越高，但搜索复杂性也随之提高。

##### M_IVFPQ

- **是否必填**：是
- **描述**：IVFPQ 专用参数。将原始向量分割成的子向量的个数。IVFPQ 索引会将一个 `dim` 维度的向量分割成 `M_IVFPQ` 个等长子向量。因此，该值必须是 `dim` 值的一个因数。

#### 添加向量索引

可以使用 [CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md) 或 [ALTER TABLE ADD INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 向现有表中添加向量索引。

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

您可以使用 [SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) 语句查看向量索引的定义：

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

您可以使用 [DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md) 或 [ALTER TABLE DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md) 删除向量索引。

```SQL
DROP INDEX ivfpq_vector ON ivfpq;
ALTER TABLE ivfpq DROP INDEX ivfpq_vector;
```

### 使用向量索引进行近似最近邻搜索（ANNS）

在执行向量搜索之前，请确保 FE 配置项 `enable_experimental_vector` 设置为 `true`。

#### 基于向量索引查询的要求

```SQL
SELECT *, <vector_index_distance_func>(v1, [1,2,3]) as dis
FROM table_name
WHERE <vector_index_distance_func>(v1, [1,2,3]) <= 10
ORDER BY <vector_index_distance_func>(v1, [1,2,3]) 
LIMIT 10
```

基于向量索引进行查询，需要满足以下条件：

- **ORDER BY 要求：**

  - ORDER BY 语句格式： 格式必须是 `ORDER BY <vector_index_distance_func>(vector_column, constant_array)`，且不能包含其他 ORDER BY 列。
    - `<vector_index_distance_func>` 函数名要求：
      - 当 `metric_type` 为 `l2_distance` 时，函数名应为 `approx_l2_distance`。
      - 当 `metric_type` 为 `cosine_similarity` 时，函数名应为 `approx_cosine_similarity`。
    - `<vector_index_distance_func>` 参数要求：
      - 其中一列 `constant_array` 必须是常量 `ARRAY<FLOAT>` 类型，其维度需与向量索引的 `dim` 相同。
      - 另一列 `vector_column` 为与向量索引对应的列。
  - ORDER 顺序要求：
    - 当 `metric_type` 为 `l2_distance` 时，需为 `ASC`。
    - 当 `metric_type` 为 `cosine_similarity` 时，需为 `DESC`。
  - 必须包含 `LIMIT N`。

- **谓词要求：**

  - 1.  所有谓词必须为 `<vector_index_distance_func>`，由 AND 运算符和 `>`、`<` 组合构成，并且 `>`、`<` 的方向需与 ASC/DESC 顺序一致。具体要求如下：

- 要求一：

  - 当 `metric_type` 为 `l2_distance` 时：`col_ref <= constant`。
  - 当 `metric_type` 为 `cosine_similarity` 时：`col_ref >= constant`。
  - 其中，`col_ref` 是 `<vector_index_distance_func>(vector_column, constant_array)` 的计算结果，可转换为 `FLOAT` or `DOUBLE` 类型，例如：
    - `approx_l2_distance(v1, [1,2,3])`
    - `CAST(approx_l2_distance(v1, [1,2,3]) AS FLOAT)`
    - `CAST(approx_l2_distance(v1, [1,2,3]) AS DOUBLE)`

- 要求二：

  - 为 AND 谓词，且每个子谓词均需满足要求一。

#### 准备工作

创建带有向量索引的表并向其中插入向量数据：

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

以下示例检索向量 `[1,1,1,1,1]` 的近似最近邻。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### 标量-向量联合搜索

您可以将标量搜索与向量搜索结合使用。

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### 范围搜索

您可以在向量数据上执行范围搜索。

以下示例将 `score < 40` 条件下推至索引进行过滤，按 `score` 范围过滤向量。

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

精确计算会忽略向量索引，直接计算向量之间的距离以获得精确结果。

```SQL
SELECT id, l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw WHERE id = 1 
ORDER BY l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

> **说明**
>
> 不同的距离度量函数定义“相似性”的方式不同。对于 `l2_distance`，较小的值表示更高的相似性；对于 `cosine_similarity`，较大的值表示更高的相似性。因此，在计算 `topN` 时，排序（ORDER BY）方向应与度量的相似性方向一致。`l2_distance` 使用 `ORDER BY ASC LIMIT x`，`cosine_similarity` 使用 `ORDER BY DESC LIMIT x`。

#### 调优搜索的索引参数

在向量搜索中，参数调优对性能和精度至关重要。建议在小数据集上调优搜索参数，达到预期的召回率和延迟后再转移至大数据集。

搜索参数通过 SQL 语句中的 Hint 传递。

##### 对于 HNSW 索引

在继续操作之前，确保向量列已构建 HNSW 索引。

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{efsearch=256}') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**参数：**

###### efsearch

- **默认值**：16
- **是否必填**：否
- **描述**：控制精度和速度之间的权衡。在层次化图结构搜索中，该参数控制搜索过程中的候选列表大小。`efsearch` 值越大，准确性越高，但速度越慢。

##### 对于 IVFPQ 索引

在继续操作之前，确保向量列已构建 IVFPQ 索引。

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

**参数：**

###### nprobe

- **默认值**：1
- **是否必填**：否
- **描述**：搜索过程中检查的倒排列表数量。`nprobe` 值越大，准确性越高，但速度越慢。

###### max_codes

- **默认值**：0
- **是否必填**：否
- **描述**：每个倒排列表检查的最大编码数。此参数也会影响准确性和速度。

###### scan_table_threshold

- **默认值**：0
- **是否必填**：否
- **描述**：控制多义性哈希的参数。当元素的哈希与待搜索向量的哈希之间的汉明距离小于此阈值时，元素会被加入候选列表。

###### polysemous_ht

- **默认值**：0
- **是否必填**：否
- **描述**：控制多义性哈希的参数。当元素的哈希与待搜索向量的哈希之间的汉明距离小于此阈值时，元素会直接加入结果中。

###### range_search_confidence

- **默认值**：0.1
- **是否必填**：否
- **描述**：近似范围搜索的置信度。取值范围为 [0, 1]。设为 `1` 时结果最精确。

#### 计算近似召回率

您可以通过将暴力检索的 `topK` 元素与近似检索的 `topK` 元素求交集来计算近似召回率：`Recall = TP / (TP + FN)`。

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

在上述示例中，近似检索返回的结果为 8、9、7 和 5，而正确结果为 8、9、5、7 和 10。因此，召回率为 4/5=80%。

#### 检查向量索引是否生效

针对查询语句执行 [EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md)，如果 `OlapScanNode` 属性显示 `VECTORINDEX: ON`，则表示向量索引已应用于近似向量搜索。

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

- 每张表只支持创建一个向量索引。
- 不支持在单个查询语句中对多个向量索引进行搜索。
- 向量搜索无法在子查询或 Join 使用。
