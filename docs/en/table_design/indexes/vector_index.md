---
displayed_sidebar: docs
sidebar_position: 60
---

# Vector Index

This topic introduces the vector index feature of StarRocks and how to perform an approximate nearest neighbor search (ANNS) with it.

The vector index feature is only supported in shared-nothing clusters of v3.4 or later.

## Overview

Currently, StarRocks supports vector indexing at the Segment file level. The index maps each search item to its row ID within Segment files, allowing fast data retrieval by directly locating the corresponding data rows without brute-force vector distance calculations. The system now provides two types of vector indexing: Inverted File with Product Quantization (IVFPQ) and Hierarchical Navigable Small World (HNSW), each with its own organizational structure.

### Inverted File with Product Quantization (IVFPQ)

Inverted File with Product Quantization (IVFPQ) is a method for approximate nearest neighbor search in large-scale high-dimensional vectors, commonly used in vector retrieval tasks within deep learning and machine learning. IVFPQ consists of two main components: the inverted file and product quantization.

- **Inverted File**: This component is an indexing method. It divides the dataset into multiple clusters (or Voronoi cells), each with a centroid (seed point), and assigns each data point (vector) to its nearest cluster center. For a vector search, IVFPQ only needs to search the nearest cluster, significantly reducing the search scope and complexity.
- **Product Quantization**: This component is a data compression technique. It splits high-dimensional vectors into subvectors and quantizes each subvector to map it to the nearest point in a predefined set, reducing storage and computation costs while retaining high precision.

By combining inverted files and product quantization, IVFPQ enables efficient approximate nearest neighbor search in large, high-dimensional datasets.

### Hierarchical Navigable Small World (HNSW)

Hierarchical Navigable Small World (HNSW) is a graph-based algorithm for high-dimensional nearest neighbor search, also widely used in vector retrieval tasks. 

HNSW builds a hierarchical graph structure in which each layer is a navigable small world (NSW) graph. In the graph, each vertex represents a data point, and edges indicate similarity between vertices. The higher layers of the graph contain fewer vertices with sparser connections for fast global search, while the lower layers include all vertices with denser connections for precise local search.

For a vector search, HNSW searches at the top layer first, quickly identifying an approximate nearest neighbor region, then moves down layer by layer to find the exact nearest neighbor at the bottom layer.

HNSW offers both efficiency and precision, making it adaptable to various data and query distributions.

### Comparison between IVFPQ and HNSW

- **Data Compression Ratio**: IVFPQ has a higher compression ratio (around 1:0.15). The indexing calculation only provides preliminary sorting results after rough ranking because PQ will compress the vectors. It requires additional fine ranking for final sorting results, leading to higher computation and latency. HNSW has a lower compression ratio (around 1:0.8) and provides precise ranking without extra processing, resulting in lower computation costs and latency and higher storage costs.
- **Recall Adjustment**: Both indexes support recall rate tuning through parameter adjustments, but IVFPQ incurs higher computation costs at similar recall rates.
- **Caching Strategy**: IVFPQ allows balancing memory cost and computation latency by adjusting the cache proportion of index blocks, while HNSW currently only supports full-file caching.

## Usage

Each table supports only one vector index.

### Prerequisites

Before creating vector indexes, you must enable it by setting the FE configuration item `enable_experimental_vector` to `true`.

Execute the following statement to enable it dynamically:

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_vector" = "true");
```

To enable it permanently, you must add `enable_experimental_vector = true` to the FE configuration file `fe.conf` and restart FE.

### Create vector index

This tutorial creates vector indexes while creating tables. You can also append vector indexes to an existing table. See [Append vector index](#append-vector-index) for detailed instructions.

- The following example creates an HNSW vector index `hnsw_vector` on column `vector` in table `hnsw`.

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
- The following example creates an IVFPQ vector index `ivfpq_vector` on column `vector` in table `ivfpq`.

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

#### Index construction parameters

##### USING VECTOR

- **Default**: N/A
- **Required**: Yes
- **Description**: Creates a vector index.

##### index_type

- **Default**: N/A
- **Required**: Yes
- **Description**: Vector index type. Valid Values: `hnsw` and `ivfpq`.

##### dim

- **Default**: N/A
- **Required**: Yes
- **Description**: Dimension of the index. After the index is built, vectors that do not meet the dimension requirements will be rejected for loading into the base column. It must be an integer greater than or equal to `1`.

##### metric_type

- **Default**: N/A
- **Required**: Yes
- **Description**: Metric type (measurement function) of the vector index. Valid values:
  - `l2_distance`: Euclidean distance. The smaller the value, the higher the similarity.
  - `cosine_similarity`: Cosine similarity. The larger the value, the higher the similarity.

##### is_vector_normed

- **Default**: false
- **Required**: No
- **Description**: Whether the vectors are normalized. Valid values are `true` and `false`. It takes effect only when `metric_type` is `cosine_similarity`. If the vectors are normalized, the value of the calculated distances will be within [-1, 1]. The vectors must satisfy that the sum of the squares is `1`, otherwise an error is returned.

##### M

- **Default**: 16
- **Required**: No
- **Description**: HNSW-specific parameter. The number of bi-directional connections created for every new element during graph construction. It must be an integer greater than or equal to `2`. The value of `M` directly affects the efficiency and accuracy of graph construction and search. During graph construction, each vertex will attempt to establish connections with its nearest `M` vertices. If a vertex already has `M` connections but finds a closer vertex, the farthest connection will be deleted and a new connection will be established with the closer vertex. A vector search will start from an entry point and find the nearest neighbor along the vertices connected to it. Therefore, the larger the value of `M`, the larger the search scope for each vertex, the higher the search efficiency, but the higher the cost of graph construction and storage.

##### efconstruction

- **Default**: 40
- **Required**: No
- **Description**: HNSW-specific parameter. The size of the candidate list containing the nearest neighbors.  It must be an integer greater than or equal to `1`. It is used to control the search depth during the graph construction process. Specifically, `efconstruction` defines the size of the search list (also known as the candidate list) for each vertex during the graph construction process. This candidate list is used to store the neighbor candidates of the current vertex, and the size of the list is `efconstruction`. The larger the value of `efconstruction`, the more candidates is considered as the neighbors of the vertex during the graph construction process, and, as a result, the better quality (such as better connectivity) of the graph, but also the higher time consumption and computation complexity of graph construction.

##### nbits

- **Default**: 16
- **Required**: No
- **Description**: IVFPQ-specific parameter. Precision of Product Quantization (PQ).  It must be the multiplier of `8`. With IVFPQ, each vector is divided into multiple subvectors, and then each subvector is quantized. `Nbits` defines the precision of quantization, that is, how many binary digits each subvector is quantized into. The larger the value of `nbits`, the higher the quantization precision, but the higher storage and computation costs.

##### nlist

- **Default**: 16
- **Required**: No
- **Description**: IVFPQ-specific parameter. Number of clusters, or inverted lists. It must be an integer greater than or equal to `1`. With IVFPQ, the dataset is divided into clusters, and the centroid of each cluster corresponds to an inverted list. A vector search will first find the cluster centroid closest to the data point, and then retrieve the nearest neibors in the corresponding inverted list. Therefore, the value of `nlist` will affect the accuracy and efficiency of the search. The larger the value of `nlist`, the finer the granularity of clustering, thus the higher the search accuracy, but also the higher the search complexity.

##### M_IVFPQ

- **Required**: Yes
- **Description**: IVFPQ-specific parameter. Number of the subvectors the original vector will be split into. The IVFPQ index will divide a `dim`-dimensional vector into `M_IVFPQ` equal-length subvectors. Therefore, it must be a factor of the value of `dim`.

#### Append vector index

You can also add vector indexes to an existing table using [CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md) or [ALTER TABLE ADD INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md).

Example:

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

### Manage vector indexes

#### View vector index

You can view the definition of a vector index using the [SHOW CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_CREATE_TABLE.md) statement:

Example:

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

#### Drop vector index

You can drop vector indexes using [DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md) or [ALTER TABLE DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md).

```SQL
DROP INDEX ivfpq_vector ON ivfpq;
ALTER TABLE ivfpq DROP INDEX ivfpq_vector;
```

### Perform ANNS with vector indexes

Before running a vector search, make sure the FE configuration item `enable_experimental_vector` is set to `true`.

#### Requirements for vector index-based queries

```SQL
SELECT *, <vector_index_distance_func>(v1, [1,2,3]) as dis
FROM table_name
WHERE <vector_index_distance_func>(v1, [1,2,3]) <= 10
ORDER BY <vector_index_distance_func>(v1, [1,2,3]) 
LIMIT 10
```

To use vector index in queries, all the following requirements must be met:

- **ORDER BY requirements:**
  - ORDER BY clause format: The `ORDER BY` clause must follow the format `ORDER BY <vector_index_distance_func>(vector_column, constant_array)` without including additional ORDER BY columns.
    - Function name requirements for `<vector_index_distance_func>`:
      - If `metric_type` is `l2_distance`, the function name must be `approx_l2_distance`.
      - If `metric_type` is `cosine_similarity`, the function name must be `approx_cosine_similarity`.
    - Parameter requirements for `<vector_index_distance_func>`:
      - One of the column `constant_array` must be a constant `ARRAY<FLOAT>` with dimensions matching the vector index `dim`.
      - The other column `vector_column` must be the column corresponding to the vector index.
  - ORDER direction requirements:
    - If `metric_type` is `l2_distance`, the order must be `ASC`.
    - If `metric_type` is `cosine_similarity`, the order must be `DESC`.
  - A `LIMIT N` clause is required.
- **Predicate Requirements:**
  - All predicates must be `<vector_index_distance_func>` expressions, combined using `AND` and comparison operators (`>` or `<`). The comparison operator direction must align with the `ASC`/`DESC` order. Specifically:
  - Requirement 1:
    - If `metric_type` is `l2_distance`: `col_ref <= constant`.
    - If `metric_type` is `cosine_similarity`: `col_ref >= constant`.
    - Here, `col_ref` refers to the result of `<vector_index_distance_func>(vector_column, constant_array)` and can be cast to `FLOAT` or `DOUBLE` types, for example:
      - `approx_l2_distance(v1, [1,2,3])`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS FLOAT)`
      - `CAST(approx_l2_distance(v1, [1,2,3]) AS DOUBLE)`
  - Requirement 2:
    - The predicate must use `AND`, with each child predicate meeting Requirement 1.

#### Preparation

Create tables with vector indexes and insert vector data into them:

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

#### Perform vector search

##### Approximate search

Approximate searches will hit the vector index and thereby accelerate the search process.

The following example searches for the top 1 approximate nearest neighbor of the vector `[1,1,1,1,1]`.

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### Scalar-vector joint search

You can combine scalar search with vector search.

```SQL
SELECT id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

##### Range search

You can perform range search on vector data.

The following example pushes the `score < 40` condition down to the index and filters vectors by `score` range.

```SQL
SELECT * FROM (
    SELECT id, approx_l2_distance([1,1,1,1,1], vector) score 
    FROM test_hnsw
) a 
WHERE score < 40 
ORDER BY score 
LIMIT 1;
```

##### Precise calculation

Precise calculations will ignore the vector index, and directly calculate distances between the vectors for precise results.

```SQL
SELECT id, l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw WHERE id = 1 
ORDER BY l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

> **NOTE**
>
> Different distance metric functions define "similarity" differently. For `l2_distance`, smaller values indicate higher similarity; for `cosine_similarity`, larger values indicate higher similarity. Therefore, when calculating `topN`, the sorting (ORDER BY) direction should match the similarity direction of the metric. Use `ORDER BY ASC LIMIT x` for `l2_distance`, and `ORDER BY DESC LIMIT x` for `cosine_similarity`.

#### Fine tune index parameters for search

Parameter tuning is essential in vector search, as it affects both performance and accuracy. It is recommended to tune the search paramters on a small dataset and move to large datasets only when expected recall and latency are achieved.

Search parameters are passed through hints in SQL statements.

##### For HNSW index

Before proceeding, make sure the vector column is built with the HNSW index.

```SQL
SELECT 
    /*+ SET_VAR (ann_params='{efsearch=256}') */ 
    id, approx_l2_distance([1,1,1,1,1], vector) 
FROM test_hnsw 
WHERE id = 1 
ORDER BY approx_l2_distance([1,1,1,1,1], vector) 
LIMIT 1;
```

**Parameter**:

###### efsearch

- **Default**: 16
- **Required**: No
- **Description**: Parameter that controls precision-speed tradeoff. During a hierarchical graph structure search, this parameter controls the size of the candidate list during the search. The larger the value of `efsearch`, the higher the accuracy, but the lower the speed.

##### For IVFPQ index

Before proceeding, make sure the vector column is built with the IVFPQ index.

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

**Parameter**:

###### nprobe

- **Default**: 1
- **Required**: No
- **Description**: Number of inverted lists checked during the search. The larger the value of `nprobe`, the higher the accuracy, but the lower the speed.

###### max_codes

- **Default**: 0
- **Required**: No
- **Description**: Maximum codes checked per inverted list. This parameter will also affect accuracy and speed.

###### scan_table_threshold

- **Default**: 0
- **Required**: No
- **Description**: Parameter that controls polysemous hashing. When the Hamming distance between an element's hash and the hash of the vector to be searched is below this threshold, the element is added to the candidate list.

###### polysemous_ht

- **Default**: 0
- **Required**: No
- **Description**: Parameter that controls polysemous hashing. When the Hamming distance between an element's hash and the hash of the vector to be searched is below this threshold, the element is directly added to the results.

###### range_search_confidence

- **Default**: 0.1
- **Required**: No
- **Description**: Confidence of approximate range searches. Value range: [0, 1]. Setting it to `1` produces the most accurate results.

#### Calculate approximate recall

You can calculate approximate recall by intersecting the `topK` elements from brute-force retrieval with those from approximate retrieval: `Recall = TP / (TP + FN)`.

```SQL
-- Approximate retrieval
SELECT id 
FROM test_hnsw 
ORDER BY approx_l2_distance([1,1,1,1,1], vector)
LIMIT 5;
8
9
7
5
1

-- Brute-force retrieval
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

In the preceding example, approximate retrieval returns 8, 9, 7, and 5. However, the correct result is 8, 9, 5, 7, and 10. In this case, recall is 4/5=80%.

#### Check if vector indexes take effect

Execute [EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) against the query statement. If `OlapScanNode` properties show `VECTORINDEX: ON`, it indicates that the vector index has been applied to approximate vector searches.

Example:

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

## Limitations

- Each table supports only one vector index.
- Searches on multiple vector indexes in a single query statement are unsupported.
- Vector search cannot be used as subqueries or in joins.

