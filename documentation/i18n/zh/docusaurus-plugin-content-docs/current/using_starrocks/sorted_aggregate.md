# Sorted streaming aggregate

数据库常见的聚合方法有 Hash 聚合和排序聚合。

从 2.5 版本开始，StarRocks 支持了有序的排序聚合 (Sorted streaming aggregate)。

## 原理

聚合节点主要处理 GROUP BY 分组和聚合函数计算。

Sorted streaming aggregate 可以根据输入 key 的有序性，通过直接比较 GROUP BY 列的方式直接进行分组，不需要构建 hash 表，可以有效的减少聚合计算中的内存使用。在聚合基数比较高的情况下，可以提升聚合性能，降低内存使用。

您可以通过设置变量来开启 Sorted streaming aggregate:

```SQL
set enable_sort_aggregate=true;
```

## 使用限制

- GROUP BY 里的 key 需要是做好排序的:
  比如表的排序列是 `k1,k2,k3`，那么:
  - `GROUP BY k1` 和 `GROUP BY k1, k2` 是可以的。
  - `GROUP BY k1, k3` 是不能保证排序的，所以 Sorted streaming aggregate 无法生效。
- 选择的分区只能是一个分区 (因为不同分区中同一个 key 可能分布在不同机器上)。
- GROUP BY 中的 key 需要和建表时指定的分桶键的分布顺序一致。
  比如同样有三列数据 `k1,k2,k3`，可以让数据根据 `k1` 或者 `k1,k2` 进行分桶。
  - 如果根据 `k1` 进行分桶，支持 `GROUP BY k1`，`GROUP BY k1, k2`，`GROUP BY k1,k2,k3`。
  - 如果根据 `k1,k2` 进行分桶，支持 `GROUP BY k1,k2`，`GROUP BY k1,k2,k3`。
  - 对于不符合条件的查询 plan，即使开启了 `enable_sort_aggregate`，sorted streaming aggregate 也无法生效。
- 必须是一阶段聚合（即 AGG 节点下面只连接一个 Scan node）。

## 使用示例

1. 建表并插入数据。

    ```SQL
    CREATE TABLE `test_sorted_streaming_agg_basic`
    (
        `id_int` int(11) NOT NULL COMMENT "",
        `id_string` varchar(100) NOT NULL COMMENT ""
    )
    ENGINE=OLAP 
    DUPLICATE KEY(`id_int`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id_int`)
    PROPERTIES (
    "replication_num" = "3"
    ); 

    INSERT INTO test_sorted_streaming_agg_basic VALUES
    (1, 'v1'),
    (2, 'v2'),
    (3, 'v3'),
    (1, 'v4');
    ```

2. 开启 Sorted streaming aggregate。使用 EXPLAIN 查看。

    ```SQL
    set enable_sort_aggregate = true;

    explain costs select id_int, max(id_string)
    from test_sorted_streaming_agg_basic
    group by id_int;
    ```

## 判断是否开启

查看 Explain costs 的结果。如果可以从 AGG 节点中找到 sorted streaming 字段，证明已开启。

```C++
|                                                                                                                                    |
|   1:AGGREGATE (update finalize)                                                                                                    |
|   |  aggregate: max[([2: id_string, VARCHAR, false]); args: VARCHAR; result: VARCHAR; args nullable: false; result nullable: true] |
|   |  group by: [1: id_int, INT, false]                                                                                             |
|   |  sorted streaming: true                                                                                                        |
|   |  cardinality: 1                                                                                                                |
|   |  column statistics:                                                                                                            |
|   |  * id_int-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                       |
|   |  * max-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                          |
|   |                                                                                                                                |
|   0:OlapScanNode                                                                                                                   |
|      table: test_sorted_streaming_agg_basic, rollup: test_sorted_streaming_agg_basic                                               |
|      preAggregation: on                                                                                                            |
|      partitionsRatio=1/1, tabletsRatio=10/10                                                                                       |
|      tabletList=30672,30674,30676,30678,30680,30682,30684,30686,30688,30690                                                        |
|      actualRows=0, avgRowSize=2.0                                                                                                  |
|      cardinality: 1                                                                                                                |
|      column statistics:                                                                                                            |
|      * id_int-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                       |
|      * id_string-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN                                                                    |
```
