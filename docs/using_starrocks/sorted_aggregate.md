# Sorted streaming aggregate

Common aggregation methods in database systems include hash aggregate and sort aggregate.

From v2.5 onwards, StarRocks supports **sorted streaming aggregate**.

## Working principles

Aggregate nodes (AGG) are mainly responsible for handling GROUP BY and aggregate functions.

Sorted streaming aggregate can group data by comparing GROUP BY keys according to the sequence of the keys, without the need to create a hash table. This effectively reduces memory resources consumed by aggregation. For queries with a high aggregation cardinality, sorted streaming aggregate improves aggregation performance and reduces memory usage.

You can enable sorted streaming aggregate by setting the following variable:

```SQL
set enable_sort_aggregate=true;
```

## Limits

- The keys in GROUP BY must have a sequence. For example, if the sort key is `k1, k2, k3`, then:
  - `GROUP BY k1`  and  `GROUP BY k1, k2` are allowed.
  - `GROUP BY k1, k3` does not follow the sort key sequence. Therefore, sorted streaming aggregate cannot take effect on such a clause.
- The selected partition must be a single partition (because the same key may be distributed on different machines in different partitions).
- The GROUP BY keys must have the same distribution as the bucket keys specified when you create a table. For example, if a table has three columns `k1, k2, k3`, the bucket key can be `k1` or `k1, k2`.
  - If the bucket key is `k1`, the `GROUP BY` key can be `k1`, `k1, k2`, or `k1, k2, k3`.
  - If the bucket key is `k1, k2`, the GROUP BY key can be `k1, k2` or `k1, k2, k3`.
  - If the query plans do not meet this requirement, the sorted streaming aggregate feature cannot take effect even if this feature is enabled.
- Sorted streaming aggregate works only for first-stage aggregation (that is, there is only one Scan node under the AGG node).

## Examples

1. Create a table and insert data.

    ```SQL
    CREATE TABLE `test_sorted_streaming_agg_basic`
    (
        `id_int` int(11) NOT NULL COMMENT "",
        `id_string` varchar(100) NOT NULL COMMENT ""
    ) 
    ENGINE=OLAP 
    DUPLICATE KEY(`id_int`)COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id_int`) BUCKETS 10 
    PROPERTIES
    ("replication_num" = "1"); 

    INSERT INTO test_sorted_streaming_agg_basic VALUES
    (1, 'v1'),
    (2, 'v2'),
    (3, 'v3'),
    (1, 'v4');
    ```

2. Enable sorted streaming aggregate and use EXPLAIN to query the SQL profile.

    ```SQL
    set enable_sort_aggregate = true;

    explain costs select id_int, max(id_string)
    from test_sorted_streaming_agg_basic
    group by id_int;
    ```

## Check whether sorted streaming aggregate is enabled

View the results of `EXPLAIN costs`. If the `sorted streaming` field is `true` in the AGG node, this feature is enabled.

```Plain
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