# HLL (HyperLogLog)

## Description

HLL is used for approximate count distinct.

The storage space used by HLL is determined by the distinct values in the hash value. The storage space varies depending on three conditions:

- HLL is empty. No value is inserted into HLL and the storage cost is the lowest, which is 80 bytes.
- The number of distinct hash values in HLL is less than or equal to 160. The highest storage cost is 1360 bytes (80 + 160 * 8 = 1360).
- The number of distinct hash values in HLL is greater than 160. The storage cost is fixed at 16,464 bytes (80 + 16 * 1024 = 16464).

In actual business scenarios, data volume and data distribution affect the memory usage of queries and the accuracy of the approximate result. You need to consider these two factors:

- Data volume: HLL returns an approximate value. A larger data volume results in a more accurate result. A smaller data volume results in larger deviation.
- Data distribution：In the case of large data volume and high-cardinality dimension column for GROUP BY，data computation will use more memory. HLL is not recommended in this situation. It is recommended when you perform no-group-by count distinct or GROUP BY on low-cardinality dimension columns.
- Query granularity: If you query data at a large query granularity, we recommend you use the Aggregate table or materialized view to pre-aggregate data to reduce data volume.

For details about using HLL, see [Use HLL for approximate count distinct](../../../using_starrocks/Using_HLL.md) and [HLL](../data-definition/HLL.md).

## Examples

Specify the column type as HLL when you create a table and use the hll_union() function to aggregate data.

~~~sql
CREATE TABLE hllDemo
(
    k1 TINYINT,
    v1 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1);
~~~
