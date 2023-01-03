# HLL (HyperLogLog)

## Description

HLL columns can only be queried or used through the matching [hll_union_agg](../../sql-functions/aggregate-functions/hll_union_agg.md), [Hll_cardinality](../../sql-functions/scalar-functions/hll_cardinality.md), and [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) functions.

## Example

Specify the data type as HLL when you create a table.

~~~sql
CREATE TABLE hllDemo
(
    k1 TINYINT,
    v1 HLL HLL_UNION
)
ENGINE=olap
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 8;
~~~
