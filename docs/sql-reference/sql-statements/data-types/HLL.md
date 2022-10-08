# HLL (HyperLogLog)

## Description

HLL columns can only be queried or used through the matching hll_union_agg, hll_raw_agg, hll_cardinality, and hll_hash functions.

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
