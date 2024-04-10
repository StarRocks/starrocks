[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:32: expr
PARTITION: UNPARTITIONED

RESULT SINK

9:Project
|  <slot 32> : 100.0 * 30: sum / 31: sum
|
8:AGGREGATE (merge finalize)
|  output: sum(30: sum), sum(31: sum)
|  group by:
|
7:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:AGGREGATE (update serialize)
|  output: sum(if(22: P_TYPE LIKE 'PROMO%', 34: multiply, 0.0)), sum(29: expr)
|  group by:
|
5:Project
|  <slot 22> : 22: P_TYPE
|  <slot 29> : 34: multiply
|  <slot 34> : clone(34: multiply)
|  common expressions:
|  <slot 33> : 1.0 - 7: L_DISCOUNT
|  <slot 34> : 6: L_EXTENDEDPRICE * 33: subtract
|
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 18: P_PARTKEY = 2: L_PARTKEY
|
|----3:EXCHANGE
|
0:OlapScanNode
TABLE: part
PREAGGREGATION: ON
partitions=1/1
rollup: part
tabletRatio=10/10
cardinality=20000000
avgRowSize=33.0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
BUCKET_SHUFFLE_HASH_PARTITIONED: 2: L_PARTKEY

2:Project
|  <slot 2> : 2: L_PARTKEY
|  <slot 6> : 6: L_EXTENDEDPRICE
|  <slot 7> : 7: L_DISCOUNT
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 11: L_SHIPDATE >= '1997-02-01', 11: L_SHIPDATE < '1997-03-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=6653465
avgRowSize=28.0
[end]

