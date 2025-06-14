[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:10: P_BRAND | 11: P_TYPE | 12: P_SIZE | 26: count
PARTITION: UNPARTITIONED

RESULT SINK

15:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:SORT
|  order by: <slot 26> 26: count DESC, <slot 10> 10: P_BRAND ASC, <slot 11> 11: P_TYPE ASC, <slot 12> 12: P_SIZE ASC
|  offset: 0
|
13:AGGREGATE (update finalize)
|  output: count(2: PS_SUPPKEY)
|  group by: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
|
12:AGGREGATE (merge serialize)
|  group by: 2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
|
11:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 11
HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE

10:AGGREGATE (update serialize)
|  STREAMING
|  group by: 2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
|
9:Project
|  <slot 2> : 2: PS_SUPPKEY
|  <slot 10> : 10: P_BRAND
|  <slot 11> : 11: P_TYPE
|  <slot 12> : 12: P_SIZE
|
8:HASH JOIN
|  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 2: PS_SUPPKEY = 17: S_SUPPKEY
|
|----7:EXCHANGE
|
4:Project
|  <slot 2> : 2: PS_SUPPKEY
|  <slot 10> : 10: P_BRAND
|  <slot 11> : 11: P_TYPE
|  <slot 12> : 12: P_SIZE
|
3:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 1: PS_PARTKEY = 7: P_PARTKEY
|
|----2:EXCHANGE
|
0:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
cardinality=80000000
avgRowSize=16.0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:Project
|  <slot 17> : 17: S_SUPPKEY
|
5:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
PREDICATES: 23: S_COMMENT LIKE '%Customer%Complaints%'
partitions=1/1
rollup: supplier
tabletRatio=1/1
cardinality=250000
avgRowSize=105.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
BUCKET_SHUFFLE_HASH_PARTITIONED: 7: P_PARTKEY

1:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 10: P_BRAND != 'Brand#43', NOT (11: P_TYPE LIKE 'PROMO BURNISHED%'), 12: P_SIZE IN (31, 43, 9, 6, 18, 11, 25, 1)
partitions=1/1
rollup: part
tabletRatio=10/10
cardinality=2304000
avgRowSize=47.0
[end]

