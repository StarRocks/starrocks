[sql]
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#35'
  and p_container = 'JUMBO CASE'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:49: expr
PARTITION: UNPARTITIONED

RESULT SINK

16:Project
|  <slot 49> : 48: sum / 7.0
|
15:AGGREGATE (merge finalize)
|  output: sum(48: sum)
|  group by:
|
14:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 14
UNPARTITIONED

13:AGGREGATE (update serialize)
|  output: sum(6: L_EXTENDEDPRICE)
|  group by:
|
12:Project
|  <slot 6> : 6: L_EXTENDEDPRICE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 2: L_PARTKEY = 18: P_PARTKEY
|  other join predicates: 5: L_QUANTITY < 0.2 * 45: avg
|
|----10:EXCHANGE
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=24.0
numNodes=0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 29: L_PARTKEY

STREAM DATA SINK
EXCHANGE ID: 10
UNPARTITIONED

9:Project
|  <slot 18> : 18: P_PARTKEY
|  <slot 45> : 45: avg
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  colocate: false, reason:
|  equal join conjunct: 29: L_PARTKEY = 18: P_PARTKEY
|
|----7:EXCHANGE
|
4:AGGREGATE (merge finalize)
|  output: avg(45: avg)
|  group by: 29: L_PARTKEY
|
3:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
HASH_PARTITIONED: 18: P_PARTKEY

6:Project
|  <slot 18> : 18: P_PARTKEY
|
5:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 24: P_CONTAINER = 'JUMBO CASE', 21: P_BRAND = 'Brand#35'
partitions=1/1
rollup: part
tabletRatio=10/10
cardinality=20000
avgRowSize=28.0
numNodes=0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
HASH_PARTITIONED: 29: L_PARTKEY

2:AGGREGATE (update serialize)
|  STREAMING
|  output: avg(32: L_QUANTITY)
|  group by: 29: L_PARTKEY
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=16.0
numNodes=0
[end]

