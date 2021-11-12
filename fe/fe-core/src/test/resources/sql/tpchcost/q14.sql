[sql]
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
        l_partkey = p_partkey
  and l_shipdate >= date '1997-02-01'
  and l_shipdate < date '1997-03-01';
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:32: expr
PARTITION: UNPARTITIONED

RESULT SINK

9:Project
|  <slot 32> : 100.0 * 30: sum(28: case) / 31: sum(29: expr)
|  use vectorized: true
|
8:AGGREGATE (merge finalize)
|  output: sum(30: sum(28: case)), sum(31: sum(29: expr))
|  group by:
|  use vectorized: true
|
7:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:AGGREGATE (update serialize)
|  output: sum(28: case), sum(29: expr)
|  group by:
|  use vectorized: true
|
5:Project
|  <slot 28> : if(22: P_TYPE LIKE 'PROMO%', 34: multiply, 0.0)
|  <slot 29> : 34: multiply
|  common expressions:
|  <slot 33> : 1.0 - 7: L_DISCOUNT
|  <slot 34> : 6: L_EXTENDEDPRICE * 33: subtract
|  use vectorized: true
|
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 18: P_PARTKEY = 2: L_PARTKEY
|  use vectorized: true
|
|----3:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: part
PREAGGREGATION: ON
partitions=1/1
rollup: part
tabletRatio=10/10
tabletList=10190,10192,10194,10196,10198,10200,10202,10204,10206,10208
cardinality=20000000
avgRowSize=33.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
BUCKET_SHFFULE_HASH_PARTITIONED: 2: L_PARTKEY

2:Project
|  <slot 2> : 2: L_PARTKEY
|  <slot 6> : 6: L_EXTENDEDPRICE
|  <slot 7> : 7: L_DISCOUNT
|  use vectorized: true
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 11: L_SHIPDATE >= '1997-02-01', 11: L_SHIPDATE < '1997-03-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=6653465
avgRowSize=28.0
numNodes=0
use vectorized: true
[end]

