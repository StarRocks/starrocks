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

14:Project
|  <slot 49> : 48: sum / 7.0
|  use vectorized: true
|
13:AGGREGATE (merge finalize)
|  output: sum(48: sum)
|  group by:
|  use vectorized: true
|
12:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 12
UNPARTITIONED

11:AGGREGATE (update serialize)
|  output: sum(6: L_EXTENDEDPRICE)
|  group by:
|  use vectorized: true
|
10:Project
|  <slot 6> : 6: L_EXTENDEDPRICE
|  use vectorized: true
|
9:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 2: L_PARTKEY = 18: P_PARTKEY
|  other join predicates: 5: L_QUANTITY < 0.2 * 45: avg
|  use vectorized: true
|
|----8:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=600000000
avgRowSize=24.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 18: P_PARTKEY, 29: L_PARTKEY

STREAM DATA SINK
EXCHANGE ID: 08
UNPARTITIONED

7:AGGREGATE (update finalize)
|  output: avg(32: L_QUANTITY)
|  group by: 18: P_PARTKEY, 29: L_PARTKEY
|  use vectorized: true
|
6:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 06
HASH_PARTITIONED: 18: P_PARTKEY, 29: L_PARTKEY

5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 29: L_PARTKEY = 18: P_PARTKEY
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=600000000
avgRowSize=16.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:Project
|  <slot 18> : 18: P_PARTKEY
|  use vectorized: true
|
2:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 21: P_BRAND = 'Brand#35', 24: P_CONTAINER = 'JUMBO CASE'
partitions=1/1
rollup: part
tabletRatio=10/10
tabletList=10190,10192,10194,10196,10198,10200,10202,10204,10206,10208
cardinality=20000
avgRowSize=28.0
numNodes=0
use vectorized: true
[end]

