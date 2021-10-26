[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:19: sum(18: expr)
PARTITION: UNPARTITIONED

RESULT SINK

4:AGGREGATE (merge finalize)
|  output: sum(19: sum(18: expr))
|  group by:
|  use vectorized: true
|
3:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
UNPARTITIONED

2:AGGREGATE (update serialize)
|  output: sum(18: expr)
|  group by:
|  use vectorized: true
|
1:Project
|  <slot 18> : 6: L_EXTENDEDPRICE * 7: L_DISCOUNT
|  use vectorized: true
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 11: L_SHIPDATE >= '1995-01-01', 11: L_SHIPDATE < '1996-01-01', 7: L_DISCOUNT >= 0.02, 7: L_DISCOUNT <= 0.04, 5: L_QUANTITY < 24.0
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=8142251
avgRowSize=36.0
numNodes=0
use vectorized: true
[end]

