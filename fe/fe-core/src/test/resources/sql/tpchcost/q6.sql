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
OUTPUT EXPRS:19: sum
PARTITION: UNPARTITIONED

RESULT SINK

4:AGGREGATE (merge finalize)
|  output: sum(19: sum)
|  group by:
|
3:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
UNPARTITIONED

2:AGGREGATE (update serialize)
|  output: sum(6: L_EXTENDEDPRICE * 7: L_DISCOUNT)
|  group by:
|
1:Project
|  <slot 6> : 6: L_EXTENDEDPRICE
|  <slot 7> : 7: L_DISCOUNT
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 11: L_SHIPDATE >= '1995-01-01', 11: L_SHIPDATE < '1996-01-01', 7: L_DISCOUNT >= 0.02, 7: L_DISCOUNT <= 0.04, 5: L_QUANTITY < 24.0
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=8142251
avgRowSize=36.0
[end]

