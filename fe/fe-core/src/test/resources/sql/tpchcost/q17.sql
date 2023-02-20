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
|
13:AGGREGATE (merge finalize)
|  output: sum(48: sum)
|  group by:
|
12:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 18: P_PARTKEY

STREAM DATA SINK
EXCHANGE ID: 12
UNPARTITIONED

11:AGGREGATE (update serialize)
|  output: sum(6: L_EXTENDEDPRICE)
|  group by:
|
10:Project
|  <slot 6> : 6: L_EXTENDEDPRICE
|
9:SELECT
      |  predicates: 5: L_QUANTITY < 0.2 * 50: avg
      |
      8:ANALYTIC
      |  functions: [, avg(5: L_QUANTITY), ]
      |  partition by: 18: P_PARTKEY
      |
      7:SORT
      |  order by: <slot 18> 18: P_PARTKEY ASC
      |  offset: 0
      |
      6:EXCHANGE

      PLAN FRAGMENT 2
      OUTPUT EXPRS:
      PARTITION: RANDOM

      STREAM DATA SINK
      EXCHANGE ID: 06
      HASH_PARTITIONED: 18: P_PARTKEY

      5:Project
      |  <slot 5> : 5: L_QUANTITY
      |  <slot 6> : 6: L_EXTENDEDPRICE
      |  <slot 18> : 18: P_PARTKEY
      |
      4:HASH JOIN
      |  join op: INNER JOIN (BROADCAST)
      |  colocate: false, reason:
      |  equal join conjunct: 2: L_PARTKEY = 18: P_PARTKEY
      |
      |----3:EXCHANGE
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

      PLAN FRAGMENT 3
      OUTPUT EXPRS:
      PARTITION: RANDOM

      STREAM DATA SINK
      EXCHANGE ID: 03
      UNPARTITIONED

      2:Project
      |  <slot 18> : 18: P_PARTKEY
      |
      1:OlapScanNode
      TABLE: part
      PREAGGREGATION: ON
      PREDICATES: 21: P_BRAND = 'Brand#35', 24: P_CONTAINER = 'JUMBO CASE'
      partitions=1/1
      rollup: part
      tabletRatio=10/10
      cardinality=20000
      avgRowSize=28.0
      numNodes=0
[end]

