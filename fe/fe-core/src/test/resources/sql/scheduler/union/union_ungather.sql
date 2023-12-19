[sql]
with
    w1 as (select * from lineitem where l_orderkey = 1),
    w2 as (select * from w1 UNION ALL select * from w1)
select /*+SET_VAR(cbo_cte_reuse=true,cbo_cte_reuse_rate=0)*/ count(1) from w2 group by L_SUPPKEY
[scheduler]
    PLAN FRAGMENT 0(F04)
  DOP: 16
  INSTANCES
    INSTANCE(0-F04#0)
      BE: 10002

PLAN FRAGMENT 1(F01)
  DOP: 16
  INSTANCES
    INSTANCE(1-F01#0)
      DESTINATIONS: 0-F04#0
      BE: 10002

PLAN FRAGMENT 2(F03)
  DOP: 16
  INSTANCES
    INSTANCE(2-F03#0)
      DESTINATIONS: 1-F01#0
      BE: 10002

PLAN FRAGMENT 3(F02)
  DOP: 16
  INSTANCES
    INSTANCE(3-F02#0)
      DESTINATIONS: 1-F01#0
      BE: 10002

PLAN FRAGMENT 4(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 3-F02#0,2-F03#0
      BE: 10002
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1006
[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:69: count
  PARTITION: HASH_PARTITIONED: 54: L_SUPPKEY

  RESULT SINK

  12:Project
  |  <slot 69> : 69: count
  |
  11:AGGREGATE (merge finalize)
  |  output: count(69: count)
  |  group by: 54: L_SUPPKEY
  |
  10:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
  EXCHANGE ID: 10
  HASH_PARTITIONED: 54: L_SUPPKEY

  9:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(1)
  |  group by: 54: L_SUPPKEY
  |
  2:UNION
  |
  |----8:EXCHANGE
  |
  5:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    RANDOM

  7:Project
  |  <slot 37> : 3: L_SUPPKEY
  |
  6:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    RANDOM

  4:Project
  |  <slot 20> : 3: L_SUPPKEY
  |
  3:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:3: L_SUPPKEY
  PARTITION: RANDOM

  MultiCastDataSinks
  STREAM DATA SINK
    EXCHANGE ID: 03
    RANDOM
  STREAM DATA SINK
    EXCHANGE ID: 06
    RANDOM

  1:Project
  |  <slot 3> : 3: L_SUPPKEY
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 1: L_ORDERKEY = 1
     partitions=1/1
     rollup: lineitem
     tabletRatio=1/20
     tabletList=1006
     cardinality=1
     avgRowSize=12.0
[end]

