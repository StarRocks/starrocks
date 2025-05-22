[sql]
select n_nationkey from nation
UNION ALL
select r_regionkey from region
[scheduler]
PLAN FRAGMENT 0(F05)
  DOP: 16
  INSTANCES
    INSTANCE(0-F05#0)
      BE: 10001

PLAN FRAGMENT 1(F04)
  DOP: 16
  INSTANCES
    INSTANCE(1-F04#0)
      DESTINATIONS: 0-F05#0
      BE: 10003
    INSTANCE(2-F04#1)
      DESTINATIONS: 0-F05#0
      BE: 10001

PLAN FRAGMENT 2(F02)
  DOP: 16
  INSTANCES
    INSTANCE(3-F02#0)
      DESTINATIONS: 1-F04#0,2-F04#1
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=2568,tabletID=2569

PLAN FRAGMENT 3(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F04#0,2-F04#1
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=2442,tabletID=2443

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:8: n_nationkey
  PARTITION: UNPARTITIONED

  RESULT SINK

  5:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    UNPARTITIONED

  0:UNION
  |  
  |----4:EXCHANGE
  |    
  2:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    RANDOM

  3:OlapScanNode
     TABLE: region
     PREAGGREGATION: ON
     partitions=1/1
     rollup: region
     tabletRatio=1/1
     tabletList=2569
     cardinality=1
     avgRowSize=4.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    RANDOM

  1:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=2443
     cardinality=1
     avgRowSize=4.0
[end]

