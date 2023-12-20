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
      BE: 10002
    INSTANCE(3-F04#2)
      DESTINATIONS: 0-F05#0
      BE: 10001

PLAN FRAGMENT 2(F02)
  DOP: 16
  INSTANCES
    INSTANCE(4-F02#0)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10001
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
    INSTANCE(5-F02#1)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10002
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
    INSTANCE(6-F02#2)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10003
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434

PLAN FRAGMENT 3(F00)
  DOP: 16
  INSTANCES
    INSTANCE(7-F00#0)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10001
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(8-F00#1)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10002
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(9-F00#2)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
      BE: 10003
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:48: expr
  PARTITION: UNPARTITIONED

  RESULT SINK

  14:Project
  |  <slot 48> : 47: sum / 7.0
  |  
  13:AGGREGATE (merge finalize)
  |  output: sum(47: sum)
  |  group by: 
  |  
  12:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 2: L_PARTKEY

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
  |  predicates: 5: L_QUANTITY < 0.2 * 49: avg
  |  
  8:ANALYTIC
  |  functions: [, avg(5: L_QUANTITY), ]
  |  partition by: 18: p_partkey
  |  
  7:SORT
  |  order by: <slot 18> 18: p_partkey ASC
  |  offset: 0
  |  
  6:Project
  |  <slot 5> : 5: L_QUANTITY
  |  <slot 6> : 6: L_EXTENDEDPRICE
  |  <slot 18> : 18: p_partkey
  |  
  5:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 2: L_PARTKEY = 18: p_partkey
  |  
  |----4:EXCHANGE
  |    
  1:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    HASH_PARTITIONED: 18: p_partkey

  3:Project
  |  <slot 18> : 18: p_partkey
  |  
  2:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 21: p_brand = 'Brand#35', 24: p_container = 'JUMBO CASE'
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=28.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 01
    HASH_PARTITIONED: 2: L_PARTKEY

  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=24.0
[end]

