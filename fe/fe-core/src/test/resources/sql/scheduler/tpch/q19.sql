[scheduler]
PLAN FRAGMENT 0(F03)
  DOP: 16
  INSTANCES
    INSTANCE(0-F03#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS: 0-F03#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
    INSTANCE(2-F00#1)
      DESTINATIONS: 0-F03#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
    INSTANCE(3-F00#2)
      DESTINATIONS: 0-F03#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434

PLAN FRAGMENT 2(F01)
  DOP: 16
  INSTANCES
    INSTANCE(4-F01#0)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(5-F01#1)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(6-F01#2)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:28: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  8:AGGREGATE (merge finalize)
  |  output: sum(28: sum)
  |  group by: 
  |  
  7:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    UNPARTITIONED

  6:AGGREGATE (update serialize)
  |  output: sum(6: L_EXTENDEDPRICE * 1.0 - 7: L_DISCOUNT)
  |  group by: 
  |  
  5:Project
  |  <slot 6> : 6: L_EXTENDEDPRICE
  |  <slot 7> : 7: L_DISCOUNT
  |  
  4:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 18: p_partkey = 2: L_PARTKEY
  |  other join predicates: (((((21: p_brand = 'Brand#45') AND (24: p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) AND ((5: L_QUANTITY >= 5.0) AND (5: L_QUANTITY <= 15.0))) AND (23: p_size <= 5)) OR ((((21: p_brand = 'Brand#11') AND (24: p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) AND ((5: L_QUANTITY >= 15.0) AND (5: L_QUANTITY <= 25.0))) AND (23: p_size <= 10))) OR ((((21: p_brand = 'Brand#21') AND (24: p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) AND ((5: L_QUANTITY >= 25.0) AND (5: L_QUANTITY <= 35.0))) AND (23: p_size <= 15))
  |  
  |----3:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 21: p_brand IN ('Brand#45', 'Brand#11', 'Brand#21'), 23: p_size <= 15, 24: p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'), 23: p_size >= 1
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=32.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    BUCKET_SHUFFLE_HASH_PARTITIONED: 2: L_PARTKEY

  2:Project
  |  <slot 2> : 2: L_PARTKEY
  |  <slot 5> : 5: L_QUANTITY
  |  <slot 6> : 6: L_EXTENDEDPRICE
  |  <slot 7> : 7: L_DISCOUNT
  |  
  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 5: L_QUANTITY >= 5.0, 5: L_QUANTITY <= 35.0, 15: L_SHIPMODE IN ('AIR', 'AIR REG'), 14: L_SHIPINSTRUCT = 'DELIVER IN PERSON'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=67.0
[end]

