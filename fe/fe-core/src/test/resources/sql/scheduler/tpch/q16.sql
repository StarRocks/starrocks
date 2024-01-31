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

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
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
        1:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
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
        1:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F04#0,2-F04#1,3-F04#2
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
        1:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 3(F02)
  DOP: 16
  INSTANCES
    INSTANCE(7-F02#0)
      DESTINATIONS: 4-F00#0,5-F00#1,6-F00#2
      BE: 10001
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(8-F02#1)
      DESTINATIONS: 4-F00#0,5-F00#1,6-F00#2
      BE: 10002
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(9-F02#2)
      DESTINATIONS: 4-F00#0,5-F00#1,6-F00#2
      BE: 10003
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:9: p_brand | 10: p_type | 11: p_size | 23: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  14:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size

  STREAM DATA SINK
    EXCHANGE ID: 14
    UNPARTITIONED

  13:SORT
  |  order by: <slot 23> 23: count DESC, <slot 9> 9: p_brand ASC, <slot 10> 10: p_type ASC, <slot 11> 11: p_size ASC
  |  offset: 0
  |  
  12:AGGREGATE (update finalize)
  |  output: count(2: ps_suppkey)
  |  group by: 9: p_brand, 10: p_type, 11: p_size
  |  
  11:AGGREGATE (merge serialize)
  |  group by: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size
  |  
  10:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size

  9:AGGREGATE (update serialize)
  |  STREAMING
  |  group by: 2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size
  |  
  8:Project
  |  <slot 2> : 2: ps_suppkey
  |  <slot 9> : 9: p_brand
  |  <slot 10> : 10: p_type
  |  <slot 11> : 11: p_size
  |  
  7:HASH JOIN
  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 2: ps_suppkey = 15: s_suppkey
  |  
  |----6:EXCHANGE
  |    
  3:Project
  |  <slot 2> : 2: ps_suppkey
  |  <slot 9> : 9: p_brand
  |  <slot 10> : 10: p_type
  |  <slot 11> : 11: p_size
  |  
  2:HASH JOIN
  |  join op: INNER JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 6: p_partkey = 1: ps_partkey
  |  
  |----1:OlapScanNode
  |       TABLE: partsupp
  |       PREAGGREGATION: ON
  |       partitions=1/1
  |       rollup: partsupp
  |       tabletRatio=18/18
  |       tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
  |       cardinality=1
  |       avgRowSize=16.0
  |    
  0:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 9: p_brand != 'Brand#43', NOT (10: p_type LIKE 'PROMO BURNISHED%'), 11: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=47.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    UNPARTITIONED

  5:Project
  |  <slot 15> : 15: s_suppkey
  |  
  4:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     PREDICATES: 21: s_comment LIKE '%Customer%Complaints%'
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=105.0
[end]

