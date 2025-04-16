
[scheduler]
PLAN FRAGMENT 0(F07)
  DOP: 16
  INSTANCES
    INSTANCE(0-F07#0)
      BE: 10001

PLAN FRAGMENT 1(F02)
  DOP: 16
  INSTANCES
    INSTANCE(1-F02#0)
      DESTINATIONS: 0-F07#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1400
          2. partitionID=1397,tabletID=1406
          3. partitionID=1397,tabletID=1412
          4. partitionID=1397,tabletID=1418
          5. partitionID=1397,tabletID=1424
          6. partitionID=1397,tabletID=1430
        3:OlapScanNode
          1. partitionID=1665,tabletID=1668
          2. partitionID=1665,tabletID=1674
          3. partitionID=1665,tabletID=1680
          4. partitionID=1665,tabletID=1686
          5. partitionID=1665,tabletID=1692
          6. partitionID=1665,tabletID=1698
    INSTANCE(2-F02#1)
      DESTINATIONS: 0-F07#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1402
          2. partitionID=1397,tabletID=1408
          3. partitionID=1397,tabletID=1414
          4. partitionID=1397,tabletID=1420
          5. partitionID=1397,tabletID=1426
          6. partitionID=1397,tabletID=1432
        3:OlapScanNode
          1. partitionID=1665,tabletID=1670
          2. partitionID=1665,tabletID=1676
          3. partitionID=1665,tabletID=1682
          4. partitionID=1665,tabletID=1688
          5. partitionID=1665,tabletID=1694
          6. partitionID=1665,tabletID=1700
    INSTANCE(3-F02#2)
      DESTINATIONS: 0-F07#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1398
          2. partitionID=1397,tabletID=1404
          3. partitionID=1397,tabletID=1410
          4. partitionID=1397,tabletID=1416
          5. partitionID=1397,tabletID=1422
          6. partitionID=1397,tabletID=1428
        3:OlapScanNode
          1. partitionID=1665,tabletID=1666
          2. partitionID=1665,tabletID=1672
          3. partitionID=1665,tabletID=1678
          4. partitionID=1665,tabletID=1684
          5. partitionID=1665,tabletID=1690
          6. partitionID=1665,tabletID=1696

PLAN FRAGMENT 2(F05)
  DOP: 16
  INSTANCES
    INSTANCE(4-F05#0)
      DESTINATIONS: 1-F02#0,2-F02#1,3-F02#2
      BE: 10001
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2488,tabletID=2491
          2. partitionID=2488,tabletID=2497
          3. partitionID=2488,tabletID=2503
          4. partitionID=2488,tabletID=2509
          5. partitionID=2488,tabletID=2515
          6. partitionID=2488,tabletID=2521
    INSTANCE(5-F05#1)
      DESTINATIONS: 1-F02#0,2-F02#1,3-F02#2
      BE: 10002
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2488,tabletID=2493
          2. partitionID=2488,tabletID=2499
          3. partitionID=2488,tabletID=2505
          4. partitionID=2488,tabletID=2511
          5. partitionID=2488,tabletID=2517
          6. partitionID=2488,tabletID=2523
    INSTANCE(6-F05#2)
      DESTINATIONS: 1-F02#0,2-F02#1,3-F02#2
      BE: 10003
      SCAN RANGES
        9:OlapScanNode
          1. partitionID=2488,tabletID=2489
          2. partitionID=2488,tabletID=2495
          3. partitionID=2488,tabletID=2501
          4. partitionID=2488,tabletID=2507
          5. partitionID=2488,tabletID=2513
          6. partitionID=2488,tabletID=2519

PLAN FRAGMENT 3(F03)
  DOP: 16
  INSTANCES
    INSTANCE(7-F03#0)
      DESTINATIONS: 3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1
      BE: 10001
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2448,tabletID=2451
          2. partitionID=2448,tabletID=2457
          3. partitionID=2448,tabletID=2463
          4. partitionID=2448,tabletID=2469
          5. partitionID=2448,tabletID=2475
          6. partitionID=2448,tabletID=2481
    INSTANCE(8-F03#1)
      DESTINATIONS: 3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1
      BE: 10002
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2448,tabletID=2453
          2. partitionID=2448,tabletID=2459
          3. partitionID=2448,tabletID=2465
          4. partitionID=2448,tabletID=2471
          5. partitionID=2448,tabletID=2477
          6. partitionID=2448,tabletID=2483
    INSTANCE(9-F03#2)
      DESTINATIONS: 3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1,3-F02#2,1-F02#0,2-F02#1
      BE: 10003
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2448,tabletID=2449
          2. partitionID=2448,tabletID=2455
          3. partitionID=2448,tabletID=2461
          4. partitionID=2448,tabletID=2467
          5. partitionID=2448,tabletID=2473
          6. partitionID=2448,tabletID=2479

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:35: L_ORDERKEY | 36: L_PARTKEY
  PARTITION: UNPARTITIONED

  RESULT SINK

  13:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 13
    UNPARTITIONED

  12:Project
  |  <slot 35> : 35: L_ORDERKEY
  |  <slot 36> : 36: L_PARTKEY
  |  
  11:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 36: L_PARTKEY = 46: p_partkey
  |  
  |----10:EXCHANGE
  |    
  8:Project
  |  <slot 35> : 35: L_ORDERKEY
  |  <slot 36> : 36: L_PARTKEY
  |  
  7:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 35: L_ORDERKEY = 37: o_orderkey
  |  
  |----6:EXCHANGE
  |    
  0:INTERSECT
  |  
  |----4:Project
  |    |  <slot 18> : 18: L_ORDERKEY
  |    |  <slot 19> : 19: L_PARTKEY
  |    |  
  |    3:OlapScanNode
  |       TABLE: lineitem1
  |       PREAGGREGATION: ON
  |       partitions=1/7
  |       rollup: lineitem1
  |       tabletRatio=18/18
  |       tabletList=1666,1668,1670,1672,1674,1676,1678,1680,1682,1684 ...
  |       cardinality=1
  |       avgRowSize=3.0
  |    
  2:Project
  |  <slot 1> : 1: L_ORDERKEY
  |  <slot 2> : 2: L_PARTKEY
  |  
  1:OlapScanNode
     TABLE: lineitem0
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem0
     tabletRatio=18/18
     tabletList=1398,1400,1402,1404,1406,1408,1410,1412,1414,1416 ...
     cardinality=1
     avgRowSize=3.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    UNPARTITIONED

  9:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=2489,2491,2493,2495,2497,2499,2501,2503,2505,2507 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    BUCKET_SHUFFLE_HASH_PARTITIONED: 37: o_orderkey

  5:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=2449,2451,2453,2455,2457,2459,2461,2463,2465,2467 ...
     cardinality=1
     avgRowSize=8.0
[end]

