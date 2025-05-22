
[scheduler]
PLAN FRAGMENT 0(F14)
  DOP: 16
  INSTANCES
    INSTANCE(0-F14#0)
      BE: 10002

PLAN FRAGMENT 1(F13)
  DOP: 16
  INSTANCES
    INSTANCE(1-F13#0)
      DESTINATIONS: 0-F14#0
      BE: 10003
    INSTANCE(2-F13#1)
      DESTINATIONS: 0-F14#0
      BE: 10002
    INSTANCE(3-F13#2)
      DESTINATIONS: 0-F14#0
      BE: 10001

PLAN FRAGMENT 2(F12)
  DOP: 16
  INSTANCES
    INSTANCE(4-F12#0)
      DESTINATIONS: 1-F13#0,2-F13#1,3-F13#2
      BE: 10001
    INSTANCE(5-F12#1)
      DESTINATIONS: 1-F13#0,2-F13#1,3-F13#2
      BE: 10003
    INSTANCE(6-F12#2)
      DESTINATIONS: 1-F13#0,2-F13#1,3-F13#2
      BE: 10002

PLAN FRAGMENT 3(F06)
  DOP: 16
  INSTANCES
    INSTANCE(7-F06#0)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        12:OlapScanNode
          1. partitionID=1665,tabletID=1668
          2. partitionID=1665,tabletID=1674
          3. partitionID=1665,tabletID=1680
          4. partitionID=1665,tabletID=1686
          5. partitionID=1665,tabletID=1692
          6. partitionID=1665,tabletID=1698
    INSTANCE(8-F06#1)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        12:OlapScanNode
          1. partitionID=1665,tabletID=1670
          2. partitionID=1665,tabletID=1676
          3. partitionID=1665,tabletID=1682
          4. partitionID=1665,tabletID=1688
          5. partitionID=1665,tabletID=1694
          6. partitionID=1665,tabletID=1700
    INSTANCE(9-F06#2)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        12:OlapScanNode
          1. partitionID=1665,tabletID=1666
          2. partitionID=1665,tabletID=1672
          3. partitionID=1665,tabletID=1678
          4. partitionID=1665,tabletID=1684
          5. partitionID=1665,tabletID=1690
          6. partitionID=1665,tabletID=1696

PLAN FRAGMENT 4(F09)
  DOP: 16
  INSTANCES
    INSTANCE(10-F09#0)
      DESTINATIONS: 7-F06#0,8-F06#1,9-F06#2
      BE: 10001
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=2488,tabletID=2491
          2. partitionID=2488,tabletID=2497
          3. partitionID=2488,tabletID=2503
          4. partitionID=2488,tabletID=2509
          5. partitionID=2488,tabletID=2515
          6. partitionID=2488,tabletID=2521
    INSTANCE(11-F09#1)
      DESTINATIONS: 7-F06#0,8-F06#1,9-F06#2
      BE: 10002
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=2488,tabletID=2493
          2. partitionID=2488,tabletID=2499
          3. partitionID=2488,tabletID=2505
          4. partitionID=2488,tabletID=2511
          5. partitionID=2488,tabletID=2517
          6. partitionID=2488,tabletID=2523
    INSTANCE(12-F09#2)
      DESTINATIONS: 7-F06#0,8-F06#1,9-F06#2
      BE: 10003
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=2488,tabletID=2489
          2. partitionID=2488,tabletID=2495
          3. partitionID=2488,tabletID=2501
          4. partitionID=2488,tabletID=2507
          5. partitionID=2488,tabletID=2513
          6. partitionID=2488,tabletID=2519

PLAN FRAGMENT 5(F07)
  DOP: 16
  INSTANCES
    INSTANCE(13-F07#0)
      DESTINATIONS: 9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1
      BE: 10001
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=2448,tabletID=2451
          2. partitionID=2448,tabletID=2457
          3. partitionID=2448,tabletID=2463
          4. partitionID=2448,tabletID=2469
          5. partitionID=2448,tabletID=2475
          6. partitionID=2448,tabletID=2481
    INSTANCE(14-F07#1)
      DESTINATIONS: 9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1
      BE: 10002
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=2448,tabletID=2453
          2. partitionID=2448,tabletID=2459
          3. partitionID=2448,tabletID=2465
          4. partitionID=2448,tabletID=2471
          5. partitionID=2448,tabletID=2477
          6. partitionID=2448,tabletID=2483
    INSTANCE(15-F07#2)
      DESTINATIONS: 9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1,9-F06#2,7-F06#0,8-F06#1
      BE: 10003
      SCAN RANGES
        14:OlapScanNode
          1. partitionID=2448,tabletID=2449
          2. partitionID=2448,tabletID=2455
          3. partitionID=2448,tabletID=2461
          4. partitionID=2448,tabletID=2467
          5. partitionID=2448,tabletID=2473
          6. partitionID=2448,tabletID=2479

PLAN FRAGMENT 6(F00)
  DOP: 16
  INSTANCES
    INSTANCE(16-F00#0)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
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
    INSTANCE(17-F00#1)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
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
    INSTANCE(18-F00#2)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
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

PLAN FRAGMENT 7(F03)
  DOP: 16
  INSTANCES
    INSTANCE(19-F03#0)
      DESTINATIONS: 16-F00#0,17-F00#1,18-F00#2
      BE: 10001
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=2488,tabletID=2491
          2. partitionID=2488,tabletID=2497
          3. partitionID=2488,tabletID=2503
          4. partitionID=2488,tabletID=2509
          5. partitionID=2488,tabletID=2515
          6. partitionID=2488,tabletID=2521
    INSTANCE(20-F03#1)
      DESTINATIONS: 16-F00#0,17-F00#1,18-F00#2
      BE: 10002
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=2488,tabletID=2493
          2. partitionID=2488,tabletID=2499
          3. partitionID=2488,tabletID=2505
          4. partitionID=2488,tabletID=2511
          5. partitionID=2488,tabletID=2517
          6. partitionID=2488,tabletID=2523
    INSTANCE(21-F03#2)
      DESTINATIONS: 16-F00#0,17-F00#1,18-F00#2
      BE: 10003
      SCAN RANGES
        7:OlapScanNode
          1. partitionID=2488,tabletID=2489
          2. partitionID=2488,tabletID=2495
          3. partitionID=2488,tabletID=2501
          4. partitionID=2488,tabletID=2507
          5. partitionID=2488,tabletID=2513
          6. partitionID=2488,tabletID=2519

PLAN FRAGMENT 8(F01)
  DOP: 16
  INSTANCES
    INSTANCE(22-F01#0)
      DESTINATIONS: 18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=2448,tabletID=2451
          2. partitionID=2448,tabletID=2457
          3. partitionID=2448,tabletID=2463
          4. partitionID=2448,tabletID=2469
          5. partitionID=2448,tabletID=2475
          6. partitionID=2448,tabletID=2481
    INSTANCE(23-F01#1)
      DESTINATIONS: 18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1
      BE: 10002
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=2448,tabletID=2453
          2. partitionID=2448,tabletID=2459
          3. partitionID=2448,tabletID=2465
          4. partitionID=2448,tabletID=2471
          5. partitionID=2448,tabletID=2477
          6. partitionID=2448,tabletID=2483
    INSTANCE(24-F01#2)
      DESTINATIONS: 18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1,18-F00#2,16-F00#0,17-F00#1
      BE: 10003
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=2448,tabletID=2449
          2. partitionID=2448,tabletID=2455
          3. partitionID=2448,tabletID=2461
          4. partitionID=2448,tabletID=2467
          5. partitionID=2448,tabletID=2473
          6. partitionID=2448,tabletID=2479

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:71: o_orderdate | 72: p_name
  PARTITION: UNPARTITIONED

  RESULT SINK

  26:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 71: o_orderdate, 72: p_name

  STREAM DATA SINK
    EXCHANGE ID: 26
    UNPARTITIONED

  25:AGGREGATE (merge finalize)
  |  group by: 71: o_orderdate, 72: p_name
  |  
  24:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 24
    HASH_PARTITIONED: 71: o_orderdate, 72: p_name

  23:AGGREGATE (update serialize)
  |  STREAMING
  |  group by: 71: o_orderdate, 72: p_name
  |  
  0:UNION
  |  
  |----22:EXCHANGE
  |    
  11:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 22
    RANDOM

  21:Project
  |  <slot 54> : 54: o_orderdate
  |  <slot 63> : 63: p_name
  |  
  20:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 37: L_PARTKEY = 62: p_partkey
  |  
  |----19:EXCHANGE
  |    
  17:Project
  |  <slot 37> : 37: L_PARTKEY
  |  <slot 54> : 54: o_orderdate
  |  
  16:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 36: L_ORDERKEY = 53: o_orderkey
  |  
  |----15:EXCHANGE
  |    
  13:Project
  |  <slot 36> : 36: L_ORDERKEY
  |  <slot 37> : 37: L_PARTKEY
  |  
  12:OlapScanNode
     TABLE: lineitem1
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem1
     tabletRatio=18/18
     tabletList=1666,1668,1670,1672,1674,1676,1678,1680,1682,1684 ...
     cardinality=1
     avgRowSize=3.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 19
    UNPARTITIONED

  18:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=2489,2491,2493,2495,2497,2499,2501,2503,2505,2507 ...
     cardinality=1
     avgRowSize=63.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 15
    BUCKET_SHUFFLE_HASH_PARTITIONED: 53: o_orderkey

  14:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=2449,2451,2453,2455,2457,2459,2461,2463,2465,2467 ...
     cardinality=1
     avgRowSize=12.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 11
    RANDOM

  10:Project
  |  <slot 19> : 19: o_orderdate
  |  <slot 28> : 28: p_name
  |  
  9:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 2: L_PARTKEY = 27: p_partkey
  |  
  |----8:EXCHANGE
  |    
  6:Project
  |  <slot 2> : 2: L_PARTKEY
  |  <slot 19> : 19: o_orderdate
  |  
  5:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: L_ORDERKEY = 18: o_orderkey
  |  
  |----4:EXCHANGE
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

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    UNPARTITIONED

  7:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=2489,2491,2493,2495,2497,2499,2501,2503,2505,2507 ...
     cardinality=1
     avgRowSize=63.0

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    BUCKET_SHUFFLE_HASH_PARTITIONED: 18: o_orderkey

  3:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=2449,2451,2453,2455,2457,2459,2461,2463,2465,2467 ...
     cardinality=1
     avgRowSize=12.0
[end]

