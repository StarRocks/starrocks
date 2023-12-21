[scheduler]
PLAN FRAGMENT 0(F18)
  DOP: 16
  INSTANCES
    INSTANCE(0-F18#0)
      BE: 10001

PLAN FRAGMENT 1(F17)
  DOP: 16
  INSTANCES
    INSTANCE(1-F17#0)
      DESTINATIONS: 0-F18#0
      BE: 10003
    INSTANCE(2-F17#1)
      DESTINATIONS: 0-F18#0
      BE: 10002
    INSTANCE(3-F17#2)
      DESTINATIONS: 0-F18#0
      BE: 10001

PLAN FRAGMENT 2(F16)
  DOP: 16
  INSTANCES
    INSTANCE(4-F16#0)
      DESTINATIONS: 1-F17#0,2-F17#1,3-F17#2
      BE: 10001
    INSTANCE(5-F16#1)
      DESTINATIONS: 1-F17#0,2-F17#1,3-F17#2
      BE: 10003
    INSTANCE(6-F16#2)
      DESTINATIONS: 1-F17#0,2-F17#1,3-F17#2
      BE: 10002

PLAN FRAGMENT 3(F14)
  DOP: 16
  INSTANCES
    INSTANCE(7-F14#0)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10001
    INSTANCE(8-F14#1)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10002
    INSTANCE(9-F14#2)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10003

PLAN FRAGMENT 4(F12)
  DOP: 16
  INSTANCES
    INSTANCE(10-F12#0)
      DESTINATIONS: 7-F14#0,8-F14#1,9-F14#2
      BE: 10003
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 5(F10)
  DOP: 16
  INSTANCES
    INSTANCE(11-F10#0)
      DESTINATIONS: 7-F14#0,8-F14#1,9-F14#2
      BE: 10001
      SCAN RANGES
        16:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(12-F10#1)
      DESTINATIONS: 7-F14#0,8-F14#1,9-F14#2
      BE: 10002
      SCAN RANGES
        16:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(13-F10#2)
      DESTINATIONS: 7-F14#0,8-F14#1,9-F14#2
      BE: 10003
      SCAN RANGES
        16:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 6(F06)
  DOP: 16
  INSTANCES
    INSTANCE(14-F06#0)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10003
    INSTANCE(15-F06#1)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10002
    INSTANCE(16-F06#2)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10001

PLAN FRAGMENT 7(F07)
  DOP: 16
  INSTANCES
    INSTANCE(17-F07#0)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10001
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(18-F07#1)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10002
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(19-F07#2)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10003
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 8(F04)
  DOP: 16
  INSTANCES
    INSTANCE(20-F04#0)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10001
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
    INSTANCE(21-F04#1)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10002
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
    INSTANCE(22-F04#2)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10003
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434

PLAN FRAGMENT 9(F00)
  DOP: 16
  INSTANCES
    INSTANCE(23-F00#0)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(24-F00#1)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(25-F00#2)
      DESTINATIONS: 14-F06#0,15-F06#1,16-F06#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

PLAN FRAGMENT 10(F01)
  DOP: 16
  INSTANCES
    INSTANCE(26-F01#0)
      DESTINATIONS: 25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1
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
    INSTANCE(27-F01#1)
      DESTINATIONS: 25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1
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
    INSTANCE(28-F01#2)
      DESTINATIONS: 25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1,25-F00#2,23-F00#0,24-F00#1
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
 OUTPUT EXPRS:49: n_name | 52: year | 54: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  29:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 49: n_name, 52: year

  STREAM DATA SINK
    EXCHANGE ID: 29
    UNPARTITIONED

  28:SORT
  |  order by: <slot 49> 49: n_name ASC, <slot 52> 52: year DESC
  |  offset: 0
  |  
  27:AGGREGATE (merge finalize)
  |  output: sum(54: sum)
  |  group by: 49: n_name, 52: year
  |  
  26:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 19: L_SUPPKEY

  STREAM DATA SINK
    EXCHANGE ID: 26
    HASH_PARTITIONED: 49: n_name, 52: year

  25:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(53: expr)
  |  group by: 49: n_name, 52: year
  |  
  24:Project
  |  <slot 49> : 49: n_name
  |  <slot 52> : year(40: o_orderdate)
  |  <slot 53> : 22: L_EXTENDEDPRICE * 1.0 - 23: L_DISCOUNT - CAST(37: ps_supplycost AS DOUBLE) * 21: L_QUANTITY
  |  
  23:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 19: L_SUPPKEY = 10: s_suppkey
  |  
  |----22:EXCHANGE
  |    
  15:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 13: s_nationkey

  STREAM DATA SINK
    EXCHANGE ID: 22
    HASH_PARTITIONED: 10: s_suppkey

  21:Project
  |  <slot 10> : 10: s_suppkey
  |  <slot 49> : 49: n_name
  |  
  20:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 13: s_nationkey = 48: n_nationkey
  |  
  |----19:EXCHANGE
  |    
  17:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 19
    HASH_PARTITIONED: 48: n_nationkey

  18:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 17
    HASH_PARTITIONED: 13: s_nationkey

  16:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 18: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 15
    HASH_PARTITIONED: 19: L_SUPPKEY

  14:Project
  |  <slot 19> : 19: L_SUPPKEY
  |  <slot 21> : 21: L_QUANTITY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 37> : 37: ps_supplycost
  |  <slot 40> : 40: o_orderdate
  |  
  13:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))
  |  colocate: false, reason: 
  |  equal join conjunct: 19: L_SUPPKEY = 35: ps_suppkey
  |  equal join conjunct: 18: L_PARTKEY = 34: ps_partkey
  |  
  |----12:EXCHANGE
  |    
  10:Project
  |  <slot 18> : 18: L_PARTKEY
  |  <slot 19> : 19: L_SUPPKEY
  |  <slot 21> : 21: L_QUANTITY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 40> : 40: o_orderdate
  |  
  9:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 18: L_PARTKEY = 1: p_partkey
  |  
  |----8:EXCHANGE
  |    
  5:EXCHANGE

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    HASH_PARTITIONED: 34: ps_partkey

  11:OlapScanNode
     TABLE: partsupp
     PREAGGREGATION: ON
     partitions=1/1
     rollup: partsupp
     tabletRatio=18/18
     tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
     cardinality=1
     avgRowSize=24.0

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    HASH_PARTITIONED: 1: p_partkey

  7:Project
  |  <slot 1> : 1: p_partkey
  |  
  6:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 2: p_name LIKE '%peru%'
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=63.0

PLAN FRAGMENT 9
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    HASH_PARTITIONED: 18: L_PARTKEY

  4:Project
  |  <slot 18> : 18: L_PARTKEY
  |  <slot 19> : 19: L_SUPPKEY
  |  <slot 21> : 21: L_QUANTITY
  |  <slot 22> : 22: L_EXTENDEDPRICE
  |  <slot 23> : 23: L_DISCOUNT
  |  <slot 40> : 40: o_orderdate
  |  
  3:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 39: o_orderkey = 17: L_ORDERKEY
  |  
  |----2:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=12.0

PLAN FRAGMENT 10
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    BUCKET_SHUFFLE_HASH_PARTITIONED: 17: L_ORDERKEY

  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=44.0
[end]

