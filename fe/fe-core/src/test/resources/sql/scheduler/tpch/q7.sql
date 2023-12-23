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

PLAN FRAGMENT 3(F12)
  DOP: 16
  INSTANCES
    INSTANCE(7-F12#0)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0]
        16:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 4(F13)
  DOP: 16
  INSTANCES
    INSTANCE(8-F13#0)
      DESTINATIONS: 7-F12#0
      BE: 10001
      SCAN RANGES
        17:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(9-F13#1)
      DESTINATIONS: 7-F12#0
      BE: 10002
      SCAN RANGES
        17:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(10-F13#2)
      DESTINATIONS: 7-F12#0
      BE: 10003
      SCAN RANGES
        17:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 5(F10)
  DOP: 16
  INSTANCES
    INSTANCE(11-F10#0)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10001
    INSTANCE(12-F10#1)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10002
    INSTANCE(13-F10#2)
      DESTINATIONS: 4-F16#0,5-F16#1,6-F16#2
      BE: 10003

PLAN FRAGMENT 6(F08)
  DOP: 16
  INSTANCES
    INSTANCE(14-F08#0)
      DESTINATIONS: 11-F10#0,12-F10#1,13-F10#2
      BE: 10003
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 7(F06)
  DOP: 16
  INSTANCES
    INSTANCE(15-F06#0)
      DESTINATIONS: 11-F10#0,12-F10#1,13-F10#2
      BE: 10003
    INSTANCE(16-F06#1)
      DESTINATIONS: 11-F10#0,12-F10#1,13-F10#2
      BE: 10002
    INSTANCE(17-F06#2)
      DESTINATIONS: 11-F10#0,12-F10#1,13-F10#2
      BE: 10001

PLAN FRAGMENT 8(F04)
  DOP: 16
  INSTANCES
    INSTANCE(18-F04#0)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10001
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(19-F04#1)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10002
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(20-F04#2)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10003
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 9(F00)
  DOP: 16
  INSTANCES
    INSTANCE(21-F00#0)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [0, 18, 3, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(22-F00#1)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 19, 4, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(23-F00#2)
      DESTINATIONS: 15-F06#0,16-F06#1,17-F06#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 10(F01)
  DOP: 16
  INSTANCES
    INSTANCE(24-F01#0)
      DESTINATIONS: 21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(25-F01#1)
      DESTINATIONS: 21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(26-F01#2)
      DESTINATIONS: 21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1,23-F00#2,21-F00#0,22-F00#1
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:43: n_name | 47: n_name | 50: year | 52: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  28:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 43: n_name, 47: n_name, 50: year

  STREAM DATA SINK
    EXCHANGE ID: 28
    UNPARTITIONED

  27:SORT
  |  order by: <slot 43> 43: n_name ASC, <slot 47> 47: n_name ASC, <slot 50> 50: year ASC
  |  offset: 0
  |  
  26:AGGREGATE (merge finalize)
  |  output: sum(52: sum)
  |  group by: 43: n_name, 47: n_name, 50: year
  |  
  25:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 10: L_SUPPKEY

  STREAM DATA SINK
    EXCHANGE ID: 25
    HASH_PARTITIONED: 43: n_name, 47: n_name, 50: year

  24:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(51: expr)
  |  group by: 43: n_name, 47: n_name, 50: year
  |  
  23:Project
  |  <slot 43> : 43: n_name
  |  <slot 47> : 47: n_name
  |  <slot 50> : year(18: L_SHIPDATE)
  |  <slot 51> : 13: L_EXTENDEDPRICE * 1.0 - 14: L_DISCOUNT
  |  
  22:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 10: L_SUPPKEY = 1: s_suppkey
  |  other join predicates: ((43: n_name = 'CANADA') AND (47: n_name = 'IRAN')) OR ((43: n_name = 'IRAN') AND (47: n_name = 'CANADA'))
  |  
  |----21:EXCHANGE
  |    
  15:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 21
    HASH_PARTITIONED: 1: s_suppkey

  20:Project
  |  <slot 1> : 1: s_suppkey
  |  <slot 43> : 43: n_name
  |  
  19:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 42: n_nationkey = 4: s_nationkey
  |  
  |----18:EXCHANGE
  |    
  16:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 43: n_name IN ('CANADA', 'IRAN')
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 18
    BUCKET_SHUFFLE_HASH_PARTITIONED: 4: s_nationkey

  17:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 37: c_nationkey

  STREAM DATA SINK
    EXCHANGE ID: 15
    HASH_PARTITIONED: 10: L_SUPPKEY

  14:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  <slot 13> : 13: L_EXTENDEDPRICE
  |  <slot 14> : 14: L_DISCOUNT
  |  <slot 18> : 18: L_SHIPDATE
  |  <slot 47> : 47: n_name
  |  
  13:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 37: c_nationkey = 46: n_nationkey
  |  
  |----12:EXCHANGE
  |    
  10:EXCHANGE

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    HASH_PARTITIONED: 46: n_nationkey

  11:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 47: n_name IN ('IRAN', 'CANADA')
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 27: o_custkey

  STREAM DATA SINK
    EXCHANGE ID: 10
    HASH_PARTITIONED: 37: c_nationkey

  9:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  <slot 13> : 13: L_EXTENDEDPRICE
  |  <slot 14> : 14: L_DISCOUNT
  |  <slot 18> : 18: L_SHIPDATE
  |  <slot 37> : 37: c_nationkey
  |  
  8:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 27: o_custkey = 34: c_custkey
  |  
  |----7:EXCHANGE
  |    
  5:EXCHANGE

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    HASH_PARTITIONED: 34: c_custkey

  6:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=12.0

PLAN FRAGMENT 9
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    HASH_PARTITIONED: 27: o_custkey

  4:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  <slot 13> : 13: L_EXTENDEDPRICE
  |  <slot 14> : 14: L_DISCOUNT
  |  <slot 18> : 18: L_SHIPDATE
  |  <slot 27> : 27: o_custkey
  |  
  3:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 8: L_ORDERKEY = 25: o_orderkey
  |  
  |----2:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 18: L_SHIPDATE >= '1995-01-01', 18: L_SHIPDATE <= '1996-12-31'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=32.0

PLAN FRAGMENT 10
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    BUCKET_SHUFFLE_HASH_PARTITIONED: 25: o_orderkey

  1:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=16.0
[end]

