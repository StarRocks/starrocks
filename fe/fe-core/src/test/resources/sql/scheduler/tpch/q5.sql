[scheduler]
PLAN FRAGMENT 0(F16)
  DOP: 16
  INSTANCES
    INSTANCE(0-F16#0)
      BE: 10001

PLAN FRAGMENT 1(F15)
  DOP: 16
  INSTANCES
    INSTANCE(1-F15#0)
      DESTINATIONS: 0-F16#0
      BE: 10003

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(2-F00#0)
      DESTINATIONS: 1-F15#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0]
        0:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 3(F13)
  DOP: 16
  INSTANCES
    INSTANCE(3-F13#0)
      DESTINATIONS: 2-F00#0
      BE: 10001
    INSTANCE(4-F13#1)
      DESTINATIONS: 2-F00#0
      BE: 10002
    INSTANCE(5-F13#2)
      DESTINATIONS: 2-F00#0
      BE: 10003

PLAN FRAGMENT 4(F11)
  DOP: 16
  INSTANCES
    INSTANCE(6-F11#0)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10001
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(7-F11#1)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10002
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(8-F11#2)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10003
      SCAN RANGES
        18:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 5(F09)
  DOP: 16
  INSTANCES
    INSTANCE(9-F09#0)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10003
    INSTANCE(10-F09#1)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10002
    INSTANCE(11-F09#2)
      DESTINATIONS: 3-F13#0,4-F13#1,5-F13#2
      BE: 10001

PLAN FRAGMENT 6(F07)
  DOP: 16
  INSTANCES
    INSTANCE(12-F07#0)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10001
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(13-F07#1)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10002
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(14-F07#2)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10003
      SCAN RANGES
        13:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 7(F05)
  DOP: 16
  INSTANCES
    INSTANCE(15-F05#0)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10001
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(16-F05#1)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10002
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(17-F05#2)
      DESTINATIONS: 9-F09#0,10-F09#1,11-F09#2
      BE: 10003
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

PLAN FRAGMENT 8(F03)
  DOP: 16
  INSTANCES
    INSTANCE(18-F03#0)
      DESTINATIONS: 2-F00#0
      BE: 10001
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(19-F03#1)
      DESTINATIONS: 2-F00#0
      BE: 10002
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(20-F03#2)
      DESTINATIONS: 2-F00#0
      BE: 10003
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 9(F01)
  DOP: 16
  INSTANCES
    INSTANCE(21-F01#0)
      DESTINATIONS: 2-F00#0
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1479,tabletID=1482

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:43: n_name | 50: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  29:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 43: n_name

  STREAM DATA SINK
    EXCHANGE ID: 29
    UNPARTITIONED

  28:SORT
  |  order by: <slot 50> 50: sum DESC
  |  offset: 0
  |  
  27:AGGREGATE (merge finalize)
  |  output: sum(50: sum)
  |  group by: 43: n_name
  |  
  26:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 26
    HASH_PARTITIONED: 43: n_name

  25:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(49: expr)
  |  group by: 43: n_name
  |  
  24:Project
  |  <slot 43> : 43: n_name
  |  <slot 49> : 23: L_EXTENDEDPRICE * 1.0 - 24: L_DISCOUNT
  |  
  23:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 35: s_suppkey = 20: L_SUPPKEY
  |  equal join conjunct: 38: s_nationkey = 4: c_nationkey
  |  
  |----22:EXCHANGE
  |    
  9:Project
  |  <slot 35> : 35: s_suppkey
  |  <slot 38> : 38: s_nationkey
  |  <slot 43> : 43: n_name
  |  
  8:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 42: n_nationkey = 38: s_nationkey
  |  
  |----7:EXCHANGE
  |    
  5:Project
  |  <slot 42> : 42: n_nationkey
  |  <slot 43> : 43: n_name
  |  
  4:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 44: n_regionkey = 46: r_regionkey
  |  
  |----3:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=33.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 9: o_orderkey

  STREAM DATA SINK
    EXCHANGE ID: 22
    BUCKET_SHUFFLE_HASH_PARTITIONED: 4: c_nationkey

  21:Project
  |  <slot 4> : 4: c_nationkey
  |  <slot 20> : 20: L_SUPPKEY
  |  <slot 23> : 23: L_EXTENDEDPRICE
  |  <slot 24> : 24: L_DISCOUNT
  |  
  20:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 9: o_orderkey = 18: L_ORDERKEY
  |  
  |----19:EXCHANGE
  |    
  17:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 19
    HASH_PARTITIONED: 18: L_ORDERKEY

  18:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=28.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 11: o_custkey

  STREAM DATA SINK
    EXCHANGE ID: 17
    HASH_PARTITIONED: 9: o_orderkey

  16:Project
  |  <slot 4> : 4: c_nationkey
  |  <slot 9> : 9: o_orderkey
  |  
  15:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 11: o_custkey = 1: c_custkey
  |  
  |----14:EXCHANGE
  |    
  12:EXCHANGE

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 14
    HASH_PARTITIONED: 1: c_custkey

  13:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=12.0

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    HASH_PARTITIONED: 11: o_custkey

  11:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 11> : 11: o_custkey
  |  
  10:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 10: o_orderdate >= '1995-01-01', 10: o_orderdate < '1996-01-01'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=20.0

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    BUCKET_SHUFFLE_HASH_PARTITIONED: 38: s_nationkey

  6:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 9
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    UNPARTITIONED

  2:Project
  |  <slot 46> : 46: r_regionkey
  |  
  1:OlapScanNode
     TABLE: region
     PREAGGREGATION: ON
     PREDICATES: 47: r_name = 'AFRICA'
     partitions=1/1
     rollup: region
     tabletRatio=1/1
     tabletList=1482
     cardinality=1
     avgRowSize=29.0
[end]

