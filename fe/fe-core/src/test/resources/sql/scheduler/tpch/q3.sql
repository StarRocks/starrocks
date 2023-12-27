[scheduler]
PLAN FRAGMENT 0(F07)
  DOP: 16
  INSTANCES
    INSTANCE(0-F07#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS: 0-F07#0
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
    INSTANCE(2-F00#1)
      DESTINATIONS: 0-F07#0
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
    INSTANCE(3-F00#2)
      DESTINATIONS: 0-F07#0
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

PLAN FRAGMENT 2(F05)
  DOP: 16
  INSTANCES
    INSTANCE(4-F05#0)
      DESTINATIONS: 1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10003
    INSTANCE(5-F05#1)
      DESTINATIONS: 1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10002
    INSTANCE(6-F05#2)
      DESTINATIONS: 1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10001

PLAN FRAGMENT 3(F03)
  DOP: 16
  INSTANCES
    INSTANCE(7-F03#0)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10001
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(8-F03#1)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10002
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(9-F03#2)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10003
      SCAN RANGES
        4:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 4(F01)
  DOP: 16
  INSTANCES
    INSTANCE(10-F01#0)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10001
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(11-F01#1)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10002
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(12-F01#2)
      DESTINATIONS: 4-F05#0,5-F05#1,6-F05#2
      BE: 10003
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:18: L_ORDERKEY | 36: sum | 10: o_orderdate | 16: o_shippriority
  PARTITION: UNPARTITIONED

  RESULT SINK

  14:MERGING-EXCHANGE
     limit: 10

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 14
    UNPARTITIONED

  13:TOP-N
  |  order by: <slot 36> 36: sum DESC, <slot 10> 10: o_orderdate ASC
  |  offset: 0
  |  limit: 10
  |  
  12:AGGREGATE (update finalize)
  |  output: sum(35: expr)
  |  group by: 18: L_ORDERKEY, 10: o_orderdate, 16: o_shippriority
  |  
  11:Project
  |  <slot 10> : 10: o_orderdate
  |  <slot 16> : 16: o_shippriority
  |  <slot 18> : 18: L_ORDERKEY
  |  <slot 35> : 23: L_EXTENDEDPRICE * 1.0 - 24: L_DISCOUNT
  |  
  10:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 18: L_ORDERKEY = 9: o_orderkey
  |  
  |----9:EXCHANGE
  |    
  1:Project
  |  <slot 18> : 18: L_ORDERKEY
  |  <slot 23> : 23: L_EXTENDEDPRICE
  |  <slot 24> : 24: L_DISCOUNT
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 28: L_SHIPDATE > '1995-03-11'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=28.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 11: o_custkey

  STREAM DATA SINK
    EXCHANGE ID: 09
    BUCKET_SHUFFLE_HASH_PARTITIONED: 9: o_orderkey

  8:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 10> : 10: o_orderdate
  |  <slot 16> : 16: o_shippriority
  |  
  7:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 11: o_custkey = 1: c_custkey
  |  
  |----6:EXCHANGE
  |    
  3:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    HASH_PARTITIONED: 1: c_custkey

  5:Project
  |  <slot 1> : 1: c_custkey
  |  
  4:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     PREDICATES: 7: c_mktsegment = 'HOUSEHOLD'
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=18.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    HASH_PARTITIONED: 11: o_custkey

  2:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 10: o_orderdate < '1995-03-11'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=24.0
[end]

