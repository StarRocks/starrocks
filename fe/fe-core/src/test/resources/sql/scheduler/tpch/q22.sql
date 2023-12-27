[scheduler]
PLAN FRAGMENT 0(F07)
  DOP: 16
  INSTANCES
    INSTANCE(0-F07#0)
      BE: 10002

PLAN FRAGMENT 1(F06)
  DOP: 16
  INSTANCES
    INSTANCE(1-F06#0)
      DESTINATIONS: 0-F07#0
      BE: 10003
    INSTANCE(2-F06#1)
      DESTINATIONS: 0-F07#0
      BE: 10002
    INSTANCE(3-F06#2)
      DESTINATIONS: 0-F07#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 19, 4, 22, 7, 10, 13]
        0:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 20, 5, 23, 8, 11, 14]
        0:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F06#0,2-F06#1,3-F06#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 18, 3, 21, 6, 9, 12, 15]
        0:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 3(F04)
  DOP: 16
  INSTANCES
    INSTANCE(7-F04#0)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10001
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(8-F04#1)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10002
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(9-F04#2)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10003
      SCAN RANGES
        10:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(10-F02#0)
      DESTINATIONS: 4-F00#0,5-F00#1,6-F00#2
      BE: 10001

PLAN FRAGMENT 5(F01)
  DOP: 16
  INSTANCES
    INSTANCE(11-F01#0)
      DESTINATIONS: 10-F02#0
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1306,tabletID=1311
          2. partitionID=1306,tabletID=1317
          3. partitionID=1306,tabletID=1323
          4. partitionID=1306,tabletID=1329
          5. partitionID=1306,tabletID=1335
          6. partitionID=1306,tabletID=1341
          7. partitionID=1306,tabletID=1347
          8. partitionID=1306,tabletID=1353
    INSTANCE(12-F01#1)
      DESTINATIONS: 10-F02#0
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1306,tabletID=1313
          2. partitionID=1306,tabletID=1319
          3. partitionID=1306,tabletID=1325
          4. partitionID=1306,tabletID=1331
          5. partitionID=1306,tabletID=1337
          6. partitionID=1306,tabletID=1343
          7. partitionID=1306,tabletID=1349
          8. partitionID=1306,tabletID=1355
    INSTANCE(13-F01#2)
      DESTINATIONS: 10-F02#0
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:29: substring | 30: count | 31: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  18:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 29: substring

  STREAM DATA SINK
    EXCHANGE ID: 18
    UNPARTITIONED

  17:SORT
  |  order by: <slot 29> 29: substring ASC
  |  offset: 0
  |  
  16:AGGREGATE (merge finalize)
  |  output: count(30: count), sum(31: sum)
  |  group by: 29: substring
  |  
  15:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 15
    HASH_PARTITIONED: 29: substring

  14:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*), sum(6: c_acctbal)
  |  group by: 29: substring
  |  
  13:Project
  |  <slot 6> : 6: c_acctbal
  |  <slot 29> : substring(5: c_phone, 1, 2)
  |  
  12:HASH JOIN
  |  join op: LEFT ANTI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: c_custkey = 21: o_custkey
  |  
  |----11:EXCHANGE
  |    
  9:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 5> : 5: c_phone
  |  <slot 6> : 6: c_acctbal
  |  
  8:NESTLOOP JOIN
  |  join op: INNER JOIN
  |  colocate: false, reason: 
  |  other join predicates: CAST(6: c_acctbal AS DECIMAL128(38,8)) > 17: avg
  |  
  |----7:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     PREDICATES: substring(5: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=31.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 11
    BUCKET_SHUFFLE_HASH_PARTITIONED: 21: o_custkey

  10:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: UNPARTITIONED

  STREAM DATA SINK
    EXCHANGE ID: 07
    UNPARTITIONED

  6:ASSERT NUMBER OF ROWS
  |  assert number of rows: LE 1
  |  
  5:AGGREGATE (merge finalize)
  |  output: avg(17: avg)
  |  group by: 
  |  
  4:EXCHANGE

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:AGGREGATE (update serialize)
  |  output: avg(14: c_acctbal)
  |  group by: 
  |  
  2:Project
  |  <slot 14> : 14: c_acctbal
  |  
  1:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     PREDICATES: 14: c_acctbal > 0.00, substring(13: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=23.0
[end]

