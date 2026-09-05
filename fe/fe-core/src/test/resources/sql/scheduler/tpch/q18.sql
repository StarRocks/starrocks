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
    INSTANCE(2-F00#1)
      DESTINATIONS: 0-F07#0
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
    INSTANCE(3-F00#2)
      DESTINATIONS: 0-F07#0
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

PLAN FRAGMENT 2(F01)
  DOP: 16
  INSTANCES
    INSTANCE(4-F01#0)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        1:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(5-F01#1)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        1:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(6-F01#2)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        1:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

PLAN FRAGMENT 3(F04)
  DOP: 16
  INSTANCES
    INSTANCE(7-F04#0)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10001
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(8-F04#1)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10002
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(9-F04#2)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10003
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(10-F02#0)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10001
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(11-F02#1)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10002
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(12-F02#2)
      DESTINATIONS: 6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1,6-F01#2,4-F01#0,5-F01#1
      BE: 10003
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:2: c_name | 1: c_custkey | 9: o_orderkey | 10: o_orderdate | 13: o_totalprice | 54: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  17:MERGING-EXCHANGE
     limit: 100

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 17
    UNPARTITIONED

  16:TOP-N
  |  order by: <slot 13> 13: o_totalprice DESC, <slot 10> 10: o_orderdate ASC
  |  offset: 0
  |  limit: 100
  |  
  15:AGGREGATE (update finalize)
  |  output: sum(22: L_QUANTITY)
  |  group by: 2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice
  |  
  14:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 2> : 2: c_name
  |  <slot 9> : 9: o_orderkey
  |  <slot 10> : 10: o_orderdate
  |  <slot 13> : 13: o_totalprice
  |  <slot 22> : 22: L_QUANTITY
  |  
  13:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: c_custkey = 11: o_custkey
  |  
  |----12:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=33.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    BUCKET_SHUFFLE_HASH_PARTITIONED: 11: o_custkey

  11:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 10> : 10: o_orderdate
  |  <slot 11> : 11: o_custkey
  |  <slot 13> : 13: o_totalprice
  |  <slot 22> : 22: L_QUANTITY
  |  
  10:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 9: o_orderkey = 18: L_ORDERKEY
  |  
  |----9:EXCHANGE
  |    
  7:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 10> : 10: o_orderdate
  |  <slot 11> : 11: o_custkey
  |  <slot 13> : 13: o_totalprice
  |  
  6:HASH JOIN
  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 9: o_orderkey = 35: L_ORDERKEY
  |  
  |----5:EXCHANGE
  |    
  1:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=28.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 09
    BUCKET_SHUFFLE_HASH_PARTITIONED: 18: L_ORDERKEY

  8:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=16.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    BUCKET_SHUFFLE_HASH_PARTITIONED: 35: L_ORDERKEY

  4:Project
  |  <slot 35> : 35: L_ORDERKEY
  |  
  3:AGGREGATE (update finalize)
  |  output: sum(39: L_QUANTITY)
  |  group by: 35: L_ORDERKEY
  |  having: 52: sum > 315.0
  |  
  2:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=16.0
[end]

