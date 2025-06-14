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

PLAN FRAGMENT 2(F05)
  DOP: 16
  INSTANCES
    INSTANCE(4-F05#0)
      DESTINATIONS: 1-F00#0,2-F00#1,3-F00#2
      BE: 10003
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 3(F01)
  DOP: 16
  INSTANCES
    INSTANCE(5-F01#0)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [0, 18, 3, 6, 9, 12, 15]
        1:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(6-F01#1)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 19, 4, 7, 10, 13]
        1:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(7-F01#2)
      DESTINATIONS: 3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        1:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 4(F02)
  DOP: 16
  INSTANCES
    INSTANCE(8-F02#0)
      DESTINATIONS: 5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(9-F02#1)
      DESTINATIONS: 5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1
      BE: 10002
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(10-F02#2)
      DESTINATIONS: 5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1,7-F01#2,5-F01#0,6-F01#1
      BE: 10003
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: c_custkey | 2: c_name | 40: sum | 6: c_acctbal | 36: n_name | 3: c_address | 5: c_phone | 8: c_comment
  PARTITION: UNPARTITIONED

  RESULT SINK

  17:MERGING-EXCHANGE
     limit: 20

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 17
    UNPARTITIONED

  16:TOP-N
  |  order by: <slot 40> 40: sum DESC
  |  offset: 0
  |  limit: 20
  |  
  15:AGGREGATE (update finalize)
  |  output: sum(39: expr)
  |  group by: 1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 36: n_name, 3: c_address, 8: c_comment
  |  
  14:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 2> : 2: c_name
  |  <slot 3> : 3: c_address
  |  <slot 5> : 5: c_phone
  |  <slot 6> : 6: c_acctbal
  |  <slot 8> : 8: c_comment
  |  <slot 36> : 36: n_name
  |  <slot 39> : 23: L_EXTENDEDPRICE * 1.0 - 24: L_DISCOUNT
  |  
  13:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 4: c_nationkey = 35: n_nationkey
  |  
  |----12:EXCHANGE
  |    
  10:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 2> : 2: c_name
  |  <slot 3> : 3: c_address
  |  <slot 4> : 4: c_nationkey
  |  <slot 5> : 5: c_phone
  |  <slot 6> : 6: c_acctbal
  |  <slot 8> : 8: c_comment
  |  <slot 23> : 23: L_EXTENDEDPRICE
  |  <slot 24> : 24: L_DISCOUNT
  |  
  9:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: c_custkey = 11: o_custkey
  |  
  |----8:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=217.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    UNPARTITIONED

  11:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    BUCKET_SHUFFLE_HASH_PARTITIONED: 11: o_custkey

  7:Project
  |  <slot 11> : 11: o_custkey
  |  <slot 23> : 23: L_EXTENDEDPRICE
  |  <slot 24> : 24: L_DISCOUNT
  |  
  6:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 18: L_ORDERKEY = 9: o_orderkey
  |  
  |----5:EXCHANGE
  |    
  2:Project
  |  <slot 18> : 18: L_ORDERKEY
  |  <slot 23> : 23: L_EXTENDEDPRICE
  |  <slot 24> : 24: L_DISCOUNT
  |  
  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 26: L_RETURNFLAG = 'R'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=25.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    BUCKET_SHUFFLE_HASH_PARTITIONED: 9: o_orderkey

  4:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 11> : 11: o_custkey
  |  
  3:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 10: o_orderdate >= '1994-05-01', 10: o_orderdate < '1994-08-01'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=20.0
[end]

