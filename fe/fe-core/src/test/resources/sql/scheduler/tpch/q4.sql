[scheduler]
PLAN FRAGMENT 0(F04)
  DOP: 16
  INSTANCES
    INSTANCE(0-F04#0)
      BE: 10001

PLAN FRAGMENT 1(F03)
  DOP: 16
  INSTANCES
    INSTANCE(1-F03#0)
      DESTINATIONS: 0-F04#0
      BE: 10003
    INSTANCE(2-F03#1)
      DESTINATIONS: 0-F04#0
      BE: 10002
    INSTANCE(3-F03#2)
      DESTINATIONS: 0-F04#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F03#0,2-F03#1,3-F03#2
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
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F03#0,2-F03#1,3-F03#2
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
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F03#0,2-F03#1,3-F03#2
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

PLAN FRAGMENT 3(F01)
  DOP: 16
  INSTANCES
    INSTANCE(7-F01#0)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
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
    INSTANCE(8-F01#1)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
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
    INSTANCE(9-F01#2)
      DESTINATIONS: 6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
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
 OUTPUT EXPRS:6: o_orderpriority | 28: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  11:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 6: o_orderpriority

  STREAM DATA SINK
    EXCHANGE ID: 11
    UNPARTITIONED

  10:SORT
  |  order by: <slot 6> 6: o_orderpriority ASC
  |  offset: 0
  |  
  9:AGGREGATE (merge finalize)
  |  output: count(28: count)
  |  group by: 6: o_orderpriority
  |  
  8:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    HASH_PARTITIONED: 6: o_orderpriority

  7:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*)
  |  group by: 6: o_orderpriority
  |  
  6:Project
  |  <slot 6> : 6: o_orderpriority
  |  
  5:HASH JOIN
  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: o_orderkey = 10: L_ORDERKEY
  |  
  |----4:EXCHANGE
  |    
  1:Project
  |  <slot 1> : 1: o_orderkey
  |  <slot 6> : 6: o_orderpriority
  |  
  0:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 2: o_orderdate >= '1994-09-01', 2: o_orderdate < '1994-12-01'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=27.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    BUCKET_SHUFFLE_HASH_PARTITIONED: 10: L_ORDERKEY

  3:Project
  |  <slot 10> : 10: L_ORDERKEY
  |  
  2:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 22: L_RECEIPTDATE > 21: L_COMMITDATE
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=16.0
[end]

