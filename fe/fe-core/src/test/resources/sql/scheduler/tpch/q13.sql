[sql]
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and o_comment not like '%unusual%deposits%'
        group by
            c_custkey
    ) a
group by
    c_count
order by
    custdist desc,
    c_count desc ;
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
      DESTINATIONS
        0-F04#0
      BE: 10003
    INSTANCE(2-F03#1)
      DESTINATIONS
        0-F04#0
      BE: 10002
    INSTANCE(3-F03#2)
      DESTINATIONS
        0-F04#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS
        1-F03#0,2-F03#1,3-F03#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,19,4,22,7,10,13
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
      DESTINATIONS
        1-F03#0,2-F03#1,3-F03#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,20,5,23,8,11,14
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
      DESTINATIONS
        1-F03#0,2-F03#1,3-F03#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,18,3,21,6,9,12,15
        0:OlapScanNode
          1. partitionID=1306,tabletID=1309
          2. partitionID=1306,tabletID=1315
          3. partitionID=1306,tabletID=1321
          4. partitionID=1306,tabletID=1327
          5. partitionID=1306,tabletID=1333
          6. partitionID=1306,tabletID=1339
          7. partitionID=1306,tabletID=1345
          8. partitionID=1306,tabletID=1351

PLAN FRAGMENT 3(F01)
  DOP: 16
  INSTANCES
    INSTANCE(7-F01#0)
      DESTINATIONS
        6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2
        4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0
        5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(8-F01#1)
      DESTINATIONS
        6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2
        4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0
        5-F00#1,6-F00#2,4-F00#0,5-F00#1
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(9-F01#2)
      DESTINATIONS
        6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2
        4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0
        5-F00#1,6-F00#2,4-F00#0,5-F00#1
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
 OUTPUT EXPRS:18: count | 19: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  12:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 18: count

  STREAM DATA SINK
    EXCHANGE ID: 12
    UNPARTITIONED

  11:SORT
  |  order by: <slot 19> 19: count DESC, <slot 18> 18: count DESC
  |  offset: 0
  |  
  10:AGGREGATE (merge finalize)
  |  output: count(19: count)
  |  group by: 18: count
  |  
  9:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 09
    HASH_PARTITIONED: 18: count

  8:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*)
  |  group by: 18: count
  |  
  7:Project
  |  <slot 18> : 18: count
  |  
  6:AGGREGATE (update finalize)
  |  output: count(9: o_orderkey)
  |  group by: 1: c_custkey
  |  
  5:Project
  |  <slot 1> : 1: c_custkey
  |  <slot 9> : 9: o_orderkey
  |  
  4:HASH JOIN
  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: c_custkey = 11: o_custkey
  |  
  |----3:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: customer
     PREAGGREGATION: ON
     partitions=1/1
     rollup: customer
     tabletRatio=24/24
     tabletList=1309,1311,1313,1315,1317,1319,1321,1323,1325,1327 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    BUCKET_SHUFFLE_HASH_PARTITIONED: 11: o_custkey

  2:Project
  |  <slot 9> : 9: o_orderkey
  |  <slot 11> : 11: o_custkey
  |  
  1:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: NOT (17: o_comment LIKE '%unusual%deposits%')
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=95.0
[end]

