[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[scheduler]
PLAN FRAGMENT 0(F10)
  DOP: 16
  INSTANCES
    INSTANCE(0-F10#0)
      BE: 10001

PLAN FRAGMENT 1(F09)
  DOP: 16
  INSTANCES
    INSTANCE(1-F09#0)
      DESTINATIONS
        0-F10#0
      BE: 10003
    INSTANCE(2-F09#1)
      DESTINATIONS
        0-F10#0
      BE: 10002
    INSTANCE(3-F09#2)
      DESTINATIONS
        0-F10#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS
        1-F09#0,2-F09#1,3-F09#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          2,5,8,11
        0:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(5-F00#1)
      DESTINATIONS
        1-F09#0,2-F09#1,3-F09#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9
        0:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(6-F00#2)
      DESTINATIONS
        1-F09#0,2-F09#1,3-F09#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          1,4,7,10
        0:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 3(F03)
  DOP: 16
  INSTANCES
    INSTANCE(7-F03#0)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          0,18,3,6,9,12,15
        16:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
        6:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
        13:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(8-F03#1)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,19,4,7,10,13
        16:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
        6:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
        13:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(9-F03#2)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        16:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038
        6:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038
        13:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 4(F04)
  DOP: 16
  INSTANCES
    INSTANCE(10-F04#0)
      DESTINATIONS
        7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0
        8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1
      BE: 10001
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1362,tabletID=1367
          2. partitionID=1362,tabletID=1373
          3. partitionID=1362,tabletID=1379
          4. partitionID=1362,tabletID=1385
          5. partitionID=1362,tabletID=1391
          6. partitionID=1362,tabletID=1397
    INSTANCE(11-F04#1)
      DESTINATIONS
        7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0
        8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1
      BE: 10002
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1362,tabletID=1369
          2. partitionID=1362,tabletID=1375
          3. partitionID=1362,tabletID=1381
          4. partitionID=1362,tabletID=1387
          5. partitionID=1362,tabletID=1393
          6. partitionID=1362,tabletID=1399
    INSTANCE(12-F04#2)
      DESTINATIONS
        7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0
        8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1,9-F03#2,7-F03#0,8-F03#1
      BE: 10003
      SCAN RANGES
        8:OlapScanNode
          1. partitionID=1362,tabletID=1365
          2. partitionID=1362,tabletID=1371
          3. partitionID=1362,tabletID=1377
          4. partitionID=1362,tabletID=1383
          5. partitionID=1362,tabletID=1389
          6. partitionID=1362,tabletID=1395

PLAN FRAGMENT 5(F01)
  DOP: 16
  INSTANCES
    INSTANCE(13-F01#0)
      DESTINATIONS
        4-F00#0,5-F00#1,6-F00#2
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1357,tabletID=1360

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:2: s_name | 74: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  27:MERGING-EXCHANGE
     limit: 100

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 2: s_name

  STREAM DATA SINK
    EXCHANGE ID: 27
    UNPARTITIONED

  26:TOP-N
  |  order by: <slot 74> 74: count DESC, <slot 2> 2: s_name ASC
  |  offset: 0
  |  limit: 100
  |  
  25:AGGREGATE (merge finalize)
  |  output: count(74: count)
  |  group by: 2: s_name
  |  
  24:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 24
    HASH_PARTITIONED: 2: s_name

  23:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*)
  |  group by: 2: s_name
  |  
  22:Project
  |  <slot 2> : 2: s_name
  |  
  21:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: s_suppkey = 10: L_SUPPKEY
  |  
  |----20:EXCHANGE
  |    
  5:Project
  |  <slot 1> : 1: s_suppkey
  |  <slot 2> : 2: s_name
  |  
  4:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 4: s_nationkey = 34: n_nationkey
  |  
  |----3:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=33.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 20
    BUCKET_SHUFFLE_HASH_PARTITIONED: 10: L_SUPPKEY

  19:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  
  18:HASH JOIN
  |  join op: LEFT ANTI JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 8: L_ORDERKEY = 56: L_ORDERKEY
  |  other join predicates: 58: L_SUPPKEY != 10: L_SUPPKEY
  |  
  |----17:Project
  |    |  <slot 56> : 56: L_ORDERKEY
  |    |  <slot 58> : 58: L_SUPPKEY
  |    |  
  |    16:OlapScanNode
  |       TABLE: lineitem
  |       PREAGGREGATION: ON
  |       PREDICATES: 68: L_RECEIPTDATE > 67: L_COMMITDATE
  |       partitions=1/1
  |       rollup: lineitem
  |       tabletRatio=20/20
  |       tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
  |       cardinality=1
  |       avgRowSize=20.0
  |    
  15:Project
  |  <slot 8> : 8: L_ORDERKEY
  |  <slot 10> : 10: L_SUPPKEY
  |  
  14:HASH JOIN
  |  join op: LEFT SEMI JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 8: L_ORDERKEY = 38: L_ORDERKEY
  |  other join predicates: 40: L_SUPPKEY != 10: L_SUPPKEY
  |  
  |----13:OlapScanNode
  |       TABLE: lineitem
  |       PREAGGREGATION: ON
  |       partitions=1/1
  |       rollup: lineitem
  |       tabletRatio=20/20
  |       tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
  |       cardinality=1
  |       avgRowSize=12.0
  |    
  12:Project
  |  <slot 8> : 8: L_ORDERKEY
  |  <slot 10> : 10: L_SUPPKEY
  |  
  11:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 8: L_ORDERKEY = 25: o_orderkey
  |  
  |----10:EXCHANGE
  |    
  7:Project
  |  <slot 8> : 8: L_ORDERKEY
  |  <slot 10> : 10: L_SUPPKEY
  |  
  6:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 20: L_RECEIPTDATE > 19: L_COMMITDATE
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=20.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    BUCKET_SHUFFLE_HASH_PARTITIONED: 25: o_orderkey

  9:Project
  |  <slot 25> : 25: o_orderkey
  |  
  8:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 28: o_orderstatus = 'F'
     partitions=1/1
     rollup: orders
     tabletRatio=18/18
     tabletList=1365,1367,1369,1371,1373,1375,1377,1379,1381,1383 ...
     cardinality=1
     avgRowSize=9.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    UNPARTITIONED

  2:Project
  |  <slot 34> : 34: n_nationkey
  |  
  1:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 35: n_name = 'CANADA'
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0
[end]

