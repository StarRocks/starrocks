[sql]
select
    s_name,
    s_address
from
    supplier,
    nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                        p_name like 'sienna%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date '1993-01-01'
              and l_shipdate < date '1994-01-01'
        )
    )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
order by
    s_name ;
[scheduler]
PLAN FRAGMENT 0(F09)
  DOP: 16
  INSTANCES
    INSTANCE(0-F09#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F09#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          2,5,8,11
        0:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F09#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9
        0:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F09#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          1,4,7,10
        0:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 2(F03)
  DOP: 16
  INSTANCES
    INSTANCE(4-F03#0)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,4,7,10,13
        6:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
        7:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
    INSTANCE(5-F03#1)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        6:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
        7:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
    INSTANCE(6-F03#2)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9,12,15
        6:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473
        7:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434

PLAN FRAGMENT 3(F06)
  DOP: 16
  INSTANCES
    INSTANCE(7-F06#0)
      DESTINATIONS
        6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2
        4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1
      BE: 10003
    INSTANCE(8-F06#1)
      DESTINATIONS
        6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2
        4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1
      BE: 10002
    INSTANCE(9-F06#2)
      DESTINATIONS
        6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2
        4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1,6-F03#2,4-F03#0,5-F03#1
      BE: 10001

PLAN FRAGMENT 4(F05)
  DOP: 16
  INSTANCES
    INSTANCE(10-F05#0)
      DESTINATIONS
        7-F06#0,8-F06#1,9-F06#2
      BE: 10001
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(11-F05#1)
      DESTINATIONS
        7-F06#0,8-F06#1,9-F06#2
      BE: 10002
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(12-F05#2)
      DESTINATIONS
        7-F06#0,8-F06#1,9-F06#2
      BE: 10003
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 5(F01)
  DOP: 16
  INSTANCES
    INSTANCE(13-F01#0)
      DESTINATIONS
        1-F00#0,2-F00#1,3-F00#2
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1357,tabletID=1360

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:2: s_name | 3: s_address
  PARTITION: UNPARTITIONED

  RESULT SINK

  23:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 23
    UNPARTITIONED

  22:SORT
  |  order by: <slot 2> 2: s_name ASC
  |  offset: 0
  |  
  21:Project
  |  <slot 2> : 2: s_name
  |  <slot 3> : 3: s_address
  |  
  20:HASH JOIN
  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: s_suppkey = 13: ps_suppkey
  |  
  |----19:EXCHANGE
  |    
  5:Project
  |  <slot 1> : 1: s_suppkey
  |  <slot 2> : 2: s_name
  |  <slot 3> : 3: s_address
  |  
  4:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 4: s_nationkey = 8: n_nationkey
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
     avgRowSize=73.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 19
    BUCKET_SHUFFLE_HASH_PARTITIONED: 13: ps_suppkey

  18:Project
  |  <slot 13> : 13: ps_suppkey
  |  
  17:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 12: ps_partkey = 28: L_PARTKEY
  |  equal join conjunct: 13: ps_suppkey = 29: L_SUPPKEY
  |  other join predicates: CAST(14: ps_availqty AS DOUBLE) > 0.5 * 44: sum
  |  
  |----16:EXCHANGE
  |    
  10:Project
  |  <slot 12> : 12: ps_partkey
  |  <slot 13> : 13: ps_suppkey
  |  <slot 14> : 14: ps_availqty
  |  
  9:HASH JOIN
  |  join op: LEFT SEMI JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 12: ps_partkey = 17: p_partkey
  |  
  |----8:Project
  |    |  <slot 17> : 17: p_partkey
  |    |  
  |    7:OlapScanNode
  |       TABLE: part
  |       PREAGGREGATION: ON
  |       PREDICATES: 18: p_name LIKE 'sienna%'
  |       partitions=1/1
  |       rollup: part
  |       tabletRatio=18/18
  |       tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
  |       cardinality=1
  |       avgRowSize=63.0
  |    
  6:OlapScanNode
     TABLE: partsupp
     PREAGGREGATION: ON
     partitions=1/1
     rollup: partsupp
     tabletRatio=18/18
     tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
     cardinality=1
     avgRowSize=20.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 28: L_PARTKEY, 29: L_SUPPKEY

  STREAM DATA SINK
    EXCHANGE ID: 16
    BUCKET_SHUFFLE_HASH_PARTITIONED: 28: L_PARTKEY

  15:AGGREGATE (merge finalize)
  |  output: sum(44: sum)
  |  group by: 28: L_PARTKEY, 29: L_SUPPKEY
  |  
  14:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 14
    HASH_PARTITIONED: 28: L_PARTKEY, 29: L_SUPPKEY

  13:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(31: L_QUANTITY)
  |  group by: 28: L_PARTKEY, 29: L_SUPPKEY
  |  
  12:Project
  |  <slot 28> : 28: L_PARTKEY
  |  <slot 29> : 29: L_SUPPKEY
  |  <slot 31> : 31: L_QUANTITY
  |  
  11:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 37: L_SHIPDATE >= '1993-01-01', 37: L_SHIPDATE < '1994-01-01'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=24.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    UNPARTITIONED

  2:Project
  |  <slot 8> : 8: n_nationkey
  |  
  1:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 9: n_name = 'ARGENTINA'
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0
[end]

