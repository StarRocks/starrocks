[sql]
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
        p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 12
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
        partsupp,
        supplier,
        nation,
        region
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'AMERICA'
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey limit 100;
[scheduler]
PLAN FRAGMENT 0(F09)
  DOP: 16
  INSTANCES
    INSTANCE(0-F09#0)
      BE: 10001

PLAN FRAGMENT 1(F08)
  DOP: 16
  INSTANCES
    INSTANCE(1-F08#0)
      DESTINATIONS
        0-F09#0
      BE: 10003
    INSTANCE(2-F08#1)
      DESTINATIONS
        0-F09#0
      BE: 10002
    INSTANCE(3-F08#2)
      DESTINATIONS
        0-F09#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS
        1-F08#0,2-F08#1,3-F08#2
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
        1-F08#0,2-F08#1,3-F08#2
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
        1-F08#0,2-F08#1,3-F08#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          1,4,7,10
        0:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 3(F05)
  DOP: 16
  INSTANCES
    INSTANCE(7-F05#0)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,4,7,10,13
        10:OlapScanNode
          1. partitionID=1401,tabletID=1406
          2. partitionID=1401,tabletID=1412
          3. partitionID=1401,tabletID=1418
          4. partitionID=1401,tabletID=1424
          5. partitionID=1401,tabletID=1430
          6. partitionID=1401,tabletID=1436
        12:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(8-F05#1)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        10:OlapScanNode
          1. partitionID=1401,tabletID=1408
          2. partitionID=1401,tabletID=1414
          3. partitionID=1401,tabletID=1420
          4. partitionID=1401,tabletID=1426
          5. partitionID=1401,tabletID=1432
          6. partitionID=1401,tabletID=1438
        12:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(9-F05#2)
      DESTINATIONS
        5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1,6-F00#2,4-F00#0,5-F00#1
        6-F00#2,4-F00#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9,12,15
        10:OlapScanNode
          1. partitionID=1401,tabletID=1404
          2. partitionID=1401,tabletID=1410
          3. partitionID=1401,tabletID=1416
          4. partitionID=1401,tabletID=1422
          5. partitionID=1401,tabletID=1428
          6. partitionID=1401,tabletID=1434
        12:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 4(F01)
  DOP: 16
  INSTANCES
    INSTANCE(10-F01#0)
      DESTINATIONS
        4-F00#0,5-F00#1,6-F00#2
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 5(F02)
  DOP: 16
  INSTANCES
    INSTANCE(11-F02#0)
      DESTINATIONS
        10-F01#0
      BE: 10001
      SCAN RANGES
        2:OlapScanNode
          1. partitionID=1479,tabletID=1482

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:15: s_acctbal | 11: s_name | 23: n_name | 1: p_partkey | 3: p_mfgr | 12: s_address | 14: s_phone | 16: s_comment
  PARTITION: UNPARTITIONED

  RESULT SINK

  24:MERGING-EXCHANGE
     limit: 100

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 1: p_partkey

  STREAM DATA SINK
    EXCHANGE ID: 24
    UNPARTITIONED

  23:TOP-N
  |  order by: <slot 15> 15: s_acctbal DESC, <slot 23> 23: n_name ASC, <slot 11> 11: s_name ASC, <slot 1> 1: p_partkey ASC
  |  offset: 0
  |  limit: 100
  |  
  22:Project
  |  <slot 1> : 1: p_partkey
  |  <slot 3> : 3: p_mfgr
  |  <slot 11> : 11: s_name
  |  <slot 12> : 12: s_address
  |  <slot 14> : 14: s_phone
  |  <slot 15> : 15: s_acctbal
  |  <slot 16> : 16: s_comment
  |  <slot 23> : 23: n_name
  |  
  21:SELECT
  |  predicates: 20: ps_supplycost = 50: min
  |  
  20:ANALYTIC
  |  functions: [, min(20: ps_supplycost), ]
  |  partition by: 1: p_partkey
  |  
  19:SORT
  |  order by: <slot 1> 1: p_partkey ASC
  |  offset: 0
  |  
  18:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 18
    HASH_PARTITIONED: 1: p_partkey

  17:Project
  |  <slot 1> : 1: p_partkey
  |  <slot 3> : 3: p_mfgr
  |  <slot 11> : 11: s_name
  |  <slot 12> : 12: s_address
  |  <slot 14> : 14: s_phone
  |  <slot 15> : 15: s_acctbal
  |  <slot 16> : 16: s_comment
  |  <slot 20> : 20: ps_supplycost
  |  <slot 23> : 23: n_name
  |  
  16:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 10: s_suppkey = 18: ps_suppkey
  |  
  |----15:EXCHANGE
  |    
  9:Project
  |  <slot 10> : 10: s_suppkey
  |  <slot 11> : 11: s_name
  |  <slot 12> : 12: s_address
  |  <slot 14> : 14: s_phone
  |  <slot 15> : 15: s_acctbal
  |  <slot 16> : 16: s_comment
  |  <slot 23> : 23: n_name
  |  
  8:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 13: s_nationkey = 22: n_nationkey
  |  
  |----7:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=197.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 15
    BUCKET_SHUFFLE_HASH_PARTITIONED: 18: ps_suppkey

  14:Project
  |  <slot 1> : 1: p_partkey
  |  <slot 3> : 3: p_mfgr
  |  <slot 18> : 18: ps_suppkey
  |  <slot 20> : 20: ps_supplycost
  |  
  13:HASH JOIN
  |  join op: INNER JOIN (COLOCATE)
  |  colocate: true
  |  equal join conjunct: 1: p_partkey = 17: ps_partkey
  |  
  |----12:OlapScanNode
  |       TABLE: partsupp
  |       PREAGGREGATION: ON
  |       partitions=1/1
  |       rollup: partsupp
  |       tabletRatio=18/18
  |       tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
  |       cardinality=1
  |       avgRowSize=24.0
  |    
  11:Project
  |  <slot 1> : 1: p_partkey
  |  <slot 3> : 3: p_mfgr
  |  
  10:OlapScanNode
     TABLE: part
     PREAGGREGATION: ON
     PREDICATES: 6: p_size = 12, 5: p_type LIKE '%COPPER'
     partitions=1/1
     rollup: part
     tabletRatio=18/18
     tabletList=1404,1406,1408,1410,1412,1414,1416,1418,1420,1422 ...
     cardinality=1
     avgRowSize=62.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    UNPARTITIONED

  6:Project
  |  <slot 22> : 22: n_nationkey
  |  <slot 23> : 23: n_name
  |  
  5:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 24: n_regionkey = 26: r_regionkey
  |  
  |----4:EXCHANGE
  |    
  1:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=33.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:Project
  |  <slot 26> : 26: r_regionkey
  |  
  2:OlapScanNode
     TABLE: region
     PREAGGREGATION: ON
     PREDICATES: 27: r_name = 'AMERICA'
     partitions=1/1
     rollup: region
     tabletRatio=1/1
     tabletList=1482
     cardinality=1
     avgRowSize=29.0
[end]

