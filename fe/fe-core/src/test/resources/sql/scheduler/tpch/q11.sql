[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    partsupp,
    supplier,
    nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[scheduler]
PLAN FRAGMENT 0(F14)
  DOP: 16
  INSTANCES
    INSTANCE(0-F14#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F14#0
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES
          16,1,4,7,10,13
        0:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F14#0
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES
          17,2,5,8,11,14
        0:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F14#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0,3,6,9,12,15
        0:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 2(F12)
  DOP: 16
  INSTANCES
    INSTANCE(4-F12#0)
      DESTINATIONS
        1-F00#0,2-F00#1,3-F00#2
      BE: 10001

PLAN FRAGMENT 3(F11)
  DOP: 16
  INSTANCES
    INSTANCE(5-F11#0)
      DESTINATIONS
        4-F12#0
      BE: 10003
    INSTANCE(6-F11#1)
      DESTINATIONS
        4-F12#0
      BE: 10002
    INSTANCE(7-F11#2)
      DESTINATIONS
        4-F12#0
      BE: 10001

PLAN FRAGMENT 4(F07)
  DOP: 16
  INSTANCES
    INSTANCE(8-F07#0)
      DESTINATIONS
        5-F11#0,6-F11#1,7-F11#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0
        13:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 5(F08)
  DOP: 16
  INSTANCES
    INSTANCE(9-F08#0)
      DESTINATIONS
        8-F07#0
      BE: 10001
      SCAN RANGES
        15:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(10-F08#1)
      DESTINATIONS
        8-F07#0
      BE: 10002
      SCAN RANGES
        15:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(11-F08#2)
      DESTINATIONS
        8-F07#0
      BE: 10003
      SCAN RANGES
        15:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 6(F05)
  DOP: 16
  INSTANCES
    INSTANCE(12-F05#0)
      DESTINATIONS
        5-F11#0,6-F11#1,7-F11#2
      BE: 10001
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1445
          2. partitionID=1440,tabletID=1451
          3. partitionID=1440,tabletID=1457
          4. partitionID=1440,tabletID=1463
          5. partitionID=1440,tabletID=1469
          6. partitionID=1440,tabletID=1475
    INSTANCE(13-F05#1)
      DESTINATIONS
        5-F11#0,6-F11#1,7-F11#2
      BE: 10002
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1447
          2. partitionID=1440,tabletID=1453
          3. partitionID=1440,tabletID=1459
          4. partitionID=1440,tabletID=1465
          5. partitionID=1440,tabletID=1471
          6. partitionID=1440,tabletID=1477
    INSTANCE(14-F05#2)
      DESTINATIONS
        5-F11#0,6-F11#1,7-F11#2
      BE: 10003
      SCAN RANGES
        11:OlapScanNode
          1. partitionID=1440,tabletID=1443
          2. partitionID=1440,tabletID=1449
          3. partitionID=1440,tabletID=1455
          4. partitionID=1440,tabletID=1461
          5. partitionID=1440,tabletID=1467
          6. partitionID=1440,tabletID=1473

PLAN FRAGMENT 7(F01)
  DOP: 16
  INSTANCES
    INSTANCE(15-F01#0)
      DESTINATIONS
        1-F00#0,2-F00#1,3-F00#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          0
        1:OlapScanNode
          1. partitionID=1357,tabletID=1360

PLAN FRAGMENT 8(F02)
  DOP: 16
  INSTANCES
    INSTANCE(16-F02#0)
      DESTINATIONS
        15-F01#0
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1484,tabletID=1491
          2. partitionID=1484,tabletID=1497
          3. partitionID=1484,tabletID=1503
          4. partitionID=1484,tabletID=1509
    INSTANCE(17-F02#1)
      DESTINATIONS
        15-F01#0
      BE: 10002
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1484,tabletID=1487
          2. partitionID=1484,tabletID=1493
          3. partitionID=1484,tabletID=1499
          4. partitionID=1484,tabletID=1505
    INSTANCE(18-F02#2)
      DESTINATIONS
        15-F01#0
      BE: 10003
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: ps_partkey | 18: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  31:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 31
    UNPARTITIONED

  30:SORT
  |  order by: <slot 18> 18: sum DESC
  |  offset: 0
  |  
  29:Project
  |  <slot 1> : 1: ps_partkey
  |  <slot 18> : 18: sum
  |  
  28:NESTLOOP JOIN
  |  join op: INNER JOIN
  |  colocate: false, reason: 
  |  other join predicates: CAST(18: sum AS DOUBLE) > CAST(37: expr AS DOUBLE)
  |  
  |----27:EXCHANGE
  |    
  10:AGGREGATE (update finalize)
  |  output: sum(17: expr)
  |  group by: 1: ps_partkey
  |  
  9:Project
  |  <slot 1> : 1: ps_partkey
  |  <slot 17> : CAST(4: ps_supplycost AS DECIMAL128(15,2)) * CAST(3: ps_availqty AS DECIMAL128(9,0))
  |  
  8:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 2: ps_suppkey = 6: s_suppkey
  |  
  |----7:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: partsupp
     PREAGGREGATION: ON
     partitions=1/1
     rollup: partsupp
     tabletRatio=18/18
     tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
     cardinality=1
     avgRowSize=28.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: UNPARTITIONED

  STREAM DATA SINK
    EXCHANGE ID: 27
    UNPARTITIONED

  26:ASSERT NUMBER OF ROWS
  |  assert number of rows: LE 1
  |  
  25:Project
  |  <slot 37> : 36: sum * 0.0001000000
  |  
  24:AGGREGATE (merge finalize)
  |  output: sum(36: sum)
  |  group by: 
  |  
  23:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 20: ps_suppkey

  STREAM DATA SINK
    EXCHANGE ID: 23
    UNPARTITIONED

  22:AGGREGATE (update serialize)
  |  output: sum(CAST(22: ps_supplycost AS DECIMAL128(15,2)) * CAST(21: ps_availqty AS DECIMAL128(9,0)))
  |  group by: 
  |  
  21:Project
  |  <slot 21> : 21: ps_availqty
  |  <slot 22> : 22: ps_supplycost
  |  
  20:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 20: ps_suppkey = 24: s_suppkey
  |  
  |----19:EXCHANGE
  |    
  12:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 19
    HASH_PARTITIONED: 24: s_suppkey

  18:Project
  |  <slot 24> : 24: s_suppkey
  |  
  17:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 31: n_nationkey = 27: s_nationkey
  |  
  |----16:EXCHANGE
  |    
  14:Project
  |  <slot 31> : 31: n_nationkey
  |  
  13:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 32: n_name = 'PERU'
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 16
    BUCKET_SHUFFLE_HASH_PARTITIONED: 27: s_nationkey

  15:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 12
    HASH_PARTITIONED: 20: ps_suppkey

  11:OlapScanNode
     TABLE: partsupp
     PREAGGREGATION: ON
     partitions=1/1
     rollup: partsupp
     tabletRatio=18/18
     tabletList=1443,1445,1447,1449,1451,1453,1455,1457,1459,1461 ...
     cardinality=1
     avgRowSize=20.0

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    UNPARTITIONED

  6:Project
  |  <slot 6> : 6: s_suppkey
  |  
  5:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 13: n_nationkey = 9: s_nationkey
  |  
  |----4:EXCHANGE
  |    
  2:Project
  |  <slot 13> : 13: n_nationkey
  |  
  1:OlapScanNode
     TABLE: nation
     PREAGGREGATION: ON
     PREDICATES: 14: n_name = 'PERU'
     partitions=1/1
     rollup: nation
     tabletRatio=1/1
     tabletList=1360
     cardinality=1
     avgRowSize=29.0

PLAN FRAGMENT 8
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    BUCKET_SHUFFLE_HASH_PARTITIONED: 9: s_nationkey

  3:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=8.0
[end]

