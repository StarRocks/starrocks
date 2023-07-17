[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        (	select
                 l_suppkey as supplier_no,
                 sum(l_extendedprice * (1 - l_discount)) as total_revenue
             from
                 lineitem
             where
                     l_shipdate >= date '1995-07-01'
               and l_shipdate < date '1995-10-01'
             group by
                 l_suppkey) b
)
order by
    s_suppkey;
[scheduler]
PLAN FRAGMENT 0(F08)
  DOP: 16
  INSTANCES
    INSTANCE(0-F08#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F08#0
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
        0-F08#0
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
        0-F08#0
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES
          1,4,7,10
        0:OlapScanNode
          1. partitionID=1484,tabletID=1489
          2. partitionID=1484,tabletID=1495
          3. partitionID=1484,tabletID=1501
          4. partitionID=1484,tabletID=1507

PLAN FRAGMENT 2(F02)
  DOP: 16
  INSTANCES
    INSTANCE(4-F02#0)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10001
    INSTANCE(5-F02#1)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10002
    INSTANCE(6-F02#2)
      DESTINATIONS
        2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1,3-F00#2,1-F00#0,2-F00#1
        3-F00#2,1-F00#0
      BE: 10003

PLAN FRAGMENT 3(F05)
  DOP: 16
  INSTANCES
    INSTANCE(7-F05#0)
      DESTINATIONS
        4-F02#0,5-F02#1,6-F02#2
      BE: 10001

PLAN FRAGMENT 4(F04)
  DOP: 16
  INSTANCES
    INSTANCE(8-F04#0)
      DESTINATIONS
        7-F05#0
      BE: 10003
    INSTANCE(9-F04#1)
      DESTINATIONS
        7-F05#0
      BE: 10002
    INSTANCE(10-F04#2)
      DESTINATIONS
        7-F05#0
      BE: 10001

PLAN FRAGMENT 5(F03)
  DOP: 16
  INSTANCES
    INSTANCE(11-F03#0)
      DESTINATIONS
        8-F04#0,9-F04#1,10-F04#2
      BE: 10001
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(12-F03#1)
      DESTINATIONS
        8-F04#0,9-F04#1,10-F04#2
      BE: 10002
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(13-F03#2)
      DESTINATIONS
        8-F04#0,9-F04#1,10-F04#2
      BE: 10003
      SCAN RANGES
        6:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

PLAN FRAGMENT 6(F01)
  DOP: 16
  INSTANCES
    INSTANCE(14-F01#0)
      DESTINATIONS
        4-F02#0,5-F02#1,6-F02#2
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(15-F01#1)
      DESTINATIONS
        4-F02#0,5-F02#1,6-F02#2
      BE: 10002
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(16-F01#2)
      DESTINATIONS
        4-F02#0,5-F02#1,6-F02#2
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: s_suppkey | 2: s_name | 3: s_address | 5: s_phone | 26: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  24:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 24
    UNPARTITIONED

  23:SORT
  |  order by: <slot 1> 1: s_suppkey ASC
  |  offset: 0
  |  
  22:Project
  |  <slot 1> : 1: s_suppkey
  |  <slot 2> : 2: s_name
  |  <slot 3> : 3: s_address
  |  <slot 5> : 5: s_phone
  |  <slot 26> : 26: sum
  |  
  21:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: s_suppkey = 10: L_SUPPKEY
  |  
  |----20:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: supplier
     PREAGGREGATION: ON
     partitions=1/1
     rollup: supplier
     tabletRatio=12/12
     tabletList=1487,1489,1491,1493,1495,1497,1499,1501,1503,1505 ...
     cardinality=1
     avgRowSize=84.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 10: L_SUPPKEY

  STREAM DATA SINK
    EXCHANGE ID: 20
    BUCKET_SHUFFLE_HASH_PARTITIONED: 10: L_SUPPKEY

  19:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  <slot 26> : 26: sum
  |  
  18:HASH JOIN
  |  join op: INNER JOIN (BROADCAST)
  |  colocate: false, reason: 
  |  equal join conjunct: 26: sum = 46: max
  |  
  |----17:EXCHANGE
  |    
  5:AGGREGATE (merge finalize)
  |  output: sum(26: sum)
  |  group by: 10: L_SUPPKEY
  |  having: 26: sum IS NOT NULL
  |  
  4:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: UNPARTITIONED

  STREAM DATA SINK
    EXCHANGE ID: 17
    UNPARTITIONED

  16:SELECT
  |  predicates: 46: max IS NOT NULL
  |  
  15:ASSERT NUMBER OF ROWS
  |  assert number of rows: LE 1
  |  
  14:AGGREGATE (merge finalize)
  |  output: max(46: max)
  |  group by: 
  |  
  13:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 29: L_SUPPKEY

  STREAM DATA SINK
    EXCHANGE ID: 13
    UNPARTITIONED

  12:AGGREGATE (update serialize)
  |  output: max(45: sum)
  |  group by: 
  |  
  11:Project
  |  <slot 45> : 45: sum
  |  
  10:AGGREGATE (merge finalize)
  |  output: sum(45: sum)
  |  group by: 29: L_SUPPKEY
  |  
  9:EXCHANGE

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 09
    HASH_PARTITIONED: 29: L_SUPPKEY

  8:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(44: expr)
  |  group by: 29: L_SUPPKEY
  |  
  7:Project
  |  <slot 29> : 29: L_SUPPKEY
  |  <slot 44> : 32: L_EXTENDEDPRICE * 1.0 - 33: L_DISCOUNT
  |  
  6:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 37: L_SHIPDATE >= '1995-07-01', 37: L_SHIPDATE < '1995-10-01'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=32.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    HASH_PARTITIONED: 10: L_SUPPKEY

  3:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(25: expr)
  |  group by: 10: L_SUPPKEY
  |  
  2:Project
  |  <slot 10> : 10: L_SUPPKEY
  |  <slot 25> : 13: L_EXTENDEDPRICE * 1.0 - 14: L_DISCOUNT
  |  
  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 18: L_SHIPDATE >= '1995-07-01', 18: L_SHIPDATE < '1995-10-01'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=32.0
[end]

