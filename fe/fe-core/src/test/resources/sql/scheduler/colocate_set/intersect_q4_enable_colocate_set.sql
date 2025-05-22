
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
      DESTINATIONS: 0-F10#0
      BE: 10001
    INSTANCE(2-F09#1)
      DESTINATIONS: 0-F10#0
      BE: 10002
    INSTANCE(3-F09#2)
      DESTINATIONS: 0-F10#0
      BE: 10003

PLAN FRAGMENT 2(F08)
  DOP: 16
  INSTANCES
    INSTANCE(4-F08#0)
      DESTINATIONS: 1-F09#0,2-F09#1,3-F09#2
      BE: 10003
    INSTANCE(5-F08#1)
      DESTINATIONS: 1-F09#0,2-F09#1,3-F09#2
      BE: 10002
    INSTANCE(6-F08#2)
      DESTINATIONS: 1-F09#0,2-F09#1,3-F09#2
      BE: 10001

PLAN FRAGMENT 3(F06)
  DOP: 16
  INSTANCES
    INSTANCE(7-F06#0)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        16:OlapScanNode
          1. partitionID=2202,tabletID=2205
          2. partitionID=2202,tabletID=2211
          3. partitionID=2202,tabletID=2217
          4. partitionID=2202,tabletID=2223
          5. partitionID=2202,tabletID=2229
          6. partitionID=2202,tabletID=2235
    INSTANCE(8-F06#1)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        16:OlapScanNode
          1. partitionID=2202,tabletID=2207
          2. partitionID=2202,tabletID=2213
          3. partitionID=2202,tabletID=2219
          4. partitionID=2202,tabletID=2225
          5. partitionID=2202,tabletID=2231
          6. partitionID=2202,tabletID=2237
    INSTANCE(9-F06#2)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        16:OlapScanNode
          1. partitionID=2202,tabletID=2203
          2. partitionID=2202,tabletID=2209
          3. partitionID=2202,tabletID=2215
          4. partitionID=2202,tabletID=2221
          5. partitionID=2202,tabletID=2227
          6. partitionID=2202,tabletID=2233

PLAN FRAGMENT 4(F04)
  DOP: 16
  INSTANCES
    INSTANCE(10-F04#0)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        11:OlapScanNode
          1. partitionID=1934,tabletID=1937
          2. partitionID=1934,tabletID=1943
          3. partitionID=1934,tabletID=1949
          4. partitionID=1934,tabletID=1955
          5. partitionID=1934,tabletID=1961
          6. partitionID=1934,tabletID=1967
    INSTANCE(11-F04#1)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        11:OlapScanNode
          1. partitionID=1934,tabletID=1939
          2. partitionID=1934,tabletID=1945
          3. partitionID=1934,tabletID=1951
          4. partitionID=1934,tabletID=1957
          5. partitionID=1934,tabletID=1963
          6. partitionID=1934,tabletID=1969
    INSTANCE(12-F04#2)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        11:OlapScanNode
          1. partitionID=1934,tabletID=1935
          2. partitionID=1934,tabletID=1941
          3. partitionID=1934,tabletID=1947
          4. partitionID=1934,tabletID=1953
          5. partitionID=1934,tabletID=1959
          6. partitionID=1934,tabletID=1965

PLAN FRAGMENT 5(F02)
  DOP: 16
  INSTANCES
    INSTANCE(13-F02#0)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        6:OlapScanNode
          1. partitionID=1665,tabletID=1668
          2. partitionID=1665,tabletID=1674
          3. partitionID=1665,tabletID=1680
          4. partitionID=1665,tabletID=1686
          5. partitionID=1665,tabletID=1692
          6. partitionID=1665,tabletID=1698
    INSTANCE(14-F02#1)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        6:OlapScanNode
          1. partitionID=1665,tabletID=1670
          2. partitionID=1665,tabletID=1676
          3. partitionID=1665,tabletID=1682
          4. partitionID=1665,tabletID=1688
          5. partitionID=1665,tabletID=1694
          6. partitionID=1665,tabletID=1700
    INSTANCE(15-F02#2)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        6:OlapScanNode
          1. partitionID=1665,tabletID=1666
          2. partitionID=1665,tabletID=1672
          3. partitionID=1665,tabletID=1678
          4. partitionID=1665,tabletID=1684
          5. partitionID=1665,tabletID=1690
          6. partitionID=1665,tabletID=1696

PLAN FRAGMENT 6(F00)
  DOP: 16
  INSTANCES
    INSTANCE(16-F00#0)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10001
      SCAN RANGES
        BUCKET SEQUENCES: [16, 1, 4, 7, 10, 13]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1400
          2. partitionID=1397,tabletID=1406
          3. partitionID=1397,tabletID=1412
          4. partitionID=1397,tabletID=1418
          5. partitionID=1397,tabletID=1424
          6. partitionID=1397,tabletID=1430
    INSTANCE(17-F00#1)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10002
      SCAN RANGES
        BUCKET SEQUENCES: [17, 2, 5, 8, 11, 14]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1402
          2. partitionID=1397,tabletID=1408
          3. partitionID=1397,tabletID=1414
          4. partitionID=1397,tabletID=1420
          5. partitionID=1397,tabletID=1426
          6. partitionID=1397,tabletID=1432
    INSTANCE(18-F00#2)
      DESTINATIONS: 4-F08#0,5-F08#1,6-F08#2
      BE: 10003
      SCAN RANGES
        BUCKET SEQUENCES: [0, 3, 6, 9, 12, 15]
        1:OlapScanNode
          1. partitionID=1397,tabletID=1398
          2. partitionID=1397,tabletID=1404
          3. partitionID=1397,tabletID=1410
          4. partitionID=1397,tabletID=1416
          5. partitionID=1397,tabletID=1422
          6. partitionID=1397,tabletID=1428

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:73: L_PARTKEY | 75: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  24:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 73: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 24
    UNPARTITIONED

  23:AGGREGATE (merge finalize)
  |  output: sum(75: sum)
  |  group by: 73: L_PARTKEY
  |  
  22:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 22
    HASH_PARTITIONED: 73: L_PARTKEY

  21:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(74: sum)
  |  group by: 73: L_PARTKEY
  |  
  0:INTERSECT
  |  
  |----10:EXCHANGE
  |    
  |----15:EXCHANGE
  |    
  |----20:EXCHANGE
  |    
  5:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 20
    HASH_PARTITIONED: 56: L_PARTKEY, 72: sum

  19:Project
  |  <slot 56> : 56: L_PARTKEY
  |  <slot 72> : 72: sum
  |  
  18:AGGREGATE (update finalize)
  |  output: sum(59: L_QUANTITY)
  |  group by: 55: L_ORDERKEY, 56: L_PARTKEY
  |  
  17:Project
  |  <slot 55> : 55: L_ORDERKEY
  |  <slot 56> : 56: L_PARTKEY
  |  <slot 59> : 59: L_QUANTITY
  |  
  16:OlapScanNode
     TABLE: lineitem3
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem3
     tabletRatio=18/18
     tabletList=2203,2205,2207,2209,2211,2213,2215,2217,2219,2221 ...
     cardinality=1
     avgRowSize=4.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 15
    HASH_PARTITIONED: 38: L_PARTKEY, 54: sum

  14:Project
  |  <slot 38> : 38: L_PARTKEY
  |  <slot 54> : 54: sum
  |  
  13:AGGREGATE (update finalize)
  |  output: sum(41: L_QUANTITY)
  |  group by: 37: L_ORDERKEY, 38: L_PARTKEY
  |  
  12:Project
  |  <slot 37> : 37: L_ORDERKEY
  |  <slot 38> : 38: L_PARTKEY
  |  <slot 41> : 41: L_QUANTITY
  |  
  11:OlapScanNode
     TABLE: lineitem2
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem2
     tabletRatio=18/18
     tabletList=1935,1937,1939,1941,1943,1945,1947,1949,1951,1953 ...
     cardinality=1
     avgRowSize=4.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 10
    HASH_PARTITIONED: 20: L_PARTKEY, 36: sum

  9:Project
  |  <slot 20> : 20: L_PARTKEY
  |  <slot 36> : 36: sum
  |  
  8:AGGREGATE (update finalize)
  |  output: sum(23: L_QUANTITY)
  |  group by: 19: L_ORDERKEY, 20: L_PARTKEY
  |  
  7:Project
  |  <slot 19> : 19: L_ORDERKEY
  |  <slot 20> : 20: L_PARTKEY
  |  <slot 23> : 23: L_QUANTITY
  |  
  6:OlapScanNode
     TABLE: lineitem1
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem1
     tabletRatio=18/18
     tabletList=1666,1668,1670,1672,1674,1676,1678,1680,1682,1684 ...
     cardinality=1
     avgRowSize=4.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    HASH_PARTITIONED: 2: L_PARTKEY, 18: sum

  4:Project
  |  <slot 2> : 2: L_PARTKEY
  |  <slot 18> : 18: sum
  |  
  3:AGGREGATE (update finalize)
  |  output: sum(5: L_QUANTITY)
  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY
  |  
  2:Project
  |  <slot 1> : 1: L_ORDERKEY
  |  <slot 2> : 2: L_PARTKEY
  |  <slot 5> : 5: L_QUANTITY
  |  
  1:OlapScanNode
     TABLE: lineitem0
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem0
     tabletRatio=18/18
     tabletList=1398,1400,1402,1404,1406,1408,1410,1412,1414,1416 ...
     cardinality=1
     avgRowSize=4.0
[end]

