
[scheduler]
PLAN FRAGMENT 0(F07)
  DOP: 16
  INSTANCES
    INSTANCE(0-F07#0)
      BE: 10001

PLAN FRAGMENT 1(F06)
  DOP: 16
  INSTANCES
    INSTANCE(1-F06#0)
      DESTINATIONS: 0-F07#0
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
        2:OlapScanNode
          1. partitionID=1665,tabletID=1668
          2. partitionID=1665,tabletID=1674
          3. partitionID=1665,tabletID=1680
          4. partitionID=1665,tabletID=1686
          5. partitionID=1665,tabletID=1692
          6. partitionID=1665,tabletID=1698
    INSTANCE(2-F06#1)
      DESTINATIONS: 0-F07#0
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
        2:OlapScanNode
          1. partitionID=1665,tabletID=1670
          2. partitionID=1665,tabletID=1676
          3. partitionID=1665,tabletID=1682
          4. partitionID=1665,tabletID=1688
          5. partitionID=1665,tabletID=1694
          6. partitionID=1665,tabletID=1700
    INSTANCE(3-F06#2)
      DESTINATIONS: 0-F07#0
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
        2:OlapScanNode
          1. partitionID=1665,tabletID=1666
          2. partitionID=1665,tabletID=1672
          3. partitionID=1665,tabletID=1678
          4. partitionID=1665,tabletID=1684
          5. partitionID=1665,tabletID=1690
          6. partitionID=1665,tabletID=1696

PLAN FRAGMENT 2(F04)
  DOP: 16
  INSTANCES
    INSTANCE(4-F04#0)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10001
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2202,tabletID=2205
          2. partitionID=2202,tabletID=2211
          3. partitionID=2202,tabletID=2217
          4. partitionID=2202,tabletID=2223
          5. partitionID=2202,tabletID=2229
          6. partitionID=2202,tabletID=2235
    INSTANCE(5-F04#1)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10002
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2202,tabletID=2207
          2. partitionID=2202,tabletID=2213
          3. partitionID=2202,tabletID=2219
          4. partitionID=2202,tabletID=2225
          5. partitionID=2202,tabletID=2231
          6. partitionID=2202,tabletID=2237
    INSTANCE(6-F04#2)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10003
      SCAN RANGES
        5:OlapScanNode
          1. partitionID=2202,tabletID=2203
          2. partitionID=2202,tabletID=2209
          3. partitionID=2202,tabletID=2215
          4. partitionID=2202,tabletID=2221
          5. partitionID=2202,tabletID=2227
          6. partitionID=2202,tabletID=2233

PLAN FRAGMENT 3(F02)
  DOP: 16
  INSTANCES
    INSTANCE(7-F02#0)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1934,tabletID=1937
          2. partitionID=1934,tabletID=1943
          3. partitionID=1934,tabletID=1949
          4. partitionID=1934,tabletID=1955
          5. partitionID=1934,tabletID=1961
          6. partitionID=1934,tabletID=1967
    INSTANCE(8-F02#1)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10002
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1934,tabletID=1939
          2. partitionID=1934,tabletID=1945
          3. partitionID=1934,tabletID=1951
          4. partitionID=1934,tabletID=1957
          5. partitionID=1934,tabletID=1963
          6. partitionID=1934,tabletID=1969
    INSTANCE(9-F02#2)
      DESTINATIONS: 3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1,3-F06#2,1-F06#0,2-F06#1
      BE: 10003
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1934,tabletID=1935
          2. partitionID=1934,tabletID=1941
          3. partitionID=1934,tabletID=1947
          4. partitionID=1934,tabletID=1953
          5. partitionID=1934,tabletID=1959
          6. partitionID=1934,tabletID=1965

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:69: L_ORDERKEY | 70: L_PARTKEY | 86: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  9:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 09
    UNPARTITIONED

  8:AGGREGATE (update finalize)
  |  output: sum(73: L_QUANTITY)
  |  group by: 69: L_ORDERKEY, 70: L_PARTKEY
  |  
  7:Project
  |  <slot 69> : 69: L_ORDERKEY
  |  <slot 70> : 70: L_PARTKEY
  |  <slot 73> : 73: L_QUANTITY
  |  
  0:EXCEPT
  |  
  |----2:OlapScanNode
  |       TABLE: lineitem1
  |       PREAGGREGATION: ON
  |       partitions=1/7
  |       rollup: lineitem1
  |       tabletRatio=18/18
  |       tabletList=1666,1668,1670,1672,1674,1676,1678,1680,1682,1684 ...
  |       cardinality=1
  |       avgRowSize=17.0
  |    
  |----4:EXCHANGE
  |    
  |----6:EXCHANGE
  |    
  1:OlapScanNode
     TABLE: lineitem0
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem0
     tabletRatio=18/18
     tabletList=1398,1400,1402,1404,1406,1408,1410,1412,1414,1416 ...
     cardinality=1
     avgRowSize=17.0

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    BUCKET_SHUFFLE_HASH_PARTITIONED: 52: L_ORDERKEY

  5:OlapScanNode
     TABLE: lineitem3
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem3
     tabletRatio=18/18
     tabletList=2203,2205,2207,2209,2211,2213,2215,2217,2219,2221 ...
     cardinality=1
     avgRowSize=17.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    BUCKET_SHUFFLE_HASH_PARTITIONED: 35: L_ORDERKEY

  3:OlapScanNode
     TABLE: lineitem2
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem2
     tabletRatio=18/18
     tabletList=1935,1937,1939,1941,1943,1945,1947,1949,1951,1953 ...
     cardinality=1
     avgRowSize=17.0
[end]

