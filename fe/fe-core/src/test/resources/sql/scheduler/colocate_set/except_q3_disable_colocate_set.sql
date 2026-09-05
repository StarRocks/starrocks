
[scheduler]
PLAN FRAGMENT 0(F10)
  DOP: 16
  INSTANCES
    INSTANCE(0-F10#0)
      BE: 10002

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
        7:OlapScanNode
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
        7:OlapScanNode
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
        7:OlapScanNode
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
        5:OlapScanNode
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
        5:OlapScanNode
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
        5:OlapScanNode
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
        3:OlapScanNode
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
        3:OlapScanNode
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
        3:OlapScanNode
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
        1:OlapScanNode
          1. partitionID=1397,tabletID=1398
          2. partitionID=1397,tabletID=1404
          3. partitionID=1397,tabletID=1410
          4. partitionID=1397,tabletID=1416
          5. partitionID=1397,tabletID=1422
          6. partitionID=1397,tabletID=1428

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:69: L_ORDERKEY | 70: L_PARTKEY | 86: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  13:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 69: L_ORDERKEY, 70: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 13
    UNPARTITIONED

  12:AGGREGATE (merge finalize)
  |  output: sum(86: sum)
  |  group by: 69: L_ORDERKEY, 70: L_PARTKEY
  |  
  11:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 11
    HASH_PARTITIONED: 69: L_ORDERKEY, 70: L_PARTKEY

  10:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(73: L_QUANTITY)
  |  group by: 69: L_ORDERKEY, 70: L_PARTKEY
  |  
  9:Project
  |  <slot 69> : 69: L_ORDERKEY
  |  <slot 70> : 70: L_PARTKEY
  |  <slot 73> : 73: L_QUANTITY
  |  
  0:EXCEPT
  |  
  |----4:EXCHANGE
  |    
  |----6:EXCHANGE
  |    
  |----8:EXCHANGE
  |    
  2:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    HASH_PARTITIONED: 52: L_ORDERKEY, 53: L_PARTKEY, 54: L_SUPPKEY, 55: L_LINENUMBER, 56: L_QUANTITY, 57: L_EXTENDEDPRICE, 58: L_DISCOUNT, 59: L_TAX, 60: L_RETURNFLAG, 61: L_LINESTATUS, 62: L_SHIPDATE, 63: L_COMMITDATE, 64: L_RECEIPTDATE, 65: L_SHIPINSTRUCT, 66: L_SHIPMODE, 67: L_COMMENT, 68: PAD

  7:OlapScanNode
     TABLE: lineitem3
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem3
     tabletRatio=18/18
     tabletList=2203,2205,2207,2209,2211,2213,2215,2217,2219,2221 ...
     cardinality=1
     avgRowSize=17.0

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 06
    HASH_PARTITIONED: 35: L_ORDERKEY, 36: L_PARTKEY, 37: L_SUPPKEY, 38: L_LINENUMBER, 39: L_QUANTITY, 40: L_EXTENDEDPRICE, 41: L_DISCOUNT, 42: L_TAX, 43: L_RETURNFLAG, 44: L_LINESTATUS, 45: L_SHIPDATE, 46: L_COMMITDATE, 47: L_RECEIPTDATE, 48: L_SHIPINSTRUCT, 49: L_SHIPMODE, 50: L_COMMENT, 51: PAD

  5:OlapScanNode
     TABLE: lineitem2
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem2
     tabletRatio=18/18
     tabletList=1935,1937,1939,1941,1943,1945,1947,1949,1951,1953 ...
     cardinality=1
     avgRowSize=17.0

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    HASH_PARTITIONED: 18: L_ORDERKEY, 19: L_PARTKEY, 20: L_SUPPKEY, 21: L_LINENUMBER, 22: L_QUANTITY, 23: L_EXTENDEDPRICE, 24: L_DISCOUNT, 25: L_TAX, 26: L_RETURNFLAG, 27: L_LINESTATUS, 28: L_SHIPDATE, 29: L_COMMITDATE, 30: L_RECEIPTDATE, 31: L_SHIPINSTRUCT, 32: L_SHIPMODE, 33: L_COMMENT, 34: PAD

  3:OlapScanNode
     TABLE: lineitem1
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem1
     tabletRatio=18/18
     tabletList=1666,1668,1670,1672,1674,1676,1678,1680,1682,1684 ...
     cardinality=1
     avgRowSize=17.0

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    HASH_PARTITIONED: 1: L_ORDERKEY, 2: L_PARTKEY, 3: L_SUPPKEY, 4: L_LINENUMBER, 5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 8: L_TAX, 9: L_RETURNFLAG, 10: L_LINESTATUS, 11: L_SHIPDATE, 12: L_COMMITDATE, 13: L_RECEIPTDATE, 14: L_SHIPINSTRUCT, 15: L_SHIPMODE, 16: L_COMMENT, 17: PAD

  1:OlapScanNode
     TABLE: lineitem0
     PREAGGREGATION: ON
     partitions=1/7
     rollup: lineitem0
     tabletRatio=18/18
     tabletList=1398,1400,1402,1404,1406,1408,1410,1412,1414,1416 ...
     cardinality=1
     avgRowSize=17.0
[end]

