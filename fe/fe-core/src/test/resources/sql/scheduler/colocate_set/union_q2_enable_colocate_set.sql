
[scheduler]
PLAN FRAGMENT 0(F03)
  DOP: 16
  INSTANCES
    INSTANCE(0-F03#0)
      BE: 10001

PLAN FRAGMENT 1(F02)
  DOP: 16
  INSTANCES
    INSTANCE(1-F02#0)
      DESTINATIONS: 0-F03#0
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
    INSTANCE(2-F02#1)
      DESTINATIONS: 0-F03#0
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
    INSTANCE(3-F02#2)
      DESTINATIONS: 0-F03#0
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

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:35: L_ORDERKEY | 36: L_PARTKEY | 37: L_SUPPKEY | 38: L_LINENUMBER | 39: L_QUANTITY | 40: L_EXTENDEDPRICE | 41: L_DISCOUNT | 42: L_TAX | 43: L_RETURNFLAG | 44: L_LINESTATUS | 45: L_SHIPDATE | 46: L_COMMITDATE | 47: L_RECEIPTDATE | 48: L_SHIPINSTRUCT | 49: L_SHIPMODE | 50: L_COMMENT | 51: PAD
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:AGGREGATE (update finalize)
  |  group by: 35: L_ORDERKEY, 36: L_PARTKEY, 37: L_SUPPKEY, 38: L_LINENUMBER, 39: L_QUANTITY, 40: L_EXTENDEDPRICE, 41: L_DISCOUNT, 42: L_TAX, 43: L_RETURNFLAG, 44: L_LINESTATUS, 45: L_SHIPDATE, 46: L_COMMITDATE, 47: L_RECEIPTDATE, 48: L_SHIPINSTRUCT, 49: L_SHIPMODE, 50: L_COMMENT, 51: PAD
  |  
  0:UNION
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

