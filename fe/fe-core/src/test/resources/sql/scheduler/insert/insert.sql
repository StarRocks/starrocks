[sql]
insert into lineitem
select * from lineitem
    [scheduler]
PLAN FRAGMENT 0(F00)
  DOP: 6
  INSTANCES
    INSTANCE(0-F00#0)
      BE: 10001
      DOP: 7
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
          DriverSequence#1
            2. partitionID=1001,tabletID=1010
          DriverSequence#2
            3. partitionID=1001,tabletID=1016
          DriverSequence#3
            4. partitionID=1001,tabletID=1022
          DriverSequence#4
            5. partitionID=1001,tabletID=1028
          DriverSequence#5
            6. partitionID=1001,tabletID=1034
          DriverSequence#6
            7. partitionID=1001,tabletID=1040
    INSTANCE(1-F00#1)
      BE: 10002
      DOP: 7
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
          DriverSequence#1
            2. partitionID=1001,tabletID=1012
          DriverSequence#2
            3. partitionID=1001,tabletID=1018
          DriverSequence#3
            4. partitionID=1001,tabletID=1024
          DriverSequence#4
            5. partitionID=1001,tabletID=1030
          DriverSequence#5
            6. partitionID=1001,tabletID=1036
          DriverSequence#6
            7. partitionID=1001,tabletID=1042
    INSTANCE(2-F00#2)
      BE: 10003
      DOP: 6
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
          DriverSequence#1
            2. partitionID=1001,tabletID=1014
          DriverSequence#2
            3. partitionID=1001,tabletID=1020
          DriverSequence#3
            4. partitionID=1001,tabletID=1026
          DriverSequence#4
            5. partitionID=1001,tabletID=1032
          DriverSequence#5
            6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 2: L_PARTKEY | 3: L_SUPPKEY | 4: L_LINENUMBER | 5: L_QUANTITY | 6: L_EXTENDEDPRICE | 7: L_DISCOUNT | 8: L_TAX | 9: L_RETURNFLAG | 10: L_LINESTATUS | 11: L_SHIPDATE | 12: L_COMMITDATE | 13: L_RECEIPTDATE | 14: L_SHIPINSTRUCT | 15: L_SHIPMODE | 16: L_COMMENT | 17: PAD
  PARTITION: RANDOM

  OLAP TABLE SINK
    TABLE: lineitem
    TUPLE ID: 1
    RANDOM

  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
    partitions=1/1
    rollup: lineitem
    tabletRatio=20/20
    tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
    cardinality=1
    avgRowSize=150.0
    [end]

[sql]
insert into nation
select * from nation
UNION ALL
select r_regionkey, r_name, r_regionkey, r_comment  from region
    [scheduler]
PLAN FRAGMENT 0(F00)
  DOP: 16
  INSTANCES
    INSTANCE(0-F00#0)
      BE: 10003

PLAN FRAGMENT 1(F02)
  DOP: 16
  INSTANCES
    INSTANCE(1-F02#0)
      DESTINATIONS
        0-F00#0
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=1479,tabletID=1482

PLAN FRAGMENT 2(F01)
  DOP: 16
  INSTANCES
    INSTANCE(2-F01#0)
      DESTINATIONS
        0-F00#0
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1357,tabletID=1360

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:8: n_nationkey | 9: n_name | 10: n_regionkey | 11: n_comment
  PARTITION: RANDOM

  OLAP TABLE SINK
    TABLE: nation
    TUPLE ID: 3
    RANDOM

  0:UNION
|
|----4:EXCHANGE
|
2:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
RANDOM

3:OlapScanNode
TABLE: region
PREAGGREGATION: ON
    partitions=1/1
    rollup: region
    tabletRatio=1/1
    tabletList=1482
    cardinality=1
    avgRowSize=181.0

    PLAN FRAGMENT 2
    OUTPUT EXPRS:
    PARTITION: RANDOM

    STREAM DATA SINK
    EXCHANGE ID: 02
    RANDOM

    1:OlapScanNode
    TABLE: nation
    PREAGGREGATION: ON
    partitions=1/1
    rollup: nation
    tabletRatio=1/1
    tabletList=1360
    cardinality=1
    avgRowSize=33.0
    [end]

