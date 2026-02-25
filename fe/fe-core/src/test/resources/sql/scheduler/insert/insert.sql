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
            1. partitionID=1004,tabletID=1005
          DriverSequence#1
            2. partitionID=1004,tabletID=1011
          DriverSequence#2
            3. partitionID=1004,tabletID=1017
          DriverSequence#3
            4. partitionID=1004,tabletID=1023
          DriverSequence#4
            5. partitionID=1004,tabletID=1029
          DriverSequence#5
            6. partitionID=1004,tabletID=1035
          DriverSequence#6
            7. partitionID=1004,tabletID=1041
    INSTANCE(1-F00#1)
      BE: 10002
      DOP: 7
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1004,tabletID=1007
          DriverSequence#1
            2. partitionID=1004,tabletID=1013
          DriverSequence#2
            3. partitionID=1004,tabletID=1019
          DriverSequence#3
            4. partitionID=1004,tabletID=1025
          DriverSequence#4
            5. partitionID=1004,tabletID=1031
          DriverSequence#5
            6. partitionID=1004,tabletID=1037
          DriverSequence#6
            7. partitionID=1004,tabletID=1043
    INSTANCE(2-F00#2)
      BE: 10003
      DOP: 6
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1004,tabletID=1009
          DriverSequence#1
            2. partitionID=1004,tabletID=1015
          DriverSequence#2
            3. partitionID=1004,tabletID=1021
          DriverSequence#3
            4. partitionID=1004,tabletID=1027
          DriverSequence#4
            5. partitionID=1004,tabletID=1033
          DriverSequence#5
            6. partitionID=1004,tabletID=1039

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
     tabletList=1005,1007,1009,1011,1013,1015,1017,1019,1021,1023 ...
     cardinality=1
     avgRowSize=150.0
[end]

[sql]
insert into nation
select * from nation
UNION ALL
select r_regionkey, r_name, r_regionkey, r_comment  from region
[scheduler]
PLAN FRAGMENT 0(F04)
  DOP: 16
  INSTANCES
    INSTANCE(0-F04#0)
      BE: 10003

PLAN FRAGMENT 1(F02)
  DOP: 16
  INSTANCES
    INSTANCE(1-F02#0)
      DESTINATIONS: 0-F04#0
      BE: 10001
      SCAN RANGES
        3:OlapScanNode
          1. partitionID=2568,tabletID=2569

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(2-F00#0)
      DESTINATIONS: 0-F04#0
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=2442,tabletID=2443

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
     tabletList=2569
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
     tabletList=2443
     cardinality=1
     avgRowSize=33.0
[end]

