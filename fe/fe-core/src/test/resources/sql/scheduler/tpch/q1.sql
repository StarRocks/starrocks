[scheduler]
PLAN FRAGMENT 0(F02)
  DOP: 16
  INSTANCES
    INSTANCE(0-F02#0)
      BE: 10001

PLAN FRAGMENT 1(F01)
  DOP: 16
  INSTANCES
    INSTANCE(1-F01#0)
      DESTINATIONS: 0-F02#0
      BE: 10003
    INSTANCE(2-F01#1)
      DESTINATIONS: 0-F02#0
      BE: 10002
    INSTANCE(3-F01#2)
      DESTINATIONS: 0-F02#0
      BE: 10001

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
      BE: 10001
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
      BE: 10002
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1006
          2. partitionID=1001,tabletID=1012
          3. partitionID=1001,tabletID=1018
          4. partitionID=1001,tabletID=1024
          5. partitionID=1001,tabletID=1030
          6. partitionID=1001,tabletID=1036
          7. partitionID=1001,tabletID=1042
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
      BE: 10003
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:9: L_RETURNFLAG | 10: L_LINESTATUS | 20: sum | 21: sum | 22: sum | 23: sum | 24: avg | 25: avg | 26: avg | 27: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  6:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS

  STREAM DATA SINK
    EXCHANGE ID: 06
    UNPARTITIONED

  5:SORT
  |  order by: <slot 9> 9: L_RETURNFLAG ASC, <slot 10> 10: L_LINESTATUS ASC
  |  offset: 0
  |  
  4:AGGREGATE (merge finalize)
  |  output: sum(20: sum), sum(21: sum), sum(22: sum), sum(23: sum), avg(24: avg), avg(25: avg), avg(26: avg), count(27: count)
  |  group by: 9: L_RETURNFLAG, 10: L_LINESTATUS
  |  
  3:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS

  2:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(5: L_QUANTITY), sum(6: L_EXTENDEDPRICE), sum(18: expr), sum(19: expr), avg(5: L_QUANTITY), avg(6: L_EXTENDEDPRICE), avg(7: L_DISCOUNT), count(*)
  |  group by: 9: L_RETURNFLAG, 10: L_LINESTATUS
  |  
  1:Project
  |  <slot 5> : 5: L_QUANTITY
  |  <slot 6> : 6: L_EXTENDEDPRICE
  |  <slot 7> : 7: L_DISCOUNT
  |  <slot 9> : 9: L_RETURNFLAG
  |  <slot 10> : 10: L_LINESTATUS
  |  <slot 18> : 29: multiply
  |  <slot 19> : 29: multiply * 1.0 + 8: L_TAX
  |  common expressions:
  |  <slot 28> : 1.0 - 7: L_DISCOUNT
  |  <slot 29> : 6: L_EXTENDEDPRICE * 28: subtract
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 11: L_SHIPDATE <= '1998-12-01'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=54.0
[end]

