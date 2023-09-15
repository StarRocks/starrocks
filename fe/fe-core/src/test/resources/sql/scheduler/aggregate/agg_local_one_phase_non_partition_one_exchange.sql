[sql]
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=false,parallel_exchange_instance_num=1)*/ L_PARTKEY, count(1) FROM lineitem GROUP BY L_PARTKEY
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

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(2-F00#0)
      DESTINATIONS: 1-F01#0
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
    INSTANCE(3-F00#1)
      DESTINATIONS: 1-F01#0
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
    INSTANCE(4-F00#2)
      DESTINATIONS: 1-F01#0
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
 OUTPUT EXPRS:2: L_PARTKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 2: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:AGGREGATE (merge finalize)
  |  output: count(18: count)
  |  group by: 2: L_PARTKEY
  |  
  2:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    HASH_PARTITIONED: 2: L_PARTKEY

  1:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(1)
  |  group by: 2: L_PARTKEY
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=8.0
[end]

[sql]
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=false,parallel_exchange_instance_num=10000)*/ L_PARTKEY, count(1) FROM lineitem GROUP BY L_PARTKEY
[scheduler]
PLAN FRAGMENT 0(F02)
  DOP: 16
  INSTANCES
    INSTANCE(0-F02#0)
      BE: 10002

PLAN FRAGMENT 1(F01)
  DOP: 16
  INSTANCES
    INSTANCE(1-F01#0)
      DESTINATIONS: 0-F02#0
      BE: 10001
    INSTANCE(2-F01#1)
      DESTINATIONS: 0-F02#0
      BE: 10002
    INSTANCE(3-F01#2)
      DESTINATIONS: 0-F02#0
      BE: 10003

PLAN FRAGMENT 2(F00)
  DOP: 16
  INSTANCES
    INSTANCE(4-F00#0)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
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
    INSTANCE(5-F00#1)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
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
    INSTANCE(6-F00#2)
      DESTINATIONS: 1-F01#0,2-F01#1,3-F01#2
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
 OUTPUT EXPRS:2: L_PARTKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 2: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 04
    UNPARTITIONED

  3:AGGREGATE (merge finalize)
  |  output: count(18: count)
  |  group by: 2: L_PARTKEY
  |  
  2:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    HASH_PARTITIONED: 2: L_PARTKEY

  1:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(1)
  |  group by: 2: L_PARTKEY
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=8.0
[end]

