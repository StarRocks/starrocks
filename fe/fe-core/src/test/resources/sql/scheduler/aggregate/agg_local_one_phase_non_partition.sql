[sql]
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 16
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
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
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
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

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=3,enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 3
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 3
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
            2. partitionID=1001,tabletID=1024
            3. partitionID=1001,tabletID=1042
          DriverSequence#1
            4. partitionID=1001,tabletID=1012
            5. partitionID=1001,tabletID=1030
          DriverSequence#2
            6. partitionID=1001,tabletID=1018
            7. partitionID=1001,tabletID=1036
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
            2. partitionID=1001,tabletID=1026
          DriverSequence#1
            3. partitionID=1001,tabletID=1014
            4. partitionID=1001,tabletID=1032
          DriverSequence#2
            5. partitionID=1001,tabletID=1020
            6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
            2. partitionID=1001,tabletID=1022
            3. partitionID=1001,tabletID=1040
          DriverSequence#1
            4. partitionID=1001,tabletID=1010
            5. partitionID=1001,tabletID=1028
          DriverSequence#2
            6. partitionID=1001,tabletID=1016
            7. partitionID=1001,tabletID=1034

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=2, enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 2
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10003

PLAN FRAGMENT 1(F00)
  DOP: 2
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 2
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
            2. partitionID=1001,tabletID=1018
            3. partitionID=1001,tabletID=1030
            4. partitionID=1001,tabletID=1042
          DriverSequence#1
            5. partitionID=1001,tabletID=1012
            6. partitionID=1001,tabletID=1024
            7. partitionID=1001,tabletID=1036
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 2
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
            2. partitionID=1001,tabletID=1020
            3. partitionID=1001,tabletID=1032
          DriverSequence#1
            4. partitionID=1001,tabletID=1014
            5. partitionID=1001,tabletID=1026
            6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 2
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
            2. partitionID=1001,tabletID=1016
            3. partitionID=1001,tabletID=1028
            4. partitionID=1001,tabletID=1040
          DriverSequence#1
            5. partitionID=1001,tabletID=1010
            6. partitionID=1001,tabletID=1022
            7. partitionID=1001,tabletID=1034

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=1, enable_tablet_internal_parallel=true)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 1
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 1
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
            2. partitionID=1001,tabletID=1012
            3. partitionID=1001,tabletID=1018
            4. partitionID=1001,tabletID=1024
            5. partitionID=1001,tabletID=1030
            6. partitionID=1001,tabletID=1036
            7. partitionID=1001,tabletID=1042
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
            2. partitionID=1001,tabletID=1014
            3. partitionID=1001,tabletID=1020
            4. partitionID=1001,tabletID=1026
            5. partitionID=1001,tabletID=1032
            6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
            2. partitionID=1001,tabletID=1010
            3. partitionID=1001,tabletID=1016
            4. partitionID=1001,tabletID=1022
            5. partitionID=1001,tabletID=1028
            6. partitionID=1001,tabletID=1034
            7. partitionID=1001,tabletID=1040

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=16,enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 16
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10002

PLAN FRAGMENT 1(F00)
  DOP: 16
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
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
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
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
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
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

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=3,enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 3
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10003

PLAN FRAGMENT 1(F00)
  DOP: 3
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
            2. partitionID=1001,tabletID=1024
            3. partitionID=1001,tabletID=1042
          DriverSequence#1
            4. partitionID=1001,tabletID=1012
            5. partitionID=1001,tabletID=1030
          DriverSequence#2
            6. partitionID=1001,tabletID=1018
            7. partitionID=1001,tabletID=1036
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
            2. partitionID=1001,tabletID=1026
          DriverSequence#1
            3. partitionID=1001,tabletID=1014
            4. partitionID=1001,tabletID=1032
          DriverSequence#2
            5. partitionID=1001,tabletID=1020
            6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 3
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
            2. partitionID=1001,tabletID=1022
            3. partitionID=1001,tabletID=1040
          DriverSequence#1
            4. partitionID=1001,tabletID=1010
            5. partitionID=1001,tabletID=1028
          DriverSequence#2
            6. partitionID=1001,tabletID=1016
            7. partitionID=1001,tabletID=1034

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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
SELECT /*+SET_VAR(pipeline_dop=1, enable_tablet_internal_parallel=false)*/ l_orderkey, count(1) FROM lineitem GROUP BY l_orderkey
[scheduler]
PLAN FRAGMENT 0(F01)
  DOP: 1
  INSTANCES
    INSTANCE(0-F01#0)
      BE: 10001

PLAN FRAGMENT 1(F00)
  DOP: 1
  INSTANCES
    INSTANCE(1-F00#0)
      DESTINATIONS
        0-F01#0
      BE: 10002
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1006
            2. partitionID=1001,tabletID=1012
            3. partitionID=1001,tabletID=1018
            4. partitionID=1001,tabletID=1024
            5. partitionID=1001,tabletID=1030
            6. partitionID=1001,tabletID=1036
            7. partitionID=1001,tabletID=1042
    INSTANCE(2-F00#1)
      DESTINATIONS
        0-F01#0
      BE: 10003
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1008
            2. partitionID=1001,tabletID=1014
            3. partitionID=1001,tabletID=1020
            4. partitionID=1001,tabletID=1026
            5. partitionID=1001,tabletID=1032
            6. partitionID=1001,tabletID=1038
    INSTANCE(3-F00#2)
      DESTINATIONS
        0-F01#0
      BE: 10001
      DOP: 1
      SCAN RANGES (per driver sequence)
        0:OlapScanNode
          DriverSequence#0
            1. partitionID=1001,tabletID=1004
            2. partitionID=1001,tabletID=1010
            3. partitionID=1001,tabletID=1016
            4. partitionID=1001,tabletID=1022
            5. partitionID=1001,tabletID=1028
            6. partitionID=1001,tabletID=1034
            7. partitionID=1001,tabletID=1040

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: L_ORDERKEY | 18: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  2:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 02
    UNPARTITIONED

  1:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 1: L_ORDERKEY
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

