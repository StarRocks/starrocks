[sql]
with
    w1 as (select * from lineitem),
    w2 as (select count(1) as cnt, L_ORDERKEY, L_PARTKEY from lineitem group by L_ORDERKEY, L_PARTKEY)
select /*+SET_VAR(cbo_cte_reuse=true,cbo_cte_reuse_rate=0)*/ count(1) as cnt2, v1.L_ORDERKEY, v1.L_PARTKEY, v3.cnt
from
    w1 v1
    join w1 v2 on (v1.L_ORDERKEY = v2.L_ORDERKEY)
    join w2 v3 on (v1.L_PARTKEY = v3.L_PARTKEY)
    join w2 v4 on (v1.L_ORDERKEY = v4.cnt)
GROUP BY v1.L_ORDERKEY, v1.L_PARTKEY, v3.cnt
[scheduler]
PLAN FRAGMENT 0(F15)
  DOP: 16
  INSTANCES
    INSTANCE(0-F15#0)
      BE: 10001

PLAN FRAGMENT 1(F14)
  DOP: 16
  INSTANCES
    INSTANCE(1-F14#0)
      DESTINATIONS: 0-F15#0
      BE: 10001
    INSTANCE(2-F14#1)
      DESTINATIONS: 0-F15#0
      BE: 10003
    INSTANCE(3-F14#2)
      DESTINATIONS: 0-F15#0
      BE: 10002

PLAN FRAGMENT 2(F12)
  DOP: 16
  INSTANCES
    INSTANCE(4-F12#0)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10001
    INSTANCE(5-F12#1)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10002
    INSTANCE(6-F12#2)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10003

PLAN FRAGMENT 3(F10)
  DOP: 16
  INSTANCES
    INSTANCE(7-F10#0)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10001
    INSTANCE(8-F10#1)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10002
    INSTANCE(9-F10#2)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10003

PLAN FRAGMENT 4(F08)
  DOP: 16
  INSTANCES
    INSTANCE(10-F08#0)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10003
    INSTANCE(11-F08#1)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10002
    INSTANCE(12-F08#2)
      DESTINATIONS: 4-F12#0,5-F12#1,6-F12#2
      BE: 10001

PLAN FRAGMENT 5(F06)
  DOP: 16
  INSTANCES
    INSTANCE(13-F06#0)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10001
    INSTANCE(14-F06#1)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10002
    INSTANCE(15-F06#2)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10003

PLAN FRAGMENT 6(F04)
  DOP: 16
  INSTANCES
    INSTANCE(16-F04#0)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10001
    INSTANCE(17-F04#1)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10002
    INSTANCE(18-F04#2)
      DESTINATIONS: 10-F08#0,11-F08#1,12-F08#2
      BE: 10003

PLAN FRAGMENT 7(F02)
  DOP: 16
  INSTANCES
    INSTANCE(19-F02#0)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10001
    INSTANCE(20-F02#1)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10002
    INSTANCE(21-F02#2)
      DESTINATIONS: 1-F14#0,2-F14#1,3-F14#2
      BE: 10003

PLAN FRAGMENT 8(F01)
  DOP: 16
  INSTANCES
    INSTANCE(22-F01#0)
      DESTINATIONS: 19-F02#0,7-F10#0
      BE: 10001
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038
    INSTANCE(23-F01#1)
      DESTINATIONS: 20-F02#1,8-F10#1
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
    INSTANCE(24-F01#2)
      DESTINATIONS: 21-F02#2,9-F10#2
      BE: 10003
      SCAN RANGES
        1:OlapScanNode
          1. partitionID=1001,tabletID=1004
          2. partitionID=1001,tabletID=1010
          3. partitionID=1001,tabletID=1016
          4. partitionID=1001,tabletID=1022
          5. partitionID=1001,tabletID=1028
          6. partitionID=1001,tabletID=1034
          7. partitionID=1001,tabletID=1040

PLAN FRAGMENT 9(F00)
  DOP: 16
  INSTANCES
    INSTANCE(25-F00#0)
      DESTINATIONS: 16-F04#0,13-F06#0
      BE: 10001
      SCAN RANGES
        0:OlapScanNode
          1. partitionID=1001,tabletID=1008
          2. partitionID=1001,tabletID=1014
          3. partitionID=1001,tabletID=1020
          4. partitionID=1001,tabletID=1026
          5. partitionID=1001,tabletID=1032
          6. partitionID=1001,tabletID=1038
    INSTANCE(26-F00#1)
      DESTINATIONS: 17-F04#1,14-F06#1
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
    INSTANCE(27-F00#2)
      DESTINATIONS: 18-F04#2,15-F06#2
      BE: 10003
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
 OUTPUT EXPRS:106: count | 36: L_ORDERKEY | 37: L_PARTKEY | 87: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  29:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 71: L_PARTKEY

  STREAM DATA SINK
    EXCHANGE ID: 29
    UNPARTITIONED

  28:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 36: L_ORDERKEY, 37: L_PARTKEY, 87: count
  |  
  27:Project
  |  <slot 36> : 36: L_ORDERKEY
  |  <slot 37> : 37: L_PARTKEY
  |  <slot 87> : 87: count
  |  
  26:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 71: L_PARTKEY = 37: L_PARTKEY
  |  
  |----25:EXCHANGE
  |    
  7:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 107: cast

  STREAM DATA SINK
    EXCHANGE ID: 25
    HASH_PARTITIONED: 37: L_PARTKEY

  24:Project
  |  <slot 36> : 36: L_ORDERKEY
  |  <slot 37> : 37: L_PARTKEY
  |  
  23:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 107: cast = 105: count
  |  
  |----22:EXCHANGE
  |    
  18:EXCHANGE

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 22
    HASH_PARTITIONED: 105: count

  21:SELECT
  |  predicates: 105: count IS NOT NULL
  |  
  20:Project
  |  <slot 105> : 35: count
  |  
  19:EXCHANGE

PLAN FRAGMENT 4
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 36: L_ORDERKEY

  STREAM DATA SINK
    EXCHANGE ID: 18
    HASH_PARTITIONED: 107: cast

  17:Project
  |  <slot 36> : 36: L_ORDERKEY
  |  <slot 37> : 37: L_PARTKEY
  |  <slot 107> : CAST(36: L_ORDERKEY AS BIGINT)
  |  
  16:HASH JOIN
  |  join op: INNER JOIN (PARTITIONED)
  |  colocate: false, reason: 
  |  equal join conjunct: 36: L_ORDERKEY = 53: L_ORDERKEY
  |  
  |----15:EXCHANGE
  |    
  11:EXCHANGE

PLAN FRAGMENT 5
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 15
    HASH_PARTITIONED: 53: L_ORDERKEY

  14:SELECT
  |  predicates: CAST(53: L_ORDERKEY AS BIGINT) IS NOT NULL
  |  
  13:Project
  |  <slot 53> : 1: L_ORDERKEY
  |  
  12:EXCHANGE

PLAN FRAGMENT 6
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 11
    HASH_PARTITIONED: 36: L_ORDERKEY

  10:SELECT
  |  predicates: CAST(36: L_ORDERKEY AS BIGINT) IS NOT NULL
  |  
  9:Project
  |  <slot 36> : 1: L_ORDERKEY
  |  <slot 37> : 2: L_PARTKEY
  |  
  8:EXCHANGE

PLAN FRAGMENT 7
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 07
    HASH_PARTITIONED: 71: L_PARTKEY

  6:SELECT
  |  predicates: 71: L_PARTKEY IS NOT NULL
  |  
  5:Project
  |  <slot 71> : 19: L_PARTKEY
  |  <slot 87> : 35: count
  |  
  4:EXCHANGE

PLAN FRAGMENT 8
 OUTPUT EXPRS:19: L_PARTKEY | 35: count
  PARTITION: RANDOM

  MultiCastDataSinks
  STREAM DATA SINK
    EXCHANGE ID: 04
    RANDOM
  STREAM DATA SINK
    EXCHANGE ID: 19
    RANDOM

  3:Project
  |  <slot 19> : 19: L_PARTKEY
  |  <slot 35> : 35: count
  |  
  2:AGGREGATE (update finalize)
  |  output: count(1)
  |  group by: 18: L_ORDERKEY, 19: L_PARTKEY
  |  having: (35: count IS NOT NULL) OR (19: L_PARTKEY IS NOT NULL)
  |  
  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=16.0

PLAN FRAGMENT 9
 OUTPUT EXPRS:1: L_ORDERKEY | 2: L_PARTKEY
  PARTITION: RANDOM

  MultiCastDataSinks
  STREAM DATA SINK
    EXCHANGE ID: 08
    RANDOM
  STREAM DATA SINK
    EXCHANGE ID: 12
    RANDOM

  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: CAST(1: L_ORDERKEY AS BIGINT) IS NOT NULL
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=16.0
[end]

