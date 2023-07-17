[sql]
select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= date '1994-01-01'
  and l_shipdate < date '1994-01-01' + interval '1' year
  and l_discount between .06 - 0.01 and .06 + 0.01
  and l_quantity < 24;
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
    INSTANCE(2-F00#1)
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
    INSTANCE(3-F00#2)
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

[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:19: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  4:AGGREGATE (merge finalize)
  |  output: sum(19: sum)
  |  group by: 
  |  
  3:EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 03
    UNPARTITIONED

  2:AGGREGATE (update serialize)
  |  output: sum(6: L_EXTENDEDPRICE * 7: L_DISCOUNT)
  |  group by: 
  |  
  1:Project
  |  <slot 6> : 6: L_EXTENDEDPRICE
  |  <slot 7> : 7: L_DISCOUNT
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 11: L_SHIPDATE >= '1994-01-01', 11: L_SHIPDATE <= '1994-12-31', 7: L_DISCOUNT >= 0.05, 7: L_DISCOUNT <= 0.07, 5: L_QUANTITY < 24.0
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=1004,1006,1008,1010,1012,1014,1016,1018,1020,1022 ...
     cardinality=1
     avgRowSize=36.0
[end]

