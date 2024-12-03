[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:25: L_SHIPMODE | 30: sum | 31: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  11:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 25: L_SHIPMODE

  STREAM DATA SINK
    EXCHANGE ID: 11
    UNPARTITIONED

  10:SORT
  |  order by: <slot 25> 25: L_SHIPMODE ASC
  |  offset: 0
  |  
  9:AGGREGATE (merge finalize)
  |  output: sum(30: sum), sum(31: sum)
  |  group by: 25: L_SHIPMODE
  |  
  8:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 08
    HASH_PARTITIONED: 25: L_SHIPMODE

  7:AGGREGATE (update serialize)
  |  STREAMING
  |  output: sum(28: case), sum(29: case)
  |  group by: 25: L_SHIPMODE
  |  
  6:Project
  |  <slot 25> : 25: L_SHIPMODE
  |  <slot 28> : if((6: O_ORDERPRIORITY = '1-URGENT') OR (6: O_ORDERPRIORITY = '2-HIGH'), 1, 0)
  |  <slot 29> : if((6: O_ORDERPRIORITY != '1-URGENT') AND (6: O_ORDERPRIORITY != '2-HIGH'), 1, 0)
  |  
  5:HASH JOIN
  |  join op: INNER JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 1: O_ORDERKEY = 11: L_ORDERKEY
  |  
  |----4:EXCHANGE
  |    
  0:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     partitions=1/1
     rollup: orders
     tabletRatio=10/10
     tabletList=10220,10222,10224,10226,10228,10230,10232,10234,10236,10238
     cardinality=150000000
     avgRowSize=23.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 04
    BUCKET_SHUFFLE_HASH_PARTITIONED: 11: L_ORDERKEY

  3:SELECT
  |  predicates: 22: L_COMMITDATE < 23: L_RECEIPTDATE, 21: L_SHIPDATE < 22: L_COMMITDATE
  |  
  2:Project
  |  <slot 11> : 11: L_ORDERKEY
  |  <slot 21> : 21: L_SHIPDATE
  |  <slot 22> : 22: L_COMMITDATE
  |  <slot 23> : 23: L_RECEIPTDATE
  |  <slot 25> : 25: L_SHIPMODE
  |  
  1:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     PREDICATES: 25: L_SHIPMODE IN ('REG AIR', 'MAIL'), 23: L_RECEIPTDATE >= '1997-01-01', 23: L_RECEIPTDATE < '1998-01-01'
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=10294,10296,10298,10300,10302,10304,10306,10308,10310,10312 ...
     cardinality=24499385
     avgRowSize=30.0
[end]

