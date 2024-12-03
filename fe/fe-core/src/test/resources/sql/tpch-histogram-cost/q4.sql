[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:6: O_ORDERPRIORITY | 29: count
  PARTITION: UNPARTITIONED

  RESULT SINK

  12:MERGING-EXCHANGE

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: HASH_PARTITIONED: 6: O_ORDERPRIORITY

  STREAM DATA SINK
    EXCHANGE ID: 12
    UNPARTITIONED

  11:SORT
  |  order by: <slot 6> 6: O_ORDERPRIORITY ASC
  |  offset: 0
  |  
  10:AGGREGATE (merge finalize)
  |  output: count(29: count)
  |  group by: 6: O_ORDERPRIORITY
  |  
  9:EXCHANGE

PLAN FRAGMENT 2
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 09
    HASH_PARTITIONED: 6: O_ORDERPRIORITY

  8:AGGREGATE (update serialize)
  |  STREAMING
  |  output: count(*)
  |  group by: 6: O_ORDERPRIORITY
  |  
  7:Project
  |  <slot 6> : 6: O_ORDERPRIORITY
  |  
  6:HASH JOIN
  |  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE)
  |  colocate: false, reason: 
  |  equal join conjunct: 11: L_ORDERKEY = 1: O_ORDERKEY
  |  
  |----5:EXCHANGE
  |    
  2:SELECT
  |  predicates: 23: L_RECEIPTDATE > 22: L_COMMITDATE
  |  
  1:Project
  |  <slot 11> : 11: L_ORDERKEY
  |  <slot 22> : 22: L_COMMITDATE
  |  <slot 23> : 23: L_RECEIPTDATE
  |  
  0:OlapScanNode
     TABLE: lineitem
     PREAGGREGATION: ON
     partitions=1/1
     rollup: lineitem
     tabletRatio=20/20
     tabletList=10294,10296,10298,10300,10302,10304,10306,10308,10310,10312 ...
     cardinality=600000000
     avgRowSize=16.0

PLAN FRAGMENT 3
 OUTPUT EXPRS:
  PARTITION: RANDOM

  STREAM DATA SINK
    EXCHANGE ID: 05
    BUCKET_SHUFFLE_HASH_PARTITIONED: 1: O_ORDERKEY

  4:Project
  |  <slot 1> : 1: O_ORDERKEY
  |  <slot 6> : 6: O_ORDERPRIORITY
  |  
  3:OlapScanNode
     TABLE: orders
     PREAGGREGATION: ON
     PREDICATES: 5: O_ORDERDATE >= '1994-09-01', 5: O_ORDERDATE < '1994-12-01'
     partitions=1/1
     rollup: orders
     tabletRatio=10/10
     tabletList=10220,10222,10224,10226,10228,10230,10232,10234,10236,10238
     cardinality=5574948
     avgRowSize=27.0
[end]

