[sql]
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '1994-09-01'
  and o_orderdate < date '1994-12-01'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_receiptdate > l_commitdate
    )
group by
    o_orderpriority
order by
    o_orderpriority ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:6: O_ORDERPRIORITY | 29: count
PARTITION: UNPARTITIONED

RESULT SINK

11:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 6: O_ORDERPRIORITY

STREAM DATA SINK
EXCHANGE ID: 11
UNPARTITIONED

10:SORT
|  order by: <slot 6> 6: O_ORDERPRIORITY ASC
|  offset: 0
|
9:AGGREGATE (merge finalize)
|  output: count(29: count)
|  group by: 6: O_ORDERPRIORITY
|
8:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 08
HASH_PARTITIONED: 6: O_ORDERPRIORITY

7:AGGREGATE (update serialize)
|  STREAMING
|  output: count(*)
|  group by: 6: O_ORDERPRIORITY
|
6:Project
|  <slot 6> : 6: O_ORDERPRIORITY
|
5:HASH JOIN
|  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 11: L_ORDERKEY = 1: O_ORDERKEY
|
|----4:EXCHANGE
|
1:Project
|  <slot 11> : 11: L_ORDERKEY
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 23: L_RECEIPTDATE > 22: L_COMMITDATE
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=300000000
avgRowSize=16.0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
BUCKET_SHUFFLE_HASH_PARTITIONED: 1: O_ORDERKEY

3:Project
|  <slot 1> : 1: O_ORDERKEY
|  <slot 6> : 6: O_ORDERPRIORITY
|
2:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: 5: O_ORDERDATE >= '1994-09-01', 5: O_ORDERDATE < '1994-12-01'
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=5675676
avgRowSize=27.0
[end]

