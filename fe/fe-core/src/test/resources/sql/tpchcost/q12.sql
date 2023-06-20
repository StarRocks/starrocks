[sql]
select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then cast (1 as bigint)
            else cast(0 as bigint)
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then cast (1 as bigint)
            else cast(0 as bigint)
        end) as low_line_count
from
    orders,
    lineitem
where
        o_orderkey = l_orderkey
  and l_shipmode in ('REG AIR', 'MAIL')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1997-01-01'
  and l_receiptdate < date '1998-01-01'
group by
    l_shipmode
order by
    l_shipmode ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:25: L_SHIPMODE | 30: sum | 31: sum
PARTITION: UNPARTITIONED

RESULT SINK

10:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 25: L_SHIPMODE

STREAM DATA SINK
EXCHANGE ID: 10
UNPARTITIONED

9:SORT
|  order by: <slot 25> 25: L_SHIPMODE ASC
|  offset: 0
|
8:AGGREGATE (merge finalize)
|  output: sum(30: sum), sum(31: sum)
|  group by: 25: L_SHIPMODE
|
7:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
HASH_PARTITIONED: 25: L_SHIPMODE

6:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(28: case), sum(29: case)
|  group by: 25: L_SHIPMODE
|
5:Project
|  <slot 25> : 25: L_SHIPMODE
|  <slot 28> : if((6: O_ORDERPRIORITY = '1-URGENT') OR (6: O_ORDERPRIORITY = '2-HIGH'), 1, 0)
|  <slot 29> : if((6: O_ORDERPRIORITY != '1-URGENT') AND (6: O_ORDERPRIORITY != '2-HIGH'), 1, 0)
|
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 1: O_ORDERKEY = 11: L_ORDERKEY
|
|----3:EXCHANGE
|
0:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=150000000
avgRowSize=23.0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
BUCKET_SHUFFLE_HASH_PARTITIONED: 11: L_ORDERKEY

2:Project
|  <slot 11> : 11: L_ORDERKEY
|  <slot 25> : 25: L_SHIPMODE
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 25: L_SHIPMODE IN ('REG AIR', 'MAIL'), 22: L_COMMITDATE < 23: L_RECEIPTDATE, 21: L_SHIPDATE < 22: L_COMMITDATE, 23: L_RECEIPTDATE >= '1997-01-01', 23: L_RECEIPTDATE < '1998-01-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=6124846
avgRowSize=30.0
[end]

