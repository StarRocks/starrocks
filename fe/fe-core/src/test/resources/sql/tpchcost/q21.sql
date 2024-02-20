[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:2: S_NAME | 77: count
PARTITION: UNPARTITIONED

RESULT SINK

29:MERGING-EXCHANGE
limit: 100

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 2: S_NAME

STREAM DATA SINK
EXCHANGE ID: 29
UNPARTITIONED

28:TOP-N
|  order by: <slot 77> 77: count DESC, <slot 2> 2: S_NAME ASC
|  offset: 0
|  limit: 100
|
27:AGGREGATE (merge finalize)
|  output: count(77: count)
|  group by: 2: S_NAME
|
26:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 26
HASH_PARTITIONED: 2: S_NAME

25:AGGREGATE (update serialize)
|  STREAMING
|  output: count(*)
|  group by: 2: S_NAME
|
24:Project
|  <slot 2> : 2: S_NAME
|
23:HASH JOIN
|  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 41: L_ORDERKEY = 9: L_ORDERKEY
|  other join predicates: 43: L_SUPPKEY != 11: L_SUPPKEY
|
|----22:EXCHANGE
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=12.0
numNodes=0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 22
BUCKET_SHUFFLE_HASH_PARTITIONED: 9: L_ORDERKEY

21:Project
|  <slot 2> : 2: S_NAME
|  <slot 9> : 9: L_ORDERKEY
|  <slot 11> : 11: L_SUPPKEY
|
20:HASH JOIN
|  join op: RIGHT ANTI JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 59: L_ORDERKEY = 9: L_ORDERKEY
|  other join predicates: 61: L_SUPPKEY != 11: L_SUPPKEY
|
|----19:EXCHANGE
|
2:Project
|  <slot 59> : 59: L_ORDERKEY
|  <slot 61> : 61: L_SUPPKEY
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 71: L_RECEIPTDATE > 70: L_COMMITDATE
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=300000000
avgRowSize=20.0
numNodes=0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 19
BUCKET_SHUFFLE_HASH_PARTITIONED: 9: L_ORDERKEY

18:Project
|  <slot 2> : 2: S_NAME
|  <slot 9> : 9: L_ORDERKEY
|  <slot 11> : 11: L_SUPPKEY
|
17:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 26: O_ORDERKEY = 9: L_ORDERKEY
|
|----16:EXCHANGE
|
4:Project
|  <slot 26> : 26: O_ORDERKEY
|
3:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: 28: O_ORDERSTATUS = 'F'
partitions=1/1
rollup: orders
tabletRatio=10/10
cardinality=50000000
avgRowSize=9.0
numNodes=0

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 16
BUCKET_SHUFFLE_HASH_PARTITIONED: 9: L_ORDERKEY

15:Project
|  <slot 2> : 2: S_NAME
|  <slot 9> : 9: L_ORDERKEY
|  <slot 11> : 11: L_SUPPKEY
|
14:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 11: L_SUPPKEY = 1: S_SUPPKEY
|
|----13:EXCHANGE
|
6:Project
|  <slot 9> : 9: L_ORDERKEY
|  <slot 11> : 11: L_SUPPKEY
|
5:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 21: L_RECEIPTDATE > 20: L_COMMITDATE
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=300000000
avgRowSize=20.0
numNodes=0

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 13
UNPARTITIONED

12:Project
|  <slot 1> : 1: S_SUPPKEY
|  <slot 2> : 2: S_NAME
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 4: S_NATIONKEY = 36: N_NATIONKEY
|
|----10:EXCHANGE
|
7:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
cardinality=1000000
avgRowSize=33.0
numNodes=0

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 10
UNPARTITIONED

9:Project
|  <slot 36> : 36: N_NATIONKEY
|
8:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
PREDICATES: 37: N_NAME = 'CANADA'
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=1
avgRowSize=29.0
numNodes=0
[end]

