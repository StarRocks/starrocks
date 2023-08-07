[sql]
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-05-01'
  and o_orderdate < date '1994-08-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc limit 20;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: C_CUSTKEY | 2: C_NAME | 43: sum | 6: C_ACCTBAL | 38: N_NAME | 3: C_ADDRESS | 5: C_PHONE | 8: C_COMMENT
PARTITION: UNPARTITIONED

RESULT SINK

17:MERGING-EXCHANGE
limit: 20

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 17
UNPARTITIONED

16:TOP-N
|  order by: <slot 43> 43: sum DESC
|  offset: 0
|  limit: 20
|
15:AGGREGATE (update finalize)
|  output: sum(42: expr)
|  group by: 1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT
|
14:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 3> : 3: C_ADDRESS
|  <slot 5> : 5: C_PHONE
|  <slot 6> : 6: C_ACCTBAL
|  <slot 8> : 8: C_COMMENT
|  <slot 38> : 38: N_NAME
|  <slot 42> : 25: L_EXTENDEDPRICE * 1.0 - 26: L_DISCOUNT
|
13:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 4: C_NATIONKEY = 37: N_NATIONKEY
|
|----12:EXCHANGE
|
10:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 3> : 3: C_ADDRESS
|  <slot 4> : 4: C_NATIONKEY
|  <slot 5> : 5: C_PHONE
|  <slot 6> : 6: C_ACCTBAL
|  <slot 8> : 8: C_COMMENT
|  <slot 25> : 25: L_EXTENDEDPRICE
|  <slot 26> : 26: L_DISCOUNT
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 1: C_CUSTKEY = 11: O_CUSTKEY
|
|----8:EXCHANGE
|
0:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
cardinality=15000000
avgRowSize=217.0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 12
UNPARTITIONED

11:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
partitions=1/1
rollup: nation
tabletRatio=1/1
tabletList=10185
cardinality=25
avgRowSize=29.0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 08
BUCKET_SHUFFLE_HASH_PARTITIONED: 11: O_CUSTKEY

7:Project
|  <slot 11> : 11: O_CUSTKEY
|  <slot 25> : 25: L_EXTENDEDPRICE
|  <slot 26> : 26: L_DISCOUNT
|
6:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 20: L_ORDERKEY = 10: O_ORDERKEY
|
|----5:EXCHANGE
|
2:Project
|  <slot 20> : 20: L_ORDERKEY
|  <slot 25> : 25: L_EXTENDEDPRICE
|  <slot 26> : 26: L_DISCOUNT
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 28: L_RETURNFLAG = 'R'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=200000000
avgRowSize=25.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 05
BUCKET_SHUFFLE_HASH_PARTITIONED: 10: O_ORDERKEY

4:Project
|  <slot 10> : 10: O_ORDERKEY
|  <slot 11> : 11: O_CUSTKEY
|
3:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: 14: O_ORDERDATE >= '1994-05-01', 14: O_ORDERDATE < '1994-08-01'
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=5738046
avgRowSize=20.0
[end]

