[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 315
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate limit 100;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:2: C_NAME | 1: C_CUSTKEY | 10: O_ORDERKEY | 14: O_ORDERDATE | 13: O_TOTALPRICE | 56: sum(24: L_QUANTITY)
PARTITION: UNPARTITIONED

RESULT SINK

19:MERGING-EXCHANGE
limit: 100
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE

STREAM DATA SINK
EXCHANGE ID: 19
UNPARTITIONED

18:TOP-N
|  order by: <slot 13> 13: O_TOTALPRICE DESC, <slot 14> 14: O_ORDERDATE ASC
|  offset: 0
|  limit: 100
|  use vectorized: true
|
17:AGGREGATE (merge finalize)
|  output: sum(56: sum(24: L_QUANTITY))
|  group by: 2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE
|  use vectorized: true
|
16:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 16
HASH_PARTITIONED: 2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE

15:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(24: L_QUANTITY)
|  group by: 2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE
|  use vectorized: true
|
14:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 24> : 24: L_QUANTITY
|  <slot 10> : 10: O_ORDERKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|  use vectorized: true
|
13:HASH JOIN
|  join op: LEFT SEMI JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 10: O_ORDERKEY = 37: L_ORDERKEY
|  use vectorized: true
|
|----12:EXCHANGE
|       use vectorized: true
|
8:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 24> : 24: L_QUANTITY
|  <slot 10> : 10: O_ORDERKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|  use vectorized: true
|
7:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 11: O_CUSTKEY = 1: C_CUSTKEY
|  use vectorized: true
|
|----6:EXCHANGE
|       use vectorized: true
|
4:Project
|  <slot 24> : 24: L_QUANTITY
|  <slot 10> : 10: O_ORDERKEY
|  <slot 11> : 11: O_CUSTKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|  use vectorized: true
|
3:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 20: L_ORDERKEY = 10: O_ORDERKEY
|  use vectorized: true
|
|----2:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=600000000
avgRowSize=16.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 12
UNPARTITIONED

11:Project
|  <slot 37> : 37: L_ORDERKEY
|  use vectorized: true
|
10:AGGREGATE (update finalize)
|  output: sum(41: L_QUANTITY)
|  group by: 37: L_ORDERKEY
|  having: 54: sum(41: L_QUANTITY) > 315.0
|  use vectorized: true
|
9:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=600000000
avgRowSize=16.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 06
UNPARTITIONED

5:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
cardinality=15000000
avgRowSize=33.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
BUCKET_SHFFULE_HASH_PARTITIONED: 10: O_ORDERKEY

1:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=150000000
avgRowSize=28.0
numNodes=0
use vectorized: true
[end]

