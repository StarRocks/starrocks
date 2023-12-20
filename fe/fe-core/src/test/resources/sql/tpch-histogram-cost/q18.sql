[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:2: C_NAME | 1: C_CUSTKEY | 10: O_ORDERKEY | 14: O_ORDERDATE | 13: O_TOTALPRICE | 56: sum
PARTITION: UNPARTITIONED

RESULT SINK

17:MERGING-EXCHANGE
limit: 100

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 17
UNPARTITIONED

16:TOP-N
|  order by: <slot 13> 13: O_TOTALPRICE DESC, <slot 14> 14: O_ORDERDATE ASC
|  offset: 0
|  limit: 100
|
15:AGGREGATE (update finalize)
|  output: sum(24: L_QUANTITY)
|  group by: 2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE
|
14:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 10> : 10: O_ORDERKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|  <slot 24> : 24: L_QUANTITY
|
13:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 20: L_ORDERKEY = 10: O_ORDERKEY
|
|----12:EXCHANGE
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=16.0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 12
BUCKET_SHUFFLE_HASH_PARTITIONED: 10: O_ORDERKEY

11:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 2> : 2: C_NAME
|  <slot 10> : 10: O_ORDERKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|
10:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 11: O_CUSTKEY = 1: C_CUSTKEY
|
|----9:EXCHANGE
|
7:Project
|  <slot 10> : 10: O_ORDERKEY
|  <slot 11> : 11: O_CUSTKEY
|  <slot 13> : 13: O_TOTALPRICE
|  <slot 14> : 14: O_ORDERDATE
|
6:HASH JOIN
|  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 10: O_ORDERKEY = 37: L_ORDERKEY
|
|----5:EXCHANGE
|
1:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
cardinality=150000000
avgRowSize=28.0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 09
UNPARTITIONED

8:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
partitions=1/1
rollup: customer
tabletRatio=10/10
cardinality=15000000
avgRowSize=33.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 05
BUCKET_SHUFFLE_HASH_PARTITIONED: 37: L_ORDERKEY

4:Project
|  <slot 37> : 37: L_ORDERKEY
|
3:AGGREGATE (update finalize)
|  output: sum(41: L_QUANTITY)
|  group by: 37: L_ORDERKEY
|  having: 54: sum > 315.0
|
2:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=16.0
[end]

