[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: PS_PARTKEY | 21: sum
PARTITION: UNPARTITIONED

RESULT SINK

30:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 30
UNPARTITIONED

29:SORT
|  order by: <slot 21> 21: sum DESC
|  offset: 0
|
28:Project
|  <slot 1> : 1: PS_PARTKEY
|  <slot 21> : 21: sum
|
27:NESTLOOP JOIN
|  join op: INNER JOIN
|  colocate: false, reason:
|  other join predicates: 21: sum > 43: expr
|
|----26:EXCHANGE
|
10:AGGREGATE (update finalize)
|  output: sum(20: expr)
|  group by: 1: PS_PARTKEY
|
9:Project
|  <slot 1> : 1: PS_PARTKEY
|  <slot 20> : 4: PS_SUPPLYCOST * CAST(3: PS_AVAILQTY AS DOUBLE)
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 2: PS_SUPPKEY = 7: S_SUPPKEY
|
|----7:EXCHANGE
|
0:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
cardinality=80000000
avgRowSize=28.0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 26
UNPARTITIONED

25:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|
24:Project
|  <slot 43> : 42: sum * 1.0E-4
|
23:AGGREGATE (merge finalize)
|  output: sum(42: sum)
|  group by:
|
22:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 22
UNPARTITIONED

21:AGGREGATE (update serialize)
|  output: sum(25: PS_SUPPLYCOST * CAST(24: PS_AVAILQTY AS DOUBLE))
|  group by:
|
20:Project
|  <slot 24> : 24: PS_AVAILQTY
|  <slot 25> : 25: PS_SUPPLYCOST
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 23: PS_SUPPKEY = 28: S_SUPPKEY
|
|----18:EXCHANGE
|
11:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
cardinality=80000000
avgRowSize=20.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 18
UNPARTITIONED

17:Project
|  <slot 28> : 28: S_SUPPKEY
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 31: S_NATIONKEY = 36: N_NATIONKEY
|
|----15:EXCHANGE
|
12:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
cardinality=1000000
avgRowSize=8.0

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:Project
|  <slot 36> : 36: N_NATIONKEY
|
13:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
PREDICATES: 37: N_NAME = 'PERU'
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=1
avgRowSize=29.0

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:Project
|  <slot 7> : 7: S_SUPPKEY
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 10: S_NATIONKEY = 15: N_NATIONKEY
|
|----4:EXCHANGE
|
1:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
cardinality=1000000
avgRowSize=8.0

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:Project
|  <slot 15> : 15: N_NATIONKEY
|
2:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
PREDICATES: 16: N_NAME = 'PERU'
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=1
avgRowSize=29.0
[end]

