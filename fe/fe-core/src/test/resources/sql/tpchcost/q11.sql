[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    partsupp,
    supplier,
    nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: PS_PARTKEY | 21: sum
PARTITION: UNPARTITIONED

RESULT SINK

29:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 29
UNPARTITIONED

28:SORT
|  order by: <slot 21> 21: sum DESC
|  offset: 0
|  use vectorized: true
|
27:Project
|  <slot 1> : 1: PS_PARTKEY
|  <slot 21> : 21: sum
|  use vectorized: true
|
26:CROSS JOIN
|  cross join:
|  predicates: 21: sum > 43: expr
|  use vectorized: true
|
|----25:EXCHANGE
|       use vectorized: true
|
10:AGGREGATE (update finalize)
|  output: sum(20: expr)
|  group by: 1: PS_PARTKEY
|  use vectorized: true
|
9:Project
|  <slot 1> : 1: PS_PARTKEY
|  <slot 20> : 4: PS_SUPPLYCOST * CAST(3: PS_AVAILQTY AS DOUBLE)
|  use vectorized: true
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 2: PS_SUPPKEY = 7: S_SUPPKEY
|  use vectorized: true
|
|----7:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
tabletList=10116,10118,10120,10122,10124,10126,10128,10130,10132,10134
cardinality=80000000
avgRowSize=28.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 25
UNPARTITIONED

24:Project
|  <slot 43> : 42: sum * 1.0E-4
|  use vectorized: true
|
23:AGGREGATE (merge finalize)
|  output: sum(42: sum)
|  group by:
|  use vectorized: true
|
22:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 22
UNPARTITIONED

21:AGGREGATE (update serialize)
|  output: sum(41: expr)
|  group by:
|  use vectorized: true
|
20:Project
|  <slot 41> : 25: PS_SUPPLYCOST * CAST(24: PS_AVAILQTY AS DOUBLE)
|  use vectorized: true
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 23: PS_SUPPKEY = 28: S_SUPPKEY
|  use vectorized: true
|
|----18:EXCHANGE
|       use vectorized: true
|
11:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
tabletList=10116,10118,10120,10122,10124,10126,10128,10130,10132,10134
cardinality=80000000
avgRowSize=20.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 18
UNPARTITIONED

17:Project
|  <slot 28> : 28: S_SUPPKEY
|  use vectorized: true
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 31: S_NATIONKEY = 36: N_NATIONKEY
|  use vectorized: true
|
|----15:EXCHANGE
|       use vectorized: true
|
12:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
tabletList=10111
cardinality=1000000
avgRowSize=8.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:Project
|  <slot 36> : 36: N_NATIONKEY
|  use vectorized: true
|
13:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
PREDICATES: 37: N_NAME = 'PERU'
partitions=1/1
rollup: nation
tabletRatio=1/1
tabletList=10185
cardinality=1
avgRowSize=29.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:Project
|  <slot 7> : 7: S_SUPPKEY
|  use vectorized: true
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 10: S_NATIONKEY = 15: N_NATIONKEY
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
1:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
tabletList=10111
cardinality=1000000
avgRowSize=8.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:Project
|  <slot 15> : 15: N_NATIONKEY
|  use vectorized: true
|
2:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
PREDICATES: 16: N_NAME = 'PERU'
partitions=1/1
rollup: nation
tabletRatio=1/1
tabletList=10185
cardinality=1
avgRowSize=29.0
numNodes=0
use vectorized: true
[end]

