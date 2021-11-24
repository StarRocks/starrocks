[sql]
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and o_comment not like '%unusual%deposits%'
        group by
            c_custkey
    ) a
group by
    c_count
order by
    custdist desc,
    c_count desc ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:20: count(10: O_ORDERKEY) | 21: count()
PARTITION: UNPARTITIONED

RESULT SINK

15:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 20: count(10: O_ORDERKEY)

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:SORT
|  order by: <slot 21> 21: count() DESC, <slot 20> 20: count(10: O_ORDERKEY) DESC
|  offset: 0
|  use vectorized: true
|
13:AGGREGATE (merge finalize)
|  output: count(21: count())
|  group by: 20: count(10: O_ORDERKEY)
|  use vectorized: true
|
12:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 1: C_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 12
HASH_PARTITIONED: 20: count(10: O_ORDERKEY)

11:AGGREGATE (update serialize)
|  STREAMING
|  output: count(*)
|  group by: 20: count(10: O_ORDERKEY)
|  use vectorized: true
|
10:Project
|  <slot 20> : 20: count(10: O_ORDERKEY)
|  use vectorized: true
|
9:AGGREGATE (merge finalize)
|  output: count(20: count(10: O_ORDERKEY))
|  group by: 1: C_CUSTKEY
|  use vectorized: true
|
8:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 11: O_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 08
HASH_PARTITIONED: 1: C_CUSTKEY

7:AGGREGATE (update serialize)
|  STREAMING
|  output: count(10: O_ORDERKEY)
|  group by: 1: C_CUSTKEY
|  use vectorized: true
|
6:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 10> : 10: O_ORDERKEY
|  use vectorized: true
|
5:HASH JOIN
|  join op: RIGHT OUTER JOIN (PARTITIONED)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 11: O_CUSTKEY = 1: C_CUSTKEY
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
2:EXCHANGE
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
HASH_PARTITIONED: 1: C_CUSTKEY

3:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
cardinality=15000000
avgRowSize=8.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
HASH_PARTITIONED: 11: O_CUSTKEY

1:Project
|  <slot 10> : 10: O_ORDERKEY
|  <slot 11> : 11: O_CUSTKEY
|  use vectorized: true
|
0:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: NOT 18: O_COMMENT LIKE '%unusual%deposits%'
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=112500000
avgRowSize=95.0
numNodes=0
use vectorized: true
[end]

