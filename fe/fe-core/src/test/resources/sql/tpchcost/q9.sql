[sql]
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%peru%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:53: N_NAME | 57: year | 59: sum(58: expr)
PARTITION: UNPARTITIONED

RESULT SINK

27:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 53: N_NAME, 57: year

STREAM DATA SINK
EXCHANGE ID: 27
UNPARTITIONED

26:SORT
|  order by: <slot 53> 53: N_NAME ASC, <slot 57> 57: year DESC
|  offset: 0
|  use vectorized: true
|
25:AGGREGATE (merge finalize)
|  output: sum(59: sum(58: expr))
|  group by: 53: N_NAME, 57: year
|  use vectorized: true
|
24:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 21: L_SUPPKEY, 20: L_PARTKEY

STREAM DATA SINK
EXCHANGE ID: 24
HASH_PARTITIONED: 53: N_NAME, 57: year

23:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(58: expr)
|  group by: 53: N_NAME, 57: year
|  use vectorized: true
|
22:Project
|  <slot 53> : 53: N_NAME
|  <slot 57> : year(CAST(46: O_ORDERDATE AS DATETIME))
|  <slot 58> : 24: L_EXTENDEDPRICE * 1.0 - 25: L_DISCOUNT - 39: PS_SUPPLYCOST * 23: L_QUANTITY
|  use vectorized: true
|
21:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 21: L_SUPPKEY = 11: S_SUPPKEY
|  use vectorized: true
|
|----20:EXCHANGE
|       use vectorized: true
|
14:Project
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 39> : 39: PS_SUPPLYCOST
|  <slot 46> : 46: O_ORDERDATE
|  use vectorized: true
|
13:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 21: L_SUPPKEY = 37: PS_SUPPKEY
|  equal join conjunct: 20: L_PARTKEY = 36: PS_PARTKEY
|  use vectorized: true
|
|----12:EXCHANGE
|       use vectorized: true
|
10:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 20
UNPARTITIONED

19:Project
|  <slot 11> : 11: S_SUPPKEY
|  <slot 53> : 53: N_NAME
|  use vectorized: true
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 14: S_NATIONKEY = 52: N_NATIONKEY
|  use vectorized: true
|
|----17:EXCHANGE
|       use vectorized: true
|
15:OlapScanNode
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

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 17
UNPARTITIONED

16:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
partitions=1/1
rollup: nation
tabletRatio=1/1
tabletList=10185
cardinality=25
avgRowSize=29.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 12
HASH_PARTITIONED: 37: PS_SUPPKEY, 36: PS_PARTKEY

11:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
tabletList=10116,10118,10120,10122,10124,10126,10128,10130,10132,10134
cardinality=80000000
avgRowSize=24.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 10
HASH_PARTITIONED: 21: L_SUPPKEY, 20: L_PARTKEY

9:Project
|  <slot 20> : 20: L_PARTKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 46> : 46: O_ORDERDATE
|  use vectorized: true
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 19: L_ORDERKEY = 42: O_ORDERKEY
|  use vectorized: true
|
|----7:EXCHANGE
|       use vectorized: true
|
5:Project
|  <slot 19> : 19: L_ORDERKEY
|  <slot 20> : 20: L_PARTKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  use vectorized: true
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 20: L_PARTKEY = 1: P_PARTKEY
|  use vectorized: true
|
|----3:EXCHANGE
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
avgRowSize=44.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
BUCKET_SHFFULE_HASH_PARTITIONED: 42: O_ORDERKEY

6:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=150000000
avgRowSize=12.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 8
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
UNPARTITIONED

2:Project
|  <slot 1> : 1: P_PARTKEY
|  use vectorized: true
|
1:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 2: P_NAME LIKE '%peru%'
partitions=1/1
rollup: part
tabletRatio=10/10
tabletList=10190,10192,10194,10196,10198,10200,10202,10204,10206,10208
cardinality=5000000
avgRowSize=63.0
numNodes=0
use vectorized: true
[end]