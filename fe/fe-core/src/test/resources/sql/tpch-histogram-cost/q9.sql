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
OUTPUT EXPRS:53: N_NAME | 57: year | 59: sum
PARTITION: UNPARTITIONED

RESULT SINK

27:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 53: N_NAME, 57: year

STREAM DATA SINK
EXCHANGE ID: 27
UNPARTITIONED

26:SORT
|  order by: <slot 53> 53: N_NAME ASC, <slot 57> 57: year DESC
|  offset: 0
|
25:AGGREGATE (merge finalize)
|  output: sum(59: sum)
|  group by: 53: N_NAME, 57: year
|
24:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 21: L_SUPPKEY

STREAM DATA SINK
EXCHANGE ID: 24
HASH_PARTITIONED: 53: N_NAME, 57: year

23:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(58: expr)
|  group by: 53: N_NAME, 57: year
|
22:Project
|  <slot 53> : 53: N_NAME
|  <slot 57> : year(46: O_ORDERDATE)
|  <slot 58> : 24: L_EXTENDEDPRICE * 1.0 - 25: L_DISCOUNT - 39: PS_SUPPLYCOST * 23: L_QUANTITY
|
21:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  colocate: false, reason:
|  equal join conjunct: 21: L_SUPPKEY = 37: PS_SUPPKEY
|  equal join conjunct: 20: L_PARTKEY = 36: PS_PARTKEY
|
|----20:EXCHANGE
|
18:Project
|  <slot 20> : 20: L_PARTKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 46> : 46: O_ORDERDATE
|  <slot 53> : 53: N_NAME
|
17:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  colocate: false, reason:
|  equal join conjunct: 21: L_SUPPKEY = 11: S_SUPPKEY
|
|----16:EXCHANGE
|
10:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 20
HASH_PARTITIONED: 37: PS_SUPPKEY

19:OlapScanNode
TABLE: partsupp
PREAGGREGATION: ON
partitions=1/1
rollup: partsupp
tabletRatio=10/10
cardinality=80000000
avgRowSize=24.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 16
HASH_PARTITIONED: 11: S_SUPPKEY

15:Project
|  <slot 11> : 11: S_SUPPKEY
|  <slot 53> : 53: N_NAME
|
14:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 14: S_NATIONKEY = 52: N_NATIONKEY
|
|----13:EXCHANGE
|
11:OlapScanNode
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
EXCHANGE ID: 13
UNPARTITIONED

12:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=25
avgRowSize=29.0

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 10
HASH_PARTITIONED: 21: L_SUPPKEY

9:Project
|  <slot 20> : 20: L_PARTKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 46> : 46: O_ORDERDATE
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 19: L_ORDERKEY = 42: O_ORDERKEY
|
|----7:EXCHANGE
|
5:Project
|  <slot 19> : 19: L_ORDERKEY
|  <slot 20> : 20: L_PARTKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 23> : 23: L_QUANTITY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 20: L_PARTKEY = 1: P_PARTKEY
|
|----3:EXCHANGE
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=44.0

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
BUCKET_SHUFFLE_HASH_PARTITIONED: 42: O_ORDERKEY

6:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
cardinality=150000000
avgRowSize=12.0

PLAN FRAGMENT 8
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
UNPARTITIONED

2:Project
|  <slot 1> : 1: P_PARTKEY
|
1:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 2: P_NAME LIKE '%peru%'
partitions=1/1
rollup: part
tabletRatio=10/10
cardinality=5000000
avgRowSize=63.0
[end]
