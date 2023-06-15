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
OUTPUT EXPRS:20: count | 21: count
PARTITION: UNPARTITIONED

RESULT SINK

12:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 20: count

STREAM DATA SINK
EXCHANGE ID: 12
UNPARTITIONED

11:SORT
|  order by: <slot 21> 21: count DESC, <slot 20> 20: count DESC
|  offset: 0
|
10:AGGREGATE (update finalize)
|  output: count(*)
|  group by: 20: count
|
9:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 11: O_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 09
HASH_PARTITIONED: 20: count

8:Project
|  <slot 20> : 20: count
|
7:AGGREGATE (update finalize)
|  output: count(10: O_ORDERKEY)
|  group by: 1: C_CUSTKEY
|
6:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 10> : 10: O_ORDERKEY
|
5:HASH JOIN
|  join op: RIGHT OUTER JOIN (PARTITIONED)
|  colocate: false, reason:
|  equal join conjunct: 11: O_CUSTKEY = 1: C_CUSTKEY
|
|----4:EXCHANGE
|
2:EXCHANGE

PLAN FRAGMENT 3
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
cardinality=15000000
avgRowSize=8.0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
HASH_PARTITIONED: 11: O_CUSTKEY

1:Project
|  <slot 10> : 10: O_ORDERKEY
|  <slot 11> : 11: O_CUSTKEY
|
0:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: NOT (18: O_COMMENT LIKE '%unusual%deposits%')
partitions=1/1
rollup: orders
tabletRatio=10/10
cardinality=112500000
avgRowSize=95.0
[end]

