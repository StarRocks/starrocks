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

14:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 20: count

STREAM DATA SINK
EXCHANGE ID: 14
UNPARTITIONED

13:SORT
|  order by: <slot 21> 21: count DESC, <slot 20> 20: count DESC
|  offset: 0
|
12:AGGREGATE (update finalize)
|  output: count(*)
|  group by: 20: count
|
11:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 1: C_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 11
HASH_PARTITIONED: 20: count

10:Project
|  <slot 20> : 20: count
|
9:AGGREGATE (merge finalize)
|  output: count(20: count)
|  group by: 1: C_CUSTKEY
|
8:EXCHANGE

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

PLAN FRAGMENT 5
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
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=112500000
avgRowSize=95.0
numNodes=0
[end]

