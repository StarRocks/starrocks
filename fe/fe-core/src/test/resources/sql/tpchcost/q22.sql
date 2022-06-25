[sql]
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone , 1  ,2) as cntrycode,
            c_acctbal
        from
            customer
        where
                substring(c_phone , 1  ,2)  in
                ('21', '28', '24', '32', '35', '34', '37')
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                    c_acctbal > 0.00
              and substring(c_phone , 1  ,2)  in
                  ('21', '28', '24', '32', '35', '34', '37')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:32: substring | 33: count | 34: sum
PARTITION: UNPARTITIONED

RESULT SINK

16:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 16
UNPARTITIONED

15:SORT
|  order by: <slot 32> 32: substring ASC
|  offset: 0
|
14:AGGREGATE (update finalize)
|  output: count(*), sum(6: C_ACCTBAL)
|  group by: 32: substring
|
13:Project
|  <slot 6> : 6: C_ACCTBAL
|  <slot 32> : substring(5: C_PHONE, 1, 2)
|
12:NESTLOOP JOIN
|  join op: CROSS JOIN
|  hash predicates:
|  colocate: false, reason:
|  other join predicates: 6: C_ACCTBAL > 19: avg
|
|----11:EXCHANGE
|
4:AGGREGATE (merge finalize)
|  output: avg(19: avg)
|  group by:
|
3:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 22: O_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 11
UNPARTITIONED

10:Project
|  <slot 5> : 5: C_PHONE
|  <slot 6> : 6: C_ACCTBAL
|
9:HASH JOIN
|  join op: RIGHT ANTI JOIN (PARTITIONED)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 22: O_CUSTKEY = 1: C_CUSTKEY
|
|----8:EXCHANGE
|
6:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 08
HASH_PARTITIONED: 1: C_CUSTKEY

7:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
PREDICATES: substring(5: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
cardinality=7500000
avgRowSize=31.0
numNodes=0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 06
HASH_PARTITIONED: 22: O_CUSTKEY

5:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
cardinality=150000000
avgRowSize=8.0
numNodes=0

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
UNPARTITIONED

2:AGGREGATE (update serialize)
|  output: avg(15: C_ACCTBAL)
|  group by:
|
1:Project
|  <slot 15> : 15: C_ACCTBAL
|
0:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
PREDICATES: 15: C_ACCTBAL > 0.0, substring(14: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
cardinality=6818187
avgRowSize=23.0
numNodes=0
[end]

