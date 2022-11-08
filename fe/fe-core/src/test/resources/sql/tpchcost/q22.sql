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

18:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 32: substring

STREAM DATA SINK
EXCHANGE ID: 18
UNPARTITIONED

17:SORT
|  order by: <slot 32> 32: substring ASC
|  offset: 0
|
16:AGGREGATE (merge finalize)
|  output: count(33: count), sum(34: sum)
|  group by: 32: substring
|
15:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 15
HASH_PARTITIONED: 32: substring

14:AGGREGATE (update serialize)
|  STREAMING
|  output: count(*), sum(6: C_ACCTBAL)
|  group by: 32: substring
|
13:Project
|  <slot 6> : 6: C_ACCTBAL
|  <slot 32> : substring(5: C_PHONE, 1, 2)
|
12:HASH JOIN
|  join op: LEFT ANTI JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 1: C_CUSTKEY = 22: O_CUSTKEY
|
|----11:EXCHANGE
|
9:Project
|  <slot 1> : 1: C_CUSTKEY
|  <slot 5> : 5: C_PHONE
|  <slot 6> : 6: C_ACCTBAL
|
8:NESTLOOP JOIN
|  join op: CROSS JOIN
|  colocate: false, reason:
|  other join predicates: 6: C_ACCTBAL > 19: avg
|
|----7:EXCHANGE
|
0:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
PREDICATES: substring(5: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10227,10229,10231,10233,10235,10237,10239,10241,10243,10245
cardinality=7500000
avgRowSize=31.0
numNodes=0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 11
BUCKET_SHUFFLE_HASH_PARTITIONED: 22: O_CUSTKEY

10:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
partitions=1/1
rollup: orders
tabletRatio=10/10
tabletList=10204,10206,10208,10210,10212,10214,10216,10218,10220,10222
cardinality=150000000
avgRowSize=8.0
numNodes=0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|
5:AGGREGATE (merge finalize)
|  output: avg(19: avg)
|  group by:
|
4:EXCHANGE

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:AGGREGATE (update serialize)
|  output: avg(15: C_ACCTBAL)
|  group by:
|
2:Project
|  <slot 15> : 15: C_ACCTBAL
|
1:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
PREDICATES: 15: C_ACCTBAL > 0.0, substring(14: C_PHONE, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitions=1/1
rollup: customer
tabletRatio=10/10
tabletList=10227,10229,10231,10233,10235,10237,10239,10241,10243,10245
cardinality=6818187
avgRowSize=23.0
numNodes=0
[end]

