[sql]
select
    o_year,
    sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01' and date '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:69: year | 74: expr
PARTITION: UNPARTITIONED

RESULT SINK

38:MERGING-EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 69: year

STREAM DATA SINK
EXCHANGE ID: 38
UNPARTITIONED

37:SORT
|  order by: <slot 69> 69: year ASC
|  offset: 0
|
36:Project
|  <slot 69> : 69: year
|  <slot 74> : 72: sum / 73: sum
|
35:AGGREGATE (merge finalize)
|  output: sum(72: sum), sum(73: sum)
|  group by: 69: year
|
34:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 21: L_SUPPKEY

STREAM DATA SINK
EXCHANGE ID: 34
HASH_PARTITIONED: 69: year

33:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(71: case), sum(70: expr)
|  group by: 69: year
|
32:Project
|  <slot 69> : year(40: O_ORDERDATE)
|  <slot 70> : 76: multiply
|  <slot 71> : if(61: N_NAME = 'IRAN', 76: multiply, 0.0)
|  common expressions:
|  <slot 75> : 1.0 - 25: L_DISCOUNT
|  <slot 76> : 24: L_EXTENDEDPRICE * 75: subtract
|
31:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 14: S_NATIONKEY = 60: N_NATIONKEY
|
|----30:EXCHANGE
|
28:Project
|  <slot 14> : 14: S_NATIONKEY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 40> : 40: O_ORDERDATE
|
27:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  colocate: false, reason:
|  equal join conjunct: 21: L_SUPPKEY = 11: S_SUPPKEY
|
|----26:EXCHANGE
|
24:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 30
UNPARTITIONED

29:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=25
avgRowSize=29.0
numNodes=0

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 26
HASH_PARTITIONED: 11: S_SUPPKEY

25:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
cardinality=1000000
avgRowSize=8.0
numNodes=0

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 37: O_CUSTKEY

STREAM DATA SINK
EXCHANGE ID: 24
HASH_PARTITIONED: 21: L_SUPPKEY

23:Project
|  <slot 21> : 21: L_SUPPKEY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 40> : 40: O_ORDERDATE
|
22:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  colocate: false, reason:
|  equal join conjunct: 37: O_CUSTKEY = 46: C_CUSTKEY
|
|----21:EXCHANGE
|
10:EXCHANGE

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 21
HASH_PARTITIONED: 46: C_CUSTKEY

20:Project
|  <slot 46> : 46: C_CUSTKEY
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 49: C_NATIONKEY = 55: N_NATIONKEY
|
|----18:EXCHANGE
|
11:OlapScanNode
TABLE: customer
PREAGGREGATION: ON
partitions=1/1
rollup: customer
tabletRatio=10/10
cardinality=15000000
avgRowSize=12.0
numNodes=0

PLAN FRAGMENT 7
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 18
UNPARTITIONED

17:Project
|  <slot 55> : 55: N_NATIONKEY
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 57: N_REGIONKEY = 65: R_REGIONKEY
|
|----15:EXCHANGE
|
12:OlapScanNode
TABLE: nation
PREAGGREGATION: ON
partitions=1/1
rollup: nation
tabletRatio=1/1
cardinality=25
avgRowSize=8.0
numNodes=0

PLAN FRAGMENT 8
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:Project
|  <slot 65> : 65: R_REGIONKEY
|
13:OlapScanNode
TABLE: region
PREAGGREGATION: ON
PREDICATES: 66: R_NAME = 'MIDDLE EAST'
partitions=1/1
rollup: region
tabletRatio=1/1
cardinality=1
avgRowSize=29.0
numNodes=0

PLAN FRAGMENT 9
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 10
HASH_PARTITIONED: 37: O_CUSTKEY

9:Project
|  <slot 21> : 21: L_SUPPKEY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|  <slot 37> : 37: O_CUSTKEY
|  <slot 40> : 40: O_ORDERDATE
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  colocate: false, reason:
|  equal join conjunct: 36: O_ORDERKEY = 19: L_ORDERKEY
|
|----7:EXCHANGE
|
0:OlapScanNode
TABLE: orders
PREAGGREGATION: ON
PREDICATES: 40: O_ORDERDATE >= '1995-01-01', 40: O_ORDERDATE <= '1996-12-31'
partitions=1/1
rollup: orders
tabletRatio=10/10
cardinality=45530146
avgRowSize=20.0
numNodes=0

PLAN FRAGMENT 10
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
BUCKET_SHUFFLE_HASH_PARTITIONED: 19: L_ORDERKEY

6:Project
|  <slot 19> : 19: L_ORDERKEY
|  <slot 21> : 21: L_SUPPKEY
|  <slot 24> : 24: L_EXTENDEDPRICE
|  <slot 25> : 25: L_DISCOUNT
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 20: L_PARTKEY = 1: P_PARTKEY
|
|----4:EXCHANGE
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
partitions=1/1
rollup: lineitem
tabletRatio=20/20
cardinality=600000000
avgRowSize=36.0
numNodes=0

PLAN FRAGMENT 11
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:Project
|  <slot 1> : 1: P_PARTKEY
|
2:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 5: P_TYPE = 'ECONOMY ANODIZED STEEL'
partitions=1/1
rollup: part
tabletRatio=10/10
cardinality=133333
avgRowSize=33.0
numNodes=0
[end]

