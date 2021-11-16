[sql]
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#43'
  and p_type not like 'PROMO BURNISHED%'
  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%Customer%Complaints%'
)
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:10: P_BRAND | 11: P_TYPE | 12: P_SIZE | 26: count
PARTITION: UNPARTITIONED

RESULT SINK

14:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE

STREAM DATA SINK
EXCHANGE ID: 14
UNPARTITIONED

13:SORT
|  order by: <slot 26> 26: count DESC, <slot 10> 10: P_BRAND ASC, <slot 11> 11: P_TYPE ASC, <slot 12> 12: P_SIZE ASC
|  offset: 0
|  use vectorized: true
|
12:AGGREGATE (merge finalize)
|  output: multi_distinct_count(26: count)
|  group by: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
|  use vectorized: true
|
11:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 11
HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE

10:AGGREGATE (update serialize)
|  STREAMING
|  output: multi_distinct_count(2: PS_SUPPKEY)
|  group by: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
|  use vectorized: true
|
9:Project
|  <slot 2> : 2: PS_SUPPKEY
|  <slot 10> : 10: P_BRAND
|  <slot 11> : 11: P_TYPE
|  <slot 12> : 12: P_SIZE
|  use vectorized: true
|
8:HASH JOIN
|  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 2: PS_SUPPKEY = 17: S_SUPPKEY
|  use vectorized: true
|
|----7:EXCHANGE
|       use vectorized: true
|
4:Project
|  <slot 2> : 2: PS_SUPPKEY
|  <slot 10> : 10: P_BRAND
|  <slot 11> : 11: P_TYPE
|  <slot 12> : 12: P_SIZE
|  use vectorized: true
|
3:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 1: PS_PARTKEY = 7: P_PARTKEY
|  use vectorized: true
|
|----2:EXCHANGE
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
avgRowSize=16.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:Project
|  <slot 17> : 17: S_SUPPKEY
|  use vectorized: true
|
5:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
PREDICATES: 23: S_COMMENT LIKE '%Customer%Complaints%'
partitions=1/1
rollup: supplier
tabletRatio=1/1
tabletList=10111
cardinality=250000
avgRowSize=105.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
BUCKET_SHFFULE_HASH_PARTITIONED: 7: P_PARTKEY

1:OlapScanNode
TABLE: part
PREAGGREGATION: ON
PREDICATES: 10: P_BRAND != 'Brand#43', NOT 11: P_TYPE LIKE 'PROMO BURNISHED%', 12: P_SIZE IN (31, 43, 9, 6, 18, 11, 25, 1)
partitions=1/1
rollup: part
tabletRatio=10/10
tabletList=10190,10192,10194,10196,10198,10200,10202,10204,10206,10208
cardinality=2304000
avgRowSize=47.0
numNodes=0
use vectorized: true
[end]

