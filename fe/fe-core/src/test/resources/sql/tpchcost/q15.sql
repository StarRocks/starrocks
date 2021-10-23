[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        (	select
                 l_suppkey as supplier_no,
                 sum(l_extendedprice * (1 - l_discount)) as total_revenue
             from
                 lineitem
             where
                     l_shipdate >= date '1995-07-01'
               and l_shipdate < date '1995-10-01'
             group by
                 l_suppkey) b
)
order by
    s_suppkey;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: S_SUPPKEY | 2: S_NAME | 3: S_ADDRESS | 5: S_PHONE | 27: sum(26: expr)
PARTITION: UNPARTITIONED

RESULT SINK

22:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 22
UNPARTITIONED

21:SORT
|  order by: <slot 1> 1: S_SUPPKEY ASC
|  offset: 0
|  use vectorized: true
|
20:Project
|  <slot 1> : 1: S_SUPPKEY
|  <slot 2> : 2: S_NAME
|  <slot 3> : 3: S_ADDRESS
|  <slot 5> : 5: S_PHONE
|  <slot 27> : 27: sum(26: expr)
|  use vectorized: true
|
19:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 1: S_SUPPKEY = 11: L_SUPPKEY
|  use vectorized: true
|
|----18:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: supplier
PREAGGREGATION: ON
partitions=1/1
rollup: supplier
tabletRatio=1/1
tabletList=10111
cardinality=1000000
avgRowSize=84.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 11: L_SUPPKEY

STREAM DATA SINK
EXCHANGE ID: 18
BUCKET_SHFFULE_HASH_PARTITIONED: 11: L_SUPPKEY

17:Project
|  <slot 11> : 11: L_SUPPKEY
|  <slot 27> : 27: sum(26: expr)
|  use vectorized: true
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 27: sum(26: expr) = 47: max(46: sum(45: expr))
|  use vectorized: true
|
|----15:EXCHANGE
|       use vectorized: true
|
5:AGGREGATE (merge finalize)
|  output: sum(27: sum(26: expr))
|  group by: 11: L_SUPPKEY
|  use vectorized: true
|
4:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 15
UNPARTITIONED

14:AGGREGATE (merge finalize)
|  output: max(47: max(46: sum(45: expr)))
|  group by:
|  use vectorized: true
|
13:EXCHANGE
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 30: L_SUPPKEY

STREAM DATA SINK
EXCHANGE ID: 13
UNPARTITIONED

12:AGGREGATE (update serialize)
|  output: max(46: sum(45: expr))
|  group by:
|  use vectorized: true
|
11:Project
|  <slot 46> : 46: sum(45: expr)
|  use vectorized: true
|
10:AGGREGATE (merge finalize)
|  output: sum(46: sum(45: expr))
|  group by: 30: L_SUPPKEY
|  use vectorized: true
|
9:EXCHANGE
use vectorized: true

PLAN FRAGMENT 5
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 09
HASH_PARTITIONED: 30: L_SUPPKEY

8:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(45: expr)
|  group by: 30: L_SUPPKEY
|  use vectorized: true
|
7:Project
|  <slot 30> : 30: L_SUPPKEY
|  <slot 45> : 33: L_EXTENDEDPRICE * 1.0 - 34: L_DISCOUNT
|  use vectorized: true
|
6:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 38: L_SHIPDATE >= '1995-07-01', 38: L_SHIPDATE < '1995-10-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=21861386
avgRowSize=32.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 6
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
HASH_PARTITIONED: 11: L_SUPPKEY

3:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(26: expr)
|  group by: 11: L_SUPPKEY
|  use vectorized: true
|
2:Project
|  <slot 11> : 11: L_SUPPKEY
|  <slot 26> : 14: L_EXTENDEDPRICE * 1.0 - 15: L_DISCOUNT
|  use vectorized: true
|
1:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 19: L_SHIPDATE >= '1995-07-01', 19: L_SHIPDATE < '1995-10-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=21861386
avgRowSize=32.0
numNodes=0
use vectorized: true
[fragment statistics]
PLAN FRAGMENT 0(F08)
Output Exprs:1: S_SUPPKEY | 2: S_NAME | 3: S_ADDRESS | 5: S_PHONE | 27: sum(26: expr)
Input Partition: UNPARTITIONED
RESULT SINK

22:MERGING-EXCHANGE
cardinality: 1
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0]
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0]
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
* sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 22

21:SORT
|  order by: [1, INT, false] ASC
|  offset: 0
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0]
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0]
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
20:Project
|  output columns:
|  1 <-> [1: S_SUPPKEY, INT, false]
|  2 <-> [2: S_NAME, CHAR, false]
|  3 <-> [3: S_ADDRESS, VARCHAR, false]
|  5 <-> [5: S_PHONE, CHAR, false]
|  27 <-> [27: sum(26: expr), DOUBLE, true]
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0]
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
19:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: S_SUPPKEY, INT, false] = [11: L_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (11: L_SUPPKEY), remote = false
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0]
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0]
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
|----18:EXCHANGE
|       cardinality: 1
|
0:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10111
actualRows=0, avgRowSize=84.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (1: S_SUPPKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0]
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0]
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0]

PLAN FRAGMENT 2(F02)

Input Partition: HASH_PARTITIONED: 11: L_SUPPKEY
OutPut Partition: BUCKET_SHFFULE_HASH_PARTITIONED: 11: L_SUPPKEY
OutPut Exchange Id: 18

17:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  27 <-> [27: sum(26: expr), DOUBLE, true]
|  cardinality: 1
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: sum(26: expr), DOUBLE, true] = [47: max(46: sum(45: expr)), DOUBLE, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (47: max(46: sum(45: expr))), remote = false
|  cardinality: 1
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|  * max(46: sum(45: expr))-->[104949.5, 104949.5, 0.0, 8.0, 1.0]
|
|----15:EXCHANGE
|       cardinality: 1
|
5:AGGREGATE (merge finalize)
|  aggregate: sum[([27: sum(26: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [11: L_SUPPKEY, INT, false]
|  cardinality: 1000000
|  probe runtime filters:
|  - filter_id = 0, probe_expr = (27: sum(26: expr))
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
4:EXCHANGE
cardinality: 1000000

PLAN FRAGMENT 3(F05)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:AGGREGATE (merge finalize)
|  aggregate: max[([47: max(46: sum(45: expr)), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max(46: sum(45: expr))-->[104949.5, 104949.5, 0.0, 8.0, 1.0]
|
13:EXCHANGE
cardinality: 1

PLAN FRAGMENT 4(F04)

Input Partition: HASH_PARTITIONED: 30: L_SUPPKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:AGGREGATE (update serialize)
|  aggregate: max[([46: sum(45: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max(46: sum(45: expr))-->[104949.5, 104949.5, 0.0, 8.0, 1.0]
|
11:Project
|  output columns:
|  46 <-> [46: sum(45: expr), DOUBLE, true]
|  cardinality: 1000000
|  column statistics:
|  * sum(45: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
10:AGGREGATE (merge finalize)
|  aggregate: sum[([46: sum(45: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [30: L_SUPPKEY, INT, false]
|  cardinality: 1000000
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(45: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
9:EXCHANGE
cardinality: 1000000

PLAN FRAGMENT 5(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 30: L_SUPPKEY
OutPut Exchange Id: 09

8:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([45: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [30: L_SUPPKEY, INT, false]
|  cardinality: 1000000
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(45: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
7:Project
|  output columns:
|  30 <-> [30: L_SUPPKEY, INT, false]
|  45 <-> [33: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [34: L_DISCOUNT, DOUBLE, false]
|  cardinality: 21861386
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
6:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [38: L_SHIPDATE, DATE, false] >= '1995-07-01', [38: L_SHIPDATE, DATE, false] < '1995-10-01'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=32.0
cardinality: 21861386
column statistics:
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
* L_SHIPDATE-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0]
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]

PLAN FRAGMENT 6(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 11: L_SUPPKEY
OutPut Exchange Id: 04

3:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([26: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [11: L_SUPPKEY, INT, false]
|  cardinality: 1000000
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * sum(26: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
2:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  26 <-> [14: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [15: L_DISCOUNT, DOUBLE, false]
|  cardinality: 21861386
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [19: L_SHIPDATE, DATE, false] >= '1995-07-01', [19: L_SHIPDATE, DATE, false] < '1995-10-01'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=32.0
cardinality: 21861386
column statistics:
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0]
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
* L_SHIPDATE-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0]
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
[end]