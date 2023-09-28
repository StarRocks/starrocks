[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[fragment statistics]
PLAN FRAGMENT 0(F16)
Output Exprs:2: s_name | 71: count
Input Partition: UNPARTITIONED
RESULT SINK

31:MERGING-EXCHANGE
distribution type: GATHER
limit: 100
cardinality: 100
column statistics:
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
* count-->[0.0, 1600097.8717994562, 0.0, 8.0, 40000.0] ESTIMATE

PLAN FRAGMENT 1(F15)

Input Partition: HASH_PARTITIONED: 2: s_name
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 31

30:TOP-N
|  order by: [71, BIGINT, false] DESC, [2, VARCHAR, true] ASC
|  offset: 0
|  limit: 100
|  cardinality: 100
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 1600097.8717994562, 0.0, 8.0, 40000.0] ESTIMATE
|
29:AGGREGATE (merge finalize)
|  aggregate: count[([71: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [2: s_name, VARCHAR, true]
|  cardinality: 40000
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 1600097.8717994562, 0.0, 8.0, 40000.0] ESTIMATE
|
28:EXCHANGE
distribution type: SHUFFLE
partition exprs: [2: s_name, VARCHAR, true]
cardinality: 40000

PLAN FRAGMENT 2(F14)

Input Partition: HASH_PARTITIONED: 37: l_orderkey
OutPut Partition: HASH_PARTITIONED: 2: s_name
OutPut Exchange Id: 28

27:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [2: s_name, VARCHAR, true]
|  cardinality: 40000
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 1600097.8717994562, 0.0, 8.0, 40000.0] ESTIMATE
|
26:Project
|  output columns:
|  2 <-> [2: s_name, VARCHAR, true]
|  cardinality: 1600098
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|
25:HASH JOIN
|  join op: RIGHT SEMI JOIN (PARTITIONED)
|  equal join conjunct: [37: l_orderkey, INT, true] = [8: l_orderkey, INT, true]
|  other join predicates: [39: l_suppkey, INT, true] != [10: l_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (8: l_orderkey), remote = true
|  output columns: 2, 10, 39
|  cardinality: 1600098
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----24:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [8: l_orderkey, INT, true]
|       cardinality: 1600099
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [37: l_orderkey, INT, true]
cardinality: 600037902

PLAN FRAGMENT 3(F12)

Input Partition: HASH_PARTITIONED: 24: o_orderkey
OutPut Partition: HASH_PARTITIONED: 8: l_orderkey
OutPut Exchange Id: 24

23:Project
|  output columns:
|  2 <-> [2: s_name, VARCHAR, true]
|  8 <-> [8: l_orderkey, INT, true]
|  10 <-> [10: l_suppkey, INT, true]
|  cardinality: 1600099
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1600099.4718989278] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
22:HASH JOIN
|  join op: RIGHT ANTI JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [54: l_orderkey, INT, true] = [8: l_orderkey, INT, true]
|  other join predicates: [56: l_suppkey, INT, true] != [10: l_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (8: l_orderkey), remote = true
|  output columns: 2, 8, 10, 56
|  cardinality: 1600099
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1600099.4718989278] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----21:Project
|    |  output columns:
|    |  2 <-> [2: s_name, VARCHAR, true]
|    |  8 <-> [8: l_orderkey, INT, true]
|    |  10 <-> [10: l_suppkey, INT, true]
|    |  cardinality: 4000253
|    |  column statistics:
|    |  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|    |  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4000252.6799999997] ESTIMATE
|    |  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |
|    20:HASH JOIN
|    |  join op: INNER JOIN (PARTITIONED)
|    |  equal join conjunct: [24: o_orderkey, INT, true] = [8: l_orderkey, INT, true]
|    |  build runtime filters:
|    |  - filter_id = 2, build_expr = (8: l_orderkey), remote = true
|    |  output columns: 2, 8, 10
|    |  cardinality: 4000253
|    |  column statistics:
|    |  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|    |  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4000252.6799999997] ESTIMATE
|    |  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |
|    |----19:EXCHANGE
|    |       distribution type: SHUFFLE
|    |       partition exprs: [8: l_orderkey, INT, true]
|    |       cardinality: 12000758
|    |
|    7:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [24: o_orderkey, INT, true]
|       cardinality: 50000000
|
4:EXCHANGE
distribution type: SHUFFLE
partition exprs: [54: l_orderkey, INT, true]
cardinality: 300018951
probe runtime filters:
- filter_id = 3, probe_expr = (54: l_orderkey)

PLAN FRAGMENT 4(F06)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 8: l_orderkey
OutPut Exchange Id: 19

18:Project
|  output columns:
|  2 <-> [2: s_name, VARCHAR, true]
|  8 <-> [8: l_orderkey, INT, true]
|  10 <-> [10: l_suppkey, INT, true]
|  cardinality: 12000758
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.200075804E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
17:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [10: l_suppkey, INT, true] = [1: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (1: s_suppkey), remote = false
|  output columns: 2, 8, 10
|  cardinality: 12000758
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.200075804E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----16:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 40000
|
9:Project
|  output columns:
|  8 <-> [8: l_orderkey, INT, true]
|  10 <-> [10: l_suppkey, INT, true]
|  cardinality: 300018951
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|
8:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 20: l_receiptdate > 19: l_commitdate
partitions=1/1
avgRowSize=20.0
numNodes=0
cardinality: 300018951
probe runtime filters:
- filter_id = 1, probe_expr = (10: l_suppkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_commitdate-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* l_receiptdate-->[6.94368E8, 9.150336E8, 0.0, 4.0, 2554.0] ESTIMATE

PLAN FRAGMENT 5(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 16

15:Project
|  output columns:
|  1 <-> [1: s_suppkey, INT, true]
|  2 <-> [2: s_name, VARCHAR, true]
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|
14:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: s_nationkey, INT, true] = [33: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (33: n_nationkey), remote = false
|  output columns: 1, 2
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----13:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
10:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 1: s_suppkey IS NOT NULL
partitions=1/1
avgRowSize=33.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 0, probe_expr = (4: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:Project
|  output columns:
|  33 <-> [33: n_nationkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
11:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 34: n_name = 'CANADA'
MIN/MAX PREDICATES: 72: n_name <= 'CANADA', 73: n_name >= 'CANADA'
partitions=1/1
avgRowSize=29.0
numNodes=0
cardinality: 1
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 7(F04)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 24: o_orderkey
OutPut Exchange Id: 07

6:Project
|  output columns:
|  24 <-> [24: o_orderkey, INT, true]
|  cardinality: 50000000
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5.0E7] ESTIMATE
|
5:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 26: o_orderstatus = 'F'
MIN/MAX PREDICATES: 74: o_orderstatus <= 'F', 75: o_orderstatus >= 'F'
partitions=1/1
avgRowSize=9.0
numNodes=0
cardinality: 50000000
probe runtime filters:
- filter_id = 2, probe_expr = (24: o_orderkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5.0E7] ESTIMATE
* o_orderstatus-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE

PLAN FRAGMENT 8(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 54: l_orderkey
OutPut Exchange Id: 04

3:Project
|  output columns:
|  54 <-> [54: l_orderkey, INT, true]
|  56 <-> [56: l_suppkey, INT, true]
|  cardinality: 300018951
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|
2:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 66: l_receiptdate > 65: l_commitdate
partitions=1/1
avgRowSize=20.0
numNodes=0
cardinality: 300018951
probe runtime filters:
- filter_id = 3, probe_expr = (54: l_orderkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_commitdate-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* l_receiptdate-->[6.94368E8, 9.150336E8, 0.0, 4.0, 2554.0] ESTIMATE

PLAN FRAGMENT 9(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 37: l_orderkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 37: l_orderkey IS NOT NULL
partitions=1/1
avgRowSize=12.0
numNodes=0
cardinality: 600037902
probe runtime filters:
- filter_id = 4, probe_expr = (37: l_orderkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
[end]

