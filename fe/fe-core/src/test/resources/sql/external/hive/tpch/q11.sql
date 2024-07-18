[sql]
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
    ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
    select
    sum(ps_supplycost * ps_availqty) * 0.0001000000
    from
    partsupp,
    supplier,
    nation
    where
    ps_suppkey = s_suppkey
                  and s_nationkey = n_nationkey
                  and n_name = 'PERU'
    )
order by
    value desc ;
[fragment statistics]
PLAN FRAGMENT 0(F13)
Output Exprs:1: ps_partkey | 18: sum
Input Partition: UNPARTITIONED
RESULT SINK

32:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 1600000
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
* sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
* expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE

PLAN FRAGMENT 1(F05)

Input Partition: HASH_PARTITIONED: 1: ps_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 32

31:SORT
|  order by: [18, DECIMAL128(38,2), true] DESC
|  offset: 0
|  cardinality: 1600000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE
|
30:Project
|  output columns:
|  1 <-> [1: ps_partkey, INT, true]
|  18 <-> [18: sum, DECIMAL128(38,2), true]
|  cardinality: 1600000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
|
29:NESTLOOP JOIN
|  join op: INNER JOIN
|  other join predicates: cast([18: sum, DECIMAL128(38,2), true] as DOUBLE) > cast([37: expr, DECIMAL128(38,12), true] as DOUBLE)
|  cardinality: 1600000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE
|
|----28:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
12:AGGREGATE (merge finalize)
|  aggregate: sum[([18: sum, DECIMAL128(38,2), true]); args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [1: ps_partkey, INT, true]
|  cardinality: 3200000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
|
11:EXCHANGE
distribution type: SHUFFLE
partition exprs: [1: ps_partkey, INT, true]
cardinality: 3200000

PLAN FRAGMENT 2(F11)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 28

27:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 16.0, 1.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE
|
26:Project
|  output columns:
|  37 <-> [36: sum, DECIMAL128(38,2), true] * 0.0001000000
|  cardinality: 1
|  column statistics:
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE
|
25:AGGREGATE (merge finalize)
|  aggregate: sum[([36: sum, DECIMAL128(38,2), true]); args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 16.0, 1.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 16.0, 1.0] ESTIMATE
|
24:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 3(F06)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 24

23:AGGREGATE (update serialize)
|  aggregate: sum[(cast([22: ps_supplycost, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast([21: ps_availqty, INT, true] as DECIMAL128(9,0))); args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 16.0, 1.0] ESTIMATE
|
22:Project
|  output columns:
|  21 <-> [21: ps_availqty, INT, true]
|  22 <-> [22: ps_supplycost, DECIMAL64(15,2), true]
|  cardinality: 3200000
|  column statistics:
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [20: ps_suppkey, INT, true] = [24: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (24: s_suppkey), remote = false
|  output columns: 21, 22
|  cardinality: 3200000
|  column statistics:
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 16.0, 99864.0] ESTIMATE
|
|----20:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 40000
|
13:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 20: ps_suppkey IS NOT NULL
partitions=1/1
avgRowSize=20.0
cardinality: 80000000
probe runtime filters:
- filter_id = 3, probe_expr = (20: ps_suppkey)
column statistics:
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
* ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 4(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:Project
|  output columns:
|  24 <-> [24: s_suppkey, INT, true]
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: s_nationkey, INT, true] = [31: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (31: n_nationkey), remote = false
|  output columns: 24
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
14:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 24: s_suppkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 2, probe_expr = (27: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 5(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:Project
|  output columns:
|  31 <-> [31: n_nationkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
15:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 32: n_name = 'PERU'
MIN/MAX PREDICATES: 32: n_name <= 'PERU', 32: n_name >= 'PERU'
partitions=1/1
avgRowSize=29.0
cardinality: 1
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 6(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: ps_partkey
OutPut Exchange Id: 11

10:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([17: expr, DECIMAL128(24,2), true]); args: DECIMAL128; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [1: ps_partkey, INT, true]
|  cardinality: 3200000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 16.0, 99864.0] ESTIMATE
|
9:Project
|  output columns:
|  1 <-> [1: ps_partkey, INT, true]
|  17 <-> cast([4: ps_supplycost, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast([3: ps_availqty, INT, true] as DECIMAL128(9,0))
|  cardinality: 3200000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 16.0, 99864.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: ps_suppkey, INT, true] = [6: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (6: s_suppkey), remote = false
|  output columns: 1, 3, 4
|  cardinality: 3200000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 16.0, 99864.0] ESTIMATE
|
|----7:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 40000
|
0:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 2: ps_suppkey IS NOT NULL
partitions=1/1
avgRowSize=28.0
cardinality: 80000000
probe runtime filters:
- filter_id = 1, probe_expr = (2: ps_suppkey)
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
* ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 7(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  6 <-> [6: s_suppkey, INT, true]
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [9: s_nationkey, INT, true] = [13: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (13: n_nationkey), remote = false
|  output columns: 6
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----4:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
1:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 6: s_suppkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 0, probe_expr = (9: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 8(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 04

3:Project
|  output columns:
|  13 <-> [13: n_nationkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
2:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 14: n_name = 'PERU'
MIN/MAX PREDICATES: 14: n_name <= 'PERU', 14: n_name >= 'PERU'
partitions=1/1
avgRowSize=29.0
cardinality: 1
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[end]