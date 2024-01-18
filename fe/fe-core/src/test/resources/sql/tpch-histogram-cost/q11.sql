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
PLAN FRAGMENT 0(F12)
Output Exprs:1: PS_PARTKEY | 21: sum
Input Partition: UNPARTITIONED
RESULT SINK

30:MERGING-EXCHANGE
cardinality: 1600000
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
* sum-->[1.0, 3.204037490987743E8, 0.0, 8.0, 99864.0] ESTIMATE
* expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 30

29:SORT
|  order by: [21, DOUBLE, true] DESC
|  offset: 0
|  cardinality: 1600000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 8.0, 99864.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE
|
28:Project
|  output columns:
|  1 <-> [1: PS_PARTKEY, INT, false]
|  21 <-> [21: sum, DOUBLE, true]
|  cardinality: 1600000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 8.0, 99864.0] ESTIMATE
|
27:NESTLOOP JOIN
|  join op: INNER JOIN
|  other join predicates: [21: sum, DOUBLE, true] > [43: expr, DOUBLE, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (43: expr), remote = false
|  cardinality: 1600000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1600000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 8.0, 99864.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE
|
|----26:EXCHANGE
|       cardinality: 1
|
10:AGGREGATE (update finalize)
|  aggregate: sum[([20: expr, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [1: PS_PARTKEY, INT, false]
|  cardinality: 3200000
|  probe runtime filters:
|  - filter_id = 4, probe_expr = (21: sum)
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * sum-->[1.0, 3.204037490987743E8, 0.0, 8.0, 99864.0] ESTIMATE
|
9:Project
|  output columns:
|  1 <-> [1: PS_PARTKEY, INT, false]
|  20 <-> [4: PS_SUPPLYCOST, DOUBLE, false] * cast([3: PS_AVAILQTY, INT, false] as DOUBLE)
|  cardinality: 3200000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: PS_SUPPKEY, INT, false] = [7: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (7: S_SUPPKEY), remote = false
|  output columns: 1, 3, 4
|  cardinality: 3200000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 3200000.0] ESTIMATE
|  * PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----7:EXCHANGE
|       cardinality: 40000
|
0:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=28.0
cardinality: 80000000
probe runtime filters:
- filter_id = 1, probe_expr = (2: PS_SUPPKEY)
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
* PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 2(F10)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 26

25:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE
|
24:Project
|  output columns:
|  43 <-> [42: sum, DOUBLE, true] * 1.0E-4
|  cardinality: 1
|  column statistics:
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE
|
23:AGGREGATE (merge finalize)
|  aggregate: sum[([42: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[1.0E-4, 999.9000000000001, 0.0, 8.0, 1.0] ESTIMATE
|
22:EXCHANGE
cardinality: 1

PLAN FRAGMENT 3(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 22

21:AGGREGATE (update serialize)
|  aggregate: sum[([41: expr, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[1.0, 9999000.0, 0.0, 8.0, 1.0] ESTIMATE
|
20:Project
|  output columns:
|  41 <-> [25: PS_SUPPLYCOST, DOUBLE, false] * cast([24: PS_AVAILQTY, INT, false] as DOUBLE)
|  cardinality: 3200000
|  column statistics:
|  * expr-->[1.0, 9999000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [23: PS_SUPPKEY, INT, false] = [28: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (28: S_SUPPKEY), remote = false
|  output columns: 24, 25
|  cardinality: 3200000
|  column statistics:
|  * PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * expr-->[1.0, 9999000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----18:EXCHANGE
|       cardinality: 40000
|
11:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=20.0
cardinality: 80000000
probe runtime filters:
- filter_id = 3, probe_expr = (23: PS_SUPPKEY)
column statistics:
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
* PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 4(F06)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:Project
|  output columns:
|  28 <-> [28: S_SUPPKEY, INT, false]
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [31: S_NATIONKEY, INT, false] = [36: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (36: N_NATIONKEY), remote = false
|  output columns: 28
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----15:EXCHANGE
|       cardinality: 1
|
12:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 2, probe_expr = (31: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 5(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:Project
|  output columns:
|  36 <-> [36: N_NATIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
13:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: [37: N_NAME, CHAR, false] = 'PERU'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 6(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  7 <-> [7: S_SUPPKEY, INT, false]
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [10: S_NATIONKEY, INT, false] = [15: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (15: N_NATIONKEY), remote = false
|  output columns: 7
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----4:EXCHANGE
|       cardinality: 1
|
1:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 0, probe_expr = (10: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 7(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 04

3:Project
|  output columns:
|  15 <-> [15: N_NATIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
2:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: [16: N_NAME, CHAR, false] = 'PERU'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[end]

