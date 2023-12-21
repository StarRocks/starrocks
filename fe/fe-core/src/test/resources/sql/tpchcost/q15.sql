[fragment statistics]
PLAN FRAGMENT 0(F08)
Output Exprs:1: S_SUPPKEY | 2: S_NAME | 3: S_ADDRESS | 5: S_PHONE | 27: sum
Input Partition: UNPARTITIONED
RESULT SINK

24:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 1
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.072527529100353] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 1.072527529100353] ESTIMATE
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 1.072527529100353] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
* sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 24

23:SORT
|  order by: [1, INT, false] ASC
|  offset: 0
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.072527529100353] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 1.072527529100353] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 1.072527529100353] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
22:Project
|  output columns:
|  1 <-> [1: S_SUPPKEY, INT, false]
|  2 <-> [2: S_NAME, CHAR, false]
|  3 <-> [3: S_ADDRESS, VARCHAR, false]
|  5 <-> [5: S_PHONE, CHAR, false]
|  27 <-> [27: sum, DOUBLE, true]
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.072527529100353] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 1.072527529100353] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 1.072527529100353] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: S_SUPPKEY, INT, false] = [11: L_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (11: L_SUPPKEY), remote = false
|  output columns: 1, 2, 3, 5, 27
|  cardinality: 1
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.072527529100353] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 1.072527529100353] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 1.072527529100353] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
|----20:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [11: L_SUPPKEY, INT, false]
|       cardinality: 1
|
0:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=84.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (1: S_SUPPKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE

PLAN FRAGMENT 2(F02)

Input Partition: HASH_PARTITIONED: 11: L_SUPPKEY
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 11: L_SUPPKEY
OutPut Exchange Id: 20

19:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  27 <-> [27: sum, DOUBLE, true]
|  cardinality: 1
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: sum, DOUBLE, true] = [47: max, DOUBLE, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (47: max), remote = false
|  output columns: 11, 27
|  cardinality: 1
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1.072527529100353] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|  * max-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
5:AGGREGATE (merge finalize)
|  aggregate: sum[([27: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [11: L_SUPPKEY, INT, false]
|  having: 27: sum IS NOT NULL
|  cardinality: 1000000
|  probe runtime filters:
|  - filter_id = 0, probe_expr = (27: sum)
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 932377.0] ESTIMATE
|
4:EXCHANGE
distribution type: SHUFFLE
partition exprs: [11: L_SUPPKEY, INT, false]
cardinality: 1000000

PLAN FRAGMENT 3(F05)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:SELECT
|  predicates: 47: max IS NOT NULL
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
15:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
14:AGGREGATE (merge finalize)
|  aggregate: max[([47: max, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
13:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 4(F04)

Input Partition: HASH_PARTITIONED: 30: L_SUPPKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:AGGREGATE (update serialize)
|  aggregate: max[([46: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 112561.22791531752, 0.0, 8.0, 1.0] ESTIMATE
|
11:Project
|  output columns:
|  46 <-> [46: sum, DOUBLE, true]
|  cardinality: 1000000
|  column statistics:
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 932377.0] ESTIMATE
|
10:AGGREGATE (merge finalize)
|  aggregate: sum[([46: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [30: L_SUPPKEY, INT, false]
|  cardinality: 1000000
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 932377.0] ESTIMATE
|
9:EXCHANGE
distribution type: SHUFFLE
partition exprs: [30: L_SUPPKEY, INT, false]
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
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 932377.0] ESTIMATE
|
7:Project
|  output columns:
|  30 <-> [30: L_SUPPKEY, INT, false]
|  45 <-> [33: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [34: L_DISCOUNT, DOUBLE, false]
|  cardinality: 21861386
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
6:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [38: L_SHIPDATE, DATE, false] >= '1995-07-01', [38: L_SHIPDATE, DATE, false] < '1995-10-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=32.0
cardinality: 21861386
column statistics:
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE

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
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 112561.22791531752, 0.0, 8.0, 932377.0] ESTIMATE
|
2:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  26 <-> [14: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [15: L_DISCOUNT, DOUBLE, false]
|  cardinality: 21861386
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [19: L_SHIPDATE, DATE, false] >= '1995-07-01', [19: L_SHIPDATE, DATE, false] < '1995-10-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=32.0
cardinality: 21861386
column statistics:
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
[end]
