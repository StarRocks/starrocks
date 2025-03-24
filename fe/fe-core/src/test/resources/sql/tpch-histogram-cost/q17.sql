[fragment statistics]
PLAN FRAGMENT 0(F04)
Output Exprs:49: expr
Input Partition: UNPARTITIONED
RESULT SINK

14:Project
|  output columns:
|  49 <-> [48: sum, DOUBLE, true] / 7.0
|  cardinality: 1
|  column statistics:
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 8.0, 1.0] ESTIMATE
|
13:AGGREGATE (merge finalize)
|  aggregate: sum[([48: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 8.0, 1.0] ESTIMATE
|
12:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F03)

Input Partition: HASH_PARTITIONED: 18: P_PARTKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 12

11:AGGREGATE (update serialize)
|  aggregate: sum[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
10:Project
|  output columns:
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  cardinality: 307009
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 307008.57] ESTIMATE
|
9:SELECT
|  predicates: 5: L_QUANTITY < 0.2 * 50: avg
|  cardinality: 307009
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 307008.57] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
8:ANALYTIC
|  functions: [, avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], ]
|  partition by: [18: P_PARTKEY, INT, false]
|  cardinality: 614017
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 614017.14] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
7:SORT
|  order by: [18, INT, false] ASC
|  analytic partition by: [18: P_PARTKEY, INT, false]
|  offset: 0
|  cardinality: 614017
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 614017.14] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|
6:EXCHANGE
distribution type: SHUFFLE
partition exprs: [18: P_PARTKEY, INT, false]
cardinality: 614017

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: P_PARTKEY
OutPut Exchange Id: 06

5:Project
|  output columns:
|  5 <-> [5: L_QUANTITY, DOUBLE, false]
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  18 <-> [18: P_PARTKEY, INT, false]
|  cardinality: 614017
|  column statistics:
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 614017.14] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = false
|  output columns: 5, 6, 18
|  cardinality: 614017
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 614017.14] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 20467
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=24.0
cardinality: 600000000
probe runtime filters:
- filter_id = 0, probe_expr = (2: L_PARTKEY)
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] MCV: [[35.00:12075300][25.00:12063500][32.00:12063000][23.00:12059300][16.00:12051800]] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:Project
|  output columns:
|  18 <-> [18: P_PARTKEY, INT, false]
|  cardinality: 20467
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|
1:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: [21: P_BRAND, CHAR, false] = 'Brand#35', [24: P_CONTAINER, CHAR, false] = 'JUMBO CASE'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=28.0
cardinality: 20467
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
* P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] MCV: [[Brand#35:823300][Brand#12:816700][Brand#52:815800][Brand#33:814100][Brand#53:808800]] ESTIMATE
* P_CONTAINER-->[-Infinity, Infinity, 0.0, 10.0, 40.0] MCV: [[SM DRUM:515300][JUMBO JAR:511500][LG JAR:510300][LG BOX:509600][MED CAN:509100]] ESTIMATE
[end]