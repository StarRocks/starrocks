[fragment statistics]
PLAN FRAGMENT 0(F05)
Output Exprs:20: L_ORDERKEY | 38: sum | 14: O_ORDERDATE | 17: O_SHIPPRIORITY
Input Partition: UNPARTITIONED
RESULT SINK

13:MERGING-EXCHANGE
distribution type: GATHER
limit: 10
cardinality: 10
column statistics:
* O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
* O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
* sum-->[810.9, 5290372.920961436, 0.0, 8.0, 932377.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:TOP-N
|  order by: [38, DOUBLE, true] DESC, [14, DATE, false] ASC
|  offset: 0
|  limit: 10
|  cardinality: 10
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * sum-->[810.9, 5290372.920961436, 0.0, 8.0, 932377.0] ESTIMATE
|
11:AGGREGATE (update finalize)
|  aggregate: sum[([37: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [20: L_ORDERKEY, INT, false], [14: O_ORDERDATE, DATE, false], [17: O_SHIPPRIORITY, INT, false]
|  cardinality: 46999957
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * sum-->[810.9, 5290372.920961436, 0.0, 8.0, 932377.0] ESTIMATE
|
10:Project
|  output columns:
|  14 <-> [14: O_ORDERDATE, DATE, false]
|  17 <-> [17: O_SHIPPRIORITY, INT, false]
|  20 <-> [20: L_ORDERKEY, INT, false]
|  37 <-> [25: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 46999957
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (10: O_ORDERKEY), remote = false
|  output columns: 14, 17, 20, 25, 26
|  cardinality: 46999957
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [10: O_ORDERKEY, INT, false]
|       cardinality: 21799209
|
1:Project
|  output columns:
|  20 <-> [20: L_ORDERKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 323405941
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [30: L_SHIPDATE, DATE, false] > '1995-03-11'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=28.0
cardinality: 323405941
probe runtime filters:
- filter_id = 1, probe_expr = (20: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[7.948512E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE

PLAN FRAGMENT 2(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 10: O_ORDERKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  10 <-> [10: O_ORDERKEY, INT, false]
|  14 <-> [14: O_ORDERDATE, DATE, false]
|  17 <-> [17: O_SHIPPRIORITY, INT, false]
|  cardinality: 21799209
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [11: O_CUSTKEY, INT, false] = [1: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: C_CUSTKEY), remote = false
|  output columns: 10, 14, 17
|  cardinality: 21799209
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.1799208766687468E7] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 3000000
|
2:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [14: O_ORDERDATE, DATE, false] < '1995-03-11'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=24.0
cardinality: 72661123
probe runtime filters:
- filter_id = 0, probe_expr = (11: O_CUSTKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 7.266112266112266E7] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
* O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0] ESTIMATE
* O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE

PLAN FRAGMENT 3(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 05

4:Project
|  output columns:
|  1 <-> [1: C_CUSTKEY, INT, false]
|  cardinality: 3000000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|
3:OlapScanNode
table: customer, rollup: customer
preAggregation: on
Predicates: [7: C_MKTSEGMENT, CHAR, false] = 'HOUSEHOLD'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=18.0
cardinality: 3000000
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
* C_MKTSEGMENT-->[-Infinity, Infinity, 0.0, 10.0, 5.0] ESTIMATE
[end]