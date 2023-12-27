[fragment statistics]
PLAN FRAGMENT 0(F04)
Output Exprs:25: L_SHIPMODE | 30: sum | 31: sum
Input Partition: UNPARTITIONED
RESULT SINK

10:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 2
column statistics:
* L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
* sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
* sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE

PLAN FRAGMENT 1(F03)

Input Partition: HASH_PARTITIONED: 25: L_SHIPMODE
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:SORT
|  order by: [25, VARCHAR, false] ASC
|  offset: 0
|  cardinality: 2
|  column statistics:
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
8:AGGREGATE (merge finalize)
|  aggregate: sum[([30: sum, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], sum[([31: sum, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  group by: [25: L_SHIPMODE, VARCHAR, false]
|  cardinality: 2
|  column statistics:
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
7:EXCHANGE
distribution type: SHUFFLE
partition exprs: [25: L_SHIPMODE, VARCHAR, false]
cardinality: 2

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 25: L_SHIPMODE
OutPut Exchange Id: 07

6:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([28: case, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], sum[([29: case, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  group by: [25: L_SHIPMODE, VARCHAR, false]
|  cardinality: 2
|  column statistics:
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
5:Project
|  output columns:
|  25 <-> [25: L_SHIPMODE, VARCHAR, false]
|  28 <-> if[((6: O_ORDERPRIORITY = '1-URGENT') OR (6: O_ORDERPRIORITY = '2-HIGH'), 1, 0); args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  29 <-> if[((6: O_ORDERPRIORITY != '1-URGENT') AND (6: O_ORDERPRIORITY != '2-HIGH'), 1, 0); args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  cardinality: 6508504
|  column statistics:
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: O_ORDERKEY, INT, false] = [11: L_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (11: L_ORDERKEY), remote = false
|  output columns: 6, 25
|  cardinality: 6508504
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6508504.027934344] ESTIMATE
|  * O_ORDERPRIORITY-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6508504.027934344] ESTIMATE
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [11: L_ORDERKEY, INT, false]
|       cardinality: 6508504
|
0:OlapScanNode
table: orders, rollup: orders
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=23.0
cardinality: 150000000
probe runtime filters:
- filter_id = 0, probe_expr = (1: O_ORDERKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* O_ORDERPRIORITY-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 11: L_ORDERKEY
OutPut Exchange Id: 03

2:Project
|  output columns:
|  11 <-> [11: L_ORDERKEY, INT, false]
|  25 <-> [25: L_SHIPMODE, CHAR, false]
|  cardinality: 6508504
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6508504.027934344] ESTIMATE
|  * L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: 25: L_SHIPMODE IN ('REG AIR', 'MAIL'), [22: L_COMMITDATE, DATE, false] < [23: L_RECEIPTDATE, DATE, false], [21: L_SHIPDATE, DATE, false] < [22: L_COMMITDATE, DATE, false], [23: L_RECEIPTDATE, DATE, false] >= '1997-01-01', [23: L_RECEIPTDATE, DATE, false] < '1998-01-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=30.0
cardinality: 6508504
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6508504.027934344] ESTIMATE
* L_SHIPDATE-->[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE
* L_COMMITDATE-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* L_RECEIPTDATE-->[8.52048E8, 8.83584E8, 0.0, 4.0, 2554.0] ESTIMATE
* L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
[end]

