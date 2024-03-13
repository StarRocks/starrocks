[fragment statistics]
PLAN FRAGMENT 0(F02)
Output Exprs:9: L_RETURNFLAG | 10: L_LINESTATUS | 20: sum | 21: sum | 22: sum | 23: sum | 24: avg | 25: avg | 26: avg | 27: count
Input Partition: UNPARTITIONED
RESULT SINK

6:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 3
column statistics:
* L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
* L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
* sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
* sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
* sum-->[810.9, 113345.46000000002, 0.0, 8.0, 3.375] ESTIMATE
* avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
* avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
* avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
* count-->[0.0, 6.0E8, 0.0, 8.0, 3.375] ESTIMATE

PLAN FRAGMENT 1(F01)

Input Partition: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:SORT
|  order by: [9, VARCHAR, false] ASC, [10, VARCHAR, false] ASC
|  offset: 0
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.0E8, 0.0, 8.0, 3.375] ESTIMATE
|
4:AGGREGATE (merge finalize)
|  aggregate: sum[([20: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([21: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([22: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([23: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([24: avg, VARBINARY, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([25: avg, VARBINARY, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([26: avg, VARBINARY, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], count[([27: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [9: L_RETURNFLAG, VARCHAR, false], [10: L_LINESTATUS, VARCHAR, false]
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.0E8, 0.0, 8.0, 3.375] ESTIMATE
|
3:EXCHANGE
distribution type: SHUFFLE
partition exprs: [9: L_RETURNFLAG, VARCHAR, false], [10: L_LINESTATUS, VARCHAR, false]
cardinality: 3

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([18: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([19: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: VARBINARY; args nullable: false; result nullable: true], avg[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: VARBINARY; args nullable: false; result nullable: true], avg[([7: L_DISCOUNT, DOUBLE, false]); args: DOUBLE; result: VARBINARY; args nullable: false; result nullable: true], count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [9: L_RETURNFLAG, VARCHAR, false], [10: L_LINESTATUS, VARCHAR, false]
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.0E8, 0.0, 8.0, 3.375] ESTIMATE
|
1:Project
|  output columns:
|  5 <-> [5: L_QUANTITY, DOUBLE, false]
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  9 <-> [9: L_RETURNFLAG, CHAR, false]
|  10 <-> [10: L_LINESTATUS, CHAR, false]
|  18 <-> [29: multiply, DOUBLE, false]
|  19 <-> [29: multiply, DOUBLE, false] * 1.0 + [8: L_TAX, DOUBLE, false]
|  common expressions:
|  28 <-> 1.0 - [7: L_DISCOUNT, DOUBLE, false]
|  29 <-> [6: L_EXTENDEDPRICE, DOUBLE, false] * [28: subtract, DOUBLE, false]
|  cardinality: 600000000
|  column statistics:
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * expr-->[810.9, 113345.46, 0.0, 8.0, 932377.0] ESTIMATE
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [11: L_SHIPDATE, DATE, false] <= '1998-12-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=54.0
cardinality: 600000000
column statistics:
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_TAX-->[0.0, 0.08, 0.0, 8.0, 9.0] ESTIMATE
* L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
* L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
* L_SHIPDATE-->[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* expr-->[810.9, 113345.46, 0.0, 8.0, 932377.0] ESTIMATE
[end]