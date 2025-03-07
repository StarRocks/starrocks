[fragment statistics]
PLAN FRAGMENT 0(F01)
Output Exprs:19: sum
Input Partition: UNPARTITIONED
RESULT SINK

4:AGGREGATE (merge finalize)
|  aggregate: sum[([19: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[NaN, NaN, 0.0, 8.0, 1.0] ESTIMATE
|
3:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  aggregate: sum[([6: L_EXTENDEDPRICE, DOUBLE, false] * [7: L_DISCOUNT, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[NaN, NaN, 0.0, 8.0, 1.0] ESTIMATE
|
1:Project
|  output columns:
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  cardinality: 11504008
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[NaN, NaN, 0.0, 8.0, 11.0] MCV: [[0.02:54617300][0.04:54554500][0.03:54529300]] ESTIMATE
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [11: L_SHIPDATE, DATE, false] >= '1995-01-01', [11: L_SHIPDATE, DATE, false] < '1996-01-01', [7: L_DISCOUNT, DOUBLE, false] >= 0.02, [7: L_DISCOUNT, DOUBLE, false] <= 0.04, [5: L_QUANTITY, DOUBLE, false] < 24.0
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=36.0
cardinality: 11504008
column statistics:
* L_QUANTITY-->[NaN, NaN, 0.0, 8.0, 50.0] MCV: [[23.00:12059300][16.00:12051800][9.00:12050300][1.00:12040100][19.00:12036300]] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[NaN, NaN, 0.0, 8.0, 11.0] MCV: [[0.02:54617300][0.04:54554500][0.03:54529300]] ESTIMATE
* L_SHIPDATE-->[7.888896E8, 8.204256E8, 0.0, 4.0, 2526.0] MCV: [[1995-09-18:267300][1995-09-26:265700][1995-05-30:263000][1995-04-12:262400][1995-02-21:261100]] ESTIMATE
* expr-->[NaN, NaN, 0.0, 8.0, 932377.0] ESTIMATE
[end]