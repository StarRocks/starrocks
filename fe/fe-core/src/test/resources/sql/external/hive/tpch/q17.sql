[fragment statistics]
PLAN FRAGMENT 0(F04)
Output Exprs:46: expr
Input Partition: UNPARTITIONED
RESULT SINK

14:Project
|  output columns:
|  46 <-> [45: sum, DECIMAL128(38,2), true] / 7.0
|  cardinality: 1
|  column statistics:
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 16.0, 1.0] ESTIMATE
|
13:AGGREGATE (merge finalize)
|  aggregate: sum[([45: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 16.0, 1.0] ESTIMATE
|
12:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F03)

Input Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 12

11:AGGREGATE (update serialize)
|  aggregate: sum[([6: l_extendedprice, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
10:Project
|  output columns:
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  cardinality: 300019
|  column statistics:
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 300018.951] ESTIMATE
|
9:SELECT
                                                                                                                                                                                                                                                                            |  predicates: CAST(5: l_quantity AS DECIMAL128(38,9)) < 0.2 * 47: avg
                                                                                                                                                                                                                                                                            |  cardinality: 300019
                                                                                                                                                                                                                                                                            |  column statistics:
                                                                                                                                                                                                                                                                            |  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
                                                                                                                                                                                                                                                                            |  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
                                                                                                                                                                                                                                                                            |  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 300018.951] ESTIMATE
                                                                                                                                                                                                                                                                            |  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
                                                                                                                                                                                                                                                                            |  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
                                                                                                                                                                                                                                                                            |
                                                                                                                                                                                                                                                                            8:ANALYTIC
                                                                                                                                                                                                                                                                            |  functions: [, avg[([5: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true], ]
|  partition by: [17: p_partkey, INT, true]
|  cardinality: 600038
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 600037.902] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
7:SORT
|  order by: [17, INT, true] ASC
|  offset: 0
|  cardinality: 600038
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 600037.902] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|
6:EXCHANGE
distribution type: SHUFFLE
partition exprs: [17: p_partkey, INT, true]
cardinality: 600038

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Exchange Id: 06

5:Project
|  output columns:
|  5 <-> [5: l_quantity, DECIMAL64(15,2), true]
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  17 <-> [17: p_partkey, INT, true]
|  cardinality: 600038
|  column statistics:
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 600037.902] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: l_partkey, INT, true] = [17: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (17: p_partkey), remote = false
|  output columns: 5, 6, 17
|  cardinality: 600038
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 600037.902] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 20000
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 2: l_partkey IS NOT NULL
partitions=1/1
avgRowSize=24.0
cardinality: 600037902
probe runtime filters:
- filter_id = 0, probe_expr = (2: l_partkey)
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:Project
|  output columns:
|  17 <-> [17: p_partkey, INT, true]
|  cardinality: 20000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|
1:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 20: p_brand = 'Brand#35', 23: p_container = 'JUMBO CASE'
MIN/MAX PREDICATES: 20: p_brand <= 'Brand#35', 20: p_brand >= 'Brand#35', 23: p_container <= 'JUMBO CASE', 23: p_container >= 'JUMBO CASE'
partitions=1/1
avgRowSize=28.0
cardinality: 20000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
* p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* p_container-->[-Infinity, Infinity, 0.0, 10.0, 40.0] ESTIMATE
[end]

