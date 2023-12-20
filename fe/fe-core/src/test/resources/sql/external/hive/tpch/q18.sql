[fragment statistics]
PLAN FRAGMENT 0(F10)
Output Exprs:2: c_name | 1: c_custkey | 9: o_orderkey | 13: o_orderdate | 12: o_totalprice | 52: sum
Input Partition: UNPARTITIONED
RESULT SINK

20:MERGING-EXCHANGE
distribution type: GATHER
limit: 100
cardinality: 100
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
* c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
* o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
* o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
* sum-->[1.0, 6.000366459206501E8, 0.0, 8.0, 50.0] ESTIMATE

PLAN FRAGMENT 1(F09)

Input Partition: HASH_PARTITIONED: 34: l_orderkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:TOP-N
|  order by: [12, DECIMAL64(15,2), true] DESC, [13, DATE, true] ASC
|  offset: 0
|  limit: 100
|  cardinality: 100
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * sum-->[1.0, 6.000366459206501E8, 0.0, 8.0, 50.0] ESTIMATE
|
18:AGGREGATE (update finalize)
|  aggregate: sum[([22: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [2: c_name, VARCHAR, true], [1: c_custkey, INT, true], [9: o_orderkey, INT, true], [13: o_orderdate, DATE, true], [12: o_totalprice, DECIMAL64(15,2), true]
|  cardinality: 600036646
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * sum-->[1.0, 6.000366459206501E8, 0.0, 8.0, 50.0] ESTIMATE
|
17:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  2 <-> [2: c_name, VARCHAR, true]
|  9 <-> [9: o_orderkey, INT, true]
|  12 <-> [12: o_totalprice, DECIMAL64(15,2), true]
|  13 <-> [13: o_orderdate, DATE, true]
|  22 <-> [22: l_quantity, DECIMAL64(15,2), true]
|  cardinality: 600036646
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
16:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [18: l_orderkey, INT, true] = [9: o_orderkey, INT, true]
|  output columns: 1, 2, 9, 12, 13, 22
|  cardinality: 600036646
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|
|----15:Project
|    |  output columns:
|    |  1 <-> [1: c_custkey, INT, true]
|    |  2 <-> [2: c_name, VARCHAR, true]
|    |  9 <-> [9: o_orderkey, INT, true]
|    |  12 <-> [12: o_totalprice, DECIMAL64(15,2), true]
|    |  13 <-> [13: o_orderdate, DATE, true]
|    |  cardinality: 149999686
|    |  column statistics:
|    |  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|    |  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|    |  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|    |  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|    |  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|    |
|    14:HASH JOIN
|    |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE(S))
|    |  equal join conjunct: [9: o_orderkey, INT, true] = [34: l_orderkey, INT, true]
|    |  output columns: 1, 2, 9, 12, 13
|    |  cardinality: 149999686
|    |  column statistics:
|    |  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|    |  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|    |  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|    |  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|    |  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|    |  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|    |
|    |----13:Project
|    |    |  output columns:
|    |    |  34 <-> [34: l_orderkey, INT, true]
|    |    |  cardinality: 149999686
|    |    |  column statistics:
|    |    |  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|    |    |
|    |    12:AGGREGATE (merge finalize)
|    |    |  aggregate: sum[([50: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|    |    |  group by: [34: l_orderkey, INT, true]
|    |    |  having: [50: sum, DECIMAL128(38,2), true] > 315
|    |    |  cardinality: 149999686
|    |    |  column statistics:
|    |    |  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.499996859999979E8] ESTIMATE
|    |    |  * sum-->[315.0, 1.5E8, 0.0, 8.0, 50.0] ESTIMATE
|    |    |
|    |    11:EXCHANGE
|    |       distribution type: SHUFFLE
|    |       partition exprs: [34: l_orderkey, INT, true]
|    |       cardinality: 150000000
|    |
|    8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [9: o_orderkey, INT, true]
|       cardinality: 150000000
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [18: l_orderkey, INT, true]
cardinality: 600037902

PLAN FRAGMENT 2(F08)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 34: l_orderkey
OutPut Exchange Id: 11

10:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([38: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [34: l_orderkey, INT, true]
|  cardinality: 150000000
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * sum-->[1.0, 1.5E8, 0.0, 8.0, 50.0] ESTIMATE
|
9:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 34: l_orderkey IS NOT NULL
partitions=1/1
avgRowSize=16.0
cardinality: 600037902
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE

PLAN FRAGMENT 3(F06)

Input Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Partition: HASH_PARTITIONED: 9: o_orderkey
OutPut Exchange Id: 08

7:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  2 <-> [2: c_name, VARCHAR, true]
|  9 <-> [9: o_orderkey, INT, true]
|  12 <-> [12: o_totalprice, DECIMAL64(15,2), true]
|  13 <-> [13: o_orderdate, DATE, true]
|  cardinality: 150000000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: c_custkey), remote = true
|  output columns: 1, 2, 9, 12, 13
|  cardinality: 150000000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [1: c_custkey, INT, true]
|       cardinality: 15000000
|
3:EXCHANGE
distribution type: SHUFFLE
partition exprs: [10: o_custkey, INT, true]
cardinality: 150000000

PLAN FRAGMENT 4(F04)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Exchange Id: 05

4:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 1: c_custkey IS NOT NULL
partitions=1/1
avgRowSize=33.0
cardinality: 15000000
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE

PLAN FRAGMENT 5(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Exchange Id: 03

2:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 10: o_custkey IS NOT NULL
partitions=1/1
avgRowSize=28.0
cardinality: 150000000
probe runtime filters:
- filter_id = 0, probe_expr = (10: o_custkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* o_totalprice-->[811.73, 591036.15, 0.0, 8.0, 3.469658E7] ESTIMATE
* o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE

PLAN FRAGMENT 6(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: l_orderkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 18: l_orderkey IS NOT NULL
partitions=1/1
avgRowSize=16.0
cardinality: 600037902
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
[end]

