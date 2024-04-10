[fragment statistics]
PLAN FRAGMENT 0(F09)
Output Exprs:29: substring | 30: count | 31: sum
Input Partition: UNPARTITIONED
RESULT SINK

19:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 1500000
column statistics:
* substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
* count-->[0.0, 1500000.0, 0.0, 8.0, 1500000.0] ESTIMATE
* sum-->[-1380.4847206423183, 13804.97145129049, 0.0, 8.0, 1086564.0] ESTIMATE

PLAN FRAGMENT 1(F08)

Input Partition: HASH_PARTITIONED: 29: substring
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 19

18:SORT
|  order by: [29, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 1500000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 1500000.0] ESTIMATE
|  * sum-->[-1380.4847206423183, 13804.97145129049, 0.0, 8.0, 1086564.0] ESTIMATE
|
17:AGGREGATE (merge finalize)
|  aggregate: count[([30: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false], sum[([31: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [29: substring, VARCHAR, true]
|  cardinality: 1500000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 1500000.0] ESTIMATE
|  * sum-->[-1380.4847206423183, 13804.97145129049, 0.0, 8.0, 1086564.0] ESTIMATE
|
16:EXCHANGE
distribution type: SHUFFLE
partition exprs: [29: substring, VARCHAR, true]
cardinality: 1500000

PLAN FRAGMENT 2(F07)

Input Partition: HASH_PARTITIONED: 20: o_custkey
OutPut Partition: HASH_PARTITIONED: 29: substring
OutPut Exchange Id: 16

15:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false], sum[([6: c_acctbal, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [29: substring, VARCHAR, true]
|  cardinality: 1500000
|  column statistics:
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * count-->[0.0, 1500000.0, 0.0, 8.0, 1500000.0] ESTIMATE
|  * sum-->[-1380.4847206423183, 13804.97145129049, 0.0, 8.0, 1086564.0] ESTIMATE
|
14:Project
|  output columns:
|  6 <-> [6: c_acctbal, DECIMAL64(15,2), true]
|  29 <-> substring[([5: c_phone, VARCHAR, true], 1, 2); args: VARCHAR,INT,INT; result: VARCHAR; args nullable: true; result nullable: true]
|  cardinality: 1500000
|  column statistics:
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|
13:HASH JOIN
|  join op: RIGHT ANTI JOIN (PARTITIONED)
|  equal join conjunct: [20: o_custkey, INT, true] = [1: c_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: c_custkey), remote = true
|  output columns: 5, 6
|  cardinality: 1500000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * substring-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|
|----12:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [1: c_custkey, INT, true]
|       cardinality: 3750000
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [20: o_custkey, INT, true]
cardinality: 150000000

PLAN FRAGMENT 3(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Exchange Id: 12

11:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  5 <-> [5: c_phone, VARCHAR, true]
|  6 <-> [6: c_acctbal, DECIMAL64(15,2), true]
|  cardinality: 3750000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|
10:NESTLOOP JOIN
|  join op: INNER JOIN
|  other join predicates: cast([6: c_acctbal, DECIMAL64(15,2), true] as DECIMAL128(38,8)) > [17: avg, DECIMAL128(38,8), true]
|  cardinality: 3750000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3750000.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 3750000.0] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
|----9:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
2:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: substring(5: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
partitions=1/1
avgRowSize=31.0
cardinality: 7500000
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 7500000.0] ESTIMATE
* c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7500000.0] ESTIMATE
* c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE

PLAN FRAGMENT 4(F04)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 09

8:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  cardinality: 1
|  column statistics:
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
7:AGGREGATE (merge finalize)
|  aggregate: avg[([17: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
6:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 5(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:AGGREGATE (update serialize)
|  aggregate: avg[([14: c_acctbal, DECIMAL64(15,2), true]); args: DECIMAL64; result: VARBINARY; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * avg-->[0.0, 9999.99, 0.0, 8.0, 1.0] ESTIMATE
|
4:Project
|  output columns:
|  14 <-> [14: c_acctbal, DECIMAL64(15,2), true]
|  cardinality: 6818187
|  column statistics:
|  * c_acctbal-->[0.0, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|
3:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 14: c_acctbal > 0.00, substring(13: c_phone, 1, 2) IN ('21', '28', '24', '32', '35', '34', '37')
MIN/MAX PREDICATES: 14: c_acctbal > 0.00
partitions=1/1
avgRowSize=23.0
cardinality: 6818187
column statistics:
* c_phone-->[-Infinity, Infinity, 0.0, 15.0, 6818187.396704358] ESTIMATE
* c_acctbal-->[0.0, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE

PLAN FRAGMENT 6(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 20: o_custkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: orders
partitions=1/1
avgRowSize=8.0
cardinality: 150000000
probe runtime filters:
- filter_id = 0, probe_expr = (20: o_custkey)
column statistics:
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
[end]