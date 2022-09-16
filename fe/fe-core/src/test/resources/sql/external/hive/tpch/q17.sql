[sql]
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#35'
  and p_container = 'JUMBO CASE'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[fragment statistics]
PLAN FRAGMENT 0(F06)
Output Exprs:46: expr
Input Partition: UNPARTITIONED
RESULT SINK

16:Project
|  output columns:
|  46 <-> [45: sum, DECIMAL128(38,2), true] / 7.0
|  cardinality: 1
|  column statistics:
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 16.0, 1.0] ESTIMATE
|
15:AGGREGATE (merge finalize)
|  aggregate: sum[([45: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 16.0, 1.0] ESTIMATE
|
14:EXCHANGE
cardinality: 1

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 14

13:AGGREGATE (update serialize)
|  aggregate: sum[([6: l_extendedprice, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
12:Project
|  output columns:
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  cardinality: 300019
|  column statistics:
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 300018.951] ESTIMATE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: l_partkey, INT, true] = [17: p_partkey, INT, true]
|  other join predicates: cast([5: l_quantity, DECIMAL64(15,2), true] as DECIMAL128(38,9)) < 0.2 * [42: avg, DECIMAL128(38,8), true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (17: p_partkey), remote = false
|  output columns: 6
|  cardinality: 300019
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 300018.951] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
|----10:EXCHANGE
|       cardinality: 20000
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 2: l_partkey IS NOT NULL
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 600037902
probe runtime filters:
- filter_id = 1, probe_expr = (2: l_partkey)
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE

PLAN FRAGMENT 2(F02)

Input Partition: HASH_PARTITIONED: 27: l_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:Project
|  output columns:
|  17 <-> [17: p_partkey, INT, true]
|  42 <-> [42: avg, DECIMAL128(38,8), true]
|  cardinality: 20000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [27: l_partkey, INT, true] = [17: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (17: p_partkey), remote = true
|  output columns: 17, 42
|  cardinality: 20000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
|----7:EXCHANGE
|       cardinality: 20000
|
4:AGGREGATE (merge finalize)
|  aggregate: avg[([42: avg, VARCHAR, true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true]
|  group by: [27: l_partkey, INT, true]
|  cardinality: 20000000
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
3:EXCHANGE
cardinality: 20000000
probe runtime filters:
- filter_id = 0, probe_expr = (27: l_partkey)

PLAN FRAGMENT 3(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Exchange Id: 07

6:Project
|  output columns:
|  17 <-> [17: p_partkey, INT, true]
|  cardinality: 20000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
|
5:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 20: p_brand = 'Brand#35', 23: p_container = 'JUMBO CASE'
MIN/MAX PREDICATES: 47: p_brand <= 'Brand#35', 48: p_brand >= 'Brand#35', 49: p_container <= 'JUMBO CASE', 50: p_container >= 'JUMBO CASE'
partitions=1/1
avgRowSize=28.0
numNodes=0
cardinality: 20000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 20000.0] ESTIMATE
* p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* p_container-->[-Infinity, Infinity, 0.0, 10.0, 40.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 27: l_partkey
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: avg[([30: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: VARCHAR; args nullable: true; result nullable: true]
|  group by: [27: l_partkey, INT, true]
|  cardinality: 20000000
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
1:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 27: l_partkey IS NOT NULL
partitions=1/1
avgRowSize=16.0
numNodes=0
cardinality: 600037902
probe runtime filters:
- filter_id = 0, probe_expr = (27: l_partkey)
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
[end]

