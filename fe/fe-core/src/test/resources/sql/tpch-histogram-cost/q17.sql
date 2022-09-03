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
Output Exprs:49: expr
Input Partition: UNPARTITIONED
RESULT SINK

16:Project
|  output columns:
|  49 <-> [48: sum, DOUBLE, true] / 7.0
|  cardinality: 1
|  column statistics:
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 8.0, 1.0] ESTIMATE
|
15:AGGREGATE (merge finalize)
|  aggregate: sum[([48: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[128.71428571428572, 14992.785714285714, 0.0, 8.0, 1.0] ESTIMATE
|
14:EXCHANGE
cardinality: 1

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 14

13:AGGREGATE (update serialize)
|  aggregate: sum[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
12:Project
|  output columns:
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  cardinality: 307009
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 307008.56999999995] ESTIMATE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [2: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]
|  other join predicates: [5: L_QUANTITY, DOUBLE, false] < 0.2 * [45: avg, DOUBLE, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (18: P_PARTKEY), remote = false
|  output columns: 6
|  cardinality: 307009
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.237999999998] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 307008.56999999995] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.237999999998] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
|----10:EXCHANGE
|       cardinality: 20467
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=24.0
cardinality: 600000000
probe runtime filters:
- filter_id = 1, probe_expr = (2: L_PARTKEY)
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE

PLAN FRAGMENT 2(F02)

Input Partition: HASH_PARTITIONED: 29: L_PARTKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:Project
|  output columns:
|  18 <-> [18: P_PARTKEY, INT, false]
|  45 <-> [45: avg, DOUBLE, true]
|  cardinality: 20467
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.237999999998] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [29: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = true
|  output columns: 18, 45
|  cardinality: 20467
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.237999999998] ESTIMATE
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.237999999998] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
|----7:EXCHANGE
|       cardinality: 20467
|
4:AGGREGATE (merge finalize)
|  aggregate: avg[([45: avg, VARCHAR, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [29: L_PARTKEY, INT, false]
|  cardinality: 20000000
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
3:EXCHANGE
cardinality: 20000000
probe runtime filters:
- filter_id = 0, probe_expr = (29: L_PARTKEY)

PLAN FRAGMENT 3(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: P_PARTKEY
OutPut Exchange Id: 07

6:Project
|  output columns:
|  18 <-> [18: P_PARTKEY, INT, false]
|  cardinality: 20467
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
|
5:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: [21: P_BRAND, CHAR, false] = 'Brand#35', [24: P_CONTAINER, CHAR, false] = 'JUMBO CASE'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=28.0
cardinality: 20467
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 20467.238] ESTIMATE
* P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 1.0] ESTIMATE
* P_CONTAINER-->[-Infinity, Infinity, 0.0, 10.0, 1.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 29: L_PARTKEY
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: avg[([32: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: VARCHAR; args nullable: false; result nullable: true]
|  group by: [29: L_PARTKEY, INT, false]
|  cardinality: 20000000
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=16.0
cardinality: 600000000
probe runtime filters:
- filter_id = 0, probe_expr = (29: L_PARTKEY)
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
[end]

