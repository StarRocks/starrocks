[sql]
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
        l_partkey = p_partkey
  and l_shipdate >= date '1997-02-01'
  and l_shipdate < date '1997-03-01';
[fragment statistics]
PLAN FRAGMENT 0(F03)
Output Exprs:32: expr
Input Partition: UNPARTITIONED
RESULT SINK

9:Project
|  output columns:
|  32 <-> 100.0 * [30: sum, DOUBLE, true] / [31: sum, DOUBLE, true]
|  cardinality: 1
|  column statistics:
|  * expr-->[-Infinity, Infinity, 0.0, 8.0, 1.0] ESTIMATE
|
8:AGGREGATE (merge finalize)
|  aggregate: sum[([30: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([31: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|  * expr-->[-Infinity, Infinity, 0.0, 8.0, 1.0] ESTIMATE
|
7:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:AGGREGATE (update serialize)
|  aggregate: sum[(if[(22: P_TYPE LIKE 'PROMO%', [34: multiply, DOUBLE, true], 0.0); args: BOOLEAN,DOUBLE,DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([29: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
5:Project
|  output columns:
|  22 <-> [22: P_TYPE, VARCHAR, false]
|  29 <-> [34: multiply, DOUBLE, false]
|  34 <-> clone([34: multiply, DOUBLE, false])
|  common expressions:
|  33 <-> 1.0 - [7: L_DISCOUNT, DOUBLE, false]
|  34 <-> [6: L_EXTENDEDPRICE, DOUBLE, false] * [33: subtract, DOUBLE, false]
|  cardinality: 7013947
|  column statistics:
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [18: P_PARTKEY, INT, false] = [2: L_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (2: L_PARTKEY), remote = false
|  output columns: 6, 7, 22
|  cardinality: 7013947
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 7013946.675798152] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 7013946.675798152] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 932378.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [2: L_PARTKEY, INT, false]
|       cardinality: 7013947
|
0:OlapScanNode
table: part, rollup: part
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=33.0
cardinality: 20000000
probe runtime filters:
- filter_id = 0, probe_expr = (18: P_PARTKEY)
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE

PLAN FRAGMENT 2(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 2: L_PARTKEY
OutPut Exchange Id: 03

2:Project
|  output columns:
|  2 <-> [2: L_PARTKEY, INT, false]
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  cardinality: 7013947
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 7013946.675798152] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [11: L_SHIPDATE, DATE, false] >= '1997-02-01', [11: L_SHIPDATE, DATE, false] < '1997-03-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=28.0
cardinality: 7013947
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 7013946.675798152] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[8.547264E8, 8.571456E8, 0.0, 4.0, 2526.0] ESTIMATE
[end]

