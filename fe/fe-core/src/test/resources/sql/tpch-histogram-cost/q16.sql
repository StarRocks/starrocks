[fragment statistics]
PLAN FRAGMENT 0(F06)
Output Exprs:10: P_BRAND | 11: P_TYPE | 12: P_SIZE | 26: count
Input Partition: UNPARTITIONED
RESULT SINK

15:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 7119
column statistics:
* P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
* count-->[0.0, 6914952.0, 0.0, 8.0, 7119.140625] ESTIMATE

PLAN FRAGMENT 1(F05)

Input Partition: HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:SORT
|  order by: [26, BIGINT, false] DESC, [10, VARCHAR, false] ASC, [11, VARCHAR, false] ASC, [12, INT, false] ASC
|  offset: 0
|  cardinality: 7119
|  column statistics:
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * count-->[0.0, 6914952.0, 0.0, 8.0, 7119.140625] ESTIMATE
|
13:AGGREGATE (update finalize)
|  aggregate: count[([2: PS_SUPPKEY, INT, false]); args: INT; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [10: P_BRAND, VARCHAR, false], [11: P_TYPE, VARCHAR, false], [12: P_SIZE, INT, false]
|  cardinality: 7119
|  column statistics:
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * count-->[0.0, 6914952.0, 0.0, 8.0, 7119.140625] ESTIMATE
|
12:AGGREGATE (merge serialize)
|  group by: [2: PS_SUPPKEY, INT, false], [10: P_BRAND, VARCHAR, false], [11: P_TYPE, VARCHAR, false], [12: P_SIZE, INT, false]
|  cardinality: 6914952
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
11:EXCHANGE
distribution type: SHUFFLE
partition exprs: [10: P_BRAND, VARCHAR, false], [11: P_TYPE, VARCHAR, false], [12: P_SIZE, INT, false]
cardinality: 6914952

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: P_BRAND, 11: P_TYPE, 12: P_SIZE
OutPut Exchange Id: 11

10:AGGREGATE (update serialize)
|  STREAMING
|  group by: [2: PS_SUPPKEY, INT, false], [10: P_BRAND, VARCHAR, false], [11: P_TYPE, VARCHAR, false], [12: P_SIZE, INT, false]
|  cardinality: 6914952
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
9:Project
|  output columns:
|  2 <-> [2: PS_SUPPKEY, INT, false]
|  10 <-> [10: P_BRAND, VARCHAR, false]
|  11 <-> [11: P_TYPE, VARCHAR, false]
|  12 <-> [12: P_SIZE, INT, false]
|  cardinality: 6914952
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
8:HASH JOIN
|  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
|  equal join conjunct: [2: PS_SUPPKEY, INT, false] = [17: S_SUPPKEY, INT, false]
|  output columns: 2, 10, 11, 12
|  cardinality: 6914952
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
|
|----7:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 250000
|
4:Project
|  output columns:
|  2 <-> [2: PS_SUPPKEY, INT, false]
|  10 <-> [10: P_BRAND, CHAR, false]
|  11 <-> [11: P_TYPE, VARCHAR, false]
|  12 <-> [12: P_SIZE, INT, false]
|  cardinality: 9219936
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
3:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: PS_PARTKEY, INT, false] = [7: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (7: P_PARTKEY), remote = false
|  output columns: 2, 10, 11, 12
|  cardinality: 9219936
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2304984.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2304984.0] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
|----2:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [7: P_PARTKEY, INT, false]
|       cardinality: 2304984
|
0:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=16.0
cardinality: 80000000
probe runtime filters:
- filter_id = 0, probe_expr = (1: PS_PARTKEY)
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE

PLAN FRAGMENT 3(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  17 <-> [17: S_SUPPKEY, INT, false]
|  cardinality: 250000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
|
5:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
Predicates: 23: S_COMMENT LIKE '%Customer%Complaints%'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=105.0
cardinality: 250000
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
* S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 7: P_PARTKEY
OutPut Exchange Id: 02

1:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: [10: P_BRAND, CHAR, false] != 'Brand#43', NOT (11: P_TYPE LIKE 'PROMO BURNISHED%'), 12: P_SIZE IN (31, 43, 9, 6, 18, 11, 25, 1)
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=47.0
cardinality: 2304984
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2304984.0] ESTIMATE
* P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* P_SIZE-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
[end]