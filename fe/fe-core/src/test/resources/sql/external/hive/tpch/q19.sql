[fragment statistics]
PLAN FRAGMENT 0(F05)
Output Exprs:27: sum
Input Partition: UNPARTITIONED
RESULT SINK

9:AGGREGATE (merge finalize)
|  aggregate: sum[([27: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
8:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F04)

Input Partition: HASH_PARTITIONED: 2: l_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:AGGREGATE (update serialize)
|  aggregate: sum[(cast([6: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [7: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
6:Project
|  output columns:
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  7 <-> [7: l_discount, DECIMAL64(15,2), true]
|  cardinality: 19277
|  column statistics:
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 2856.1332873207584] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [2: l_partkey, INT, true] = [17: p_partkey, INT, true]
|  other join predicates: (((((20: p_brand = 'Brand#45') AND (23: p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) AND ((5: l_quantity >= 5) AND (5: l_quantity <= 15))) AND (22: p_size <= 5)) OR ((((20: p_brand = 'Brand#11') AND (23: p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) AND ((5: l_quantity >= 15) AND (5: l_quantity <= 25))) AND (22: p_size <= 10))) OR ((((20: p_brand = 'Brand#21') AND (23: p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) AND ((5: l_quantity >= 25) AND (5: l_quantity <= 35))) AND (22: p_size <= 15))
|  build runtime filters:
|  - filter_id = 0, build_expr = (17: p_partkey), remote = true
|  output columns: 5, 6, 7, 20, 22, 23
|  cardinality: 19277
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2856.1332873207584] ESTIMATE
|  * l_quantity-->[5.0, 35.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 2856.1332873207584] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2856.1332873207584] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_size-->[1.0, 15.0, 0.0, 4.0, 50.0] ESTIMATE
|  * p_container-->[-Infinity, Infinity, 0.0, 10.0, 4.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 2856.1332873207584] ESTIMATE
|
|----4:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [17: p_partkey, INT, true]
|       cardinality: 5714286
|
2:EXCHANGE
distribution type: SHUFFLE
partition exprs: [2: l_partkey, INT, true]
cardinality: 26240725

PLAN FRAGMENT 2(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Exchange Id: 04

3:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 20: p_brand IN ('Brand#45', 'Brand#11', 'Brand#21'), 22: p_size <= 15, 23: p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'), 22: p_size >= 1
MIN/MAX PREDICATES: 20: p_brand >= 'Brand#11', 20: p_brand <= 'Brand#45', 22: p_size <= 15, 23: p_container >= 'LG BOX', 23: p_container <= 'SM PKG', 22: p_size >= 1
partitions=1/1
avgRowSize=32.0
cardinality: 5714286
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5714285.714285714] ESTIMATE
* p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* p_size-->[1.0, 15.0, 0.0, 4.0, 50.0] ESTIMATE
* p_container-->[-Infinity, Infinity, 0.0, 10.0, 40.0] ESTIMATE

PLAN FRAGMENT 3(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 2: l_partkey
OutPut Exchange Id: 02

1:Project
|  output columns:
|  2 <-> [2: l_partkey, INT, true]
|  5 <-> [5: l_quantity, DECIMAL64(15,2), true]
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  7 <-> [7: l_discount, DECIMAL64(15,2), true]
|  cardinality: 26240725
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * l_quantity-->[5.0, 35.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 5: l_quantity >= 5, 5: l_quantity <= 35, 15: l_shipmode IN ('AIR', 'AIR REG'), 14: l_shipinstruct = 'DELIVER IN PERSON'
MIN/MAX PREDICATES: 5: l_quantity >= 5, 5: l_quantity <= 35, 15: l_shipmode >= 'AIR', 15: l_shipmode <= 'AIR REG', 14: l_shipinstruct <= 'DELIVER IN PERSON', 14: l_shipinstruct >= 'DELIVER IN PERSON'
partitions=1/1
avgRowSize=67.0
cardinality: 26240725
probe runtime filters:
- filter_id = 0, probe_expr = (2: l_partkey)
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_quantity-->[5.0, 35.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_shipinstruct-->[-Infinity, Infinity, 0.0, 25.0, 4.0] ESTIMATE
* l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
[end]

