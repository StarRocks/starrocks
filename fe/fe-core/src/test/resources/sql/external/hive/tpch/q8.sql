[fragment statistics]
PLAN FRAGMENT 0(F20)
Output Exprs:61: year | 66: expr
Input Partition: UNPARTITIONED
RESULT SINK

38:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 2
column statistics:
* year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
* sum-->[0.0, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
* expr-->[0.0, 129.42348008385744, 0.0, 16.0, 2.0] ESTIMATE

PLAN FRAGMENT 1(F19)

Input Partition: HASH_PARTITIONED: 61: year
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 38

37:SORT
|  order by: [61, SMALLINT, true] ASC
|  offset: 0
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[0.0, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|  * expr-->[0.0, 129.42348008385744, 0.0, 16.0, 2.0] ESTIMATE
|
36:Project
|  output columns:
|  61 <-> [61: year, SMALLINT, true]
|  66 <-> [64: sum, DECIMAL128(38,4), true] / [65: sum, DECIMAL128(38,4), true]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[0.0, 129.42348008385744, 0.0, 16.0, 2.0] ESTIMATE
|
35:AGGREGATE (merge finalize)
|  aggregate: sum[([65: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([64: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [61: year, SMALLINT, true]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[0.0, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|  * expr-->[0.0, 129.42348008385744, 0.0, 16.0, 2.0] ESTIMATE
|
34:EXCHANGE
distribution type: SHUFFLE
partition exprs: [61: year, SMALLINT, true]
cardinality: 2

PLAN FRAGMENT 2(F16)

Input Partition: HASH_PARTITIONED: 10: s_suppkey
OutPut Partition: HASH_PARTITIONED: 61: year
OutPut Exchange Id: 34

33:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([62: expr, DECIMAL128(31,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([63: case, DECIMAL128(31,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [61: year, SMALLINT, true]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[0.0, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 2.0] ESTIMATE
|
32:Project
|  output columns:
|  61 <-> year[([37: o_orderdate, DATE, true]); args: DATE; result: SMALLINT; args nullable: true; result nullable: true]
|  62 <-> [71: multiply, DECIMAL128(31,4), true]
|  63 <-> if[([55: n_name, VARCHAR, true] = 'IRAN', [71: multiply, DECIMAL128(31,4), true], 0); args: BOOLEAN,DECIMAL128,DECIMAL128; result: DECIMAL128(31,4); args nullable: true; result nullable: true]
|  common expressions:
|  67 <-> cast([22: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2))
|  68 <-> [23: l_discount, DECIMAL64(15,2), true]
|  69 <-> 1 - [68: cast, DECIMAL64(16,2), true]
|  70 <-> cast([69: subtract, DECIMAL64(16,2), true] as DECIMAL128(16,2))
|  71 <-> [67: cast, DECIMAL128(15,2), true] * [70: cast, DECIMAL128(16,2), true]
|  cardinality: 242843
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 242842.78223700626] ESTIMATE
|  * case-->[0.0, 104949.5, 0.0, 16.0, 242843.78223700626] ESTIMATE
|
31:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [13: s_nationkey, INT, true] = [54: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 6, build_expr = (54: n_nationkey), remote = true
|  output columns: 22, 23, 37, 55
|  cardinality: 242843
|  column statistics:
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 242842.78223700626] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 242842.78223700626] ESTIMATE
|  * case-->[0.0, 104949.5, 0.0, 16.0, 242843.78223700626] ESTIMATE
|
|----30:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
28:Project
|  output columns:
|  13 <-> [13: s_nationkey, INT, true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  37 <-> [37: o_orderdate, DATE, true]
|  cardinality: 242843
|  column statistics:
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 242842.78223700623] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|
27:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [10: s_suppkey, INT, true] = [19: l_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 5, build_expr = (19: l_suppkey), remote = true
|  output columns: 13, 22, 23, 37
|  cardinality: 242843
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 242842.78223700623] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 242842.78223700623] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 242842.78223700623] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|
|----26:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [19: l_suppkey, INT, true]
|       cardinality: 242843
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [10: s_suppkey, INT, true]
cardinality: 1000000
probe runtime filters:
- filter_id = 6, probe_expr = (13: s_nationkey)

PLAN FRAGMENT 3(F17)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 30

29:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 54: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=29.0
dataCacheOptions={populate: false}
cardinality: 25
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F14)

Input Partition: HASH_PARTITIONED: 33: o_orderkey
OutPut Partition: HASH_PARTITIONED: 19: l_suppkey
OutPut Exchange Id: 26

25:Project
|  output columns:
|  19 <-> [19: l_suppkey, INT, true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  37 <-> [37: o_orderdate, DATE, true]
|  cardinality: 242843
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 242842.78223700626] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 242842.78223700626] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|
24:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [33: o_orderkey, INT, true] = [17: l_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (17: l_orderkey), remote = true
|  output columns: 19, 22, 23, 37
|  cardinality: 242843
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 242842.78223700626] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 242842.78223700626] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----23:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [17: l_orderkey, INT, true]
|       cardinality: 4000253
|
16:EXCHANGE
distribution type: SHUFFLE
partition exprs: [33: o_orderkey, INT, true]
cardinality: 9106029

PLAN FRAGMENT 5(F10)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: l_orderkey
OutPut Exchange Id: 23

22:Project
|  output columns:
|  17 <-> [17: l_orderkey, INT, true]
|  19 <-> [19: l_suppkey, INT, true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  cardinality: 4000253
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4000252.6799999997] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [18: l_partkey, INT, true] = [1: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (1: p_partkey), remote = false
|  output columns: 17, 19, 22, 23
|  cardinality: 4000253
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 133333.33333333334] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4000252.6799999997] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 133333.33333333334] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----20:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 133333
|
17:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 18: l_partkey IS NOT NULL, 19: l_suppkey IS NOT NULL
partitions=1/1
avgRowSize=36.0
dataCacheOptions={populate: false}
cardinality: 600037902
probe runtime filters:
- filter_id = 3, probe_expr = (18: l_partkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 6(F11)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  cardinality: 133333
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 133333.33333333334] ESTIMATE
|
18:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 5: p_type = 'ECONOMY ANODIZED STEEL'
MIN/MAX PREDICATES: 5: p_type <= 'ECONOMY ANODIZED STEEL', 5: p_type >= 'ECONOMY ANODIZED STEEL'
partitions=1/1
avgRowSize=33.0
dataCacheOptions={populate: false}
cardinality: 133333
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 133333.33333333334] ESTIMATE
* p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE

PLAN FRAGMENT 7(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 33: o_orderkey
OutPut Exchange Id: 16

15:Project
|  output columns:
|  33 <-> [33: o_orderkey, INT, true]
|  37 <-> [37: o_orderdate, DATE, true]
|  cardinality: 9106029
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 9106029.106029106] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|
14:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [34: o_custkey, INT, true] = [42: c_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (42: c_custkey), remote = false
|  output columns: 33, 37
|  cardinality: 9106029
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 9106029.106029106] ESTIMATE
|  * o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----13:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 3000000
|
2:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 37: o_orderdate >= '1995-01-01', 37: o_orderdate <= '1996-12-31'
MIN/MAX PREDICATES: 37: o_orderdate >= '1995-01-01', 37: o_orderdate <= '1996-12-31'
partitions=1/1
avgRowSize=20.0
dataCacheOptions={populate: false}
cardinality: 45530146
probe runtime filters:
- filter_id = 2, probe_expr = (34: o_custkey)
- filter_id = 4, probe_expr = (33: o_orderkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.5530145530145526E7] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* o_orderdate-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2412.0] ESTIMATE

PLAN FRAGMENT 8(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:Project
|  output columns:
|  42 <-> [42: c_custkey, INT, true]
|  cardinality: 3000000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [45: c_nationkey, INT, true] = [50: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (50: n_nationkey), remote = false
|  output columns: 42
|  cardinality: 3000000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----10:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 5
|
3:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 42: c_custkey IS NOT NULL
partitions=1/1
avgRowSize=12.0
dataCacheOptions={populate: false}
cardinality: 15000000
probe runtime filters:
- filter_id = 1, probe_expr = (45: c_nationkey)
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 9(F04)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:Project
|  output columns:
|  50 <-> [50: n_nationkey, INT, true]
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [52: n_regionkey, INT, true] = [58: r_regionkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (58: r_regionkey), remote = false
|  output columns: 50
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----7:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
4:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 50: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
dataCacheOptions={populate: false}
cardinality: 25
probe runtime filters:
- filter_id = 0, probe_expr = (52: n_regionkey)
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_regionkey-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 10(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  58 <-> [58: r_regionkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
5:HdfsScanNode
TABLE: region
NON-PARTITION PREDICATES: 59: r_name = 'MIDDLE EAST'
MIN/MAX PREDICATES: 59: r_name <= 'MIDDLE EAST', 59: r_name >= 'MIDDLE EAST'
partitions=1/1
avgRowSize=10.8
dataCacheOptions={populate: false}
cardinality: 1
column statistics:
* r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* r_name-->[-Infinity, Infinity, 0.0, 6.8, 1.0] ESTIMATE

PLAN FRAGMENT 11(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: s_suppkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: supplier
partitions=1/1
avgRowSize=8.0
dataCacheOptions={populate: false}
cardinality: 1000000
probe runtime filters:
- filter_id = 5, probe_expr = (10: s_suppkey)
- filter_id = 6, probe_expr = (13: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
[end]