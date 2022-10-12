[sql]
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AFRICA'
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1996-01-01'
group by
    n_name
order by
    revenue desc ;
[fragment statistics]
PLAN FRAGMENT 0(F14)
Output Exprs:42: n_name | 49: sum
Input Partition: UNPARTITIONED
RESULT SINK

28:MERGING-EXCHANGE
cardinality: 5
column statistics:
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 16.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F13)

Input Partition: HASH_PARTITIONED: 42: n_name
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 28

27:SORT
|  order by: [49, DECIMAL128(38,4), true] DESC
|  offset: 0
|  cardinality: 5
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 5.0] ESTIMATE
|
26:AGGREGATE (merge finalize)
|  aggregate: sum[([49: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [42: n_name, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 5.0] ESTIMATE
|
25:EXCHANGE
cardinality: 5

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 42: n_name
OutPut Exchange Id: 25

24:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([48: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [42: n_name, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 5.0] ESTIMATE
|
23:Project
|  output columns:
|  42 <-> [42: n_name, VARCHAR, true]
|  48 <-> cast([23: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [24: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))
|  cardinality: 16391888
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
22:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [20: l_suppkey, INT, true] = [34: s_suppkey, INT, true]
|  equal join conjunct: [4: c_nationkey, INT, true] = [37: s_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (34: s_suppkey), remote = false
|  - filter_id = 5, build_expr = (37: s_nationkey), remote = true
|  output columns: 23, 24, 42
|  cardinality: 16391888
|  column statistics:
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
|----21:EXCHANGE
|       cardinality: 200000
|
10:Project
|  output columns:
|  4 <-> [4: c_nationkey, INT, true]
|  20 <-> [20: l_suppkey, INT, true]
|  23 <-> [23: l_extendedprice, DECIMAL64(15,2), true]
|  24 <-> [24: l_discount, DECIMAL64(15,2), true]
|  cardinality: 91066043
|  column statistics:
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [18: l_orderkey, INT, true] = [9: o_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (9: o_orderkey), remote = false
|  output columns: 4, 20, 23, 24
|  cardinality: 91066043
|  column statistics:
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----8:EXCHANGE
|       cardinality: 22765073
|       probe runtime filters:
|       - filter_id = 5, probe_expr = (4: c_nationkey)
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 18: l_orderkey IS NOT NULL
partitions=1/1
avgRowSize=28.0
numNodes=0
cardinality: 600037902
probe runtime filters:
- filter_id = 1, probe_expr = (18: l_orderkey)
- filter_id = 4, probe_expr = (20: l_suppkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 3(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 21

20:Project
|  output columns:
|  34 <-> [34: s_suppkey, INT, true]
|  37 <-> [37: s_nationkey, INT, true]
|  42 <-> [42: n_name, VARCHAR, true]
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [37: s_nationkey, INT, true] = [41: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (41: n_nationkey), remote = false
|  output columns: 34, 37, 42
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----18:EXCHANGE
|       cardinality: 5
|
11:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 34: s_suppkey IS NOT NULL, 37: s_nationkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (37: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:Project
|  output columns:
|  41 <-> [41: n_nationkey, INT, true]
|  42 <-> [42: n_name, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [43: n_regionkey, INT, true] = [45: r_regionkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (45: r_regionkey), remote = false
|  output columns: 41, 42
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----15:EXCHANGE
|       cardinality: 1
|
12:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 41: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=33.0
numNodes=0
cardinality: 25
probe runtime filters:
- filter_id = 2, probe_expr = (43: n_regionkey)
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* n_regionkey-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 5(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:Project
|  output columns:
|  45 <-> [45: r_regionkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
13:HdfsScanNode
TABLE: region
NON-PARTITION PREDICATES: 46: r_name = 'AFRICA'
MIN/MAX PREDICATES: 50: r_name <= 'AFRICA', 51: r_name >= 'AFRICA'
partitions=1/1
avgRowSize=10.8
numNodes=0
cardinality: 1
column statistics:
* r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* r_name-->[-Infinity, Infinity, 0.0, 6.8, 1.0] ESTIMATE

PLAN FRAGMENT 6(F05)

Input Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:Project
|  output columns:
|  4 <-> [4: c_nationkey, INT, true]
|  9 <-> [9: o_orderkey, INT, true]
|  cardinality: 22765073
|  column statistics:
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]
|  output columns: 4, 9
|  cardinality: 22765073
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
|----5:EXCHANGE
|       cardinality: 15000000
|
3:EXCHANGE
cardinality: 22765073

PLAN FRAGMENT 7(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Exchange Id: 05

4:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 1: c_custkey IS NOT NULL
partitions=1/1
avgRowSize=12.0
numNodes=0
cardinality: 15000000
probe runtime filters:
- filter_id = 5, probe_expr = (4: c_nationkey)
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 8(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Exchange Id: 03

2:Project
|  output columns:
|  9 <-> [9: o_orderkey, INT, true]
|  10 <-> [10: o_custkey, INT, true]
|  cardinality: 22765073
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
1:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 13: o_orderdate >= '1995-01-01', 13: o_orderdate < '1996-01-01'
MIN/MAX PREDICATES: 52: o_orderdate >= '1995-01-01', 53: o_orderdate < '1996-01-01'
partitions=1/1
avgRowSize=20.0
numNodes=0
cardinality: 22765073
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* o_orderdate-->[7.888896E8, 8.204256E8, 0.0, 4.0, 2412.0] ESTIMATE
[end]
