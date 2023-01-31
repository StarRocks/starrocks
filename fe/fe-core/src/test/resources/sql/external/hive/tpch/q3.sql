[sql]
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
  c_mktsegment = 'HOUSEHOLD'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-11'
  and l_shipdate > date '1995-03-11'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate limit 10;
[fragment statistics]
PLAN FRAGMENT 0(F07)
Output Exprs:18: l_orderkey | 35: sum | 13: o_orderdate | 16: o_shippriority
Input Partition: UNPARTITIONED
RESULT SINK

14:MERGING-EXCHANGE
limit: 10
cardinality: 10
column statistics:
* o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
* o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.685171583236066E7] ESTIMATE
* sum-->[810.9, 1315947.4994776784, 0.0, 16.0, 3736520.0] ESTIMATE

PLAN FRAGMENT 1(F06)

Input Partition: HASH_PARTITIONED: 18: l_orderkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 14

13:TOP-N
|  order by: [35, DECIMAL128(38,4), true] DESC, [13, DATE, true] ASC
|  offset: 0
|  limit: 10
|  cardinality: 10
|  column statistics:
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.685171583236066E7] ESTIMATE
|  * sum-->[810.9, 1315947.4994776784, 0.0, 16.0, 3736520.0] ESTIMATE
|
12:AGGREGATE (update finalize)
|  aggregate: sum[([34: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [18: l_orderkey, INT, true], [13: o_orderdate, DATE, true], [16: o_shippriority, INT, true]
|  cardinality: 46851716
|  column statistics:
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.685171583236066E7] ESTIMATE
|  * sum-->[810.9, 1315947.4994776784, 0.0, 16.0, 3736520.0] ESTIMATE
|
11:Project
|  output columns:
|  13 <-> [13: o_orderdate, DATE, true]
|  16 <-> [16: o_shippriority, INT, true]
|  18 <-> [18: l_orderkey, INT, true]
|  34 <-> cast([23: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [24: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))
|  cardinality: 46851716
|  column statistics:
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.685171583236066E7] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
10:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [18: l_orderkey, INT, true] = [9: o_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (9: o_orderkey), remote = true
|  output columns: 13, 16, 18, 23, 24
|  cardinality: 46851716
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 4.685171583236066E7] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
|----9:EXCHANGE
|       cardinality: 21729080
|
2:EXCHANGE
cardinality: 323426370

PLAN FRAGMENT 2(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 9: o_orderkey
OutPut Exchange Id: 09

8:Project
|  output columns:
|  9 <-> [9: o_orderkey, INT, true]
|  13 <-> [13: o_orderdate, DATE, true]
|  16 <-> [16: o_shippriority, INT, true]
|  cardinality: 21729080
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.1729079702600703E7] ESTIMATE
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|
7:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: c_custkey), remote = false
|  output columns: 9, 13, 16
|  cardinality: 21729080
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 2.1729079702600703E7] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----6:EXCHANGE
|       cardinality: 3000000
|
3:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 13: o_orderdate < '1995-03-11'
MIN/MAX PREDICATES: 37: o_orderdate < '1995-03-11'
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 72661123
probe runtime filters:
- filter_id = 0, probe_expr = (10: o_custkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 7.266112266112266E7] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* o_orderdate-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2412.0] ESTIMATE
* o_shippriority-->[0.0, 0.0, 0.0, 4.0, 1.0] ESTIMATE

PLAN FRAGMENT 3(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  cardinality: 3000000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|
4:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 7: c_mktsegment = 'HOUSEHOLD'
MIN/MAX PREDICATES: 38: c_mktsegment <= 'HOUSEHOLD', 39: c_mktsegment >= 'HOUSEHOLD'
partitions=1/1
avgRowSize=18.0
numNodes=0
cardinality: 3000000
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
* c_mktsegment-->[-Infinity, Infinity, 0.0, 10.0, 5.0] ESTIMATE

PLAN FRAGMENT 4(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: l_orderkey
OutPut Exchange Id: 02

1:Project
|  output columns:
|  18 <-> [18: l_orderkey, INT, true]
|  23 <-> [23: l_extendedprice, DECIMAL64(15,2), true]
|  24 <-> [24: l_discount, DECIMAL64(15,2), true]
|  cardinality: 323426370
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 28: l_shipdate > '1995-03-11'
MIN/MAX PREDICATES: 36: l_shipdate > '1995-03-11'
partitions=1/1
avgRowSize=28.0
numNodes=0
cardinality: 323426370
probe runtime filters:
- filter_id = 1, probe_expr = (18: l_orderkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_shipdate-->[7.948512E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE
[end]
