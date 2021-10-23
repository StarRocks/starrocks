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
PLAN FRAGMENT 0(F06)
Output Exprs:20: L_ORDERKEY | 38: sum(37: expr) | 14: O_ORDERDATE | 17: O_SHIPPRIORITY
Input Partition: UNPARTITIONED
RESULT SINK

15:MERGING-EXCHANGE
limit: 10
cardinality: 10
column statistics:
* O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
* O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
* sum(37: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]

PLAN FRAGMENT 1(F05)

Input Partition: HASH_PARTITIONED: 20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:TOP-N
|  order by: [38, DOUBLE, true] DESC, [14, DATE, false] ASC
|  offset: 0
|  limit: 10
|  cardinality: 10
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * sum(37: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
13:AGGREGATE (merge finalize)
|  aggregate: sum[([38: sum(37: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [20: L_ORDERKEY, INT, false], [14: O_ORDERDATE, DATE, false], [17: O_SHIPPRIORITY, INT, false]
|  cardinality: 5576432
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * sum(37: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
12:EXCHANGE
cardinality: 13218209

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY
OutPut Exchange Id: 12

11:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([37: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [20: L_ORDERKEY, INT, false], [14: O_ORDERDATE, DATE, false], [17: O_SHIPPRIORITY, INT, false]
|  cardinality: 13218209
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * sum(37: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
10:Project
|  output columns:
|  14 <-> [14: O_ORDERDATE, DATE, false]
|  17 <-> [17: O_SHIPPRIORITY, INT, false]
|  20 <-> [20: L_ORDERKEY, INT, false]
|  37 <-> [25: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 31332052
|  column statistics:
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (10: O_ORDERKEY), remote = false
|  cardinality: 31332052
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|
|----8:EXCHANGE
|       cardinality: 14532225
|
1:Project
|  output columns:
|  20 <-> [20: L_ORDERKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 323405941
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [30: L_SHIPDATE, DATE, false] > '1995-03-11'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=28.0
cardinality: 323405941
probe runtime filters:
- filter_id = 1, probe_expr = (20: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
* L_SHIPDATE-->[7.948512E8, 9.124416E8, 0.0, 4.0, 2526.0]

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHFFULE_HASH_PARTITIONED: 10: O_ORDERKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  10 <-> [10: O_ORDERKEY, INT, false]
|  14 <-> [14: O_ORDERDATE, DATE, false]
|  17 <-> [17: O_SHIPPRIORITY, INT, false]
|  cardinality: 14532225
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [11: O_CUSTKEY, INT, false] = [1: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: C_CUSTKEY), remote = false
|  cardinality: 14532225
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7]
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0]
|  * O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
|  * O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]
|
|----5:EXCHANGE
|       cardinality: 3000000
|
2:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [14: O_ORDERDATE, DATE, false] < '1995-03-11'
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
actualRows=0, avgRowSize=24.0
cardinality: 72661123
probe runtime filters:
- filter_id = 0, probe_expr = (11: O_CUSTKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8]
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0]
* O_ORDERDATE-->[6.941952E8, 7.948512E8, 0.0, 4.0, 2406.0]
* O_SHIPPRIORITY-->[0.0, 0.0, 0.0, 4.0, 1.0]

PLAN FRAGMENT 4(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 05

4:Project
|  output columns:
|  1 <-> [1: C_CUSTKEY, INT, false]
|  cardinality: 3000000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7]
|
3:OlapScanNode
table: customer, rollup: customer
preAggregation: on
Predicates: [7: C_MKTSEGMENT, CHAR, false] = 'HOUSEHOLD'
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
actualRows=0, avgRowSize=18.0
cardinality: 3000000
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7]
* C_MKTSEGMENT-->[-Infinity, Infinity, 0.0, 10.0, 5.0]
[end]