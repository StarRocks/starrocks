[sql]
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-05-01'
  and o_orderdate < date '1994-08-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc limit 20;
[fragment statistics]
PLAN FRAGMENT 0(F07)
Output Exprs:1: C_CUSTKEY | 2: C_NAME | 43: sum | 6: C_ACCTBAL | 38: N_NAME | 3: C_ADDRESS | 5: C_PHONE | 8: C_COMMENT
Input Partition: UNPARTITIONED
RESULT SINK

17:MERGING-EXCHANGE
distribution type: GATHER
limit: 20
cardinality: 20
column statistics:
* C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
* C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
* C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
* C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
* C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
* C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* sum-->[810.9, 635341.1576513578, 0.0, 8.0, 932377.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:TOP-N
|  order by: [43, DOUBLE, true] DESC
|  offset: 0
|  limit: 20
|  cardinality: 20
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * sum-->[810.9, 635341.1576513578, 0.0, 8.0, 932377.0] ESTIMATE
|
15:AGGREGATE (update finalize)
|  aggregate: sum[([42: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [1: C_CUSTKEY, INT, false], [2: C_NAME, VARCHAR, false], [6: C_ACCTBAL, DOUBLE, false], [5: C_PHONE, VARCHAR, false], [38: N_NAME, VARCHAR, false], [3: C_ADDRESS, VARCHAR, false], [8: C_COMMENT, VARCHAR, false]
|  cardinality: 5644405
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * sum-->[810.9, 635341.1576513578, 0.0, 8.0, 932377.0] ESTIMATE
|
14:Project
|  output columns:
|  1 <-> [1: C_CUSTKEY, INT, false]
|  2 <-> [2: C_NAME, VARCHAR, false]
|  3 <-> [3: C_ADDRESS, VARCHAR, false]
|  5 <-> [5: C_PHONE, VARCHAR, false]
|  6 <-> [6: C_ACCTBAL, DOUBLE, false]
|  8 <-> [8: C_COMMENT, VARCHAR, false]
|  38 <-> [38: N_NAME, CHAR, false]
|  42 <-> [25: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 5644405
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
13:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: C_NATIONKEY, INT, false] = [37: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (37: N_NATIONKEY), remote = false
|  output columns: 1, 2, 3, 5, 6, 8, 25, 26, 38
|  cardinality: 5644405
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
|----12:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
10:Project
|  output columns:
|  1 <-> [1: C_CUSTKEY, INT, false]
|  2 <-> [2: C_NAME, VARCHAR, false]
|  3 <-> [3: C_ADDRESS, VARCHAR, false]
|  4 <-> [4: C_NATIONKEY, INT, false]
|  5 <-> [5: C_PHONE, CHAR, false]
|  6 <-> [6: C_ACCTBAL, DOUBLE, false]
|  8 <-> [8: C_COMMENT, VARCHAR, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 5644405
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: C_CUSTKEY, INT, false] = [11: O_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (11: O_CUSTKEY), remote = false
|  output columns: 1, 2, 3, 4, 5, 6, 8, 25, 26
|  cardinality: 5644405
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5644405.0] ESTIMATE
|  * C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
|  * C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
|  * C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [11: O_CUSTKEY, INT, false]
|       cardinality: 5644405
|
0:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=217.0
cardinality: 15000000
probe runtime filters:
- filter_id = 1, probe_expr = (1: C_CUSTKEY)
- filter_id = 2, probe_expr = (4: C_NATIONKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
* C_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 150000.0] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* C_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 150000.0] ESTIMATE
* C_ACCTBAL-->[-999.99, 9999.99, 0.0, 8.0, 137439.0] ESTIMATE
* C_COMMENT-->[-Infinity, Infinity, 0.0, 117.0, 149968.0] ESTIMATE

PLAN FRAGMENT 2(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 12

11:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 11: O_CUSTKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  11 <-> [11: O_CUSTKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 5644405
|  column statistics:
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (10: O_ORDERKEY), remote = false
|  output columns: 11, 25, 26
|  cardinality: 5644405
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5644405.0] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5644405.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [10: O_ORDERKEY, INT, false]
|       cardinality: 5644405
|
2:Project
|  output columns:
|  20 <-> [20: L_ORDERKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 147857059
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.4785705894556352E8] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
1:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [28: L_RETURNFLAG, CHAR, false] = 'R'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=25.0
cardinality: 147857059
probe runtime filters:
- filter_id = 0, probe_expr = (20: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.4785705894556352E8] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 1.0] ESTIMATE

PLAN FRAGMENT 4(F02)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 10: O_ORDERKEY
OutPut Exchange Id: 05

4:Project
|  output columns:
|  10 <-> [10: O_ORDERKEY, INT, false]
|  11 <-> [11: O_CUSTKEY, INT, false]
|  cardinality: 5644405
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5644405.0] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
|
3:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [14: O_ORDERDATE, DATE, false] >= '1994-05-01', [14: O_ORDERDATE, DATE, false] < '1994-08-01'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=20.0
cardinality: 5644405
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 5644405.0] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 5644405.0] ESTIMATE
* O_ORDERDATE-->[7.677216E8, 7.756704E8, 0.0, 4.0, 2406.0] ESTIMATE
[end]