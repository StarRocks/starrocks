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
PLAN FRAGMENT 0(F10)
Output Exprs:1: c_custkey | 2: c_name | 39: sum | 6: c_acctbal | 35: n_name | 3: c_address | 5: c_phone | 8: c_comment
Input Partition: UNPARTITIONED
RESULT SINK

20:MERGING-EXCHANGE
distribution type: GATHER
limit: 20
cardinality: 20
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
* c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
* c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
* c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
* c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
* c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* sum-->[810.9, 214903.376217033, 0.0, 16.0, 3736520.0] ESTIMATE

PLAN FRAGMENT 1(F09)

Input Partition: HASH_PARTITIONED: 1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:TOP-N
|  order by: [39, DECIMAL128(38,4), true] DESC
|  offset: 0
|  limit: 20
|  cardinality: 20
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * sum-->[810.9, 214903.376217033, 0.0, 16.0, 3736520.0] ESTIMATE
|
18:AGGREGATE (merge finalize)
|  aggregate: sum[([39: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [1: c_custkey, INT, true], [2: c_name, VARCHAR, true], [6: c_acctbal, DECIMAL64(15,2), true], [5: c_phone, VARCHAR, true], [35: n_name, VARCHAR, true], [3: c_address, VARCHAR, true], [8: c_comment, VARCHAR, true]
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * sum-->[810.9, 214903.376217033, 0.0, 16.0, 3736520.0] ESTIMATE
|
17:EXCHANGE
distribution type: SHUFFLE
partition exprs: [1: c_custkey, INT, true], [2: c_name, VARCHAR, true], [6: c_acctbal, DECIMAL64(15,2), true], [5: c_phone, VARCHAR, true], [35: n_name, VARCHAR, true], [3: c_address, VARCHAR, true], [8: c_comment, VARCHAR, true]
cardinality: 7651211

PLAN FRAGMENT 2(F06)

Input Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Partition: HASH_PARTITIONED: 1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment
OutPut Exchange Id: 17

16:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([38: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [1: c_custkey, INT, true], [2: c_name, VARCHAR, true], [6: c_acctbal, DECIMAL64(15,2), true], [5: c_phone, VARCHAR, true], [35: n_name, VARCHAR, true], [3: c_address, VARCHAR, true], [8: c_comment, VARCHAR, true]
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * sum-->[810.9, 214903.376217033, 0.0, 16.0, 3736520.0] ESTIMATE
|
15:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  2 <-> [2: c_name, VARCHAR, true]
|  3 <-> [3: c_address, VARCHAR, true]
|  5 <-> [5: c_phone, VARCHAR, true]
|  6 <-> [6: c_acctbal, DECIMAL64(15,2), true]
|  8 <-> [8: c_comment, VARCHAR, true]
|  35 <-> [35: n_name, VARCHAR, true]
|  38 <-> cast([23: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [24: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
14:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: c_nationkey, INT, true] = [34: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (34: n_nationkey), remote = true
|  output columns: 1, 2, 3, 5, 6, 8, 23, 24, 35
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
|----13:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
11:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  2 <-> [2: c_name, VARCHAR, true]
|  3 <-> [3: c_address, VARCHAR, true]
|  4 <-> [4: c_nationkey, INT, true]
|  5 <-> [5: c_phone, VARCHAR, true]
|  6 <-> [6: c_acctbal, DECIMAL64(15,2), true]
|  8 <-> [8: c_comment, VARCHAR, true]
|  23 <-> [23: l_extendedprice, DECIMAL64(15,2), true]
|  24 <-> [24: l_discount, DECIMAL64(15,2), true]
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
10:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [1: c_custkey, INT, true] = [10: o_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (10: o_custkey), remote = false
|  output columns: 1, 2, 3, 4, 5, 6, 8, 23, 24
|  cardinality: 7651211
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * c_name-->[-Infinity, Infinity, 0.0, 25.0, 7651210.947193347] ESTIMATE
|  * c_address-->[-Infinity, Infinity, 0.0, 40.0, 7651210.947193347] ESTIMATE
|  * c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * c_phone-->[-Infinity, Infinity, 0.0, 15.0, 7651210.947193347] ESTIMATE
|  * c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
|  * c_comment-->[-Infinity, Infinity, 0.0, 117.0, 7651210.947193347] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----9:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [10: o_custkey, INT, true]
|       cardinality: 7651211
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [1: c_custkey, INT, true]
cardinality: 15000000
probe runtime filters:
- filter_id = 1, probe_expr = (1: c_custkey)
- filter_id = 2, probe_expr = (4: c_nationkey)

PLAN FRAGMENT 3(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 34: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=29.0
cardinality: 25
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Exchange Id: 09

8:Project
|  output columns:
|  10 <-> [10: o_custkey, INT, true]
|  23 <-> [23: l_extendedprice, DECIMAL64(15,2), true]
|  24 <-> [24: l_discount, DECIMAL64(15,2), true]
|  cardinality: 7651211
|  column statistics:
|  * o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
7:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [18: l_orderkey, INT, true] = [9: o_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (9: o_orderkey), remote = false
|  output columns: 10, 23, 24
|  cardinality: 7651211
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----6:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 5738046
|
3:Project
|  output columns:
|  18 <-> [18: l_orderkey, INT, true]
|  23 <-> [23: l_extendedprice, DECIMAL64(15,2), true]
|  24 <-> [24: l_discount, DECIMAL64(15,2), true]
|  cardinality: 200012634
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
2:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 26: l_returnflag = 'R'
MIN/MAX PREDICATES: 26: l_returnflag <= 'R', 26: l_returnflag >= 'R'
partitions=1/1
avgRowSize=25.0
cardinality: 200012634
probe runtime filters:
- filter_id = 0, probe_expr = (18: l_orderkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE

PLAN FRAGMENT 5(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:Project
|  output columns:
|  9 <-> [9: o_orderkey, INT, true]
|  10 <-> [10: o_custkey, INT, true]
|  cardinality: 5738046
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|  * o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
|
4:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 13: o_orderdate >= '1994-05-01', 13: o_orderdate < '1994-08-01'
MIN/MAX PREDICATES: 13: o_orderdate >= '1994-05-01', 13: o_orderdate < '1994-08-01'
partitions=1/1
avgRowSize=20.0
cardinality: 5738046
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 5738045.738045738] ESTIMATE
* o_orderdate-->[7.677216E8, 7.756704E8, 0.0, 4.0, 2412.0] ESTIMATE

PLAN FRAGMENT 6(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: customer
NON-PARTITION PREDICATES: 1: c_custkey IS NOT NULL
partitions=1/1
avgRowSize=217.0
cardinality: 15000000
probe runtime filters:
- filter_id = 2, probe_expr = (4: c_nationkey)
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* c_name-->[-Infinity, Infinity, 0.0, 25.0, 1.5E7] ESTIMATE
* c_address-->[-Infinity, Infinity, 0.0, 40.0, 1.5E7] ESTIMATE
* c_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* c_phone-->[-Infinity, Infinity, 0.0, 15.0, 1.5E7] ESTIMATE
* c_acctbal-->[-999.99, 9999.99, 0.0, 8.0, 1086564.0] ESTIMATE
* c_comment-->[-Infinity, Infinity, 0.0, 117.0, 1.4788744E7] ESTIMATE
[end]

