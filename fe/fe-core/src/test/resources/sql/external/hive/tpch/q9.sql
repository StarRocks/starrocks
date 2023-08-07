[sql]
select
        nation,
        o_year,
        sum(amount) as sum_profit
from
        (
                select
                        n_name as nation,
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%peru%'
        ) as profit
group by
        nation,
        o_year
order by
        nation,
        o_year desc ;
[fragment statistics]
PLAN FRAGMENT 0(F16)
Output Exprs:48: n_name | 51: year | 53: sum
Input Partition: UNPARTITIONED
RESULT SINK

28:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 98
column statistics:
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
* sum-->[-49189.1, 104948.5, 0.0, 16.0, 98.4375] ESTIMATE

PLAN FRAGMENT 1(F15)

Input Partition: HASH_PARTITIONED: 48: n_name, 51: year
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 28

27:SORT
|  order by: [48, VARCHAR, true] ASC, [51, SMALLINT, true] DESC
|  offset: 0
|  cardinality: 98
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
|  * sum-->[-49189.1, 104948.5, 0.0, 16.0, 98.4375] ESTIMATE
|
26:AGGREGATE (merge finalize)
|  aggregate: sum[([53: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [48: n_name, VARCHAR, true], [51: year, SMALLINT, true]
|  cardinality: 98
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
|  * sum-->[-49189.1, 104948.5, 0.0, 16.0, 98.4375] ESTIMATE
|
25:EXCHANGE
distribution type: SHUFFLE
partition exprs: [48: n_name, VARCHAR, true], [51: year, SMALLINT, true]
cardinality: 98

PLAN FRAGMENT 2(F12)

Input Partition: HASH_PARTITIONED: 19: l_suppkey
OutPut Partition: HASH_PARTITIONED: 48: n_name, 51: year
OutPut Exchange Id: 25

24:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([52: expr, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [48: n_name, VARCHAR, true], [51: year, SMALLINT, true]
|  cardinality: 98
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
|  * sum-->[-49189.1, 104948.5, 0.0, 16.0, 98.4375] ESTIMATE
|
23:Project
|  output columns:
|  48 <-> [48: n_name, VARCHAR, true]
|  51 <-> year[([42: o_orderdate, DATE, true]); args: DATE; result: SMALLINT; args nullable: true; result nullable: true]
|  52 <-> cast([22: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [23: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2)) - cast([36: ps_supplycost, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast([21: l_quantity, DECIMAL64(15,2), true] as DECIMAL128(15,2))
|  cardinality: 540034112
|  column statistics:
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
|  * expr-->[-49189.1, 104948.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
22:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [19: l_suppkey, INT, true] = [34: ps_suppkey, INT, true]
|  equal join conjunct: [18: l_partkey, INT, true] = [33: ps_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (34: ps_suppkey), remote = true
|  - filter_id = 4, build_expr = (33: ps_partkey), remote = false
|  output columns: 21, 22, 23, 36, 42, 48
|  cardinality: 540034112
|  column statistics:
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1992.0, 1998.0, 0.0, 2.0, 7.0] ESTIMATE
|  * expr-->[-49189.1, 104948.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
|----21:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [34: ps_suppkey, INT, true]
|       cardinality: 80000000
|
19:Project
|  output columns:
|  18 <-> [18: l_partkey, INT, true]
|  19 <-> [19: l_suppkey, INT, true]
|  21 <-> [21: l_quantity, DECIMAL64(15,2), true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  42 <-> [42: o_orderdate, DATE, true]
|  48 <-> [48: n_name, VARCHAR, true]
|  cardinality: 150009476
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [19: l_suppkey, INT, true] = [10: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (10: s_suppkey), remote = true
|  output columns: 18, 19, 21, 22, 23, 42, 48
|  cardinality: 150009476
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [10: s_suppkey, INT, true]
|       cardinality: 1000000
|       probe runtime filters:
|       - filter_id = 3, probe_expr = (10: s_suppkey)
|
11:EXCHANGE
distribution type: SHUFFLE
partition exprs: [19: l_suppkey, INT, true]
cardinality: 150009476
probe runtime filters:
- filter_id = 3, probe_expr = (19: l_suppkey)
- filter_id = 4, probe_expr = (18: l_partkey)

PLAN FRAGMENT 3(F13)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 34: ps_suppkey
OutPut Exchange Id: 21

20:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 34: ps_suppkey IS NOT NULL, 33: ps_partkey IS NOT NULL
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 80000000
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 4(F08)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: s_suppkey
OutPut Exchange Id: 17

16:Project
|  output columns:
|  10 <-> [10: s_suppkey, INT, true]
|  48 <-> [48: n_name, VARCHAR, true]
|  cardinality: 1000000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
15:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [13: s_nationkey, INT, true] = [47: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (47: n_nationkey), remote = false
|  output columns: 10, 48
|  cardinality: 1000000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
|----14:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
12:HdfsScanNode
TABLE: supplier
partitions=1/1
avgRowSize=8.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (13: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 5(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 14

13:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 47: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=29.0
numNodes=0
cardinality: 25
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F06)

Input Partition: HASH_PARTITIONED: 17: l_orderkey
OutPut Partition: HASH_PARTITIONED: 19: l_suppkey
OutPut Exchange Id: 11

10:Project
|  output columns:
|  18 <-> [18: l_partkey, INT, true]
|  19 <-> [19: l_suppkey, INT, true]
|  21 <-> [21: l_quantity, DECIMAL64(15,2), true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  42 <-> [42: o_orderdate, DATE, true]
|  cardinality: 150009476
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [17: l_orderkey, INT, true] = [38: o_orderkey, INT, true]
|  output columns: 18, 19, 21, 22, 23, 42
|  cardinality: 150009476
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [38: o_orderkey, INT, true]
|       cardinality: 150000000
|
6:EXCHANGE
distribution type: SHUFFLE
partition exprs: [17: l_orderkey, INT, true]
cardinality: 150009476

PLAN FRAGMENT 7(F04)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 38: o_orderkey
OutPut Exchange Id: 08

7:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 38: o_orderkey IS NOT NULL
partitions=1/1
avgRowSize=12.0
numNodes=0
cardinality: 150000000
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* o_orderdate-->[6.941952E8, 9.019872E8, 0.0, 4.0, 2412.0] ESTIMATE

PLAN FRAGMENT 8(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: l_orderkey
OutPut Exchange Id: 06

5:Project
|  output columns:
|  17 <-> [17: l_orderkey, INT, true]
|  18 <-> [18: l_partkey, INT, true]
|  19 <-> [19: l_suppkey, INT, true]
|  21 <-> [21: l_quantity, DECIMAL64(15,2), true]
|  22 <-> [22: l_extendedprice, DECIMAL64(15,2), true]
|  23 <-> [23: l_discount, DECIMAL64(15,2), true]
|  cardinality: 150009476
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [18: l_partkey, INT, true] = [1: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: p_partkey), remote = false
|  output columns: 17, 18, 19, 21, 22, 23
|  cardinality: 150009476
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 5000000
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 19: l_suppkey IS NOT NULL, 18: l_partkey IS NOT NULL
partitions=1/1
avgRowSize=44.0
numNodes=0
cardinality: 600037902
probe runtime filters:
- filter_id = 0, probe_expr = (18: l_partkey)
- filter_id = 2, probe_expr = (19: l_suppkey)
- filter_id = 3, probe_expr = (19: l_suppkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 9(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  cardinality: 5000000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|
1:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 2: p_name LIKE '%peru%'
partitions=1/1
avgRowSize=63.0
numNodes=0
cardinality: 5000000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
* p_name-->[-Infinity, Infinity, 0.0, 55.0, 5000000.0] ESTIMATE
[end]
