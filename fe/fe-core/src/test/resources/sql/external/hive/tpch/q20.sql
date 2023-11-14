[sql]
select
    s_name,
    s_address
from
    supplier,
    nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                        p_name like 'sienna%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date '1993-01-01'
              and l_shipdate < date '1994-01-01'
        )
    )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
order by
    s_name ;
[fragment statistics]
PLAN FRAGMENT 0(F14)
Output Exprs:2: s_name | 3: s_address
Input Partition: UNPARTITIONED
RESULT SINK

26:MERGING-EXCHANGE
cardinality: 40000
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 40000.0] ESTIMATE

PLAN FRAGMENT 1(F13)

Input Partition: HASH_PARTITIONED: 13: ps_suppkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 26

25:SORT
|  order by: [2, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 40000.0] ESTIMATE
|
24:Project
|  output columns:
|  2 <-> [2: s_name, VARCHAR, true]
|  3 <-> [3: s_address, VARCHAR, true]
|  cardinality: 40000
|  column statistics:
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
|
23:HASH JOIN
|  join op: RIGHT SEMI JOIN (PARTITIONED)
|  equal join conjunct: [13: ps_suppkey, INT, true] = [1: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (1: s_suppkey), remote = true
|  output columns: 2, 3
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 40000.0] ESTIMATE
|
|----22:EXCHANGE
|       cardinality: 40000
|
15:EXCHANGE
cardinality: 39032168

PLAN FRAGMENT 2(F09)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: s_suppkey
OutPut Exchange Id: 22

21:Project
|  output columns:
|  1 <-> [1: s_suppkey, INT, true]
|  2 <-> [2: s_name, VARCHAR, true]
|  3 <-> [3: s_address, VARCHAR, true]
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
|
20:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: s_nationkey, INT, true] = [8: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (8: n_nationkey), remote = false
|  output columns: 1, 2, 3
|  cardinality: 40000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 40000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----19:EXCHANGE
|       cardinality: 1
|
16:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 4: s_nationkey IS NOT NULL
partitions=1/1
avgRowSize=73.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (4: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 3(F10)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 19

18:Project
|  output columns:
|  8 <-> [8: n_nationkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
17:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 9: n_name = 'ARGENTINA'
MIN/MAX PREDICATES: 49: n_name <= 'ARGENTINA', 50: n_name >= 'ARGENTINA'
partitions=1/1
avgRowSize=29.0
numNodes=0
cardinality: 1
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: HASH_PARTITIONED: 28: l_partkey
OutPut Partition: HASH_PARTITIONED: 13: ps_suppkey
OutPut Exchange Id: 15

14:Project
|  output columns:
|  13 <-> [13: ps_suppkey, INT, true]
|  cardinality: 39032168
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|
13:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [28: l_partkey, INT, true] = [12: ps_partkey, INT, true]
|  equal join conjunct: [29: l_suppkey, INT, true] = [13: ps_suppkey, INT, true]
|  other join predicates: cast([14: ps_availqty, INT, true] as DECIMAL128(38,3)) > 0.5 * [43: sum, DECIMAL128(38,2), true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (12: ps_partkey), remote = true
|  - filter_id = 2, build_expr = (13: ps_suppkey), remote = false
|  output columns: 13
|  cardinality: 39032168
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673815217029703E7, 0.0, 8.0, 50.0] ESTIMATE
|
|----12:EXCHANGE
|       cardinality: 20000000
|
4:AGGREGATE (merge finalize)
|  aggregate: sum[([43: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [28: l_partkey, INT, true], [29: l_suppkey, INT, true]
|  cardinality: 86738152
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673815217029703E7, 0.0, 8.0, 50.0] ESTIMATE
|
3:EXCHANGE
cardinality: 86738152
probe runtime filters:
- filter_id = 1, probe_expr = (28: l_partkey)
- filter_id = 2, probe_expr = (29: l_suppkey)

PLAN FRAGMENT 5(F06)

Input Partition: HASH_PARTITIONED: 12: ps_partkey
OutPut Partition: HASH_PARTITIONED: 12: ps_partkey
OutPut Exchange Id: 12

11:Project
|  output columns:
|  12 <-> [12: ps_partkey, INT, true]
|  13 <-> [13: ps_suppkey, INT, true]
|  14 <-> [14: ps_availqty, INT, true]
|  cardinality: 20000000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|
10:HASH JOIN
|  join op: LEFT SEMI JOIN (PARTITIONED)
|  equal join conjunct: [12: ps_partkey, INT, true] = [17: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (17: p_partkey), remote = true
|  output columns: 12, 13, 14
|  cardinality: 20000000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|
|----9:EXCHANGE
|       cardinality: 5000000
|
6:EXCHANGE
cardinality: 80000000

PLAN FRAGMENT 6(F04)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 17: p_partkey
OutPut Exchange Id: 09

8:Project
|  output columns:
|  17 <-> [17: p_partkey, INT, true]
|  cardinality: 5000000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|
7:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 17: p_partkey IS NOT NULL, 18: p_name LIKE 'sienna%'
partitions=1/1
avgRowSize=63.0
numNodes=0
cardinality: 5000000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
* p_name-->[-Infinity, Infinity, 0.0, 55.0, 5000000.0] ESTIMATE

PLAN FRAGMENT 7(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 12: ps_partkey
OutPut Exchange Id: 06

5:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 13: ps_suppkey IS NOT NULL
partitions=1/1
avgRowSize=20.0
numNodes=0
cardinality: 80000000
probe runtime filters:
- filter_id = 0, probe_expr = (12: ps_partkey)
- filter_id = 4, probe_expr = (13: ps_suppkey)
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_availqty-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE

PLAN FRAGMENT 8(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 28: l_partkey
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([31: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true]
|  group by: [28: l_partkey, INT, true], [29: l_suppkey, INT, true]
|  cardinality: 86738152
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673815217029703E7, 0.0, 8.0, 50.0] ESTIMATE
|
1:Project
|  output columns:
|  28 <-> [28: l_partkey, INT, true]
|  29 <-> [29: l_suppkey, INT, true]
|  31 <-> [31: l_quantity, DECIMAL64(15,2), true]
|  cardinality: 86738152
|  column statistics:
|  * l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 29: l_suppkey IS NOT NULL, 37: l_shipdate >= '1993-01-01', 37: l_shipdate < '1994-01-01'
MIN/MAX PREDICATES: 47: l_shipdate >= '1993-01-01', 48: l_shipdate < '1994-01-01'
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 86738152
probe runtime filters:
- filter_id = 1, probe_expr = (28: l_partkey)
- filter_id = 4, probe_expr = (29: l_suppkey)
column statistics:
* l_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* l_shipdate-->[7.258176E8, 7.573536E8, 0.0, 4.0, 2526.0] ESTIMATE
[end]
