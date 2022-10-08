[sql]
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
        p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 12
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
        partsupp,
        supplier,
        nation,
        region
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'AMERICA'
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey limit 100;
[fragment statistics]
PLAN FRAGMENT 0(F20)
Output Exprs:15: s_acctbal | 11: s_name | 23: n_name | 1: p_partkey | 3: p_mfgr | 12: s_address | 14: s_phone | 16: s_comment
Input Partition: UNPARTITIONED
RESULT SINK

40:MERGING-EXCHANGE
limit: 100
cardinality: 100
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
* p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 180000.00000000003] ESTIMATE
* s_phone-->[-Infinity, Infinity, 0.0, 15.0, 180000.00000000003] ESTIMATE
* s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 180000.00000000003] ESTIMATE
* s_comment-->[-Infinity, Infinity, 0.0, 101.0, 180000.00000000003] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F19)

Input Partition: HASH_PARTITIONED: 10: s_suppkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 40

39:TOP-N
|  order by: [15, DECIMAL64(15,2), true] DESC, [23, VARCHAR, true] ASC, [11, VARCHAR, true] ASC, [1, INT, true] ASC
|  offset: 0
|  limit: 100
|  cardinality: 100
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 180000.00000000003] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 180000.00000000003] ESTIMATE
|  * s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * s_comment-->[-Infinity, Infinity, 0.0, 101.0, 180000.00000000003] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
38:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  3 <-> [3: p_mfgr, VARCHAR, true]
|  11 <-> [11: s_name, VARCHAR, true]
|  12 <-> [12: s_address, VARCHAR, true]
|  14 <-> [14: s_phone, VARCHAR, true]
|  15 <-> [15: s_acctbal, DECIMAL64(15,2), true]
|  16 <-> [16: s_comment, VARCHAR, true]
|  23 <-> [23: n_name, VARCHAR, true]
|  cardinality: 180000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 180000.00000000003] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 180000.00000000003] ESTIMATE
|  * s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * s_comment-->[-Infinity, Infinity, 0.0, 101.0, 180000.00000000003] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
37:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [10: s_suppkey, INT, true] = [18: ps_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 8, build_expr = (18: ps_suppkey), remote = true
|  output columns: 1, 3, 11, 12, 14, 15, 16, 23
|  cardinality: 180000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 180000.00000000003] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 180000.00000000003] ESTIMATE
|  * s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * s_comment-->[-Infinity, Infinity, 0.0, 101.0, 180000.00000000003] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----36:EXCHANGE
|       cardinality: 360000
|
10:EXCHANGE
cardinality: 200000

PLAN FRAGMENT 2(F06)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: ps_suppkey
OutPut Exchange Id: 36

35:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  3 <-> [3: p_mfgr, VARCHAR, true]
|  18 <-> [18: ps_suppkey, INT, true]
|  cardinality: 360000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 400000.0] ESTIMATE
|
34:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [20: ps_supplycost, DECIMAL64(15,2), true] = [48: min, DECIMAL64(15,2), true]
|  equal join conjunct: [17: ps_partkey, INT, true] = [1: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 6, build_expr = (48: min), remote = false
|  - filter_id = 7, build_expr = (1: p_partkey), remote = false
|  output columns: 1, 3, 18
|  cardinality: 360000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 400000.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----33:EXCHANGE
|       cardinality: 100000
|
11:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 17: ps_partkey IS NOT NULL, 18: ps_suppkey IS NOT NULL
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 80000000
probe runtime filters:
- filter_id = 6, probe_expr = (20: ps_supplycost)
- filter_id = 7, probe_expr = (17: ps_partkey)
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 3(F14)

Input Partition: HASH_PARTITIONED: 29: ps_partkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 33

32:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  3 <-> [3: p_mfgr, VARCHAR, true]
|  48 <-> [48: min, DECIMAL64(15,2), true]
|  cardinality: 100000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
31:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [29: ps_partkey, INT, true] = [1: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 5, build_expr = (1: p_partkey), remote = true
|  output columns: 1, 3, 48
|  cardinality: 100000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----30:EXCHANGE
|       cardinality: 100000
|
27:AGGREGATE (update finalize)
|  aggregate: min[([32: ps_supplycost, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL64(15,2); args nullable: true; result nullable: true]
|  group by: [29: ps_partkey, INT, true]
|  having: 48: min IS NOT NULL
|  cardinality: 16000000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
26:EXCHANGE
cardinality: 16000000
probe runtime filters:
- filter_id = 5, probe_expr = (29: ps_partkey)

PLAN FRAGMENT 4(F15)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: p_partkey
OutPut Exchange Id: 30

29:Project
|  output columns:
|  1 <-> [1: p_partkey, INT, true]
|  3 <-> [3: p_mfgr, VARCHAR, true]
|  cardinality: 100000
|  column statistics:
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
28:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 6: p_size = 12, 5: p_type LIKE '%COPPER'
MIN/MAX PREDICATES: 54: p_size <= 12, 55: p_size >= 12
partitions=1/1
avgRowSize=62.0
numNodes=0
cardinality: 100000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
* p_mfgr-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* p_size-->[12.0, 12.0, 0.0, 4.0, 50.0] ESTIMATE

PLAN FRAGMENT 5(F07)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 29: ps_partkey
OutPut Exchange Id: 26

25:Project
|  output columns:
|  29 <-> [29: ps_partkey, INT, true]
|  32 <-> [32: ps_supplycost, DECIMAL64(15,2), true]
|  cardinality: 16000000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
24:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [30: ps_suppkey, INT, true] = [34: s_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 4, build_expr = (34: s_suppkey), remote = false
|  output columns: 29, 32
|  cardinality: 16000000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 200000.0] ESTIMATE
|  * ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|
|----23:EXCHANGE
|       cardinality: 200000
|
12:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 29: ps_partkey IS NOT NULL, 30: ps_suppkey IS NOT NULL
partitions=1/1
avgRowSize=24.0
numNodes=0
cardinality: 80000000
probe runtime filters:
- filter_id = 4, probe_expr = (30: ps_suppkey)
- filter_id = 5, probe_expr = (29: ps_partkey)
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* ps_supplycost-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 6(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 23

22:Project
|  output columns:
|  34 <-> [34: s_suppkey, INT, true]
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [37: s_nationkey, INT, true] = [41: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 3, build_expr = (41: n_nationkey), remote = false
|  output columns: 34
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
|----20:EXCHANGE
|       cardinality: 5
|
13:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 34: s_suppkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (37: s_nationkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 7(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:Project
|  output columns:
|  41 <-> [41: n_nationkey, INT, true]
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [43: n_regionkey, INT, true] = [45: r_regionkey, INT, true]
|  build runtime filters:
|  - filter_id = 2, build_expr = (45: r_regionkey), remote = false
|  output columns: 41
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----17:EXCHANGE
|       cardinality: 1
|
14:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 41: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=8.0
numNodes=0
cardinality: 25
probe runtime filters:
- filter_id = 2, probe_expr = (43: n_regionkey)
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_regionkey-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 8(F10)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:Project
|  output columns:
|  45 <-> [45: r_regionkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
15:HdfsScanNode
TABLE: region
NON-PARTITION PREDICATES: 46: r_name = 'AMERICA'
MIN/MAX PREDICATES: 50: r_name <= 'AMERICA', 51: r_name >= 'AMERICA'
partitions=1/1
avgRowSize=10.8
numNodes=0
cardinality: 1
column statistics:
* r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* r_name-->[-Infinity, Infinity, 0.0, 6.8, 1.0] ESTIMATE

PLAN FRAGMENT 9(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: s_suppkey
OutPut Exchange Id: 10

9:Project
|  output columns:
|  10 <-> [10: s_suppkey, INT, true]
|  11 <-> [11: s_name, VARCHAR, true]
|  12 <-> [12: s_address, VARCHAR, true]
|  14 <-> [14: s_phone, VARCHAR, true]
|  15 <-> [15: s_acctbal, DECIMAL64(15,2), true]
|  16 <-> [16: s_comment, VARCHAR, true]
|  23 <-> [23: n_name, VARCHAR, true]
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 200000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 200000.0] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 200000.0] ESTIMATE
|  * s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 200000.0] ESTIMATE
|  * s_comment-->[-Infinity, Infinity, 0.0, 101.0, 200000.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [13: s_nationkey, INT, true] = [22: n_nationkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (22: n_nationkey), remote = false
|  output columns: 10, 11, 12, 14, 15, 16, 23
|  cardinality: 200000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 200000.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 200000.0] ESTIMATE
|  * s_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 200000.0] ESTIMATE
|  * s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 200000.0] ESTIMATE
|  * s_comment-->[-Infinity, Infinity, 0.0, 101.0, 200000.0] ESTIMATE
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----7:EXCHANGE
|       cardinality: 5
|
0:HdfsScanNode
TABLE: supplier
partitions=1/1
avgRowSize=197.0
numNodes=0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (13: s_nationkey)
- filter_id = 8, probe_expr = (10: s_suppkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 1000000.0] ESTIMATE
* s_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1000000.0] ESTIMATE
* s_acctbal-->[-998.22, 9999.72, 0.0, 8.0, 656145.0] ESTIMATE
* s_comment-->[-Infinity, Infinity, 0.0, 101.0, 984748.0] ESTIMATE

PLAN FRAGMENT 10(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  22 <-> [22: n_nationkey, INT, true]
|  23 <-> [23: n_name, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [24: n_regionkey, INT, true] = [26: r_regionkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (26: r_regionkey), remote = false
|  output columns: 22, 23
|  cardinality: 5
|  column statistics:
|  * n_nationkey-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * n_name-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * n_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----4:EXCHANGE
|       cardinality: 1
|
1:HdfsScanNode
TABLE: nation
NON-PARTITION PREDICATES: 22: n_nationkey IS NOT NULL
partitions=1/1
avgRowSize=33.0
numNodes=0
cardinality: 25
probe runtime filters:
- filter_id = 0, probe_expr = (24: n_regionkey)
column statistics:
* n_nationkey-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* n_name-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* n_regionkey-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 11(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 04

3:Project
|  output columns:
|  26 <-> [26: r_regionkey, INT, true]
|  cardinality: 1
|  column statistics:
|  * r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
2:HdfsScanNode
TABLE: region
NON-PARTITION PREDICATES: 27: r_name = 'AMERICA'
MIN/MAX PREDICATES: 52: r_name <= 'AMERICA', 53: r_name >= 'AMERICA'
partitions=1/1
avgRowSize=10.8
numNodes=0
cardinality: 1
column statistics:
* r_regionkey-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* r_name-->[-Infinity, Infinity, 0.0, 6.8, 1.0] ESTIMATE
[end]

