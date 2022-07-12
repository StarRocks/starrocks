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
PLAN FRAGMENT 0(F17)
Output Exprs:16: S_ACCTBAL | 12: S_NAME | 26: N_NAME | 1: P_PARTKEY | 3: P_MFGR | 13: S_ADDRESS | 15: S_PHONE | 17: S_COMMENT
Input Partition: UNPARTITIONED
RESULT SINK

38:MERGING-EXCHANGE
limit: 100
cardinality: 100
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
* P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
* S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
* S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 38

37:TOP-N
|  order by: [16, DOUBLE, false] DESC, [26, VARCHAR, false] ASC, [12, VARCHAR, false] ASC, [1, INT, false] ASC
|  offset: 0
|  limit: 100
|  cardinality: 100
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
|  * S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
|  * S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
36:Project
|  output columns:
|  1 <-> [1: P_PARTKEY, INT, false]
|  3 <-> [3: P_MFGR, VARCHAR, false]
|  12 <-> [12: S_NAME, VARCHAR, false]
|  13 <-> [13: S_ADDRESS, VARCHAR, false]
|  15 <-> [15: S_PHONE, VARCHAR, false]
|  16 <-> [16: S_ACCTBAL, DOUBLE, false]
|  17 <-> [17: S_COMMENT, VARCHAR, false]
|  26 <-> [26: N_NAME, VARCHAR, false]
|  cardinality: 180000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
|  * S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
|  * S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
35:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [11: S_SUPPKEY, INT, false] = [20: PS_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 8, build_expr = (20: PS_SUPPKEY), remote = false
|  output columns: 1, 3, 12, 13, 15, 16, 17, 26
|  cardinality: 180000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 180000.00000000003] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 180000.00000000003] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
|  * S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
|  * S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 180000.00000000003] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----34:EXCHANGE
|       cardinality: 360000
|
9:Project
|  output columns:
|  11 <-> [11: S_SUPPKEY, INT, false]
|  12 <-> [12: S_NAME, CHAR, false]
|  13 <-> [13: S_ADDRESS, VARCHAR, false]
|  15 <-> [15: S_PHONE, CHAR, false]
|  16 <-> [16: S_ACCTBAL, DOUBLE, false]
|  17 <-> [17: S_COMMENT, VARCHAR, false]
|  26 <-> [26: N_NAME, VARCHAR, false]
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 200000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
|  * S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
|  * S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [14: S_NATIONKEY, INT, false] = [25: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (25: N_NATIONKEY), remote = false
|  output columns: 11, 12, 13, 15, 16, 17, 26
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 200000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
|  * S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
|  * S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----7:EXCHANGE
|       cardinality: 5
|
0:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=197.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (14: S_NATIONKEY)
- filter_id = 8, probe_expr = (11: S_SUPPKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* S_PHONE-->[-Infinity, Infinity, 0.0, 15.0, 10000.0] ESTIMATE
* S_ACCTBAL-->[-998.22, 9999.72, 0.0, 8.0, 9955.0] ESTIMATE
* S_COMMENT-->[-Infinity, Infinity, 0.0, 101.0, 10000.0] ESTIMATE

PLAN FRAGMENT 2(F05)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 20: PS_SUPPKEY
OutPut Exchange Id: 34

33:Project
|  output columns:
|  1 <-> [1: P_PARTKEY, INT, false]
|  3 <-> [3: P_MFGR, VARCHAR, false]
|  20 <-> [20: PS_SUPPKEY, INT, false]
|  cardinality: 360000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 400000.0] ESTIMATE
|
32:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [22: PS_SUPPLYCOST, DOUBLE, false] = [57: min, DOUBLE, true]
|  equal join conjunct: [19: PS_PARTKEY, INT, false] = [1: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 6, build_expr = (57: min), remote = false
|  - filter_id = 7, build_expr = (1: P_PARTKEY), remote = false
|  output columns: 1, 3, 20
|  cardinality: 360000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 400000.0] ESTIMATE
|  * PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----31:EXCHANGE
|       cardinality: 100000
|
10:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=24.0
cardinality: 80000000
probe runtime filters:
- filter_id = 6, probe_expr = (22: PS_SUPPLYCOST)
- filter_id = 7, probe_expr = (19: PS_PARTKEY)
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 3(F06)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 1: P_PARTKEY
OutPut Exchange Id: 31

30:Project
|  output columns:
|  1 <-> [1: P_PARTKEY, INT, false]
|  3 <-> [3: P_MFGR, VARCHAR, false]
|  57 <-> [57: min, DOUBLE, true]
|  cardinality: 100000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
29:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [34: PS_PARTKEY, INT, false] = [1: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 5, build_expr = (1: P_PARTKEY), remote = false
|  output columns: 1, 3, 57
|  cardinality: 100000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
|----28:EXCHANGE
|       cardinality: 100000
|
25:AGGREGATE (update finalize)
|  aggregate: min[([37: PS_SUPPLYCOST, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [34: PS_PARTKEY, INT, false]
|  having: 57: min IS NOT NULL
|  cardinality: 16000000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * min-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
24:Project
|  output columns:
|  34 <-> [34: PS_PARTKEY, INT, false]
|  37 <-> [37: PS_SUPPLYCOST, DOUBLE, false]
|  cardinality: 16000000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|
23:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [35: PS_SUPPKEY, INT, false] = [40: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (40: S_SUPPKEY), remote = false
|  output columns: 34, 37
|  cardinality: 16000000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 1.6E7] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 200000.0] ESTIMATE
|  * PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|
|----22:EXCHANGE
|       cardinality: 200000
|
11:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=24.0
cardinality: 80000000
probe runtime filters:
- filter_id = 4, probe_expr = (35: PS_SUPPKEY)
- filter_id = 5, probe_expr = (34: PS_PARTKEY)
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* PS_SUPPLYCOST-->[1.0, 1000.0, 0.0, 8.0, 99864.0] ESTIMATE

PLAN FRAGMENT 4(F13)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 1: P_PARTKEY
OutPut Exchange Id: 28

27:Project
|  output columns:
|  1 <-> [1: P_PARTKEY, INT, false]
|  3 <-> [3: P_MFGR, CHAR, false]
|  cardinality: 100000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
|  * P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
26:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: [6: P_SIZE, INT, false] = 12, 5: P_TYPE LIKE '%COPPER'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=62.0
cardinality: 100000
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 100000.0] ESTIMATE
* P_MFGR-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* P_SIZE-->[12.0, 12.0, 0.0, 4.0, 50.0] ESTIMATE

PLAN FRAGMENT 5(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 22

21:Project
|  output columns:
|  40 <-> [40: S_SUPPKEY, INT, false]
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|
20:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [43: S_NATIONKEY, INT, false] = [48: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (48: N_NATIONKEY), remote = false
|  output columns: 40
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
|----19:EXCHANGE
|       cardinality: 5
|
12:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (43: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 19

18:Project
|  output columns:
|  48 <-> [48: N_NATIONKEY, INT, false]
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
17:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [50: N_REGIONKEY, INT, false] = [53: R_REGIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (53: R_REGIONKEY), remote = false
|  output columns: 48
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----16:EXCHANGE
|       cardinality: 1
|
13:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 25
probe runtime filters:
- filter_id = 2, probe_expr = (50: N_REGIONKEY)
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 7(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 16

15:Project
|  output columns:
|  53 <-> [53: R_REGIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
14:OlapScanNode
table: region, rollup: region
preAggregation: on
Predicates: [54: R_NAME, CHAR, false] = 'AMERICA'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* R_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 8(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:Project
|  output columns:
|  25 <-> [25: N_NATIONKEY, INT, false]
|  26 <-> [26: N_NAME, CHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: N_REGIONKEY, INT, false] = [30: R_REGIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (30: R_REGIONKEY), remote = false
|  output columns: 25, 26
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----4:EXCHANGE
|       cardinality: 1
|
1:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=33.0
cardinality: 25
probe runtime filters:
- filter_id = 0, probe_expr = (27: N_REGIONKEY)
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 9(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 04

3:Project
|  output columns:
|  30 <-> [30: R_REGIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
2:OlapScanNode
table: region, rollup: region
preAggregation: on
Predicates: [31: R_NAME, CHAR, false] = 'AMERICA'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* R_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[end]

