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
PLAN FRAGMENT 0(F12)
Output Exprs:2: S_NAME | 3: S_ADDRESS
Input Partition: UNPARTITIONED
RESULT SINK

25:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 40000
column statistics:
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE

PLAN FRAGMENT 1(F11)

Input Partition: HASH_PARTITIONED: 15: PS_SUPPKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 25

24:SORT
|  order by: [2, VARCHAR, false] ASC
|  offset: 0
|  cardinality: 40000
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
23:Project
|  output columns:
|  2 <-> [2: S_NAME, VARCHAR, false]
|  3 <-> [3: S_ADDRESS, VARCHAR, false]
|  cardinality: 40000
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|
22:HASH JOIN
|  join op: RIGHT SEMI JOIN (PARTITIONED)
|  equal join conjunct: [15: PS_SUPPKEY, INT, false] = [1: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (1: S_SUPPKEY), remote = true
|  output columns: 2, 3
|  cardinality: 40000
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----21:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [1: S_SUPPKEY, INT, false]
|       cardinality: 40000
|
14:EXCHANGE
distribution type: SHUFFLE
partition exprs: [15: PS_SUPPKEY, INT, false]
cardinality: 39029703

PLAN FRAGMENT 2(F07)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: S_SUPPKEY
OutPut Exchange Id: 21

20:Project
|  output columns:
|  1 <-> [1: S_SUPPKEY, INT, false]
|  2 <-> [2: S_NAME, CHAR, false]
|  3 <-> [3: S_ADDRESS, VARCHAR, false]
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: S_NATIONKEY, INT, false] = [9: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (9: N_NATIONKEY), remote = false
|  output columns: 1, 2, 3
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----18:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
15:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=73.0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (4: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* S_ADDRESS-->[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 3(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:Project
|  output columns:
|  9 <-> [9: N_NATIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
16:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: [10: N_NAME, CHAR, false] = 'ARGENTINA'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: HASH_PARTITIONED: 32: L_PARTKEY
OutPut Partition: HASH_PARTITIONED: 15: PS_SUPPKEY
OutPut Exchange Id: 14

13:Project
|  output columns:
|  15 <-> [15: PS_SUPPKEY, INT, false]
|  cardinality: 39029703
|  column statistics:
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|
12:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  equal join conjunct: [32: L_PARTKEY, INT, false] = [14: PS_PARTKEY, INT, false]
|  equal join conjunct: [33: L_SUPPKEY, INT, false] = [15: PS_SUPPKEY, INT, false]
|  other join predicates: cast([16: PS_AVAILQTY, INT, false] as DOUBLE) > 0.5 * [48: sum, DOUBLE, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (14: PS_PARTKEY), remote = true
|  - filter_id = 2, build_expr = (15: PS_SUPPKEY), remote = false
|  output columns: 15, 16, 48
|  cardinality: 39029703
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673267326732674E7, 0.0, 8.0, 50.0] ESTIMATE
|
|----11:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [14: PS_PARTKEY, INT, false]
|       cardinality: 20000000
|
4:AGGREGATE (merge finalize)
|  aggregate: sum[([48: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [33: L_SUPPKEY, INT, false], [32: L_PARTKEY, INT, false]
|  cardinality: 86732673
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673267326732674E7, 0.0, 8.0, 50.0] ESTIMATE
|
3:EXCHANGE
distribution type: SHUFFLE
partition exprs: [32: L_PARTKEY, INT, false]
cardinality: 86732673
probe runtime filters:
- filter_id = 1, probe_expr = (32: L_PARTKEY)
- filter_id = 2, probe_expr = (33: L_SUPPKEY)

PLAN FRAGMENT 5(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 14: PS_PARTKEY
OutPut Exchange Id: 11

10:Project
|  output columns:
|  14 <-> [14: PS_PARTKEY, INT, false]
|  15 <-> [15: PS_SUPPKEY, INT, false]
|  16 <-> [16: PS_AVAILQTY, INT, false]
|  cardinality: 20000000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|
9:HASH JOIN
|  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [14: PS_PARTKEY, INT, false] = [20: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (20: P_PARTKEY), remote = false
|  output columns: 14, 15, 16
|  cardinality: 20000000
|  column statistics:
|  * PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|  * PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [20: P_PARTKEY, INT, false]
|       cardinality: 5000000
|
5:OlapScanNode
table: partsupp, rollup: partsupp
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=20.0
cardinality: 80000000
probe runtime filters:
- filter_id = 0, probe_expr = (14: PS_PARTKEY)
- filter_id = 4, probe_expr = (15: PS_SUPPKEY)
column statistics:
* PS_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* PS_SUPPKEY-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
* PS_AVAILQTY-->[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE

PLAN FRAGMENT 6(F03)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 20: P_PARTKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  20 <-> [20: P_PARTKEY, INT, false]
|  cardinality: 5000000
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
|
6:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: 21: P_NAME LIKE 'sienna%'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=63.0
cardinality: 5000000
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 5000000.0] ESTIMATE
* P_NAME-->[-Infinity, Infinity, 0.0, 55.0, 5000000.0] ESTIMATE

PLAN FRAGMENT 7(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 32: L_PARTKEY
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([35: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [33: L_SUPPKEY, INT, false], [32: L_PARTKEY, INT, false]
|  cardinality: 86732673
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[1.0, 8.673267326732674E7, 0.0, 8.0, 50.0] ESTIMATE
|
1:Project
|  output columns:
|  32 <-> [32: L_PARTKEY, INT, false]
|  33 <-> [33: L_SUPPKEY, INT, false]
|  35 <-> [35: L_QUANTITY, DOUBLE, false]
|  cardinality: 86732673
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [41: L_SHIPDATE, DATE, false] >= '1993-01-01', [41: L_SHIPDATE, DATE, false] < '1994-01-01'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=24.0
cardinality: 86732673
probe runtime filters:
- filter_id = 1, probe_expr = (32: L_PARTKEY)
- filter_id = 4, probe_expr = (33: L_SUPPKEY)
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* L_SHIPDATE-->[7.258176E8, 7.573536E8, 0.0, 4.0, 2526.0] ESTIMATE
[dump]
{
  "statement": "select s_name, s_address from supplier, nation where s_suppkey in ( select ps_suppkey from partsupp where ps_partkey in ( select p_partkey from part where p_name like \u0027sienna%\u0027 ) and ps_availqty \u003e ( select 0.5 * sum(l_quantity) from lineitem where l_partkey \u003d ps_partkey and l_suppkey \u003d ps_suppkey and l_shipdate \u003e\u003d date \u00271993-01-01\u0027 and l_shipdate \u003c date \u00271994-01-01\u0027 ) ) and s_nationkey \u003d n_nationkey and n_name \u003d \u0027ARGENTINA\u0027 order by s_name ; ",
  "table_meta": {
    "test.supplier": "CREATE TABLE `supplier` (\n  `S_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `S_NAME` char(25) NOT NULL COMMENT \"\",\n  `S_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `S_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `S_PHONE` char(15) NOT NULL COMMENT \"\",\n  `S_ACCTBAL` double NOT NULL COMMENT \"\",\n  `S_COMMENT` varchar(101) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`S_SUPPKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`S_SUPPKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"fast_schema_evolution\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.nation": "CREATE TABLE `nation` (\n  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_NAME` char(25) NOT NULL COMMENT \"\",\n  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`N_NATIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"fast_schema_evolution\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.partsupp": "CREATE TABLE `partsupp` (\n  `PS_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `PS_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `PS_AVAILQTY` int(11) NOT NULL COMMENT \"\",\n  `PS_SUPPLYCOST` double NOT NULL COMMENT \"\",\n  `PS_COMMENT` varchar(199) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`PS_PARTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`PS_PARTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"fast_schema_evolution\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.part": "CREATE TABLE `part` (\n  `P_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `P_NAME` varchar(55) NOT NULL COMMENT \"\",\n  `P_MFGR` char(25) NOT NULL COMMENT \"\",\n  `P_BRAND` char(10) NOT NULL COMMENT \"\",\n  `P_TYPE` varchar(25) NOT NULL COMMENT \"\",\n  `P_SIZE` int(11) NOT NULL COMMENT \"\",\n  `P_CONTAINER` char(10) NOT NULL COMMENT \"\",\n  `P_RETAILPRICE` double NOT NULL COMMENT \"\",\n  `P_COMMENT` varchar(23) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`P_PARTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`P_PARTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"fast_schema_evolution\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"fast_schema_evolution\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);"
  },
  "table_row_count": {
    "test.nation": {
      "nation": 25
    },
    "test.partsupp": {
      "partsupp": 80000000
    },
    "test.lineitem": {
      "lineitem": 600000000
    },
    "test.supplier": {
      "supplier": 1000000
    },
    "test.part": {
      "part": 20000000
    }
  },
  "column_statistics": {
    "test.nation": {
      "N_NAME": "[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE",
      "N_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE"
    },
    "test.partsupp": {
      "PS_SUPPKEY": "[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE",
      "PS_AVAILQTY": "[1.0, 9999.0, 0.0, 4.0, 9999.0] ESTIMATE",
      "PS_PARTKEY": "[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE"
    },
    "test.lineitem": {
      "L_PARTKEY": "[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE",
      "L_SHIPDATE": "[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE",
      "L_SUPPKEY": "[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE",
      "L_QUANTITY": "[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE"
    },
    "test.supplier": {
      "S_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE",
      "S_NAME": "[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE",
      "S_ADDRESS": "[-Infinity, Infinity, 0.0, 40.0, 10000.0] ESTIMATE",
      "S_SUPPKEY": "[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE"
    },
    "test.part": {
      "P_PARTKEY": "[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE",
      "P_NAME": "[-Infinity, Infinity, 0.0, 55.0, 1.99997E7] ESTIMATE"
    }
  },
  "be_number": 3,
  "be_core_stat": {
    "numOfHardwareCoresPerBe": "{}",
    "cachedAvgNumOfHardwareCores": -1
  },
  "exception": []
}
[end]
