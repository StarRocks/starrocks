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
PLAN FRAGMENT 0(F12)
Output Exprs:46: N_NAME | 55: sum
Input Partition: UNPARTITIONED
RESULT SINK

27:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 5
column statistics:
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F11)

Input Partition: HASH_PARTITIONED: 46: N_NAME
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 27

26:SORT
|  order by: [55, DOUBLE, true] DESC
|  offset: 0
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
25:AGGREGATE (merge finalize)
|  aggregate: sum[([55: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
24:EXCHANGE
distribution type: SHUFFLE
partition exprs: [46: N_NAME, VARCHAR, false]
cardinality: 5

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 46: N_NAME
OutPut Exchange Id: 24

23:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([54: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
22:Project
|  output columns:
|  46 <-> [46: N_NAME, VARCHAR, false]
|  54 <-> [25: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 16390852
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [40: S_NATIONKEY, INT, false] = [4: C_NATIONKEY, INT, false]
|  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (4: C_NATIONKEY), remote = false
|  - filter_id = 5, build_expr = (10: O_ORDERKEY), remote = false
|  output columns: 25, 26, 46
|  cardinality: 16390852
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
|----20:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [10: O_ORDERKEY, INT, false]
|       cardinality: 22765073
|
13:Project
|  output columns:
|  20 <-> [20: L_ORDERKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  40 <-> [40: S_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, VARCHAR, false]
|  cardinality: 120000000
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.2E8] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
12:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [22: L_SUPPKEY, INT, false] = [37: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (37: S_SUPPKEY), remote = false
|  output columns: 20, 25, 26, 40, 46
|  cardinality: 120000000
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.2E8] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----11:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 200000
|       probe runtime filters:
|       - filter_id = 4, probe_expr = (40: S_NATIONKEY)
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=28.0
cardinality: 600000000
probe runtime filters:
- filter_id = 2, probe_expr = (22: L_SUPPKEY)
- filter_id = 5, probe_expr = (20: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 3(F07)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 10: O_ORDERKEY
OutPut Exchange Id: 20

19:Project
|  output columns:
|  4 <-> [4: C_NATIONKEY, INT, false]
|  10 <-> [10: O_ORDERKEY, INT, false]
|  cardinality: 22765073
|  column statistics:
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [1: C_CUSTKEY, INT, false] = [11: O_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (11: O_CUSTKEY), remote = false
|  output columns: 4, 10
|  cardinality: 22765073
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [11: O_CUSTKEY, INT, false]
|       cardinality: 22765073
|
14:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=12.0
cardinality: 15000000
probe runtime filters:
- filter_id = 3, probe_expr = (1: C_CUSTKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F08)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 11: O_CUSTKEY
OutPut Exchange Id: 17

16:Project
|  output columns:
|  10 <-> [10: O_ORDERKEY, INT, false]
|  11 <-> [11: O_CUSTKEY, INT, false]
|  cardinality: 22765073
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|
15:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [14: O_ORDERDATE, DATE, false] >= '1995-01-01', [14: O_ORDERDATE, DATE, false] < '1996-01-01'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=20.0
cardinality: 22765073
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
* O_ORDERDATE-->[7.888896E8, 8.204256E8, 0.0, 4.0, 2406.0] ESTIMATE

PLAN FRAGMENT 5(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 11

10:Project
|  output columns:
|  37 <-> [37: S_SUPPKEY, INT, false]
|  40 <-> [40: S_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, VARCHAR, false]
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [40: S_NATIONKEY, INT, false] = [45: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (45: N_NATIONKEY), remote = false
|  output columns: 37, 40, 46
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 5
|
1:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (40: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:Project
|  output columns:
|  45 <-> [45: N_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, CHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [47: N_REGIONKEY, INT, false] = [50: R_REGIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (50: R_REGIONKEY), remote = false
|  output columns: 45, 46
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
2:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=33.0
cardinality: 25
probe runtime filters:
- filter_id = 0, probe_expr = (47: N_REGIONKEY)
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 7(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 05

4:Project
|  output columns:
|  50 <-> [50: R_REGIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
3:OlapScanNode
table: region, rollup: region
preAggregation: on
Predicates: [51: R_NAME, CHAR, false] = 'AFRICA'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* R_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[dump]
{
  "statement": "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey \u003d o_custkey and l_orderkey \u003d o_orderkey and l_suppkey \u003d s_suppkey and c_nationkey \u003d s_nationkey and s_nationkey \u003d n_nationkey and n_regionkey \u003d r_regionkey and r_name \u003d \u0027AFRICA\u0027 and o_orderdate \u003e\u003d date \u00271995-01-01\u0027 and o_orderdate \u003c date \u00271996-01-01\u0027 group by n_name order by revenue desc ; ",
  "table_meta": {
    "test.customer": "CREATE TABLE `customer` (\n  `C_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `C_NAME` varchar(25) NOT NULL COMMENT \"\",\n  `C_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `C_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `C_PHONE` char(15) NOT NULL COMMENT \"\",\n  `C_ACCTBAL` double NOT NULL COMMENT \"\",\n  `C_MKTSEGMENT` char(10) NOT NULL COMMENT \"\",\n  `C_COMMENT` varchar(117) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`C_CUSTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`C_CUSTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.orders": "CREATE TABLE `orders` (\n  `O_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `O_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `O_ORDERSTATUS` char(1) NOT NULL COMMENT \"\",\n  `O_TOTALPRICE` double NOT NULL COMMENT \"\",\n  `O_ORDERDATE` date NOT NULL COMMENT \"\",\n  `O_ORDERPRIORITY` char(15) NOT NULL COMMENT \"\",\n  `O_CLERK` char(15) NOT NULL COMMENT \"\",\n  `O_SHIPPRIORITY` int(11) NOT NULL COMMENT \"\",\n  `O_COMMENT` varchar(79) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`O_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`O_ORDERKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.supplier": "CREATE TABLE `supplier` (\n  `S_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `S_NAME` char(25) NOT NULL COMMENT \"\",\n  `S_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `S_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `S_PHONE` char(15) NOT NULL COMMENT \"\",\n  `S_ACCTBAL` double NOT NULL COMMENT \"\",\n  `S_COMMENT` varchar(101) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`S_SUPPKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`S_SUPPKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.nation": "CREATE TABLE `nation` (\n  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_NAME` char(25) NOT NULL COMMENT \"\",\n  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`N_NATIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.region": "CREATE TABLE `region` (\n  `R_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `R_NAME` char(25) NOT NULL COMMENT \"\",\n  `R_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`R_REGIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`R_REGIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);"
  },
  "table_row_count": {
    "test.nation": {
      "nation": 25
    },
    "test.lineitem": {
      "lineitem": 600000000
    },
    "test.region": {
      "region": 5
    },
    "test.supplier": {
      "supplier": 1000000
    },
    "test.customer": {
      "customer": 15000000
    },
    "test.orders": {
      "orders": 150000000
    }
  },
  "column_statistics": {
    "test.nation": {
      "N_NAME": "[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE",
      "N_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE",
      "N_REGIONKEY": "[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE"
    },
    "test.lineitem": {
      "L_SUPPKEY": "[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE",
      "L_EXTENDEDPRICE": "[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE",
      "L_DISCOUNT": "[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE",
      "L_ORDERKEY": "[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE"
    },
    "test.region": {
      "R_NAME": "[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE",
      "R_REGIONKEY": "[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE"
    },
    "test.supplier": {
      "S_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE",
      "S_SUPPKEY": "[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE"
    },
    "test.customer": {
      "C_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE",
      "C_CUSTKEY": "[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE"
    },
    "test.orders": {
      "O_ORDERDATE": "[6.941952E8, 9.019872E8, 0.0, 4.0, 2406.0] ESTIMATE",
      "O_ORDERKEY": "[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE",
      "O_CUSTKEY": "[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE"
    }
  },
  "be_number": 3,
  "be_core_stat": {
    "numOfHardwareCoresPerBe": "{}",
    "cachedAvgNumOfHardwareCores": 0
  },
  "exception": []
}
[end]
