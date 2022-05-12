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
PLAN FRAGMENT 0(F14)
Output Exprs:46: N_NAME | 55: sum
Input Partition: UNPARTITIONED
RESULT SINK

28:MERGING-EXCHANGE
cardinality: 5
column statistics:
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F13)

Input Partition: HASH_PARTITIONED: 46: N_NAME
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 28

27:SORT
|  order by: [55, DOUBLE, true] DESC
|  offset: 0
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
26:AGGREGATE (merge finalize)
|  aggregate: sum[([55: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
25:EXCHANGE
cardinality: 5

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 46: N_NAME
OutPut Exchange Id: 25

24:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([54: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 5.0] ESTIMATE
|
23:Project
|  output columns:
|  46 <-> [46: N_NAME, VARCHAR, false]
|  54 <-> [25: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 16390852
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
22:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: C_NATIONKEY, INT, false] = [40: S_NATIONKEY, INT, false]
|  equal join conjunct: [22: L_SUPPKEY, INT, false] = [37: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (40: S_NATIONKEY), remote = true
|  - filter_id = 5, build_expr = (37: S_SUPPKEY), remote = false
|  cardinality: 16390852
|  column statistics:
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|
|----21:EXCHANGE
|       cardinality: 200000
|
10:Project
|  output columns:
|  4 <-> [4: C_NATIONKEY, INT, false]
|  22 <-> [22: L_SUPPKEY, INT, false]
|  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]
|  26 <-> [26: L_DISCOUNT, DOUBLE, false]
|  cardinality: 91060291
|  column statistics:
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (10: O_ORDERKEY), remote = false
|  cardinality: 91060291
|  column statistics:
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----8:EXCHANGE
|       cardinality: 22765073
|       probe runtime filters:
|       - filter_id = 4, probe_expr = (4: C_NATIONKEY)
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=28.0
cardinality: 600000000
probe runtime filters:
- filter_id = 1, probe_expr = (20: L_ORDERKEY)
- filter_id = 5, probe_expr = (22: L_SUPPKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 3(F07)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 21

20:Project
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
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [40: S_NATIONKEY, INT, false] = [45: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (45: N_NATIONKEY), remote = false
|  cardinality: 200000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 200000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
|----18:EXCHANGE
|       cardinality: 5
|
11:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10111
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 3, probe_expr = (40: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:Project
|  output columns:
|  45 <-> [45: N_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, CHAR, false]
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|
16:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [47: N_REGIONKEY, INT, false] = [50: R_REGIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (50: R_REGIONKEY), remote = false
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 5.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----15:EXCHANGE
|       cardinality: 1
|
12:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10185
actualRows=0, avgRowSize=33.0
cardinality: 25
probe runtime filters:
- filter_id = 2, probe_expr = (47: N_REGIONKEY)
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 5(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 15

14:Project
|  output columns:
|  50 <-> [50: R_REGIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
13:OlapScanNode
table: region, rollup: region
preAggregation: on
Predicates: [51: R_NAME, CHAR, false] = 'AFRICA'
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10106
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* R_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 6(F05)

Input Partition: HASH_PARTITIONED: 11: O_CUSTKEY
OutPut Partition: BUCKET_SHFFULE_HASH_PARTITIONED: 10: O_ORDERKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  4 <-> [4: C_NATIONKEY, INT, false]
|  10 <-> [10: O_ORDERKEY, INT, false]
|  cardinality: 22765073
|  column statistics:
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [11: O_CUSTKEY, INT, false] = [1: C_CUSTKEY, INT, false]
|  cardinality: 22765073
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|
|----5:EXCHANGE
|       cardinality: 15000000
|
3:EXCHANGE
cardinality: 22765073

PLAN FRAGMENT 7(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: C_CUSTKEY
OutPut Exchange Id: 05

4:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
actualRows=0, avgRowSize=12.0
cardinality: 15000000
probe runtime filters:
- filter_id = 4, probe_expr = (4: C_NATIONKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 8(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 11: O_CUSTKEY
OutPut Exchange Id: 03

2:Project
|  output columns:
|  10 <-> [10: O_ORDERKEY, INT, false]
|  11 <-> [11: O_CUSTKEY, INT, false]
|  cardinality: 22765073
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|
1:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [14: O_ORDERDATE, DATE, false] >= '1995-01-01', [14: O_ORDERDATE, DATE, false] < '1996-01-01'
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
actualRows=0, avgRowSize=20.0
cardinality: 22765073
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2.2765072765072763E7] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
* O_ORDERDATE-->[7.888896E8, 8.204256E8, 0.0, 4.0, 2406.0] ESTIMATE
[dump]
{
  "statement": "select\n    n_name,\n    sum(l_extendedprice * (1 - l_discount)) as revenue\nfrom\n    customer,\n    orders,\n    lineitem,\n    supplier,\n    nation,\n    region\nwhere\n        c_custkey \u003d o_custkey\n  and l_orderkey \u003d o_orderkey\n  and l_suppkey \u003d s_suppkey\n  and c_nationkey \u003d s_nationkey\n  and s_nationkey \u003d n_nationkey\n  and n_regionkey \u003d r_regionkey\n  and r_name \u003d \u0027AFRICA\u0027\n  and o_orderdate \u003e\u003d date \u00271995-01-01\u0027\n  and o_orderdate \u003c date \u00271996-01-01\u0027\ngroup by\n    n_name\norder by\n    revenue desc ;\n",
  "table_meta": {
    "test.customer": "CREATE TABLE `customer` (\n  `C_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `C_NAME` varchar(25) NOT NULL COMMENT \"\",\n  `C_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `C_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `C_PHONE` char(15) NOT NULL COMMENT \"\",\n  `C_ACCTBAL` double NOT NULL COMMENT \"\",\n  `C_MKTSEGMENT` char(10) NOT NULL COMMENT \"\",\n  `C_COMMENT` varchar(117) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`C_CUSTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`C_CUSTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.nation": "CREATE TABLE `nation` (\n  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_NAME` char(25) NOT NULL COMMENT \"\",\n  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`N_NATIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.region": "CREATE TABLE `region` (\n  `R_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `R_NAME` char(25) NOT NULL COMMENT \"\",\n  `R_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`R_REGIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`R_REGIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.orders": "CREATE TABLE `orders` (\n  `O_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `O_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `O_ORDERSTATUS` char(1) NOT NULL COMMENT \"\",\n  `O_TOTALPRICE` double NOT NULL COMMENT \"\",\n  `O_ORDERDATE` date NOT NULL COMMENT \"\",\n  `O_ORDERPRIORITY` char(15) NOT NULL COMMENT \"\",\n  `O_CLERK` char(15) NOT NULL COMMENT \"\",\n  `O_SHIPPRIORITY` int(11) NOT NULL COMMENT \"\",\n  `O_COMMENT` varchar(79) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`O_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`O_ORDERKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.supplier": "CREATE TABLE `supplier` (\n  `S_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `S_NAME` char(25) NOT NULL COMMENT \"\",\n  `S_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `S_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `S_PHONE` char(15) NOT NULL COMMENT \"\",\n  `S_ACCTBAL` double NOT NULL COMMENT \"\",\n  `S_COMMENT` varchar(101) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`S_SUPPKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`S_SUPPKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);"
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
  "exception": []
}
[end]