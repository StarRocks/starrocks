[sql]
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
                s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
                (n1.n_name = 'CANADA' and n2.n_name = 'IRAN')
                or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')
            )
          and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year ;
[fragment statistics]
PLAN FRAGMENT 0(F12)
Output Exprs:46: N_NAME | 51: N_NAME | 55: year | 57: sum
Input Partition: UNPARTITIONED
RESULT SINK

25:MERGING-EXCHANGE
cardinality: 250
column statistics:
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE

PLAN FRAGMENT 1(F11)

Input Partition: HASH_PARTITIONED: 46: N_NAME, 51: N_NAME, 55: year
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 25

24:SORT
|  order by: [46, VARCHAR, false] ASC, [51, VARCHAR, false] ASC, [55, INT, true] ASC
|  offset: 0
|  cardinality: 250
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
23:AGGREGATE (merge finalize)
|  aggregate: sum[([57: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false], [51: N_NAME, VARCHAR, false], [55: year, INT, true]
|  cardinality: 250
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
22:EXCHANGE
cardinality: 297

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 46: N_NAME, 51: N_NAME, 55: year
OutPut Exchange Id: 22

21:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([56: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false], [51: N_NAME, VARCHAR, false], [55: year, INT, true]
|  cardinality: 297
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
20:Project
|  output columns:
|  46 <-> [46: N_NAME, VARCHAR, false]
|  51 <-> [51: N_NAME, CHAR, false]
|  55 <-> year[(cast([19: L_SHIPDATE, DATE, false] as DATETIME)); args: DATETIME; result: INT; args nullable: true; result nullable: true]
|  56 <-> [14: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [15: L_DISCOUNT, DOUBLE, false]
|  cardinality: 554645
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
19:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [39: C_NATIONKEY, INT, false] = [50: N_NATIONKEY, INT, false]
|  other join predicates: ((46: N_NAME = 'CANADA') AND (51: N_NAME = 'IRAN')) OR ((46: N_NAME = 'IRAN') AND (51: N_NAME = 'CANADA'))
|  build runtime filters:
|  - filter_id = 3, build_expr = (50: N_NATIONKEY), remote = true
|  cardinality: 554645
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 4.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
|----18:EXCHANGE
|       cardinality: 25
|
16:Project
|  output columns:
|  14 <-> [14: L_EXTENDEDPRICE, DOUBLE, false]
|  15 <-> [15: L_DISCOUNT, DOUBLE, false]
|  19 <-> [19: L_SHIPDATE, DATE, false]
|  39 <-> [39: C_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, VARCHAR, false]
|  cardinality: 173465347
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
15:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [11: L_SUPPKEY, INT, false] = [1: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (1: S_SUPPKEY), remote = false
|  cardinality: 173465347
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
|----14:EXCHANGE
|       cardinality: 1000000
|
8:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  14 <-> [14: L_EXTENDEDPRICE, DOUBLE, false]
|  15 <-> [15: L_DISCOUNT, DOUBLE, false]
|  19 <-> [19: L_SHIPDATE, DATE, false]
|  39 <-> [39: C_NATIONKEY, INT, false]
|  cardinality: 173465347
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|
7:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [9: L_ORDERKEY, INT, false] = [26: O_ORDERKEY, INT, false]
|  cardinality: 173465347
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|
|----6:EXCHANGE
|       cardinality: 150000000
|       probe runtime filters:
|       - filter_id = 3, probe_expr = (39: C_NATIONKEY)
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [19: L_SHIPDATE, DATE, false] >= '1995-01-01', [19: L_SHIPDATE, DATE, false] <= '1996-12-31'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=32.0
cardinality: 173465347
probe runtime filters:
- filter_id = 2, probe_expr = (11: L_SUPPKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE

PLAN FRAGMENT 3(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 18

17:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: 51: N_NAME IN ('IRAN', 'CANADA')
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10185
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 14

13:Project
|  output columns:
|  1 <-> [1: S_SUPPKEY, INT, false]
|  46 <-> [46: N_NAME, CHAR, false]
|  cardinality: 1000000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
12:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: S_NATIONKEY, INT, false] = [45: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (45: N_NATIONKEY), remote = false
|  cardinality: 1000000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|
|----11:EXCHANGE
|       cardinality: 25
|
9:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10111
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (4: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 5(F06)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 11

10:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: 46: N_NAME IN ('CANADA', 'IRAN')
partitionsRatio=1/1, tabletsRatio=1/1
tabletList=10185
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHFFULE_HASH_PARTITIONED: 26: O_ORDERKEY
OutPut Exchange Id: 06

5:Project
|  output columns:
|  26 <-> [26: O_ORDERKEY, INT, false]
|  39 <-> [39: C_NATIONKEY, INT, false]
|  cardinality: 150000000
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: O_CUSTKEY, INT, false] = [36: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (36: C_CUSTKEY), remote = false
|  cardinality: 150000000
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|  * C_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|
|----3:EXCHANGE
|       cardinality: 15000000
|
1:OlapScanNode
table: orders, rollup: orders
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10139,10141,10143,10145,10147,10149,10151,10153,10155,10157
actualRows=0, avgRowSize=16.0
cardinality: 150000000
probe runtime filters:
- filter_id = 0, probe_expr = (27: O_CUSTKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE

PLAN FRAGMENT 7(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10162,10164,10166,10168,10170,10172,10174,10176,10178,10180
actualRows=0, avgRowSize=12.0
cardinality: 15000000
probe runtime filters:
- filter_id = 3, probe_expr = (39: C_NATIONKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
[dump]
{
  "statement": "select\n    supp_nation,\n    cust_nation,\n    l_year,\n    sum(volume) as revenue\nfrom\n    (\n        select\n            n1.n_name as supp_nation,\n            n2.n_name as cust_nation,\n            extract(year from l_shipdate) as l_year,\n            l_extendedprice * (1 - l_discount) as volume\n        from\n            supplier,\n            lineitem,\n            orders,\n            customer,\n            nation n1,\n            nation n2\n        where\n                s_suppkey \u003d l_suppkey\n          and o_orderkey \u003d l_orderkey\n          and c_custkey \u003d o_custkey\n          and s_nationkey \u003d n1.n_nationkey\n          and c_nationkey \u003d n2.n_nationkey\n          and (\n                (n1.n_name \u003d \u0027CANADA\u0027 and n2.n_name \u003d \u0027IRAN\u0027)\n                or (n1.n_name \u003d \u0027IRAN\u0027 and n2.n_name \u003d \u0027CANADA\u0027)\n            )\n          and l_shipdate between date \u00271995-01-01\u0027 and date \u00271996-12-31\u0027\n    ) as shipping\ngroup by\n    supp_nation,\n    cust_nation,\n    l_year\norder by\n    supp_nation,\n    cust_nation,\n    l_year ;\n",
  "table_meta": {
    "test.customer": "CREATE TABLE `customer` (\n  `C_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `C_NAME` varchar(25) NOT NULL COMMENT \"\",\n  `C_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `C_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `C_PHONE` char(15) NOT NULL COMMENT \"\",\n  `C_ACCTBAL` double NOT NULL COMMENT \"\",\n  `C_MKTSEGMENT` char(10) NOT NULL COMMENT \"\",\n  `C_COMMENT` varchar(117) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`C_CUSTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`C_CUSTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
    "test.nation": "CREATE TABLE `nation` (\n  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_NAME` char(25) NOT NULL COMMENT \"\",\n  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`N_NATIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);",
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
      "N_NATIONKEY": "[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE"
    },
    "test.lineitem": {
      "L_SHIPDATE": "[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE",
      "L_SUPPKEY": "[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE",
      "L_EXTENDEDPRICE": "[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE",
      "L_DISCOUNT": "[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE",
      "L_ORDERKEY": "[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE"
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
      "O_ORDERKEY": "[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE",
      "O_CUSTKEY": "[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE"
    }
  },
  "be_number": 3,
  "exception": []
}
[end]

