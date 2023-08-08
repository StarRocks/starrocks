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

24:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 352
column statistics:
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
* year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 351.5625] ESTIMATE

PLAN FRAGMENT 1(F11)

Input Partition: HASH_PARTITIONED: 46: N_NAME, 51: N_NAME, 55: year
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 24

23:SORT
|  order by: [46, VARCHAR, false] ASC, [51, VARCHAR, false] ASC, [55, SMALLINT, false] ASC
|  offset: 0
|  cardinality: 352
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 351.5625] ESTIMATE
|
22:AGGREGATE (merge finalize)
|  aggregate: sum[([57: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false], [51: N_NAME, VARCHAR, false], [55: year, SMALLINT, false]
|  cardinality: 352
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 351.5625] ESTIMATE
|
21:EXCHANGE
distribution type: SHUFFLE
partition exprs: [46: N_NAME, VARCHAR, false], [51: N_NAME, VARCHAR, false], [55: year, SMALLINT, false]
cardinality: 352

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 46: N_NAME, 51: N_NAME, 55: year
OutPut Exchange Id: 21

20:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([56: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [46: N_NAME, VARCHAR, false], [51: N_NAME, VARCHAR, false], [55: year, SMALLINT, false]
|  cardinality: 352
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 351.5625] ESTIMATE
|
19:Project
|  output columns:
|  46 <-> [46: N_NAME, VARCHAR, false]
|  51 <-> [51: N_NAME, VARCHAR, false]
|  55 <-> year[([19: L_SHIPDATE, DATE, false]); args: DATE; result: SMALLINT; args nullable: false; result nullable: false]
|  56 <-> [14: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [15: L_DISCOUNT, DOUBLE, false]
|  cardinality: 554645
|  column statistics:
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [45: N_NATIONKEY, INT, false] = [4: S_NATIONKEY, INT, false]
|  equal join conjunct: [11: L_SUPPKEY, INT, false] = [1: S_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (4: S_NATIONKEY), remote = true
|  - filter_id = 4, build_expr = (1: S_SUPPKEY), remote = false
|  output columns: 14, 15, 19, 46, 51
|  cardinality: 554645
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 277544.5544554456] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1000000
|
15:Project
|  output columns:
|  11 <-> [11: L_SUPPKEY, INT, false]
|  14 <-> [14: L_EXTENDEDPRICE, DOUBLE, false]
|  15 <-> [15: L_DISCOUNT, DOUBLE, false]
|  19 <-> [19: L_SHIPDATE, DATE, false]
|  45 <-> [45: N_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, VARCHAR, false]
|  51 <-> [51: N_NAME, VARCHAR, false]
|  cardinality: 6938614
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
14:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [9: L_ORDERKEY, INT, false] = [26: O_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (26: O_ORDERKEY), remote = false
|  output columns: 11, 14, 15, 19, 45, 46, 51
|  cardinality: 6938614
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
|----13:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [26: O_ORDERKEY, INT, false]
|       cardinality: 6000000
|       probe runtime filters:
|       - filter_id = 3, probe_expr = (45: N_NATIONKEY)
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [19: L_SHIPDATE, DATE, false] >= '1995-01-01', [19: L_SHIPDATE, DATE, false] <= '1996-12-31'
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=32.0
cardinality: 173465347
probe runtime filters:
- filter_id = 2, probe_expr = (9: L_ORDERKEY)
- filter_id = 4, probe_expr = (11: L_SUPPKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2526.0] ESTIMATE

PLAN FRAGMENT 3(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 26: O_ORDERKEY
OutPut Exchange Id: 13

12:Project
|  output columns:
|  26 <-> [26: O_ORDERKEY, INT, false]
|  45 <-> [45: N_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, VARCHAR, false]
|  51 <-> [51: N_NAME, VARCHAR, false]
|  cardinality: 6000000
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6000000.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [27: O_CUSTKEY, INT, false] = [36: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (36: C_CUSTKEY), remote = false
|  output columns: 26, 45, 46, 51
|  cardinality: 6000000
|  probe runtime filters:
|  - filter_id = 3, probe_expr = (45: N_NATIONKEY)
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6000000.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
|----10:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 600000
|
1:OlapScanNode
table: orders, rollup: orders
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=16.0
cardinality: 150000000
probe runtime filters:
- filter_id = 1, probe_expr = (27: O_CUSTKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE

PLAN FRAGMENT 5(F02)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:Project
|  output columns:
|  36 <-> [36: C_CUSTKEY, INT, false]
|  45 <-> [45: N_NATIONKEY, INT, false]
|  46 <-> [46: N_NAME, CHAR, false]
|  51 <-> [51: N_NAME, CHAR, false]
|  cardinality: 600000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 600000.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [39: C_NATIONKEY, INT, false] = [50: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (50: N_NATIONKEY), remote = false
|  output columns: 36, 45, 46, 51
|  cardinality: 600000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 600000.0] ESTIMATE
|  * C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
|----7:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
2:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=12.0
cardinality: 15000000
probe runtime filters:
- filter_id = 0, probe_expr = (39: C_NATIONKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 07

6:NESTLOOP JOIN
|  join op: INNER JOIN
|  other join predicates: ((46: N_NAME = 'CANADA') AND (51: N_NAME = 'IRAN')) OR ((46: N_NAME = 'IRAN') AND (51: N_NAME = 'CANADA'))
|  cardinality: 1
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
3:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: 46: N_NAME IN ('CANADA', 'IRAN')
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 7(F04)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 05

4:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: 51: N_NAME IN ('IRAN', 'CANADA')
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
[dump]
{
  "statement": "select supp_nation, cust_nation, l_year, sum(volume) as revenue from ( select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey \u003d l_suppkey and o_orderkey \u003d l_orderkey and c_custkey \u003d o_custkey and s_nationkey \u003d n1.n_nationkey and c_nationkey \u003d n2.n_nationkey and ( (n1.n_name \u003d \u0027CANADA\u0027 and n2.n_name \u003d \u0027IRAN\u0027) or (n1.n_name \u003d \u0027IRAN\u0027 and n2.n_name \u003d \u0027CANADA\u0027) ) and l_shipdate between date \u00271995-01-01\u0027 and date \u00271996-12-31\u0027 ) as shipping group by supp_nation, cust_nation, l_year order by supp_nation, cust_nation, l_year ; ",
  "table_meta": {
    "test.supplier": "CREATE TABLE `supplier` (\n  `S_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `S_NAME` char(25) NOT NULL COMMENT \"\",\n  `S_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `S_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `S_PHONE` char(15) NOT NULL COMMENT \"\",\n  `S_ACCTBAL` double NOT NULL COMMENT \"\",\n  `S_COMMENT` varchar(101) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`S_SUPPKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`S_SUPPKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.orders": "CREATE TABLE `orders` (\n  `O_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `O_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `O_ORDERSTATUS` char(1) NOT NULL COMMENT \"\",\n  `O_TOTALPRICE` double NOT NULL COMMENT \"\",\n  `O_ORDERDATE` date NOT NULL COMMENT \"\",\n  `O_ORDERPRIORITY` char(15) NOT NULL COMMENT \"\",\n  `O_CLERK` char(15) NOT NULL COMMENT \"\",\n  `O_SHIPPRIORITY` int(11) NOT NULL COMMENT \"\",\n  `O_COMMENT` varchar(79) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`O_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`O_ORDERKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.customer": "CREATE TABLE `customer` (\n  `C_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n  `C_NAME` varchar(25) NOT NULL COMMENT \"\",\n  `C_ADDRESS` varchar(40) NOT NULL COMMENT \"\",\n  `C_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `C_PHONE` char(15) NOT NULL COMMENT \"\",\n  `C_ACCTBAL` double NOT NULL COMMENT \"\",\n  `C_MKTSEGMENT` char(10) NOT NULL COMMENT \"\",\n  `C_COMMENT` varchar(117) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`C_CUSTKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`C_CUSTKEY`) BUCKETS 10 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);",
    "test.nation": "CREATE TABLE `nation` (\n  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_NAME` char(25) NOT NULL COMMENT \"\",\n  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`N_NATIONKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"enable_persistent_index\" \u003d \"false\",\n\"replicated_storage\" \u003d \"true\",\n\"compression\" \u003d \"LZ4\"\n);"
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
  "be_core_stat": {
    "numOfHardwareCoresPerBe": "{}",
    "cachedAvgNumOfHardwareCores": 0
  },
  "exception": []
}
[end]

