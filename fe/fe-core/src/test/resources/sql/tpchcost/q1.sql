[sql]
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus ;
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:9: L_RETURNFLAG | 10: L_LINESTATUS | 20: sum(5: L_QUANTITY) | 21: sum(6: L_EXTENDEDPRICE) | 22: sum(18: expr) | 23: sum(19: expr) | 24: avg(5: L_QUANTITY) | 25: avg(6: L_EXTENDEDPRICE) | 26: avg(7: L_DISCOUNT) | 27: count()
PARTITION: UNPARTITIONED

RESULT SINK

6:MERGING-EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS

STREAM DATA SINK
EXCHANGE ID: 06
UNPARTITIONED

5:SORT
|  order by: <slot 9> 9: L_RETURNFLAG ASC, <slot 10> 10: L_LINESTATUS ASC
|  offset: 0
|  use vectorized: true
|
4:AGGREGATE (merge finalize)
|  output: sum(20: sum(5: L_QUANTITY)), sum(21: sum(6: L_EXTENDEDPRICE)), sum(22: sum(18: expr)), sum(23: sum(19: expr)), avg(24: avg(5: L_QUANTITY)), avg(25: avg(6: L_EXTENDEDPRICE)), avg(26: avg(7: L_DISCOUNT)), count(27: count())
|  group by: 9: L_RETURNFLAG, 10: L_LINESTATUS
|  use vectorized: true
|
3:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS

2:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(5: L_QUANTITY), sum(6: L_EXTENDEDPRICE), sum(18: expr), sum(19: expr), avg(5: L_QUANTITY), avg(6: L_EXTENDEDPRICE), avg(7: L_DISCOUNT), count(*)
|  group by: 9: L_RETURNFLAG, 10: L_LINESTATUS
|  use vectorized: true
|
1:Project
|  <slot 5> : 5: L_QUANTITY
|  <slot 6> : 6: L_EXTENDEDPRICE
|  <slot 7> : 7: L_DISCOUNT
|  <slot 9> : 9: L_RETURNFLAG
|  <slot 10> : 10: L_LINESTATUS
|  <slot 18> : 29: multiply
|  <slot 19> : 29: multiply * 1.0 + 8: L_TAX
|  common expressions:
|  <slot 28> : 1.0 - 7: L_DISCOUNT
|  <slot 29> : 6: L_EXTENDEDPRICE * 28: subtract
|  use vectorized: true
|
0:OlapScanNode
TABLE: lineitem
PREAGGREGATION: ON
PREDICATES: 11: L_SHIPDATE <= '1998-12-01'
partitions=1/1
rollup: lineitem
tabletRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
cardinality=600000000
avgRowSize=54.0
numNodes=0
use vectorized: true
[fragment statistics]
PLAN FRAGMENT 0(F02)
Output Exprs:9: L_RETURNFLAG | 10: L_LINESTATUS | 20: sum(5: L_QUANTITY) | 21: sum(6: L_EXTENDEDPRICE) | 22: sum(18: expr) | 23: sum(19: expr) | 24: avg(5: L_QUANTITY) | 25: avg(6: L_EXTENDEDPRICE) | 26: avg(7: L_DISCOUNT) | 27: count()
Input Partition: UNPARTITIONED
RESULT SINK

6:MERGING-EXCHANGE
cardinality: 3
column statistics:
* L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
* L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
* sum(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
* sum(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* sum(18: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
* sum(19: expr)-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
* avg(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
* avg(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* avg(7: L_DISCOUNT)-->[0.0, 0.1, 0.0, 8.0, 11.0]
* count()-->[-Infinity, Infinity, 0.0, 1.0, 1.0]

PLAN FRAGMENT 1(F01)

Input Partition: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:SORT
|  order by: [9, VARCHAR, false] ASC, [10, VARCHAR, false] ASC
|  offset: 0
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
|  * sum(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * sum(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(18: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(19: expr)-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
|  * avg(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * avg(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * avg(7: L_DISCOUNT)-->[0.0, 0.1, 0.0, 8.0, 11.0]
|  * count()-->[-Infinity, Infinity, 0.0, 1.0, 1.0]
|
4:AGGREGATE (merge finalize)
|  aggregate: sum[([20: sum(5: L_QUANTITY), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([21: sum(6: L_EXTENDEDPRICE), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([22: sum(18: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([23: sum(19: expr), DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([24: avg(5: L_QUANTITY), VARCHAR, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([25: avg(6: L_EXTENDEDPRICE), VARCHAR, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], avg[([26: avg(7: L_DISCOUNT), VARCHAR, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], count[([27: count(), BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [9: L_RETURNFLAG, VARCHAR, false], [10: L_LINESTATUS, VARCHAR, false]
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
|  * sum(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * sum(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(18: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(19: expr)-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
|  * avg(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * avg(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * avg(7: L_DISCOUNT)-->[0.0, 0.1, 0.0, 8.0, 11.0]
|  * count()-->[-Infinity, Infinity, 0.0, 1.0, 1.0]
|
3:EXCHANGE
cardinality: 3

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 9: L_RETURNFLAG, 10: L_LINESTATUS
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([18: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], sum[([19: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true], avg[([5: L_QUANTITY, DOUBLE, false]); args: DOUBLE; result: VARCHAR; args nullable: false; result nullable: true], avg[([6: L_EXTENDEDPRICE, DOUBLE, false]); args: DOUBLE; result: VARCHAR; args nullable: false; result nullable: true], avg[([7: L_DISCOUNT, DOUBLE, false]); args: DOUBLE; result: VARCHAR; args nullable: false; result nullable: true], count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [9: L_RETURNFLAG, VARCHAR, false], [10: L_LINESTATUS, VARCHAR, false]
|  cardinality: 3
|  column statistics:
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
|  * sum(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * sum(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(18: expr)-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|  * sum(19: expr)-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
|  * avg(5: L_QUANTITY)-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * avg(6: L_EXTENDEDPRICE)-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * avg(7: L_DISCOUNT)-->[0.0, 0.1, 0.0, 8.0, 11.0]
|  * count()-->[-Infinity, Infinity, 0.0, 1.0, 1.0]
|
1:Project
|  output columns:
|  5 <-> [5: L_QUANTITY, DOUBLE, false]
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  9 <-> [9: L_RETURNFLAG, CHAR, false]
|  10 <-> [10: L_LINESTATUS, CHAR, false]
|  18 <-> [29: multiply, DOUBLE, false]
|  19 <-> [29: multiply, DOUBLE, false] * 1.0 + [8: L_TAX, DOUBLE, false]
|  common expressions:
|  28 <-> 1.0 - [7: L_DISCOUNT, DOUBLE, false]
|  29 <-> [6: L_EXTENDEDPRICE, DOUBLE, false] * [28: subtract, DOUBLE, false]
|  cardinality: 600000000
|  column statistics:
|  * L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0]
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
|  * L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
|  * L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
|  * expr-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [11: L_SHIPDATE, DATE, false] <= '1998-12-01'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10213,10215,10217,10219,10221,10223,10225,10227,10229,10231 ...
actualRows=0, avgRowSize=54.0
cardinality: 600000000
column statistics:
* L_QUANTITY-->[1.0, 50.0, 0.0, 8.0, 50.0]
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]
* L_TAX-->[0.0, 0.08, 0.0, 8.0, 9.0]
* L_RETURNFLAG-->[-Infinity, Infinity, 0.0, 1.0, 3.0]
* L_LINESTATUS-->[-Infinity, Infinity, 0.0, 1.0, 2.0]
* L_SHIPDATE-->[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0]
* expr-->[810.9, 104949.5, 0.0, 8.0, 932377.0]
* expr-->[810.9, 113345.46, 0.0, 8.0, 932377.0]
[dump]
{
  "statement": "select\n    l_returnflag,\n    l_linestatus,\n    sum(l_quantity) as sum_qty,\n    sum(l_extendedprice) as sum_base_price,\n    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n    avg(l_quantity) as avg_qty,\n    avg(l_extendedprice) as avg_price,\n    avg(l_discount) as avg_disc,\n    count(*) as count_order\nfrom\n    lineitem\nwhere\n    l_shipdate \u003c\u003d date \u00271998-12-01\u0027\ngroup by\n    l_returnflag,\n    l_linestatus\norder by\n    l_returnflag,\n    l_linestatus ;\n",
  "table_meta": {
    "test.lineitem": "CREATE TABLE `lineitem` (\n  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n  `L_QUANTITY` double NOT NULL COMMENT \"\",\n  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n  `L_TAX` double NOT NULL COMMENT \"\",\n  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n  `PAD` char(1) NOT NULL COMMENT \"\"\n) ENGINE\u003dOLAP \nDUPLICATE KEY(`L_ORDERKEY`)\nCOMMENT \"OLAP\"\nDISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 20 \nPROPERTIES (\n\"replication_num\" \u003d \"1\",\n\"in_memory\" \u003d \"false\",\n\"storage_format\" \u003d \"DEFAULT\"\n);"
  },
  "table_row_count": {
    "test.lineitem": {
      "lineitem": 600000000
    }
  },
  "column_statistics": {
    "test.lineitem": {
      "L_TAX": "[0.0, 0.08, 0.0, 8.0, 9.0]",
      "L_SHIPDATE": "[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0]",
      "L_EXTENDEDPRICE": "[901.0, 104949.5, 0.0, 8.0, 932377.0]",
      "L_DISCOUNT": "[0.0, 0.1, 0.0, 8.0, 11.0]",
      "L_RETURNFLAG": "[-Infinity, Infinity, 0.0, 1.0, 3.0]",
      "L_LINESTATUS": "[-Infinity, Infinity, 0.0, 1.0, 2.0]",
      "L_QUANTITY": "[1.0, 50.0, 0.0, 8.0, 50.0]"
    }
  },
  "be_number": 3,
  "exception": []
}
[end]