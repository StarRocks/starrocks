[sql]
select
    o_year,
    sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = 'MIDDLE EAST'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01' and date '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year ;
[fragment statistics]
PLAN FRAGMENT 0(F18)
Output Exprs:69: year | 74: expr
Input Partition: UNPARTITIONED
RESULT SINK

37:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 2
column statistics:
* year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
* sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 8.0, 2.0] ESTIMATE
* expr-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE

PLAN FRAGMENT 1(F17)

Input Partition: HASH_PARTITIONED: 69: year
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 37

36:SORT
|  order by: [69, SMALLINT, false] ASC
|  offset: 0
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 2.0] ESTIMATE
|  * expr-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
35:Project
|  output columns:
|  69 <-> [69: year, SMALLINT, false]
|  74 <-> [72: sum, DOUBLE, true] / [73: sum, DOUBLE, true]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
34:AGGREGATE (merge finalize)
|  aggregate: sum[([72: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([73: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  group by: [69: year, SMALLINT, false]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 2.0] ESTIMATE
|  * expr-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|
33:EXCHANGE
distribution type: SHUFFLE
partition exprs: [69: year, SMALLINT, false]
cardinality: 2

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 69: year
OutPut Exchange Id: 33

32:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([71: case, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true], sum[([70: expr, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  group by: [69: year, SMALLINT, false]
|  cardinality: 2
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * sum-->[-Infinity, Infinity, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 2.0] ESTIMATE
|
31:Project
|  output columns:
|  69 <-> year[([40: O_ORDERDATE, DATE, false]); args: DATE; result: SMALLINT; args nullable: false; result nullable: false]
|  70 <-> [76: multiply, DOUBLE, false]
|  71 <-> if[([61: N_NAME, CHAR, false] = 'IRAN', [76: multiply, DOUBLE, false], 0.0); args: BOOLEAN,DOUBLE,DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  common expressions:
|  75 <-> 1.0 - [25: L_DISCOUNT, DOUBLE, false]
|  76 <-> [24: L_EXTENDEDPRICE, DOUBLE, false] * [75: subtract, DOUBLE, false]
|  cardinality: 390833
|  column statistics:
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 390833.1128124515] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 390834.1128124515] ESTIMATE
|
30:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [14: S_NATIONKEY, INT, false] = [60: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 6, build_expr = (60: N_NATIONKEY), remote = false
|  output columns: 24, 25, 40, 61
|  cardinality: 390833
|  column statistics:
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 390833.1128124515] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE
|  * year-->[1995.0, 1996.0, 0.0, 2.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 390833.1128124515] ESTIMATE
|  * case-->[-Infinity, Infinity, 0.0, 8.0, 390834.1128124515] ESTIMATE
|
|----29:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 25
|
27:Project
|  output columns:
|  14 <-> [14: S_NATIONKEY, INT, false]
|  24 <-> [24: L_EXTENDEDPRICE, DOUBLE, false]
|  25 <-> [25: L_DISCOUNT, DOUBLE, false]
|  40 <-> [40: O_ORDERDATE, DATE, false]
|  cardinality: 390833
|  column statistics:
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 390833.1128124515] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|
26:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [11: S_SUPPKEY, INT, false] = [21: L_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 5, build_expr = (21: L_SUPPKEY), remote = false
|  output columns: 14, 24, 25, 40
|  cardinality: 390833
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 390833.1128124515] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 390833.1128124515] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 390833.1128124515] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|
|----25:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [21: L_SUPPKEY, INT, false]
|       cardinality: 390833
|
0:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 1000000
probe runtime filters:
- filter_id = 5, probe_expr = (11: S_SUPPKEY)
- filter_id = 6, probe_expr = (14: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 3(F15)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 29

28:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 25
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 25.0] ESTIMATE

PLAN FRAGMENT 4(F13)

Input Partition: HASH_PARTITIONED: 37: O_CUSTKEY
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 21: L_SUPPKEY
OutPut Exchange Id: 25

24:Project
|  output columns:
|  21 <-> [21: L_SUPPKEY, INT, false]
|  24 <-> [24: L_EXTENDEDPRICE, DOUBLE, false]
|  25 <-> [25: L_DISCOUNT, DOUBLE, false]
|  40 <-> [40: O_ORDERDATE, DATE, false]
|  cardinality: 390833
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 390833.11281245155] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 390833.11281245155] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|
23:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [37: O_CUSTKEY, INT, false] = [46: C_CUSTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (46: C_CUSTKEY), remote = true
|  output columns: 21, 24, 25, 40
|  cardinality: 390833
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 390833.11281245155] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 390833.11281245155] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----22:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [46: C_CUSTKEY, INT, false]
|       cardinality: 3000000
|
11:EXCHANGE
distribution type: SHUFFLE
partition exprs: [37: O_CUSTKEY, INT, false]
cardinality: 1954166

PLAN FRAGMENT 5(F07)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 46: C_CUSTKEY
OutPut Exchange Id: 22

21:Project
|  output columns:
|  46 <-> [46: C_CUSTKEY, INT, false]
|  cardinality: 3000000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|
20:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [49: C_NATIONKEY, INT, false] = [55: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (55: N_NATIONKEY), remote = false
|  output columns: 46
|  cardinality: 3000000
|  column statistics:
|  * C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 3000000.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----19:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 5
|
12:OlapScanNode
table: customer, rollup: customer
preAggregation: on
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=12.0
cardinality: 15000000
probe runtime filters:
- filter_id = 3, probe_expr = (49: C_NATIONKEY)
column statistics:
* C_CUSTKEY-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE
* C_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F08)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 19

18:Project
|  output columns:
|  55 <-> [55: N_NATIONKEY, INT, false]
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|
17:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [57: N_REGIONKEY, INT, false] = [65: R_REGIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (65: R_REGIONKEY), remote = false
|  output columns: 55
|  cardinality: 5
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 5.0] ESTIMATE
|  * N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----16:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
13:OlapScanNode
table: nation, rollup: nation
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=8.0
cardinality: 25
probe runtime filters:
- filter_id = 2, probe_expr = (57: N_REGIONKEY)
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE
* N_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 5.0] ESTIMATE

PLAN FRAGMENT 7(F09)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 16

15:Project
|  output columns:
|  65 <-> [65: R_REGIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
|
14:OlapScanNode
table: region, rollup: region
preAggregation: on
Predicates: [66: R_NAME, CHAR, false] = 'MIDDLE EAST'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* R_REGIONKEY-->[0.0, 4.0, 0.0, 4.0, 1.0] ESTIMATE
* R_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE

PLAN FRAGMENT 8(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 37: O_CUSTKEY
OutPut Exchange Id: 11

10:Project
|  output columns:
|  21 <-> [21: L_SUPPKEY, INT, false]
|  24 <-> [24: L_EXTENDEDPRICE, DOUBLE, false]
|  25 <-> [25: L_DISCOUNT, DOUBLE, false]
|  37 <-> [37: O_CUSTKEY, INT, false]
|  40 <-> [40: O_ORDERDATE, DATE, false]
|  cardinality: 1954166
|  column statistics:
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 1954165.5640622578] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|
9:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [36: O_ORDERKEY, INT, false] = [19: L_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 1, build_expr = (19: L_ORDERKEY), remote = false
|  output columns: 21, 24, 25, 37, 40
|  cardinality: 1954166
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 1954165.5640622578] ESTIMATE
|  * O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [19: L_ORDERKEY, INT, false]
|       cardinality: 6425045
|
1:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [40: O_ORDERDATE, DATE, false] >= '1995-01-01', [40: O_ORDERDATE, DATE, false] <= '1996-12-31'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=20.0
cardinality: 45622224
probe runtime filters:
- filter_id = 1, probe_expr = (36: O_ORDERKEY)
- filter_id = 4, probe_expr = (37: O_CUSTKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 4.5622224E7] ESTIMATE
* O_CUSTKEY-->[1.0, 1.49999E7, 0.0, 8.0, 9999600.0] ESTIMATE
* O_ORDERDATE-->[7.888896E8, 8.519616E8, 0.0, 4.0, 2406.0] ESTIMATE

PLAN FRAGMENT 9(F02)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 19: L_ORDERKEY
OutPut Exchange Id: 08

7:Project
|  output columns:
|  19 <-> [19: L_ORDERKEY, INT, false]
|  21 <-> [21: L_SUPPKEY, INT, false]
|  24 <-> [24: L_EXTENDEDPRICE, DOUBLE, false]
|  25 <-> [25: L_DISCOUNT, DOUBLE, false]
|  cardinality: 6425045
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6425044.833617464] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [20: L_PARTKEY, INT, false] = [1: P_PARTKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: P_PARTKEY), remote = false
|  output columns: 19, 21, 24, 25
|  cardinality: 6425045
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 6425044.833617464] ESTIMATE
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
|----5:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 214168
|
2:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=36.0
cardinality: 600000000
probe runtime filters:
- filter_id = 0, probe_expr = (20: L_PARTKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE

PLAN FRAGMENT 10(F03)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 05

4:Project
|  output columns:
|  1 <-> [1: P_PARTKEY, INT, false]
|  cardinality: 214168
|  column statistics:
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
|
3:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: [5: P_TYPE, VARCHAR, false] = 'ECONOMY ANODIZED STEEL'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=33.0
cardinality: 214168
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 214168.16112058214] ESTIMATE
* P_TYPE-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[end]

