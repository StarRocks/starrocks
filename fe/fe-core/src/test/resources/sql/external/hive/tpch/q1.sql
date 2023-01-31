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
[fragment statistics]
PLAN FRAGMENT 0(F02)
Output Exprs:9: l_returnflag | 10: l_linestatus | 19: sum | 20: sum | 21: sum | 22: sum | 23: avg | 24: avg | 25: avg | 26: count
Input Partition: UNPARTITIONED
RESULT SINK

6:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 3
column statistics:
* l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
* l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
* sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
* sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 16.0, 3.375] ESTIMATE
* sum-->[810.9, 113345.46000000002, 0.0, 16.0, 3.375] ESTIMATE
* avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
* avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
* avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
* count-->[0.0, 6.00037902E8, 0.0, 8.0, 3.375] ESTIMATE

PLAN FRAGMENT 1(F01)

Input Partition: HASH_PARTITIONED: 9: l_returnflag, 10: l_linestatus
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 06

5:SORT
|  order by: [9, VARCHAR, true] ASC, [10, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 3
|  column statistics:
|  * l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 16.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.00037902E8, 0.0, 8.0, 3.375] ESTIMATE
|
4:AGGREGATE (merge finalize)
|  aggregate: sum[([19: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true], sum[([20: sum, DECIMAL128(38,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true], sum[([21: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([22: sum, DECIMAL128(38,6), true]); args: DECIMAL128; result: DECIMAL128(38,6); args nullable: true; result nullable: true], avg[([23: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true], avg[([24: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true], avg[([25: avg, VARBINARY, true]); args: DECIMAL64; result: DECIMAL128(38,8); args nullable: true; result nullable: true], count[([26: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [9: l_returnflag, VARCHAR, true], [10: l_linestatus, VARCHAR, true]
|  cardinality: 3
|  column statistics:
|  * l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 16.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.00037902E8, 0.0, 8.0, 3.375] ESTIMATE
|
3:EXCHANGE
distribution type: SHUFFLE
partition exprs: [9: l_returnflag, VARCHAR, true], [10: l_linestatus, VARCHAR, true]
cardinality: 3

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 9: l_returnflag, 10: l_linestatus
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([5: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true], sum[([6: l_extendedprice, DECIMAL64(15,2), true]); args: DECIMAL64; result: DECIMAL128(38,2); args nullable: true; result nullable: true], sum[([17: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true], sum[([18: expr, DECIMAL128(38,6), true]); args: DECIMAL128; result: DECIMAL128(38,6); args nullable: true; result nullable: true], avg[([5: l_quantity, DECIMAL64(15,2), true]); args: DECIMAL64; result: VARBINARY; args nullable: true; result nullable: true], avg[([6: l_extendedprice, DECIMAL64(15,2), true]); args: DECIMAL64; result: VARBINARY; args nullable: true; result nullable: true], avg[([7: l_discount, DECIMAL64(15,2), true]); args: DECIMAL64; result: VARBINARY; args nullable: true; result nullable: true], count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [9: l_returnflag, VARCHAR, true], [10: l_linestatus, VARCHAR, true]
|  cardinality: 3
|  column statistics:
|  * l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * sum-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 3.375] ESTIMATE
|  * sum-->[810.9, 113345.46000000002, 0.0, 16.0, 3.375] ESTIMATE
|  * avg-->[1.0, 50.0, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[901.0, 104949.5, 0.0, 8.0, 3.375] ESTIMATE
|  * avg-->[0.0, 0.1, 0.0, 8.0, 3.375] ESTIMATE
|  * count-->[0.0, 6.00037902E8, 0.0, 8.0, 3.375] ESTIMATE
|
1:Project
|  output columns:
|  5 <-> [5: l_quantity, DECIMAL64(15,2), true]
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  7 <-> [7: l_discount, DECIMAL64(15,2), true]
|  9 <-> [9: l_returnflag, VARCHAR, true]
|  10 <-> [10: l_linestatus, VARCHAR, true]
|  17 <-> [32: multiply, DECIMAL128(33,4), true]
|  18 <-> [32: multiply, DECIMAL128(33,4), true] * 1 + cast([8: l_tax, DECIMAL64(15,2), true] as DECIMAL128(19,2))
|  common expressions:
|  32 <-> [28: cast, DECIMAL128(15,2), true] * [31: cast, DECIMAL128(18,2), true]
|  28 <-> cast([6: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2))
|  29 <-> [7: l_discount, DECIMAL64(15,2), true]
|  30 <-> 1 - [29: cast, DECIMAL64(18,2), true]
|  31 <-> cast([30: subtract, DECIMAL64(18,2), true] as DECIMAL128(18,2))
|  cardinality: 600037902
|  column statistics:
|  * l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
|  * l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|  * expr-->[810.9, 113345.46, 0.0, 16.0, 3736520.0] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 11: l_shipdate <= '1998-12-01'
MIN/MAX PREDICATES: 27: l_shipdate <= '1998-12-01'
partitions=1/1
avgRowSize=70.0
numNodes=0
cardinality: 600037902
column statistics:
* l_quantity-->[1.0, 50.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_tax-->[0.0, 0.08, 0.0, 8.0, 9.0] ESTIMATE
* l_returnflag-->[-Infinity, Infinity, 0.0, 1.0, 3.0] ESTIMATE
* l_linestatus-->[-Infinity, Infinity, 0.0, 1.0, 2.0] ESTIMATE
* l_shipdate-->[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
* expr-->[810.9, 113345.46, 0.0, 16.0, 3736520.0] ESTIMATE
[end]