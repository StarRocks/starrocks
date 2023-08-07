[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[fragment statistics]
PLAN FRAGMENT 0(F01)
Output Exprs:18: sum
Input Partition: UNPARTITIONED
RESULT SINK

4:AGGREGATE (merge finalize)
|  aggregate: sum[([18: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[18.02, 4197.9800000000005, 0.0, 16.0, 1.0] ESTIMATE
|
3:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:AGGREGATE (update serialize)
|  aggregate: sum[(cast([6: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast([7: l_discount, DECIMAL64(15,2), true] as DECIMAL128(15,2))); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[18.02, 4197.9800000000005, 0.0, 16.0, 1.0] ESTIMATE
|
1:Project
|  output columns:
|  6 <-> [6: l_extendedprice, DECIMAL64(15,2), true]
|  7 <-> [7: l_discount, DECIMAL64(15,2), true]
|  cardinality: 8142765
|  column statistics:
|  * l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
|  * l_discount-->[0.02, 0.04, 0.0, 8.0, 11.0] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 11: l_shipdate >= '1995-01-01', 11: l_shipdate < '1996-01-01', 7: l_discount >= 0.02, 7: l_discount <= 0.04, 5: l_quantity < 24
MIN/MAX PREDICATES: 19: l_shipdate >= '1995-01-01', 20: l_shipdate < '1996-01-01', 21: l_discount >= 0.02, 22: l_discount <= 0.04, 23: l_quantity < 24
partitions=1/1
avgRowSize=44.0
cardinality: 8142765
column statistics:
* l_quantity-->[1.0, 24.0, 0.0, 8.0, 50.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.02, 0.04, 0.0, 8.0, 11.0] ESTIMATE
* l_shipdate-->[7.888896E8, 8.204256E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[18.02, 4197.9800000000005, 0.0, 16.0, 3736520.0] ESTIMATE
[end]

