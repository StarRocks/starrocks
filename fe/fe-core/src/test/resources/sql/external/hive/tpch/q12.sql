[fragment statistics]
PLAN FRAGMENT 0(F04)
Output Exprs:24: l_shipmode | 28: sum | 29: sum
Input Partition: UNPARTITIONED
RESULT SINK

10:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 2
column statistics:
* l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
* sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
* sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE

PLAN FRAGMENT 1(F03)

Input Partition: HASH_PARTITIONED: 24: l_shipmode
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:SORT
|  order by: [24, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 2
|  column statistics:
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|
8:AGGREGATE (merge finalize)
|  aggregate: sum[([28: sum, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], sum[([29: sum, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  group by: [24: l_shipmode, VARCHAR, true]
|  cardinality: 2
|  column statistics:
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|
7:EXCHANGE
distribution type: SHUFFLE
partition exprs: [24: l_shipmode, VARCHAR, true]
cardinality: 2

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 24: l_shipmode
OutPut Exchange Id: 07

6:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([26: case, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true], sum[([27: case, BIGINT, true]); args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  group by: [24: l_shipmode, VARCHAR, true]
|  cardinality: 2
|  column statistics:
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|  * sum-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|
5:Project
|  output columns:
|  24 <-> [24: l_shipmode, VARCHAR, true]
|  26 <-> if[((6: o_orderpriority = '1-URGENT') OR (6: o_orderpriority = '2-HIGH'), 1, 0); args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  27 <-> if[((6: o_orderpriority != '1-URGENT') AND (6: o_orderpriority != '2-HIGH'), 1, 0); args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: true; result nullable: true]
|  cardinality: 6125233
|  column statistics:
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * case-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|  * case-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [1: o_orderkey, INT, true] = [10: l_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (10: l_orderkey), remote = false
|  output columns: 6, 24
|  cardinality: 6125233
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 6125233.086195324] ESTIMATE
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 6125233.086195324] ESTIMATE
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|  * case-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|  * case-->[0.0, 1.0, 0.0, 8.0, 2.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 6125233
|
0:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 1: o_orderkey IS NOT NULL
partitions=1/1
avgRowSize=23.0
dataCacheOptions={populate: false}
cardinality: 150000000
probe runtime filters:
- filter_id = 0, probe_expr = (1: o_orderkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 03

2:Project
|  output columns:
|  10 <-> [10: l_orderkey, INT, true]
|  24 <-> [24: l_shipmode, VARCHAR, true]
|  cardinality: 6125233
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 6125233.086195324] ESTIMATE
|  * l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
|
1:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 24: l_shipmode IN ('REG AIR', 'MAIL'), 21: l_commitdate < 22: l_receiptdate, 20: l_shipdate < 21: l_commitdate, 22: l_receiptdate >= '1997-01-01', 22: l_receiptdate < '1998-01-01'
MIN/MAX PREDICATES: 24: l_shipmode >= 'MAIL', 24: l_shipmode <= 'REG AIR', 22: l_receiptdate >= '1997-01-01', 22: l_receiptdate < '1998-01-01'
partitions=1/1
avgRowSize=30.0
dataCacheOptions={populate: false}
cardinality: 6125233
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 6125233.086195324] ESTIMATE
* l_shipdate-->[6.942816E8, 9.124416E8, 0.0, 4.0, 2526.0] ESTIMATE
* l_commitdate-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* l_receiptdate-->[8.52048E8, 8.83584E8, 0.0, 4.0, 2554.0] ESTIMATE
* l_shipmode-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
[end]