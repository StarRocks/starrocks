[sql]
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '1994-09-01'
  and o_orderdate < date '1994-12-01'
  and exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_receiptdate > l_commitdate
    )
group by
    o_orderpriority
order by
    o_orderpriority ;
[fragment statistics]
PLAN FRAGMENT 0(F06)
Output Exprs:6: o_orderpriority | 27: count
Input Partition: UNPARTITIONED
RESULT SINK

12:MERGING-EXCHANGE
cardinality: 5
column statistics:
* o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
* count-->[0.0, 5675675.675675674, 0.0, 8.0, 5.0] ESTIMATE

PLAN FRAGMENT 1(F05)

Input Partition: HASH_PARTITIONED: 6: o_orderpriority
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 12

11:SORT
|  order by: [6, VARCHAR, true] ASC
|  offset: 0
|  cardinality: 5
|  column statistics:
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * count-->[0.0, 5675675.675675674, 0.0, 8.0, 5.0] ESTIMATE
|
10:AGGREGATE (merge finalize)
|  aggregate: count[([27: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [6: o_orderpriority, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * count-->[0.0, 5675675.675675674, 0.0, 8.0, 5.0] ESTIMATE
|
9:EXCHANGE
cardinality: 5

PLAN FRAGMENT 2(F04)

Input Partition: HASH_PARTITIONED: 10: l_orderkey
OutPut Partition: HASH_PARTITIONED: 6: o_orderpriority
OutPut Exchange Id: 09

8:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [6: o_orderpriority, VARCHAR, true]
|  cardinality: 5
|  column statistics:
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * count-->[0.0, 5675675.675675674, 0.0, 8.0, 5.0] ESTIMATE
|
7:Project
|  output columns:
|  6 <-> [6: o_orderpriority, VARCHAR, true]
|  cardinality: 5675676
|  column statistics:
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|
6:HASH JOIN
|  join op: RIGHT SEMI JOIN (PARTITIONED)
|  equal join conjunct: [10: l_orderkey, INT, true] = [1: o_orderkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: o_orderkey), remote = true
|  output columns: 6
|  cardinality: 5675676
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5675675.675675674] ESTIMATE
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5675675.675675674] ESTIMATE
|
|----5:EXCHANGE
|       cardinality: 5675676
|
2:EXCHANGE
cardinality: 300018951

PLAN FRAGMENT 3(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: o_orderkey
OutPut Exchange Id: 05

4:Project
|  output columns:
|  1 <-> [1: o_orderkey, INT, true]
|  6 <-> [6: o_orderpriority, VARCHAR, true]
|  cardinality: 5675676
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5675675.675675674] ESTIMATE
|  * o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE
|
3:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: 5: o_orderdate >= '1994-09-01', 5: o_orderdate < '1994-12-01'
MIN/MAX PREDICATES: 28: o_orderdate >= '1994-09-01', 29: o_orderdate < '1994-12-01'
partitions=1/1
avgRowSize=27.0
numNodes=0
cardinality: 5675676
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 5675675.675675674] ESTIMATE
* o_orderdate-->[7.783488E8, 7.862112E8, 0.0, 4.0, 2412.0] ESTIMATE
* o_orderpriority-->[-Infinity, Infinity, 0.0, 15.0, 5.0] ESTIMATE

PLAN FRAGMENT 4(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: l_orderkey
OutPut Exchange Id: 02

1:Project
|  output columns:
|  10 <-> [10: l_orderkey, INT, true]
|  cardinality: 300018951
|  column statistics:
|  * l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|
0:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 22: l_receiptdate > 21: l_commitdate
partitions=1/1
avgRowSize=16.0
numNodes=0
cardinality: 300018951
probe runtime filters:
- filter_id = 0, probe_expr = (10: l_orderkey)
column statistics:
* l_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* l_commitdate-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* l_receiptdate-->[6.94368E8, 9.150336E8, 0.0, 4.0, 2554.0] ESTIMATE
[end]

