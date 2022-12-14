[sql]
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            customer left outer join orders on
                        c_custkey = o_custkey
                    and o_comment not like '%unusual%deposits%'
        group by
            c_custkey
    ) a
group by
    c_count
order by
    custdist desc,
    c_count desc ;
[fragment statistics]
PLAN FRAGMENT 0(F06)
Output Exprs:18: count | 19: count
Input Partition: UNPARTITIONED
RESULT SINK

12:MERGING-EXCHANGE
cardinality: 10031873
column statistics:
* count-->[0.0, 1.125E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* count-->[0.0, 1.0031873E7, 0.0, 8.0, 1.0031873E7] ESTIMATE

PLAN FRAGMENT 1(F05)

Input Partition: HASH_PARTITIONED: 18: count
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 12

11:SORT
|  order by: [19, BIGINT, false] DESC, [18, BIGINT, true] DESC
|  offset: 0
|  hasNullableGenerateChild: true
|  cardinality: 10031873
|  column statistics:
|  * count-->[0.0, 1.125E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * count-->[0.0, 1.0031873E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
10:AGGREGATE (update finalize)
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [18: count, BIGINT, true]
|  hasNullableGenerateChild: true
|  cardinality: 10031873
|  column statistics:
|  * count-->[0.0, 1.125E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * count-->[0.0, 1.0031873E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
9:EXCHANGE
cardinality: 10031873

PLAN FRAGMENT 2(F04)

Input Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Partition: HASH_PARTITIONED: 18: count
OutPut Exchange Id: 09

8:Project
|  output columns:
|  18 <-> [18: count, BIGINT, false]
|  hasNullableGenerateChild: true
|  cardinality: 10031873
|  column statistics:
|  * count-->[0.0, 1.125E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
7:AGGREGATE (update finalize)
|  aggregate: count[([9: o_orderkey, INT, true]); args: INT; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [1: c_custkey, INT, true]
|  hasNullableGenerateChild: true
|  cardinality: 10031873
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * count-->[0.0, 1.125E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
6:Project
|  output columns:
|  1 <-> [1: c_custkey, INT, true]
|  9 <-> [9: o_orderkey, INT, true]
|  hasNullableGenerateChild: true
|  cardinality: 112500000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.125E8] ESTIMATE
|
5:HASH JOIN
|  join op: RIGHT OUTER JOIN (PARTITIONED)
|  equal join conjunct: [10: o_custkey, INT, true] = [1: c_custkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (1: c_custkey), remote = true
|  output columns: 1, 9
|  cardinality: 112500000
|  column statistics:
|  * c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.125E8] ESTIMATE
|  * o_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
|----4:EXCHANGE
|       cardinality: 15000000
|
2:EXCHANGE
cardinality: 112500000

PLAN FRAGMENT 3(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: c_custkey
OutPut Exchange Id: 04

3:HdfsScanNode
TABLE: customer
partitions=1/1
avgRowSize=8.0
numNodes=0
cardinality: 15000000
column statistics:
* c_custkey-->[1.0, 1.5E7, 0.0, 8.0, 1.5E7] ESTIMATE

PLAN FRAGMENT 4(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: o_custkey
OutPut Exchange Id: 02

1:Project
|  output columns:
|  9 <-> [9: o_orderkey, INT, true]
|  10 <-> [10: o_custkey, INT, true]
|  cardinality: 112500000
|  column statistics:
|  * o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.125E8] ESTIMATE
|  * o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
|
0:HdfsScanNode
TABLE: orders
NON-PARTITION PREDICATES: NOT (17: o_comment LIKE '%unusual%deposits%')
partitions=1/1
avgRowSize=95.0
numNodes=0
cardinality: 112500000
probe runtime filters:
- filter_id = 0, probe_expr = (10: o_custkey)
column statistics:
* o_orderkey-->[1.0, 6.0E8, 0.0, 8.0, 1.125E8] ESTIMATE
* o_custkey-->[1.0, 1.5E8, 0.0, 8.0, 1.0031873E7] ESTIMATE
* o_comment-->[-Infinity, Infinity, 0.0, 79.0, 1.10204136E8] ESTIMATE
[end]

