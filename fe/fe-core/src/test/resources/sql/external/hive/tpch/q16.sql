[sql]
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#43'
  and p_type not like 'PROMO BURNISHED%'
  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%Customer%Complaints%'
)
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size ;
[fragment statistics]
PLAN FRAGMENT 0(F08)
Output Exprs:9: p_brand | 10: p_type | 11: p_size | 23: count
Input Partition: UNPARTITIONED
RESULT SINK

16:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 7119
column statistics:
* p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
* count-->[0.0, 6912000.0, 0.0, 8.0, 7119.140625] ESTIMATE

PLAN FRAGMENT 1(F07)

Input Partition: HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 16

15:SORT
|  order by: [23, BIGINT, false] DESC, [9, VARCHAR, true] ASC, [10, VARCHAR, true] ASC, [11, INT, true] ASC
|  offset: 0
|  cardinality: 7119
|  column statistics:
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * count-->[0.0, 6912000.0, 0.0, 8.0, 7119.140625] ESTIMATE
|
14:AGGREGATE (update finalize)
|  aggregate: count[([2: ps_suppkey, INT, true]); args: INT; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [9: p_brand, VARCHAR, true], [10: p_type, VARCHAR, true], [11: p_size, INT, true]
|  cardinality: 7119
|  column statistics:
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * count-->[0.0, 6912000.0, 0.0, 8.0, 7119.140625] ESTIMATE
|
13:AGGREGATE (merge serialize)
|  group by: [2: ps_suppkey, INT, true], [9: p_brand, VARCHAR, true], [10: p_type, VARCHAR, true], [11: p_size, INT, true]
|  cardinality: 6912000
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
12:EXCHANGE
distribution type: SHUFFLE
partition exprs: [9: p_brand, VARCHAR, true], [10: p_type, VARCHAR, true], [11: p_size, INT, true]
cardinality: 6912000

PLAN FRAGMENT 2(F04)

Input Partition: HASH_PARTITIONED: 1: ps_partkey
OutPut Partition: HASH_PARTITIONED: 9: p_brand, 10: p_type, 11: p_size
OutPut Exchange Id: 12

11:AGGREGATE (update serialize)
|  STREAMING
|  group by: [2: ps_suppkey, INT, true], [9: p_brand, VARCHAR, true], [10: p_type, VARCHAR, true], [11: p_size, INT, true]
|  cardinality: 6912000
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
10:Project
|  output columns:
|  2 <-> [2: ps_suppkey, INT, true]
|  9 <-> [9: p_brand, VARCHAR, true]
|  10 <-> [10: p_type, VARCHAR, true]
|  11 <-> [11: p_size, INT, true]
|  cardinality: 6912000
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
9:HASH JOIN
|  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)
|  equal join conjunct: [2: ps_suppkey, INT, true] = [15: s_suppkey, INT, true]
|  output columns: 2, 9, 10, 11
|  cardinality: 6912000
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 250000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
|
|----8:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 250000
|
5:Project
|  output columns:
|  2 <-> [2: ps_suppkey, INT, true]
|  9 <-> [9: p_brand, VARCHAR, true]
|  10 <-> [10: p_type, VARCHAR, true]
|  11 <-> [11: p_size, INT, true]
|  cardinality: 9216000
|  column statistics:
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
4:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [1: ps_partkey, INT, true] = [6: p_partkey, INT, true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (6: p_partkey), remote = true
|  output columns: 2, 9, 10, 11
|  cardinality: 9216000
|  column statistics:
|  * ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2304000.0] ESTIMATE
|  * ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
|  * p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2304000.0] ESTIMATE
|  * p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
|  * p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
|  * p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE
|
|----3:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [6: p_partkey, INT, true]
|       cardinality: 2304000
|
1:EXCHANGE
distribution type: SHUFFLE
partition exprs: [1: ps_partkey, INT, true]
cardinality: 80000000

PLAN FRAGMENT 3(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:Project
|  output columns:
|  15 <-> [15: s_suppkey, INT, true]
|  cardinality: 250000
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
|
6:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 21: s_comment LIKE '%Customer%Complaints%'
partitions=1/1
avgRowSize=105.0
numNodes=0
cardinality: 250000
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 250000.0] ESTIMATE
* s_comment-->[-Infinity, Infinity, 0.0, 101.0, 250000.0] ESTIMATE

PLAN FRAGMENT 4(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 6: p_partkey
OutPut Exchange Id: 03

2:HdfsScanNode
TABLE: part
NON-PARTITION PREDICATES: 9: p_brand != 'Brand#43', NOT (10: p_type LIKE 'PROMO BURNISHED%'), 11: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)
MIN/MAX PREDICATES: 24: p_size >= 1, 25: p_size <= 43
partitions=1/1
avgRowSize=47.0
numNodes=0
cardinality: 2304000
column statistics:
* p_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2304000.0] ESTIMATE
* p_brand-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* p_type-->[-Infinity, Infinity, 0.0, 25.0, 150.0] ESTIMATE
* p_size-->[1.0, 43.0, 0.0, 4.0, 8.0] ESTIMATE

PLAN FRAGMENT 5(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 1: ps_partkey
OutPut Exchange Id: 01

0:HdfsScanNode
TABLE: partsupp
NON-PARTITION PREDICATES: 1: ps_partkey IS NOT NULL
partitions=1/1
avgRowSize=16.0
numNodes=0
cardinality: 80000000
probe runtime filters:
- filter_id = 0, probe_expr = (1: ps_partkey)
column statistics:
* ps_partkey-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* ps_suppkey-->[1.0, 1000000.0, 0.0, 8.0, 1000000.0] ESTIMATE
[end]

