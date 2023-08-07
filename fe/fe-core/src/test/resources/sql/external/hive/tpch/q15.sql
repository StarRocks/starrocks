[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        (	select
                 l_suppkey as supplier_no,
                 sum(l_extendedprice * (1 - l_discount)) as total_revenue
             from
                 lineitem
             where
                     l_shipdate >= date '1995-07-01'
               and l_shipdate < date '1995-10-01'
             group by
                 l_suppkey) b
)
order by
    s_suppkey;
[fragment statistics]
PLAN FRAGMENT 0(F08)
Output Exprs:1: s_suppkey | 2: s_name | 3: s_address | 5: s_phone | 25: sum
Input Partition: UNPARTITIONED
RESULT SINK

24:MERGING-EXCHANGE
distribution type: GATHER
cardinality: 1
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 1.0] ESTIMATE
* s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1.0] ESTIMATE
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
* sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE

PLAN FRAGMENT 1(F00)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 24

23:SORT
|  order by: [1, INT, true] ASC
|  offset: 0
|  cardinality: 1
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 1.0] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
22:Project
|  output columns:
|  1 <-> [1: s_suppkey, INT, true]
|  2 <-> [2: s_name, VARCHAR, true]
|  3 <-> [3: s_address, VARCHAR, true]
|  5 <-> [5: s_phone, VARCHAR, true]
|  25 <-> [25: sum, DECIMAL128(38,4), true]
|  cardinality: 1
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 1.0] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
21:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [1: s_suppkey, INT, true] = [10: l_suppkey, INT, true]
|  build runtime filters:
|  - filter_id = 1, build_expr = (10: l_suppkey), remote = false
|  output columns: 1, 2, 3, 5, 25
|  cardinality: 1
|  column statistics:
|  * s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * s_name-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
|  * s_address-->[-Infinity, Infinity, 0.0, 40.0, 1.0] ESTIMATE
|  * s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1.0] ESTIMATE
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
|----20:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
0:HdfsScanNode
TABLE: supplier
NON-PARTITION PREDICATES: 1: s_suppkey IS NOT NULL
partitions=1/1
avgRowSize=84.0
cardinality: 1000000
probe runtime filters:
- filter_id = 1, probe_expr = (1: s_suppkey)
column statistics:
* s_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* s_name-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* s_address-->[-Infinity, Infinity, 0.0, 40.0, 1000000.0] ESTIMATE
* s_phone-->[-Infinity, Infinity, 0.0, 15.0, 1000000.0] ESTIMATE

PLAN FRAGMENT 2(F02)

Input Partition: HASH_PARTITIONED: 10: l_suppkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 20

19:Project
|  output columns:
|  10 <-> [10: l_suppkey, INT, true]
|  25 <-> [25: sum, DECIMAL128(38,4), true]
|  cardinality: 1
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
18:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [25: sum, DECIMAL128(38,4), true] = [44: max, DECIMAL128(38,4), true]
|  build runtime filters:
|  - filter_id = 0, build_expr = (44: max), remote = false
|  output columns: 10, 25
|  cardinality: 1
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|  * max-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
|----17:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
5:AGGREGATE (merge finalize)
|  aggregate: sum[([25: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [10: l_suppkey, INT, true]
|  having: 25: sum IS NOT NULL
|  cardinality: 1000000
|  probe runtime filters:
|  - filter_id = 0, probe_expr = (25: sum)
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1000000.0] ESTIMATE
|
4:EXCHANGE
distribution type: SHUFFLE
partition exprs: [10: l_suppkey, INT, true]
cardinality: 1000000

PLAN FRAGMENT 3(F05)

Input Partition: UNPARTITIONED
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 17

16:SELECT
|  predicates: 44: max IS NOT NULL
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
15:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
14:AGGREGATE (merge finalize)
|  aggregate: max[([44: max, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
13:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 4(F04)

Input Partition: HASH_PARTITIONED: 28: l_suppkey
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:AGGREGATE (update serialize)
|  aggregate: max[([43: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * max-->[810.9, 104949.5, 0.0, 16.0, 1.0] ESTIMATE
|
11:Project
|  output columns:
|  43 <-> [43: sum, DECIMAL128(38,4), true]
|  cardinality: 1000000
|  column statistics:
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1000000.0] ESTIMATE
|
10:AGGREGATE (merge finalize)
|  aggregate: sum[([43: sum, DECIMAL128(38,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [28: l_suppkey, INT, true]
|  cardinality: 1000000
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1000000.0] ESTIMATE
|
9:EXCHANGE
distribution type: SHUFFLE
partition exprs: [28: l_suppkey, INT, true]
cardinality: 1000000

PLAN FRAGMENT 5(F03)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 28: l_suppkey
OutPut Exchange Id: 09

8:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([42: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [28: l_suppkey, INT, true]
|  cardinality: 1000000
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1000000.0] ESTIMATE
|
7:Project
|  output columns:
|  28 <-> [28: l_suppkey, INT, true]
|  42 <-> cast([31: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [32: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))
|  cardinality: 21862767
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
6:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 36: l_shipdate >= '1995-07-01', 36: l_shipdate < '1995-10-01'
MIN/MAX PREDICATES: 46: l_shipdate >= '1995-07-01', 47: l_shipdate < '1995-10-01'
partitions=1/1
avgRowSize=40.0
cardinality: 21862767
column statistics:
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_shipdate-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE

PLAN FRAGMENT 6(F01)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 10: l_suppkey
OutPut Exchange Id: 04

3:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: sum[([24: expr, DECIMAL128(33,4), true]); args: DECIMAL128; result: DECIMAL128(38,4); args nullable: true; result nullable: true]
|  group by: [10: l_suppkey, INT, true]
|  cardinality: 1000000
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * sum-->[810.9, 104949.5, 0.0, 16.0, 1000000.0] ESTIMATE
|
2:Project
|  output columns:
|  10 <-> [10: l_suppkey, INT, true]
|  24 <-> cast([13: l_extendedprice, DECIMAL64(15,2), true] as DECIMAL128(15,2)) * cast(1 - [14: l_discount, DECIMAL64(15,2), true] as DECIMAL128(18,2))
|  cardinality: 21862767
|  column statistics:
|  * l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
|
1:HdfsScanNode
TABLE: lineitem
NON-PARTITION PREDICATES: 18: l_shipdate >= '1995-07-01', 18: l_shipdate < '1995-10-01'
MIN/MAX PREDICATES: 48: l_shipdate >= '1995-07-01', 49: l_shipdate < '1995-10-01'
partitions=1/1
avgRowSize=40.0
cardinality: 21862767
column statistics:
* l_suppkey-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* l_extendedprice-->[901.0, 104949.5, 0.0, 8.0, 3736520.0] ESTIMATE
* l_discount-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* l_shipdate-->[8.04528E8, 8.124768E8, 0.0, 4.0, 2526.0] ESTIMATE
* expr-->[810.9, 104949.5, 0.0, 16.0, 3736520.0] ESTIMATE
[end]
