[sql]
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[fragment statistics]
PLAN FRAGMENT 0(F11)
Output Exprs:2: S_NAME | 77: count
Input Partition: UNPARTITIONED
RESULT SINK

28:MERGING-EXCHANGE
distribution type: GATHER
limit: 100
cardinality: 100
column statistics:
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
* count-->[0.0, 2334116.9317591335, 0.0, 8.0, 40000.0] ESTIMATE

PLAN FRAGMENT 1(F10)

Input Partition: HASH_PARTITIONED: 2: S_NAME
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 28

27:TOP-N
|  order by: [77, BIGINT, false] DESC, [2, VARCHAR, false] ASC
|  offset: 0
|  limit: 100
|  cardinality: 100
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 2334116.9317591335, 0.0, 8.0, 40000.0] ESTIMATE
|
26:AGGREGATE (merge finalize)
|  aggregate: count[([77: count, BIGINT, false]); args: ; result: BIGINT; args nullable: true; result nullable: false]
|  group by: [2: S_NAME, VARCHAR, false]
|  cardinality: 40000
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 2334116.9317591335, 0.0, 8.0, 40000.0] ESTIMATE
|
25:EXCHANGE
distribution type: SHUFFLE
partition exprs: [2: S_NAME, VARCHAR, false]
cardinality: 40000

PLAN FRAGMENT 2(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 2: S_NAME
OutPut Exchange Id: 25

24:AGGREGATE (update serialize)
|  STREAMING
|  aggregate: count[(*); args: ; result: BIGINT; args nullable: false; result nullable: false]
|  group by: [2: S_NAME, VARCHAR, false]
|  cardinality: 40000
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * count-->[0.0, 2334116.9317591335, 0.0, 8.0, 40000.0] ESTIMATE
|
23:Project
|  output columns:
|  2 <-> [2: S_NAME, VARCHAR, false]
|  cardinality: 2334117
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|
22:HASH JOIN
|  join op: RIGHT SEMI JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [41: L_ORDERKEY, INT, false] = [9: L_ORDERKEY, INT, false]
|  other join predicates: [43: L_SUPPKEY, INT, false] != [11: L_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 4, build_expr = (9: L_ORDERKEY), remote = false
|  output columns: 2, 11, 43
|  cardinality: 2334117
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----21:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [9: L_ORDERKEY, INT, false]
|       cardinality: 2334119
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=12.0
cardinality: 600000000
probe runtime filters:
- filter_id = 4, probe_expr = (41: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE

PLAN FRAGMENT 3(F01)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 9: L_ORDERKEY
OutPut Exchange Id: 21

20:Project
|  output columns:
|  2 <-> [2: S_NAME, VARCHAR, false]
|  9 <-> [9: L_ORDERKEY, INT, false]
|  11 <-> [11: L_SUPPKEY, INT, false]
|  cardinality: 2334119
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2334119.2658784] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
19:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE)
|  equal join conjunct: [26: O_ORDERKEY, INT, false] = [9: L_ORDERKEY, INT, false]
|  build runtime filters:
|  - filter_id = 3, build_expr = (9: L_ORDERKEY), remote = false
|  output columns: 2, 9, 11
|  cardinality: 2334119
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 2334119.2658784] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----18:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [9: L_ORDERKEY, INT, false]
|       cardinality: 4799995
|
2:Project
|  output columns:
|  26 <-> [26: O_ORDERKEY, INT, false]
|  cardinality: 72941300
|  column statistics:
|  * O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 7.29413E7] ESTIMATE
|
1:OlapScanNode
table: orders, rollup: orders
preAggregation: on
Predicates: [28: O_ORDERSTATUS, CHAR, false] = 'F'
partitionsRatio=1/1, tabletsRatio=10/10
actualRows=0, avgRowSize=9.0
cardinality: 72941300
probe runtime filters:
- filter_id = 3, probe_expr = (26: O_ORDERKEY)
column statistics:
* O_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 7.29413E7] ESTIMATE
* O_ORDERSTATUS-->[-Infinity, Infinity, 0.0, 1.0, 1.0] ESTIMATE

PLAN FRAGMENT 4(F02)

Input Partition: RANDOM
OutPut Partition: BUCKET_SHUFFLE_HASH_PARTITIONED: 9: L_ORDERKEY
OutPut Exchange Id: 18

17:Project
|  output columns:
|  2 <-> [2: S_NAME, VARCHAR, false]
|  9 <-> [9: L_ORDERKEY, INT, false]
|  11 <-> [11: L_SUPPKEY, INT, false]
|  cardinality: 4799995
|  column statistics:
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 4799995.2] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
16:HASH JOIN
|  join op: RIGHT ANTI JOIN (COLOCATE)
|  colocate: true
|  equal join conjunct: [59: L_ORDERKEY, INT, false] = [9: L_ORDERKEY, INT, false]
|  other join predicates: [61: L_SUPPKEY, INT, false] != [11: L_SUPPKEY, INT, false]
|  build runtime filters:
|  - filter_id = 2, build_expr = (9: L_ORDERKEY), remote = false
|  output columns: 2, 9, 11, 61
|  cardinality: 4799995
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 4799995.2] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|
|----15:Project
|    |  output columns:
|    |  2 <-> [2: S_NAME, VARCHAR, false]
|    |  9 <-> [9: L_ORDERKEY, INT, false]
|    |  11 <-> [11: L_SUPPKEY, INT, false]
|    |  cardinality: 12000000
|    |  column statistics:
|    |  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|    |  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.2E7] ESTIMATE
|    |  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |
|    14:HASH JOIN
|    |  join op: INNER JOIN (BROADCAST)
|    |  equal join conjunct: [11: L_SUPPKEY, INT, false] = [1: S_SUPPKEY, INT, false]
|    |  build runtime filters:
|    |  - filter_id = 1, build_expr = (1: S_SUPPKEY), remote = false
|    |  output columns: 2, 9, 11
|    |  cardinality: 12000000
|    |  column statistics:
|    |  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|    |  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.2E7] ESTIMATE
|    |  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|    |
|    |----13:EXCHANGE
|    |       distribution type: BROADCAST
|    |       cardinality: 40000
|    |
|    6:Project
|    |  output columns:
|    |  9 <-> [9: L_ORDERKEY, INT, false]
|    |  11 <-> [11: L_SUPPKEY, INT, false]
|    |  cardinality: 300000000
|    |  column statistics:
|    |  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|    |  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|    |
|    5:OlapScanNode
|       table: lineitem, rollup: lineitem
|       preAggregation: on
|       Predicates: [21: L_RECEIPTDATE, DATE, false] > [20: L_COMMITDATE, DATE, false]
|       partitionsRatio=1/1, tabletsRatio=20/20
|       actualRows=0, avgRowSize=20.0
|       cardinality: 300000000
|       probe runtime filters:
|       - filter_id = 1, probe_expr = (11: L_SUPPKEY)
|       column statistics:
|       * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|       * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|       * L_COMMITDATE-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
|       * L_RECEIPTDATE-->[6.94368E8, 9.150336E8, 0.0, 4.0, 2554.0] ESTIMATE
|
4:Project
|  output columns:
|  59 <-> [59: L_ORDERKEY, INT, false]
|  61 <-> [61: L_SUPPKEY, INT, false]
|  cardinality: 300000000
|  column statistics:
|  * L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
|  * L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
|
3:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [71: L_RECEIPTDATE, DATE, false] > [70: L_COMMITDATE, DATE, false]
partitionsRatio=1/1, tabletsRatio=20/20
actualRows=0, avgRowSize=20.0
cardinality: 300000000
probe runtime filters:
- filter_id = 2, probe_expr = (59: L_ORDERKEY)
column statistics:
* L_ORDERKEY-->[1.0, 6.0E8, 0.0, 8.0, 1.5E8] ESTIMATE
* L_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* L_COMMITDATE-->[6.967872E8, 9.097632E8, 0.0, 4.0, 2466.0] ESTIMATE
* L_RECEIPTDATE-->[6.94368E8, 9.150336E8, 0.0, 4.0, 2554.0] ESTIMATE

PLAN FRAGMENT 5(F04)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 13

12:Project
|  output columns:
|  1 <-> [1: S_SUPPKEY, INT, false]
|  2 <-> [2: S_NAME, CHAR, false]
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|
11:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  equal join conjunct: [4: S_NATIONKEY, INT, false] = [36: N_NATIONKEY, INT, false]
|  build runtime filters:
|  - filter_id = 0, build_expr = (36: N_NATIONKEY), remote = false
|  output columns: 1, 2
|  cardinality: 40000
|  column statistics:
|  * S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 40000.0] ESTIMATE
|  * S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 40000.0] ESTIMATE
|  * S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
|----10:EXCHANGE
|       distribution type: BROADCAST
|       cardinality: 1
|
7:OlapScanNode
table: supplier, rollup: supplier
preAggregation: on
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=33.0
cardinality: 1000000
probe runtime filters:
- filter_id = 0, probe_expr = (4: S_NATIONKEY)
column statistics:
* S_SUPPKEY-->[1.0, 1000000.0, 0.0, 4.0, 1000000.0] ESTIMATE
* S_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1000000.0] ESTIMATE
* S_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 25.0] ESTIMATE

PLAN FRAGMENT 6(F05)

Input Partition: RANDOM
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 10

9:Project
|  output columns:
|  36 <-> [36: N_NATIONKEY, INT, false]
|  cardinality: 1
|  column statistics:
|  * N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
|
8:OlapScanNode
table: nation, rollup: nation
preAggregation: on
Predicates: [37: N_NAME, CHAR, false] = 'CANADA'
partitionsRatio=1/1, tabletsRatio=1/1
actualRows=0, avgRowSize=29.0
cardinality: 1
column statistics:
* N_NATIONKEY-->[0.0, 24.0, 0.0, 4.0, 1.0] ESTIMATE
* N_NAME-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
[end]

