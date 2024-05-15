[sql]
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#45'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 5 and l_quantity <= 5 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#11'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 15 and l_quantity <= 15 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#21'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 25 and l_quantity <= 25 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
    ) ;
[fragment statistics]
PLAN FRAGMENT 0(F05)
Output Exprs:29: sum
Input Partition: UNPARTITIONED
RESULT SINK

9:AGGREGATE (merge finalize)
|  aggregate: sum[([29: sum, DOUBLE, true]); args: DOUBLE; result: DOUBLE; args nullable: true; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
8:EXCHANGE
distribution type: GATHER
cardinality: 1

PLAN FRAGMENT 1(F04)

Input Partition: HASH_PARTITIONED: 2: L_PARTKEY
OutPut Partition: UNPARTITIONED
OutPut Exchange Id: 08

7:AGGREGATE (update serialize)
|  aggregate: sum[([6: L_EXTENDEDPRICE, DOUBLE, false] * 1.0 - [7: L_DISCOUNT, DOUBLE, false]); args: DOUBLE; result: DOUBLE; args nullable: false; result nullable: true]
|  cardinality: 1
|  column statistics:
|  * sum-->[810.9, 104949.5, 0.0, 8.0, 1.0] ESTIMATE
|
6:Project
|  output columns:
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  cardinality: 21702
|  column statistics:
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 21702.324135626324] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
5:HASH JOIN
|  join op: INNER JOIN (PARTITIONED)
|  equal join conjunct: [2: L_PARTKEY, INT, false] = [18: P_PARTKEY, INT, false]
|  other join predicates: (((((21: P_BRAND = 'Brand#45') AND (24: P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))) AND ((5: L_QUANTITY >= 5.0) AND (5: L_QUANTITY <= 15.0))) AND (23: P_SIZE <= 5)) OR ((((21: P_BRAND = 'Brand#11') AND (24: P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))) AND ((5: L_QUANTITY >= 15.0) AND (5: L_QUANTITY <= 25.0))) AND (23: P_SIZE <= 10))) OR ((((21: P_BRAND = 'Brand#21') AND (24: P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))) AND ((5: L_QUANTITY >= 25.0) AND (5: L_QUANTITY <= 35.0))) AND (23: P_SIZE <= 15))
|  build runtime filters:
|  - filter_id = 0, build_expr = (18: P_PARTKEY), remote = true
|  output columns: 5, 6, 7, 21, 23, 24
|  cardinality: 21702
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 21702.324135626324] ESTIMATE
|  * L_QUANTITY-->[NaN, NaN, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 21702.324135626324] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|  * P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 21702.324135626324] ESTIMATE
|  * P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 3.0] ESTIMATE
|  * P_SIZE-->[NaN, NaN, 0.0, 4.0, 50.0] ESTIMATE
|  * P_CONTAINER-->[-Infinity, Infinity, 0.0, 10.0, 12.0] ESTIMATE
|  * expr-->[810.9, 104949.5, 0.0, 8.0, 21702.324135626324] ESTIMATE
|
|----4:EXCHANGE
|       distribution type: SHUFFLE
|       partition exprs: [18: P_PARTKEY, INT, false]
|       cardinality: 6051300
|
2:EXCHANGE
distribution type: SHUFFLE
partition exprs: [2: L_PARTKEY, INT, false]
cardinality: 26568218

PLAN FRAGMENT 2(F02)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 18: P_PARTKEY
OutPut Exchange Id: 04

3:OlapScanNode
table: part, rollup: part
preAggregation: on
Predicates: 21: P_BRAND IN ('Brand#45', 'Brand#11', 'Brand#21'), [23: P_SIZE, INT, false] <= 15, 24: P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'), [23: P_SIZE, INT, false] >= 1
partitionsRatio=1/1, tabletsRatio=10/10
tabletList=10268,10270,10272,10274,10276,10278,10280,10282,10284,10286
actualRows=0, avgRowSize=32.0
cardinality: 6051300
column statistics:
* P_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 6051299.999999999] ESTIMATE
* P_BRAND-->[-Infinity, Infinity, 0.0, 10.0, 25.0] ESTIMATE
* P_SIZE-->[NaN, NaN, 0.0, 4.0, 50.0] ESTIMATE
* P_CONTAINER-->[-Infinity, Infinity, 0.0, 10.0, 40.0] ESTIMATE

PLAN FRAGMENT 3(F00)

Input Partition: RANDOM
OutPut Partition: HASH_PARTITIONED: 2: L_PARTKEY
OutPut Exchange Id: 02

1:Project
|  output columns:
|  2 <-> [2: L_PARTKEY, INT, false]
|  5 <-> [5: L_QUANTITY, DOUBLE, false]
|  6 <-> [6: L_EXTENDEDPRICE, DOUBLE, false]
|  7 <-> [7: L_DISCOUNT, DOUBLE, false]
|  cardinality: 26568218
|  column statistics:
|  * L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
|  * L_QUANTITY-->[NaN, NaN, 0.0, 8.0, 50.0] ESTIMATE
|  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
|  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
|
0:OlapScanNode
table: lineitem, rollup: lineitem
preAggregation: on
Predicates: [5: L_QUANTITY, DOUBLE, false] >= 5.0, [5: L_QUANTITY, DOUBLE, false] <= 35.0, 15: L_SHIPMODE IN ('AIR', 'AIR REG'), [14: L_SHIPINSTRUCT, CHAR, false] = 'DELIVER IN PERSON'
partitionsRatio=1/1, tabletsRatio=20/20
tabletList=10291,10293,10295,10297,10299,10301,10303,10305,10307,10309 ...
actualRows=0, avgRowSize=67.0
cardinality: 26568218
probe runtime filters:
- filter_id = 0, probe_expr = (2: L_PARTKEY)
column statistics:
* L_PARTKEY-->[1.0, 2.0E7, 0.0, 8.0, 2.0E7] ESTIMATE
* L_QUANTITY-->[NaN, NaN, 0.0, 8.0, 50.0] ESTIMATE
* L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0] ESTIMATE
* L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0] ESTIMATE
* L_SHIPINSTRUCT-->[-Infinity, Infinity, 0.0, 25.0, 1.0] ESTIMATE
* L_SHIPMODE-->[-Infinity, Infinity, 0.0, 10.0, 2.0] ESTIMATE
[end]