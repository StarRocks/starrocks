[sql]
select t0.v1 from t0 where t0.v2 = (select t3.v11 from t3)
[result]
INNER JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 IS NOT NULL])
    EXCHANGE BROADCAST
        PREDICATE 5: v11 IS NOT NULL
            ASSERT LE 1
                EXCHANGE GATHER
                    SCAN (columns[5: v11] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

8:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 08
UNPARTITIONED

7:Project
|  <slot 1> : 1: v1
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  colocate: false, reason:
|  equal join conjunct: 2: v2 = 5: v11
|
|----5:EXCHANGE
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
PREDICATES: 2: v2 IS NOT NULL
partitions=1/1
rollup: t0
tabletRatio=3/3
cardinality=1
avgRowSize=2.0
numNodes=0

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 05
UNPARTITIONED

4:SELECT
|  predicates: 5: v11 IS NOT NULL
|
3:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|
2:EXCHANGE

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
UNPARTITIONED

1:OlapScanNode
TABLE: t3
PREAGGREGATION: ON
partitions=1/1
rollup: t3
tabletRatio=3/3
cardinality=1
avgRowSize=1.0
numNodes=0
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select t3.v11 from t3 where t3.v12 > 3)
[result]
CROSS JOIN (join-predicate [2: v2 < 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[5: v11, 6: v12] predicate[6: v12 > 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v11) from t3 where t0.v3 = t3.v12)
[result]
INNER JOIN (join-predicate [3: v3 = 6: v12 AND 2: v2 < 7: sum] post-join-predicate [null])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
    AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[6: v12]] having [null]
        EXCHANGE SHUFFLE[6]
            AGGREGATE ([LOCAL] aggregate [{7: sum=sum(5: v11)}] group by [[6: v12]] having [null]
                SCAN (columns[5: v11, 6: v12] predicate[6: v12 IS NOT NULL])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

8:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 6: v12

STREAM DATA SINK
EXCHANGE ID: 08
UNPARTITIONED

7:Project
|  <slot 1> : 1: v1
|
6:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  colocate: false, reason:
|  equal join conjunct: 3: v3 = 6: v12
|  other join predicates: 2: v2 < 7: sum
|
|----5:AGGREGATE (merge finalize)
|    |  output: sum(7: sum)
|    |  group by: 6: v12
|    |
|    4:EXCHANGE
|
1:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 04
HASH_PARTITIONED: 6: v12

3:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(5: v11)
|  group by: 6: v12
|
2:OlapScanNode
TABLE: t3
PREAGGREGATION: ON
PREDICATES: 6: v12 IS NOT NULL
partitions=1/1
rollup: t3
tabletRatio=3/3
cardinality=1
avgRowSize=2.0
numNodes=0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 01
HASH_PARTITIONED: 3: v3

0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
PREDICATES: 3: v3 IS NOT NULL
partitions=1/1
rollup: t0
tabletRatio=3/3
cardinality=1
avgRowSize=3.0
numNodes=0
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v11) from t3 where t0.v3 = t3.v12 and t0.v1 = t3.v10)
[result]
INNER JOIN (join-predicate [3: v3 = 6: v12 AND 1: v1 = 4: v10 AND 2: v2 < 7: sum] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL AND 1: v1 IS NOT NULL])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[4: v10, 6: v12]] having [null]
            AGGREGATE ([LOCAL] aggregate [{7: sum=sum(5: v11)}] group by [[4: v10, 6: v12]] having [null]
                SCAN (columns[4: v10, 5: v11, 6: v12] predicate[6: v12 IS NOT NULL AND 4: v10 IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v11) from t3 where t0.v3 = t3.v12 and abs(t0.v1) = abs(t3.v10))
[result]
INNER JOIN (join-predicate [3: v3 = 6: v12 AND 10: abs = 9: abs AND 2: v2 < 7: sum] post-join-predicate [null])
    EXCHANGE SHUFFLE[3, 10]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL AND abs(1: v1) IS NOT NULL])
    AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[6: v12, 9: abs]] having [null]
        EXCHANGE SHUFFLE[6, 9]
            AGGREGATE ([LOCAL] aggregate [{7: sum=sum(5: v11)}] group by [[6: v12, 9: abs]] having [null]
                SCAN (columns[4: v10, 5: v11, 6: v12] predicate[6: v12 IS NOT NULL AND abs(4: v10) IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(abs(t3.v11)) from t3 where t0.v3 = t3.v12 and abs(t0.v1) = abs(t3.v10))
[result]
INNER JOIN (join-predicate [3: v3 = 6: v12 AND 11: abs = 10: abs AND cast(2: v2 as largeint(40)) < 8: sum] post-join-predicate [null])
    EXCHANGE SHUFFLE[3, 11]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL AND abs(1: v1) IS NOT NULL])
    AGGREGATE ([GLOBAL] aggregate [{8: sum=sum(8: sum)}] group by [[6: v12, 10: abs]] having [null]
        EXCHANGE SHUFFLE[6, 10]
            AGGREGATE ([LOCAL] aggregate [{8: sum=sum(7: abs)}] group by [[6: v12, 10: abs]] having [null]
                SCAN (columns[4: v10, 5: v11, 6: v12] predicate[6: v12 IS NOT NULL AND abs(4: v10) IS NOT NULL])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

10:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 6: v12, 10: abs

STREAM DATA SINK
EXCHANGE ID: 10
UNPARTITIONED

9:Project
|  <slot 1> : 1: v1
|
8:HASH JOIN
|  join op: INNER JOIN (BUCKET_SHUFFLE(S))
|  colocate: false, reason:
|  equal join conjunct: 3: v3 = 6: v12
|  equal join conjunct: 11: abs = 10: abs
|  other join predicates: CAST(2: v2 AS LARGEINT) < 8: sum
|
|----7:AGGREGATE (merge finalize)
|    |  output: sum(8: sum)
|    |  group by: 6: v12, 10: abs
|    |
|    6:EXCHANGE
|
2:EXCHANGE

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 06
HASH_PARTITIONED: 6: v12, 10: abs

5:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(7: abs)
|  group by: 6: v12, 10: abs
|
4:Project
|  <slot 6> : 6: v12
|  <slot 7> : abs(5: v11)
|  <slot 10> : abs(4: v10)
|
3:OlapScanNode
TABLE: t3
PREAGGREGATION: ON
PREDICATES: 6: v12 IS NOT NULL, abs(4: v10) IS NOT NULL
partitions=1/1
rollup: t3
tabletRatio=3/3
cardinality=1
avgRowSize=5.0
numNodes=0

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
HASH_PARTITIONED: 3: v3, 11: abs

1:Project
|  <slot 1> : 1: v1
|  <slot 2> : 2: v2
|  <slot 3> : 3: v3
|  <slot 11> : abs(1: v1)
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
PREDICATES: 3: v3 IS NOT NULL, abs(1: v1) IS NOT NULL
partitions=1/1
rollup: t0
tabletRatio=3/3
cardinality=1
avgRowSize=4.0
numNodes=0
[end]

[sql]
select v1 from t0 group by v1 having sum(v3) < (100 + 5) * (select max(v4) from t1);
[result]
CROSS JOIN (join-predicate [4: sum < multiply(105, 8: max)] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: sum=sum(4: sum)}] group by [[1: v1]] having [null]
        AGGREGATE ([LOCAL] aggregate [{4: sum=sum(3: v3)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: max=max(8: max)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{8: max=max(5: v4)}] group by [[]] having [null]
                    SCAN (columns[5: v4] predicate[null])
[end]

[sql]
select v1 from t0,t1 where v1 between (select v4 from t1) and (select v4 from t1)
[result]
CROSS JOIN (join-predicate [1: v1 <= 11: v4] post-join-predicate [null])
    CROSS JOIN (join-predicate [1: v1 >= 7: v4] post-join-predicate [null])
        CROSS JOIN (join-predicate [null] post-join-predicate [null])
            SCAN (columns[1: v1] predicate[null])
            EXCHANGE BROADCAST
                SCAN (columns[4: v4] predicate[null])
        EXCHANGE BROADCAST
            ASSERT LE 1
                EXCHANGE GATHER
                    SCAN (columns[7: v4] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[11: v4] predicate[null])[end]

[sql]
select * from t0 where v3 = (select * from (values(2)) t);
[result]
INNER JOIN (join-predicate [3: v3 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
    EXCHANGE BROADCAST
        PREDICATE cast(4: column_0 as bigint(20)) IS NOT NULL
            ASSERT LE 1
                VALUES (2)
[end]

[sql]
select * from t0 where v3 > (select * from (values(2)) t);
[result]
CROSS JOIN (join-predicate [3: v3 > cast(4: column_0 as bigint(20))] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            VALUES (2)
[end]

[sql]
select v3 from t0 group by v3 having sum(v2) > (select * from (values(2)) t);
[result]
CROSS JOIN (join-predicate [4: sum > cast(5: column_0 as bigint(20))] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: sum=sum(4: sum)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: sum=sum(2: v2)}] group by [[3: v3]] having [null]
                SCAN (columns[2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            VALUES (2)
[end]

[sql]
select v1 from t0 where v2 > (select count(v4) from t1 where v3 = v5)
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [2: v2 > ifnull(7: count, 0)])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count)}] group by [[5: v5]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{7: count=count(4: v4)}] group by [[5: v5]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1 from t0 where v2 > (select max(v4) from t1 where v3 = v5)
[result]
INNER JOIN (join-predicate [3: v3 = 5: v5 AND 2: v2 > 7: max] post-join-predicate [null])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[5: v5]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{7: max=max(4: v4)}] group by [[5: v5]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[5: v5 IS NOT NULL])
[end]

[sql]
select v1 from t0 where t0.v1 = 123 or v2 > (select max(v4) from t1 where v3 = v5)
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [1: v1 = 123 OR 2: v2 > 7: max])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[5: v5]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{7: max=max(4: v4)}] group by [[5: v5]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1 from t0 where case when (select max(v4) from t1 where v3 = v5) > 1 then 2 else 3 end > 2
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [if(7: max > 1, 2, 3) > 2])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 3: v3] predicate[null])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[5: v5]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{7: max=max(4: v4)}] group by [[5: v5]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select t0.v1, case when (select max(v4) from t1 where t0.v3 = t1.v5) > 1 then 4 else 5 end from t0;
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [null])
    EXCHANGE SHUFFLE[3]
        SCAN (columns[1: v1, 3: v3] predicate[null])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[5: v5]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{7: max=max(4: v4)}] group by [[5: v5]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select t0.v1, (select max(v4) from t1) / 2 from t0;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{7: max=max(4: v4)}] group by [[]] having [null]
                    SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select t0.v1, (select v4 from t1) / 2 from t0;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 = (select SUM(v4) from t1) / 2;
[result]
INNER JOIN (join-predicate [9: cast = 10: divide] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[cast(2: v2 as double) IS NOT NULL])
    EXCHANGE BROADCAST
<<<<<<< HEAD
        AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[]] having [divide(cast(7: sum as double), 2.0) IS NOT NULL]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{7: sum=sum(4: v4)}] group by [[]] having [null]
                    SCAN (columns[4: v4] predicate[null])
=======
        PREDICATE divide(cast(7: sum as double), 2) IS NOT NULL
            ASSERT LE 1
                AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[]] having [null]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{7: sum=sum(4: v4)}] group by [[]] having [null]
                            SCAN (columns[4: v4] predicate[null])
>>>>>>> d88b4657a ([BugFix] Fix double/float/date cast to string in FE (#27070))
[end]

[sql]
select * from t0 where (select v4 from t1) = (select v7 from t2)
[result]
INNER JOIN (join-predicate [7: expr = 8: v7] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE BROADCAST
            PREDICATE 4: v4 IS NOT NULL
                ASSERT LE 1
                    EXCHANGE GATHER
                        SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        PREDICATE 8: v7 IS NOT NULL
            ASSERT LE 1
                EXCHANGE GATHER
                    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select * from t0 where (select SUM(v4) from t1 where t0.v1 = t1.v4) = (select SUM(v7) from t2 where t0.v2 = t2.v8)
[result]
INNER JOIN (join-predicate [2: v2 = 10: v8 AND 8: expr = 12: sum] post-join-predicate [null])
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(7: sum)}] group by [[4: v4]] having [7: sum IS NOT NULL]
                AGGREGATE ([LOCAL] aggregate [{7: sum=sum(4: v4)}] group by [[4: v4]] having [null]
                    SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: sum=sum(12: sum)}] group by [[10: v8]] having [12: sum IS NOT NULL]
            EXCHANGE SHUFFLE[10]
                AGGREGATE ([LOCAL] aggregate [{12: sum=sum(9: v7)}] group by [[10: v8]] having [null]
                    SCAN (columns[9: v7, 10: v8] predicate[10: v8 IS NOT NULL])
[end]

[sql]
select v1 from t0 where v2 = (with cte as (select v4,v5 from t1 order by 2 limit 10) select v4 from cte order by 1 limit 1)
[result]
INNER JOIN (join-predicate [2: v2 = 7: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 IS NOT NULL])
    EXCHANGE BROADCAST
        PREDICATE 7: v4 IS NOT NULL
            ASSERT LE 1
                EXCHANGE GATHER
                    TOP-N (order by [[7: v4 ASC NULLS FIRST]])
                        TOP-N (order by [[8: v5 ASC NULLS FIRST]])
                            TOP-N (order by [[8: v5 ASC NULLS FIRST]])
                                SCAN (columns[7: v4, 8: v5] predicate[null])
[end]