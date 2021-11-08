[sql]
select t0.v1 from t0 where t0.v2 = (select t3.v2 from t3)
[result]
INNER JOIN (join-predicate [2: v2 = 5: v2] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[5: v2] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

7:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:Project
|  <slot 1> : 1: v1
|  use vectorized: true
|
5:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 2: v2 = 5: v2
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
partitions=1/1
rollup: t0
tabletRatio=3/3
tabletList=10006,10008,10010
cardinality=1
avgRowSize=2.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 04
UNPARTITIONED

3:ASSERT NUMBER OF ROWS
|  assert number of rows: LE 1
|  use vectorized: true
|
2:EXCHANGE
use vectorized: true

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
tabletList=10033,10035,10037
cardinality=1
avgRowSize=1.0
numNodes=0
use vectorized: true
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select t3.v2 from t3 where t3.v3 > 3)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [2: v2 < 5: v2])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[5: v2, 6: v3] predicate[6: v3 > 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v2) from t3 where t0.v3 = t3.v3)
[result]
INNER JOIN (join-predicate [3: v3 = 6: v3 AND 2: v2 < 7: sum(5: v2)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: sum(5: v2)=sum(7: sum(5: v2))}] group by [[6: v3]] having [null]
            EXCHANGE SHUFFLE[6]
                AGGREGATE ([LOCAL] aggregate [{7: sum(5: v2)=sum(5: v2)}] group by [[6: v3]] having [null]
                    SCAN (columns[5: v2, 6: v3] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

8:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 08
UNPARTITIONED

7:Project
|  <slot 1> : 1: v1
|  use vectorized: true
|
6:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 3: v3 = 6: v3
|  other join predicates: 2: v2 < 7: sum(5: v2)
|  use vectorized: true
|
|----5:EXCHANGE
|       use vectorized: true
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
partitions=1/1
rollup: t0
tabletRatio=3/3
tabletList=10006,10008,10010
cardinality=1
avgRowSize=3.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 6: v3

STREAM DATA SINK
EXCHANGE ID: 05
UNPARTITIONED

4:AGGREGATE (merge finalize)
|  output: sum(7: sum(5: v2))
|  group by: 6: v3
|  use vectorized: true
|
3:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
HASH_PARTITIONED: 6: v3

2:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(5: v2)
|  group by: 6: v3
|  use vectorized: true
|
1:OlapScanNode
TABLE: t3
PREAGGREGATION: ON
partitions=1/1
rollup: t3
tabletRatio=3/3
tabletList=10033,10035,10037
cardinality=1
avgRowSize=2.0
numNodes=0
use vectorized: true
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v2) from t3 where t0.v3 = t3.v3 and t0.v1 = t3.v1)
[result]
INNER JOIN (join-predicate [3: v3 = 6: v3 AND 1: v1 = 4: v1 AND 2: v2 < 7: sum(5: v2)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{7: sum(5: v2)=sum(7: sum(5: v2))}] group by [[4: v1, 6: v3]] having [null]
            AGGREGATE ([LOCAL] aggregate [{7: sum(5: v2)=sum(5: v2)}] group by [[4: v1, 6: v3]] having [null]
                SCAN (columns[4: v1, 5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(t3.v2) from t3 where t0.v3 = t3.v3 and abs(t0.v1) = abs(t3.v1))
[result]
INNER JOIN (join-predicate [3: v3 = 6: v3 AND 10: abs = 9: abs AND 2: v2 < 7: sum(5: v2)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: sum(5: v2)=sum(7: sum(5: v2))}] group by [[6: v3, 9: abs]] having [null]
            EXCHANGE SHUFFLE[6, 9]
                AGGREGATE ([LOCAL] aggregate [{7: sum(5: v2)=sum(5: v2)}] group by [[6: v3, 9: abs]] having [null]
                    SCAN (columns[4: v1, 5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 < (select SUM(abs(t3.v2)) from t3 where t0.v3 = t3.v3 and abs(t0.v1) = abs(t3.v1))
[result]
INNER JOIN (join-predicate [3: v3 = 6: v3 AND 11: abs = 10: abs AND cast(2: v2 as largeint(40)) < 8: sum(7: abs)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: sum(7: abs)=sum(8: sum(7: abs))}] group by [[6: v3, 10: abs]] having [null]
            EXCHANGE SHUFFLE[6, 10]
                AGGREGATE ([LOCAL] aggregate [{8: sum(7: abs)=sum(7: abs)}] group by [[6: v3, 10: abs]] having [null]
                    SCAN (columns[4: v1, 5: v2, 6: v3] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1
PARTITION: UNPARTITIONED

RESULT SINK

10:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 10
UNPARTITIONED

9:Project
|  <slot 1> : 1: v1
|  use vectorized: true
|
8:HASH JOIN
|  join op: INNER JOIN (BROADCAST)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 3: v3 = 6: v3
|  equal join conjunct: 11: abs = 10: abs
|  other join predicates: CAST(2: v2 AS LARGEINT) < 8: sum(7: abs)
|  use vectorized: true
|
|----7:EXCHANGE
|       use vectorized: true
|
1:Project
|  <slot 1> : 1: v1
|  <slot 2> : 2: v2
|  <slot 3> : 3: v3
|  <slot 11> : abs(1: v1)
|  use vectorized: true
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
partitions=1/1
rollup: t0
tabletRatio=3/3
tabletList=10006,10008,10010
cardinality=1
avgRowSize=4.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 6: v3, 10: abs

STREAM DATA SINK
EXCHANGE ID: 07
UNPARTITIONED

6:AGGREGATE (merge finalize)
|  output: sum(8: sum(7: abs))
|  group by: 6: v3, 10: abs
|  use vectorized: true
|
5:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 05
HASH_PARTITIONED: 6: v3, 10: abs

4:AGGREGATE (update serialize)
|  STREAMING
|  output: sum(7: abs)
|  group by: 6: v3, 10: abs
|  use vectorized: true
|
3:Project
|  <slot 6> : 6: v3
|  <slot 7> : abs(5: v2)
|  <slot 10> : abs(4: v1)
|  use vectorized: true
|
2:OlapScanNode
TABLE: t3
PREAGGREGATION: ON
partitions=1/1
rollup: t3
tabletRatio=3/3
tabletList=10033,10035,10037
cardinality=1
avgRowSize=5.0
numNodes=0
use vectorized: true
[end]

[sql]
select v1 from t0 group by v1 having sum(v3) < (100 + 5) * (select max(v4) from t1);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [4: sum(3: v3) < multiply(105, 8: max(5: v4))])
    AGGREGATE ([GLOBAL] aggregate [{8: max(5: v4)=max(8: max(5: v4))}] group by [[]] having [null]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{8: max(5: v4)=max(5: v4)}] group by [[]] having [null]
                SCAN (columns[5: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{4: sum(3: v3)=sum(4: sum(3: v3))}] group by [[1: v1]] having [null]
            AGGREGATE ([LOCAL] aggregate [{4: sum(3: v3)=sum(3: v3)}] group by [[1: v1]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v1 from t0,t1 where v1 between (select v4 from t1) and (select v4 from t1)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [1: v1 <= 11: v4])
    ASSERT LE 1
        EXCHANGE GATHER
            SCAN (columns[11: v4] predicate[null])
    EXCHANGE BROADCAST
        CROSS JOIN (join-predicate [null] post-join-predicate [1: v1 >= 7: v4])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[1: v1] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[4: v4] predicate[null])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    EXCHANGE GATHER
                        SCAN (columns[7: v4] predicate[null])
[end]

[sql]
select * from t0 where v3 = (select 6)
[result]
INNER JOIN (join-predicate [3: v3 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            VALUES (6)
[end]

[sql]
select * from t0 where v3 = (select * from (values(2)) t);
[result]
INNER JOIN (join-predicate [3: v3 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            VALUES (2)
[end]

[sql]
select * from t0 where v3 > (select * from (values(2)) t);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [3: v3 > cast(4: expr as bigint(20))])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            VALUES (2)
[end]

[sql]
select v3 from t0 group by v3 having sum(v2) > (select * from (values(2)) t);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [4: sum(2: v2) > cast(5: expr as bigint(20))])
    ASSERT LE 1
        VALUES (2)
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{4: sum(2: v2)=sum(4: sum(2: v2))}] group by [[3: v3]] having [null]
            EXCHANGE SHUFFLE[3]
                AGGREGATE ([LOCAL] aggregate [{4: sum(2: v2)=sum(2: v2)}] group by [[3: v3]] having [null]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
[end]

[sql]
select v1 from t0 where v2 > (select count(v4) from t1 where v3 = v5)
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [2: v2 > ifnull(7: count(4: v4), 0)])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: count(4: v4)=count(7: count(4: v4))}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{7: count(4: v4)=count(4: v4)}] group by [[5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1 from t0 where v2 > (select max(v4) from t1 where v3 = v5)
[result]
INNER JOIN (join-predicate [3: v3 = 5: v5 AND 2: v2 > 7: max(4: v4)] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1 from t0 where t0.v1 = 123 or v2 > (select max(v4) from t1 where v3 = v5)
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [1: v1 = 123 OR 2: v2 > 7: max(4: v4)])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1 from t0 where case when (select max(v4) from t1 where v3 = v5) > 1 then 2 else 3 end > 2
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [if(7: max(4: v4) > 1, 2, 3) > 2])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select case when (select max(v4) from t1) > 1 then 2 else 3 end
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{6: max(3: v4)=max(6: max(3: v4))}] group by [[]] having [null]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{6: max(3: v4)=max(3: v4)}] group by [[]] having [null]
                SCAN (columns[3: v4] predicate[null])
    VALUES (1)
[end]

[sql]
select 1, 2, case when (select max(v4) from t1) > 1 then 4 else 5 end
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    VALUES (1,2)
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[]] having [null]
                    SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select t0.v1, case when (select max(v4) from t1 where t0.v3 = t1.v5) > 1 then 4 else 5 end from t0;
[result]
LEFT OUTER JOIN (join-predicate [3: v3 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select t0.v1, (select max(v4) from t1) / 2 from t0;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: max(4: v4)=max(7: max(4: v4))}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{7: max(4: v4)=max(4: v4)}] group by [[]] having [null]
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
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{7: sum(4: v4)=sum(7: sum(4: v4))}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{7: sum(4: v4)=sum(4: v4)}] group by [[]] having [null]
                    SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select * from t0 where (select v4 from t1) = (select v7 from t2)
[result]
INNER JOIN (join-predicate [7: expr = 8: v7] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE BROADCAST
            ASSERT LE 1
                EXCHANGE GATHER
                    SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        ASSERT LE 1
            EXCHANGE GATHER
                SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select * from t0 where (select SUM(v4) from t1 where t0.v1 = t1.v4) = (select SUM(v7) from t2 where t0.v2 = t2.v8)
[result]
INNER JOIN (join-predicate [2: v2 = 10: v8 AND 8: expr = 12: sum(9: v7)] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[4]
            AGGREGATE ([GLOBAL] aggregate [{7: sum(4: v4)=sum(7: sum(4: v4))}] group by [[4: v4]] having [null]
                AGGREGATE ([LOCAL] aggregate [{7: sum(4: v4)=sum(4: v4)}] group by [[4: v4]] having [null]
                    SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: sum(9: v7)=sum(12: sum(9: v7))}] group by [[10: v8]] having [null]
            EXCHANGE SHUFFLE[10]
                AGGREGATE ([LOCAL] aggregate [{12: sum(9: v7)=sum(9: v7)}] group by [[10: v8]] having [null]
                    SCAN (columns[9: v7, 10: v8] predicate[null])
[end]
