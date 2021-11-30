[sql]
select * from (select sum(v1) as v, sum(v2) from t0) a left semi join (select v1,v2 from t0 order by v3) b on a.v = b.v2;
[result]
RIGHT SEMI JOIN (join-predicate [7: v2 = 4: sum(1: v1)] post-join-predicate [null])
    EXCHANGE SHUFFLE[7]
        SCAN (columns[7: v2] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1)), 5: sum(2: v2)=sum(5: sum(2: v2))}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1), 5: sum(2: v2)=sum(2: v2)}] group by [[]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select t1.* from t0 right semi join t1 on t0.v2 = t1.v5;
[result]
RIGHT SEMI JOIN (join-predicate [2: v2 = 5: v5] post-join-predicate [null])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[2: v2] predicate[null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: v4 | 5: v5 | 6: v6
PARTITION: UNPARTITIONED

RESULT SINK

6:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 2: v2

STREAM DATA SINK
EXCHANGE ID: 06
UNPARTITIONED

5:Project
|  <slot 4> : 4: v4
|  <slot 5> : 5: v5
|  <slot 6> : 6: v6
|  use vectorized: true
|
4:HASH JOIN
|  join op: RIGHT SEMI JOIN (PARTITIONED)
|  hash predicates:
|  colocate: false, reason:
|  equal join conjunct: 2: v2 = 5: v5
|  use vectorized: true
|
|----3:EXCHANGE
|       use vectorized: true
|
1:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 03
HASH_PARTITIONED: 5: v5

2:OlapScanNode
TABLE: t1
PREAGGREGATION: ON
partitions=1/1
rollup: t1
tabletRatio=3/3
tabletList=10015,10017,10019
cardinality=1
avgRowSize=3.0
numNodes=0
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 01
HASH_PARTITIONED: 2: v2

0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
partitions=1/1
rollup: t0
tabletRatio=3/3
tabletList=10006,10008,10010
cardinality=10000
avgRowSize=1.0
numNodes=0
use vectorized: true
[end]

[sql]
select t0.*,v1,t1.* from t0 join t1 on t0.v1=t1.v4;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select t0.*,v1,t1.* from t0 join t1 on t0.v1=t1.v4;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select t1.*,t0.* from t0 join t1;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select * from (select sum(v1) as v, sum(v2) from t0) a left semi join (select v1,v2,v3 from t0 order by v3) b on a.v = b.v3;
[result]
RIGHT SEMI JOIN (join-predicate [8: v3 = 4: sum(1: v1)] post-join-predicate [null])
    EXCHANGE SHUFFLE[8]
        SCAN (columns[8: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1)), 5: sum(2: v2)=sum(5: sum(2: v2))}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1), 5: sum(2: v2)=sum(2: v2)}] group by [[]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
SELECT t2.v7 FROM  t0 left SEMI JOIN t1 on t0.v1=t1.v4, t2
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: v7] predicate[null])
[end]

[sql]
SELECT t2.v7 FROM  t0 right SEMI JOIN t1 on t0.v1=t1.v4, t2
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    RIGHT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: v7] predicate[null])
[end]

[sql]
SELECT t1.* FROM  t0 right SEMI JOIN t1 on t0.v1=t1.v4, t2
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    RIGHT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select v1 from t0 inner join [shuffle] t1 on t0.v2 = t1.v4
[result]
INNER JOIN (join-predicate [2: v2 = 4: v4] post-join-predicate [null])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1 from t0 inner join [BROADCAST] t1 on t0.v3 = t1.v4
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select t1f from test_all_type left semi join (select v1,v2 from t0 order by v1) a on a.v2 = test_all_type.t1f;
[result]
RIGHT SEMI JOIN (join-predicate [14: cast = 6: t1f] post-join-predicate [null])
    EXCHANGE SHUFFLE[14]
        SCAN (columns[12: v2] predicate[null])
    EXCHANGE SHUFFLE[6]
        SCAN (columns[6: t1f] predicate[null])
[end]

[sql]
select v1,v2,v3,v4 from t0 full outer join t1 on v1=v5 and 1>2
[result]
FULL OUTER JOIN (join-predicate [1: v1 = 5: v5 AND false] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1,v2,v3,v4 from t0 left outer join t1 on v1=v5 and 1>2
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        VALUES
[end]

[sql]
select v1,v2,v3 from t0 left semi join t1 on v1=v5 and 1>2
[result]
RIGHT SEMI JOIN (join-predicate [5: v5 = 1: v1] post-join-predicate [null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v5] predicate[null])
    EXCHANGE SHUFFLE[1]
        VALUES
[end]

[sql]
select v1,v2,v3,v4 from t0 inner join t1 on v1=v5 and 1>2
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    VALUES
    EXCHANGE BROADCAST
        VALUES
[end]

[sql]
select v1,v2,v3,v4 from t0 full outer join t1 on v1=v5 and 1>2 and v1=v1 and v2 > v3
[result]
FULL OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 1: v1 = 1: v1 AND false] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select * from t0 full outer join t1 on v1 = v4 and 1 = 2 and v2 = 3
[result]
FULL OUTER JOIN (join-predicate [1: v1 = 4: v4 AND false] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
SELECT COUNT(*) FROM  t0 LEFT JOIN t1 ON v1 = v4 AND ((NULL)-(NULL)) >= ((NULL)%(NULL))
[result]
AGGREGATE ([GLOBAL] aggregate [{7: count()=count(7: count())}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{7: count()=count()}] group by [[]] having [null]
            LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                SCAN (columns[1: v1] predicate[null])
                EXCHANGE BROADCAST
                    VALUES
[end]
[sql]
select * from (select abs(v1) as t from t0 ) ta left join (select abs(v4) as t from t1 group by t) tb on ta.t = tb.t
[result]
LEFT OUTER JOIN (join-predicate [4: abs = 8: abs] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: abs]] having [null]
            EXCHANGE SHUFFLE[8]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[8: abs]] having [null]
                    SCAN (columns[5: v4] predicate[null])
[end]