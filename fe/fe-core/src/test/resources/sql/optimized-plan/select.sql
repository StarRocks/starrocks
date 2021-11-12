[sql]
select 1,2
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: expr | 2: expr
PARTITION: UNPARTITIONED

RESULT SINK

0:UNION
constant exprs:
1 | 2
use vectorized: true
[end]

[sql]
select 1,2 from t0
[result]
SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select a from (select 1 as a, 2 as b) t
[result]
VALUES (1)
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: expr
PARTITION: UNPARTITIONED

RESULT SINK

0:UNION
constant exprs:
1
use vectorized: true
[end]

[sql]
select v1,v2 from t0 union all select 1,2
[result]
UNION
    SCAN (columns[1: v1, 2: v2] predicate[null])
    VALUES (1,2)
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: v1 | 5: v2
PARTITION: UNPARTITIONED

RESULT SINK

5:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 05
UNPARTITIONED

0:UNION
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
2:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 04
RANDOM

3:UNION
constant exprs:
1 | 2
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
RANDOM

1:OlapScanNode
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
[end]

[sql]
select v1,v2 from t0 union select 1,2
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v1, 5: v2]] having [null]
    EXCHANGE SHUFFLE[4, 5]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v1, 5: v2]] having [null]
            UNION
                SCAN (columns[1: v1, 2: v2] predicate[null])
                VALUES (1,2)
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: v1 | 5: v2
PARTITION: UNPARTITIONED

RESULT SINK

8:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: HASH_PARTITIONED: 4: v1, 5: v2

STREAM DATA SINK
EXCHANGE ID: 08
UNPARTITIONED

7:AGGREGATE (merge finalize)
|  group by: 4: v1, 5: v2
|  use vectorized: true
|
6:EXCHANGE
use vectorized: true

PLAN FRAGMENT 2
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 06
HASH_PARTITIONED: 4: v1, 5: v2

5:AGGREGATE (update serialize)
|  STREAMING
|  group by: 4: v1, 5: v2
|  use vectorized: true
|
0:UNION
|  use vectorized: true
|
|----4:EXCHANGE
|       use vectorized: true
|
2:EXCHANGE
use vectorized: true

PLAN FRAGMENT 3
OUTPUT EXPRS:
PARTITION: UNPARTITIONED

STREAM DATA SINK
EXCHANGE ID: 04
HASH_PARTITIONED: <slot 6>, <slot 7>

3:UNION
constant exprs:
1 | 2
use vectorized: true

PLAN FRAGMENT 4
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
HASH_PARTITIONED: <slot 1>, <slot 2>

1:OlapScanNode
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
[end]

[sql]
select v1,v2 from t0 except select 1,2
[result]
EXCEPT
    SCAN (columns[1: v1, 2: v2] predicate[null])
    VALUES (1,2)
[end]

[sql]
select v1,v2 from t0 intersect select 1,2
[result]
INTERSECT
    SCAN (columns[1: v1, 2: v2] predicate[null])
    VALUES (1,2)
[end]

[sql]
select v1,v2,b from t0 inner join (select 1 as a,2 as b) t on v1 = a
[result]
INNER JOIN (join-predicate [1: v1 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    VALUES (1,2)
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: v1 | 2: v2 | 5: expr
PARTITION: UNPARTITIONED

RESULT SINK

5:EXCHANGE
use vectorized: true

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 05
UNPARTITIONED

4:Project
|  <slot 1> : 1: v1
|  <slot 2> : 2: v2
|  <slot 5> : 5: expr
|  use vectorized: true
|
3:HASH JOIN
|  join op: INNER JOIN (COLOCATE)
|  hash predicates:
|  colocate: true
|  equal join conjunct: 1: v1 = 6: cast
|  use vectorized: true
|
|----2:Project
|    |  <slot 5> : 5: expr
|    |  <slot 6> : CAST(4: expr AS BIGINT)
|    |  use vectorized: true
|    |
|    1:UNION
|       constant exprs:
|           1 | 2
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
[end]

[sql]
select v1 from (select * from t0 limit 0) t
[result]
VALUES
[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:1: v1
  PARTITION: UNPARTITIONED

  RESULT SINK

  0:EMPTYSET
     use vectorized: true
[end]

[sql]
select sum(v1) from t0 limit 0
[result]
VALUES
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: sum(1: v1)
PARTITION: UNPARTITIONED

RESULT SINK

0:EMPTYSET
use vectorized: true
[end]

[sql]
select sum(a) from (select v1 as a from t0 limit 0) t
[result]
AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1)}] group by [[]] having [null]
            VALUES
[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:4: sum(1: v1)
  PARTITION: UNPARTITIONED

  RESULT SINK

3:AGGREGATE (merge finalize)
  |  output: sum(4: sum(1: v1))
  |  group by:
  |  use vectorized: true
  |
2:EXCHANGE
     use vectorized: true

PLAN FRAGMENT 1
 OUTPUT EXPRS:
  PARTITION: UNPARTITIONED

  STREAM DATA SINK
EXCHANGE ID: 02
    UNPARTITIONED

1:AGGREGATE (update serialize)
  |  output: sum(1: v1)
  |  group by:
  |  use vectorized: true
  |
  0:EMPTYSET
     use vectorized: true
[end]

[sql]
select case when v2 between v1+1 and v1-1 then 1 else 3 end, case when v2  between v1+1 and v1-1 then 2 else 4 end from t0
[result]
SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select distinct * from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
    AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select distinct *,v1 from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
    AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
with t0 as (select * from t1) select * from test.t0
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
with t0 as (select * from t1) select * from t0
[result]
SCAN (columns[1: v4, 2: v5, 3: v6] predicate[null])
[end]

[sql]
select count(*) from (select v1 from t0 order by v2 limit 10,20) t
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count()=count()}] group by [[]] having [null]
    TOP-N (order by [[2: v2 ASC NULLS FIRST]])
        TOP-N (order by [[2: v2 ASC NULLS FIRST]])
            SCAN (columns[2: v2] predicate[null])
[end]

[sql]
select * from tview;
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from tview, t1;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select v1,v4 from tview, t1;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select cast(v1 as varchar) from t0 group by cast(v1 as varchar)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: cast]] having [null]
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[4: cast]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select cast(v1 as varchar) + 1 from t0 group by cast(v1 as varchar)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: cast]] having [null]
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[4: cast]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select * from t0 where v1=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 1: v1])
[end]

[sql]
select * from t0_not_null where abs(v1)=abs(v1)
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[abs(1: v1) = abs(1: v1)])
[end]

[sql]
select * from t0_not_null where v1=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[true])
[end]

[sql]
select * from t0_not_null where v1>=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[true])
[end]

[sql]
select * from t0_not_null where v1<=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[true])

[end]

[sql]
select * from t0_not_null where v1<=>v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[true])
[end]

[sql]
select * from t0_not_null where v1>v1
[result]
VALUES
[end]

[sql]
select * from t0_not_null where v1<v1
[result]
VALUES
[end]

[sql]
select * from t0_not_null where v1!=v1
[result]
VALUES
[end]

[sql]
select * from t0 where v1<=>v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[true])
[end]