[sql]
select * from (values (1,2,3), (4,5,6)) v;
[result]
VALUES (1,2,3),(4,5,6)
[end]

[sql]
select * from (values (1,2,3), (4,'a',6), (7,8,9)) v;
[result]
VALUES (1,2,3),(4,a,6),(7,8,9)
[end]

[sql]
select 1 from (select * from t0 where false) a;
[result]
VALUES
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: expr
PARTITION: UNPARTITIONED

RESULT SINK

0:EMPTYSET
[end]

[sql]
select * from t0 where v1 in (1.1, 2, null)
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[cast(1: v1 as decimal128(20, 1)) IN (1.1, 2, null)])
[end]

[sql]
select null/null;
[result]
VALUES (null)
[end]

[sql]
SELECT TIMEDIFF("1969-12-23 21:53:55", "1969-12-18")  FROM t0
[result]
SCAN (columns[1: v1] predicate[null])
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:4: timediff
PARTITION: UNPARTITIONED

RESULT SINK

2:EXCHANGE

PLAN FRAGMENT 1
OUTPUT EXPRS:
PARTITION: RANDOM

STREAM DATA SINK
EXCHANGE ID: 02
UNPARTITIONED

1:Project
|  <slot 4> : 510835.0
|
0:OlapScanNode
TABLE: t0
PREAGGREGATION: ON
partitions=1/1
rollup: t0
tabletRatio=3/3
tabletList=10006,10008,10010
cardinality=1
avgRowSize=9.0
numNodes=0
[end]

[sql]
select (CASE  WHEN false THEN NULL END)*(NULL) from t0
[result]
SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select (null * null) is null from t0
[result]
SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1 from t0 where (null * null) is null
[result]
SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1 from t0 where (null * null) is not null
[result]
VALUES
[end]

[sql]
select * from (values (1,2,3), (4,'a',6), (7,8,9)) v limit 2
[result]
VALUES (1,2,3),(4,a,6),(7,8,9)
[fragment]
PLAN FRAGMENT 0
OUTPUT EXPRS:1: column_0 | 2: column_1 | 3: column_2
PARTITION: UNPARTITIONED

RESULT SINK

0:UNION
constant exprs:
1 | '2' | 3
4 | 'a' | 6
7 | '8' | 9
limit: 2
[end]

[sql]
select 1 from (values(1,2,3),(4,5,6)) t;
[result]
VALUES (1),(4)
[end]

[sql]
select t1d from test_all_type where id_datetime = cast(cast('2020-01-1' as date) as datetime)
[result]
SCAN (columns[4: t1d, 8: id_datetime] predicate[8: id_datetime = 2020-01-01 00:00:00])
[end]

[sql]
select t1d from test_all_type where id_date = cast(cast('2020-01-01' as datetime) as date)
[result]
SCAN (columns[4: t1d, 9: id_date] predicate[9: id_date = 2020-01-01])
[end]

[sql]
select t1d from test_all_type where id_date = date('2020-01-1 01:02:03')
[result]
SCAN (columns[4: t1d, 9: id_date] predicate[9: id_date = 2020-01-01])
[end]

[sql]
select v1 from t0 where ((NULL) - (NULL)) <=> ((NULL) % (NULL))
[result]
SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1 from t0 where ((NULL) - (NULL)) <=> 1
[result]
VALUES
[end]

[sql]
select v1 from t0 where ((NULL) - (NULL)) > ((NULL) % (NULL))
[result]
VALUES
[end]

[sql]
select v1 from (select * from (select v1,sum(v2) from t0 group by v1  union all select null as v1,null ) t) temp where v1 = 1
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1]] having [null]
    AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1] predicate[1: v1 = 1])
[end]
