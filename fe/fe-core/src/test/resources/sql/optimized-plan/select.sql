[sql]
select 1,2 from t0
[result]
SCAN (columns[1: v1] predicate[null])
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
[end]

[sql]
select sum(v1) from t0 limit 0
[result]
VALUES
[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:4: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  0:EMPTYSET
[end]

[sql]
select sum(a) from (select v1 as a from t0 limit 0) t
[result]
AGGREGATE ([GLOBAL] aggregate [{4: sum=sum(1: v1)}] group by [[]] having [null]
    VALUES
[fragment]
PLAN FRAGMENT 0
 OUTPUT EXPRS:4: sum
  PARTITION: UNPARTITIONED

  RESULT SINK

  1:AGGREGATE (update finalize)
  |  output: sum(1: v1)
  |  group by:
  |
  0:EMPTYSET
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
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select distinct *,v1 from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
with t0 as (select * from t1) select * from test.t0
[result]
SCAN (columns[4: v1, 5: v2, 6: v3] predicate[null])
[end]

[sql]
with t0 as (select * from t1) select * from t0
[result]
SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select count(*) from (select v1 from t0 order by v2 limit 10,20) t
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count=count()}] group by [[]] having [null]
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
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from t0_not_null where v1>=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from t0_not_null where v1<=v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from t0_not_null where v1<=>v1
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
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
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]