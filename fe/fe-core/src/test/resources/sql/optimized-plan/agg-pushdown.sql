[sql]
select sum(v1) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(1: v1)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(v2), max(v5) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(2: v2), 8: max=max(5: v5)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(v2), max(v5) from t0 left outer join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(2: v2), 8: max=max(5: v5)}] group by [[]] having [null]
    EXCHANGE GATHER
        LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select sum(v2), max(v5) from t0 left outer join t1 on t0.v1 = t1.v4 group by t0.v3;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(2: v2), 8: max=max(5: v5)}] group by [[3: v3]] having [null]
    EXCHANGE SHUFFLE[3]
        LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select sum(v2), max(v5) from t0 left outer join t1 on t0.v1 = t1.v4 group by t0.v3, t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(2: v2), 8: max=max(5: v5)}] group by [[3: v3, 6: v6]] having [null]
    EXCHANGE SHUFFLE[3, 6]
        LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select stddev(v2), max(v5) from t0 left outer join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: stddev=stddev(2: v2), 8: max=max(5: v5)}] group by [[]] having [null]
    EXCHANGE GATHER
        LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select stddev(v2), stddev(v5) from t0 left outer join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: stddev=stddev(2: v2), 8: stddev=stddev(5: v5)}] group by [[]] having [null]
    EXCHANGE GATHER
        LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select sum(distinct v2) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(distinct 2: v2)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(v2) from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{8: sum=sum(9: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(2: v2)}] group by [[1: v1, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2 + v5) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{8: max=max(7: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2), sum(2) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: max=max(2: v2), 8: sum=sum(2)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2), sum(2) from t0 join t1 on t0.v1 = t1.v4 group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: max=max(9: max), 8: sum=sum(10: sum)}] group by [[1: v1]] having [null]
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{9: max=max(2: v2), 10: sum=sum(11: expr)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2 + 3), sum(v2 + 3) from t0 join t1 on t0.v1 = t1.v4 group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(10: sum), 8: max=max(11: max)}] group by [[1: v1]] having [null]
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{10: sum=sum(12: add), 11: max=max(13: add)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2 + 3), sum(v2 + 3) from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: max=max(11: max), 10: sum=sum(12: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{11: max=max(13: add), 12: sum=sum(14: add)}] group by [[1: v1, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(v2 + v3), sum(v2 + 3) from t0 join t1 on t0.v1 = t1.v4 group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: max=max(11: max), 10: sum=sum(12: sum)}] group by [[1: v1]] having [null]
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{11: max=max(13: add), 12: sum=sum(14: add)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(case when t1.v4 = 3 then t0.v2
                when t1.v5 = 5 then t0.v3
                else t0.v1 end), sum(v2 + 3)
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{10: max=max(12: max), 11: sum=sum(13: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(17: add), 14: max=max(2: v2), 15: max=max(3: v3), 16: max=max(1: v1)}] group by [[1: v1, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(case when t1.v4 = 3 then t0.v2
                when t1.v5 = 5 then t0.v3
                else t1.v6 end), sum(v2 + 3)
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{10: max=max(8: case), 11: sum=sum(9: expr)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(case when t1.v4 = t0.v2 then t0.v2
                when t1.v5 = 5 then t0.v3
                else t0.v1 end), sum(v2 + 3)
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{10: max=max(12: max), 11: sum=sum(13: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(17: add), 14: max=max(2: v2), 15: max=max(3: v3), 16: max=max(1: v1)}] group by [[1: v1, 2: v2, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select max(case when t1.v4 = t1.v6 then t0.v2
                when t1.v5 = 5 then t0.v3
                else t1.v6 end), sum(v2 + 3)
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{10: max=max(8: case), 11: sum=sum(9: expr)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(if(t1.v4 = t1.v6, t0.v2, t0.v3))
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(10: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{11: sum=sum(2: v2), 12: sum=sum(3: v3)}] group by [[1: v1, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(if(t1.v4 = t1.v6, t0.v2, t0.v3))
from t0 join t1 on t0.v1 = t1.v4
where t0.v2 + t1.v5 = 3
group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(10: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4 AND add(2: v2, 5: v5) = 3] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{11: sum=sum(2: v2), 12: sum=sum(3: v3)}] group by [[1: v1, 2: v2, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(if(t1.v4 = t0.v1, t0.v2, t0.v3))
from t0 join t1 on t0.v1 = t1.v4 group by t0.v3 + t1.v6;
[result]
AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(10: sum)}] group by [[7: expr]] having [null]
    EXCHANGE SHUFFLE[7]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{11: sum=sum(2: v2), 12: sum=sum(3: v3)}] group by [[1: v1, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4, 6: v6] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(v2) from (select * from t0 union all select * from t1) x group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{10: sum=sum(11: sum)}] group by [[7: v1]] having [null]
    EXCHANGE SHUFFLE[7]
        UNION
            AGGREGATE ([GLOBAL] aggregate [{12: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
            AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(5: v5)}] group by [[4: v4]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select sum(v2) from (select t0.* from t0 join t1 on t0.v1 = t1.v4 union all select * from t1) x group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(14: sum)}] group by [[10: v1]] having [null]
    EXCHANGE SHUFFLE[10]
        UNION
            INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{15: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
                EXCHANGE SHUFFLE[4]
                    SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
            AGGREGATE ([GLOBAL] aggregate [{16: sum=sum(8: v5)}] group by [[7: v4]] having [null]
                SCAN (columns[7: v4, 8: v5] predicate[null])
[end]

[sql]
select sum(v2) from (
    select t0.* from t0 join t1 on t0.v1 = t1.v4 where abs(t0.v2) = 1 union all select * from t1
) x group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(14: sum)}] group by [[10: v1]] having [null]
    EXCHANGE SHUFFLE[10]
        UNION
            INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{15: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[abs(2: v2) = 1])
                EXCHANGE SHUFFLE[4]
                    SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
            AGGREGATE ([GLOBAL] aggregate [{16: sum=sum(8: v5)}] group by [[7: v4]] having [null]
                SCAN (columns[7: v4, 8: v5] predicate[null])
[end]

[sql]
select sum(v2) from
(
    select t0.* from t0 join t1 on t0.v1 = t1.v4 union all select * from t1
) x
where abs(x.v2) = 2;
[result]
AGGREGATE ([GLOBAL] aggregate [{13: sum=sum(11: v2)}] group by [[]] having [null]
    EXCHANGE GATHER
        UNION
            INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                SCAN (columns[1: v1, 2: v2] predicate[abs(2: v2) = 2])
                EXCHANGE SHUFFLE[4]
                    SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
            SCAN (columns[8: v5] predicate[abs(8: v5) = 2])
[end]

[sql]
select sum(v2) from
(
    select t0.* from t0 join t1 on t0.v1 = t1.v4
    union all
    select t2.* from t1 join t2 on t1.v5 = t2.v8
) x
where abs(x.v2) = 2
group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{16: sum=sum(17: sum)}] group by [[13: v1]] having [null]
    EXCHANGE SHUFFLE[13]
        UNION
            INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{18: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[abs(2: v2) = 2])
                EXCHANGE SHUFFLE[4]
                    SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
            INNER JOIN (join-predicate [11: v8 = 8: v5] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(11: v8)}] group by [[10: v7, 11: v8]] having [null]
                    SCAN (columns[10: v7, 11: v8] predicate[abs(11: v8) = 2])
                EXCHANGE BROADCAST
                    SCAN (columns[8: v5] predicate[abs(8: v5) = 2])
[end]

[sql]
select sum(v2) from
(
    select t0.* from t0 join t1 on t0.v1 = t1.v4
    union all
    select t2.* from t1 join t2 on t1.v5 = t2.v8
) x
where abs(x.v2) = 2
group by x.v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{16: sum=sum(17: sum)}] group by [[13: v1]] having [null]
    EXCHANGE SHUFFLE[13]
        UNION
            INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{18: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[abs(2: v2) = 2])
                EXCHANGE SHUFFLE[4]
                    SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
            INNER JOIN (join-predicate [11: v8 = 8: v5] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(11: v8)}] group by [[10: v7, 11: v8]] having [null]
                    SCAN (columns[10: v7, 11: v8] predicate[abs(11: v8) = 2])
                EXCHANGE BROADCAST
                    SCAN (columns[8: v5] predicate[abs(8: v5) = 2])
[end]

[sql]
select sum(v2) from
(
    select * from (select * from t0 union all select * from t1) c0
    union all
    select * from (select * from t1 union all select * from t2) c1
) x
group by v1
[result]
AGGREGATE ([GLOBAL] aggregate [{22: sum=sum(23: sum)}] group by [[19: v1]] having [null]
    EXCHANGE SHUFFLE[19]
        UNION
            UNION
                AGGREGATE ([GLOBAL] aggregate [{26: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(5: v5)}] group by [[4: v4]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
            UNION
                AGGREGATE ([GLOBAL] aggregate [{28: sum=sum(11: v5)}] group by [[10: v4]] having [null]
                    SCAN (columns[10: v4, 11: v5] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{29: sum=sum(14: v8)}] group by [[13: v7]] having [null]
                    SCAN (columns[13: v7, 14: v8] predicate[null])
[end]

[sql]
select sum(v2) from
(
    select * from (select * from t0 union all select * from t1) c0
    union all
    select * from (select * from t1 union all select * from t2) c1
) x
group by v1
[result]
AGGREGATE ([GLOBAL] aggregate [{22: sum=sum(23: sum)}] group by [[19: v1]] having [null]
    EXCHANGE SHUFFLE[19]
        UNION
            UNION
                AGGREGATE ([GLOBAL] aggregate [{26: sum=sum(2: v2)}] group by [[1: v1]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(5: v5)}] group by [[4: v4]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[null])
            UNION
                AGGREGATE ([GLOBAL] aggregate [{28: sum=sum(11: v5)}] group by [[10: v4]] having [null]
                    SCAN (columns[10: v4, 11: v5] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{29: sum=sum(14: v8)}] group by [[13: v7]] having [null]
                    SCAN (columns[13: v7, 14: v8] predicate[null])
[end]

[sql]
select sum(v2) from
(
    select * from (select * from t0 union all select * from t1) c0
    join (select * from t1 union all select * from t2) c1 on c0.v2 = c1.v5
) x
where x.v2 + x.v5 = 1
group by x.v2
[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(20: sum)}] group by [[8: v2]] having [null]
    EXCHANGE SHUFFLE[8]
        INNER JOIN (join-predicate [8: v2 = 17: v5 AND add(8: v2, 17: v5) = 1] post-join-predicate [null])
            UNION
                AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(2: v2)}] group by [[2: v2]] having [null]
                    EXCHANGE SHUFFLE[2]
                        SCAN (columns[2: v2] predicate[2: v2 IS NOT NULL])
                AGGREGATE ([GLOBAL] aggregate [{22: sum=sum(5: v5)}] group by [[5: v5]] having [null]
                    EXCHANGE SHUFFLE[5]
                        SCAN (columns[5: v5] predicate[5: v5 IS NOT NULL])
            EXCHANGE BROADCAST
                UNION
                    SCAN (columns[11: v5] predicate[11: v5 IS NOT NULL])
                    SCAN (columns[14: v8] predicate[14: v8 IS NOT NULL])
[end]

[sql]
select sum(v2) from
(
    select * from (select * from t0 union all select * from t1) c0
    join (select * from t1 union all select * from t2) c1 on c0.v2 = c1.v5
    order by c0.v1 limit 10
) x
[result]
AGGREGATE ([GLOBAL] aggregate [{19: sum=sum(8: v2)}] group by [[]] having [null]
    TOP-N (order by [[7: v1 ASC NULLS FIRST]])
        TOP-N (order by [[7: v1 ASC NULLS FIRST]])
            INNER JOIN (join-predicate [8: v2 = 17: v5] post-join-predicate [null])
                UNION
                    SCAN (columns[1: v1, 2: v2] predicate[2: v2 IS NOT NULL])
                    SCAN (columns[4: v4, 5: v5] predicate[5: v5 IS NOT NULL])
                EXCHANGE BROADCAST
                    UNION
                        SCAN (columns[11: v5] predicate[11: v5 IS NOT NULL])
                        SCAN (columns[14: v8] predicate[14: v8 IS NOT NULL])
[end]

[sql]
select sum(v2), sum(vv) from
(
    select v1, v2, sum(v3) over (partition by v2) vv from t0
) x
[result]
AGGREGATE ([GLOBAL] aggregate [{5: sum=sum(2: v2), 6: sum=sum(4: sum(3: v3))}] group by [[]] having [null]
    EXCHANGE GATHER
        ANALYTIC ({4: sum(3: v3)=sum(3: v3)} [2: v2] [] )
            TOP-N (order by [[2: v2 ASC NULLS FIRST]])
                EXCHANGE SHUFFLE[2]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v2) from
(
    select v1, v2, sum(v3) from t0 group by grouping sets((v1, v2))
) x
[result]
AGGREGATE ([GLOBAL] aggregate [{6: sum=sum(2: v2)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 5: GROUPING_ID]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 5: GROUPING_ID]] having [null]
                REPEAT [[1: v1, 2: v2]]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select sum(v2) from t0, t1 group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(10: sum)}] group by [[1: v1]] having [null]
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{10: sum=sum(2: v2)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select sum(distinct v1) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(distinct 1: v1)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select avg(v1) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: avg=avg(1: v1)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select t0.v2, t1.v5 from t0 join t1 on t0.v1 = t1.v4 group by t0.v2, t1.v5;
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[2: v2, 5: v5]] having [null]
    EXCHANGE SHUFFLE[2, 5]
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v4, 5: v5]] having [null]
                    SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(1) from t0 join t1 on t0.v1 = t1.v4;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(1)}] group by [[]] having [null]
    EXCHANGE GATHER
        INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
            SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])
            EXCHANGE SHUFFLE[4]
                SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select sum(t0.v2), sum(1) from t0 join t1 on t0.v1 = t1.v4 group by v1;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: sum=sum(9: sum), 8: sum=sum(10: sum)}] group by [[1: v1]] having [null]
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{9: sum=sum(2: v2), 10: sum=sum(11: expr)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]