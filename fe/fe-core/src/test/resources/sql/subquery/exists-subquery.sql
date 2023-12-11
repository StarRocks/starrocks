[sql]
select t0.v1 from t0 where exists (select t3.v11 from t3)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        EXCHANGE GATHER
            SCAN (columns[4: v10] predicate[null]) Limit 1
[end]

[sql]
select t0.v1 from t0 where exists (select t3.v11 from t3 where t0.v3 = t3.v12)
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[6: v12] predicate[6: v12 IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where not exists (select t3.v11 from t3)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count)}] group by [[]] having [8: count = 0]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{8: count=count(1)}] group by [[]] having [null]
                    SCAN (columns[4: v10] predicate[null])
[end]

[sql]
select t0.v1 from t0 where not exists (select t3.v11 from t3 where t0.v3 = t3.v12)
[result]
LEFT ANTI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where exists (select t3.v11 from t3 where t0.v3 = t3.v12) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[6: v12] predicate[6: v12 IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where not exists (select t3.v11 from t3 where t0.v3 = t3.v12) and t0.v2 = 3
[result]
LEFT ANTI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where exists (select t3.v11 from t3 where t0.v3 = t3.v12 and t3.v10 = 123) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 6: v12] predicate[4: v10 = 123])
[end]

[sql]
select t0.v1 from t0 where not exists (select t3.v11 from t3 where t0.v3 = t3.v12 and t3.v10 = 123) and t0.v2 = 3
[result]
LEFT ANTI JOIN (join-predicate [3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 6: v12] predicate[4: v10 = 123])
[end]

[sql]
select t0.v1 from t0 where exists (select t3.v11 from t3 where t3.v10 = 123)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        EXCHANGE GATHER
            SCAN (columns[4: v10] predicate[4: v10 = 123]) Limit 1
[end]

[sql]
select t0.v1 from t0 where not exists (select t3.v11 from t3 where t3.v10 = 123)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count)}] group by [[]] having [8: count = 0]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{8: count=count(1)}] group by [[]] having [null]
                    SCAN (columns[4: v10] predicate[4: v10 = 123])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having exists (select v4 from t1 where v5 = v2);
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE SHUFFLE[6]
        SCAN (columns[6: v5] predicate[6: v5 IS NOT NULL])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having exists (select v4 from t1 where v5 = v2 and v2 < v6);
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 6: v5 AND 2: v2 < 7: v6] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE SHUFFLE[6]
        SCAN (columns[6: v5, 7: v6] predicate[6: v5 IS NOT NULL])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having exists (select v4 from t1 where v5 = v6);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        EXCHANGE GATHER
            SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 7: v6]) Limit 1
[end]

[sql]
select v3, min(v1) from t0 group by v3 having exists (select max(v4) from t1 where v5 = 5 group by v6);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        EXCHANGE GATHER
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[7: v6]] having [null]
                EXCHANGE SHUFFLE[7]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[7: v6]] having [null]
                        SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having not exists (select v4 from t1 where v5 = v2);
[result]
LEFT ANTI JOIN (join-predicate [2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE SHUFFLE[6]
        SCAN (columns[6: v5] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having not exists (select v4 from t1 where v5 = v6);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{9: count=count(9: count)}] group by [[]] having [9: count = 0]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{9: count=count(1)}] group by [[]] having [null]
                    SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 7: v6])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having not exists (select max(v4) from t1 where v5 = 5 group by v6);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{10: count=count(10: count)}] group by [[]] having [10: count = 0]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{10: count=count(1)}] group by [[]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{}] group by [[7: v6]] having [null]
                        EXCHANGE SHUFFLE[7]
                            AGGREGATE ([LOCAL] aggregate [{}] group by [[7: v6]] having [null]
                                SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v3, v1 from t0 where exists (select v6, sum(v4) from t1 where v5 = v1 group by v6);
[result]
LEFT SEMI JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[5: v5 IS NOT NULL])
[end]

[sql]
select v3, v1 from t0 where exists (select v6 from t1 limit 3);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        EXCHANGE GATHER
            SCAN (columns[4: v4] predicate[null]) Limit 1
[end]

[sql]
select v3, v1 from t0 where exists (select v6, sum(v4) from t1 where v5 = v1 group by v6 having count(*) > 1);
[result]
LEFT SEMI JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count)}] group by [[5: v5, 6: v6]] having [8: count > 1]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{8: count=count()}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or exists (select v6 from t1 where v5 = v1);
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 8: countRows IS NOT NULL])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{8: countRows=count(8: countRows)}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{8: countRows=count(1)}] group by [[5: v5]] having [null]
                    SCAN (columns[5: v5] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or exists (select v6 from t1 where v5 = v1) or t0.v1 = 4;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 8: countRows IS NOT NULL OR 1: v1 = 4])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{8: countRows=count(8: countRows)}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{8: countRows=count(1)}] group by [[5: v5]] having [null]
                    SCAN (columns[5: v5] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or (exists (select v6 from t1 where v5 = v1) and t0.v1 = 4);
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 8: countRows IS NOT NULL AND 1: v1 = 4])
    SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{8: countRows=count(8: countRows)}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{8: countRows=count(1)}] group by [[5: v5]] having [null]
                    SCAN (columns[5: v5] predicate[null])
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or not exists (select v6 from t1 where v5 = v1)) and t0.v1 = 4;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 8: countRows IS NULL])
    SCAN (columns[1: v1, 3: v3] predicate[1: v1 = 4])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{8: countRows=count(8: countRows)}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{8: countRows=count(1)}] group by [[5: v5]] having [null]
                    SCAN (columns[5: v5] predicate[5: v5 = 4])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or exists (select v6 from t1 where v5 = v1) or not exists (select v5 from t1 where v4 = v2);
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 8: v4] post-join-predicate [3: v3 = 3 OR 7: expr OR 12: countRows IS NULL])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows)}] group by [[5: v5]] having [null]
                EXCHANGE SHUFFLE[5]
                    AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1)}] group by [[5: v5]] having [null]
                        SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(1)}] group by [[8: v4]] having [null]
            SCAN (columns[8: v4] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or exists (select v6 from t1 where v5 = v1) and not exists (select v5 from t1 where v4 = v2);
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 8: v4] post-join-predicate [3: v3 = 3 OR 7: expr AND 12: countRows IS NULL])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows)}] group by [[5: v5]] having [null]
                EXCHANGE SHUFFLE[5]
                    AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1)}] group by [[5: v5]] having [null]
                        SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(1)}] group by [[8: v4]] having [null]
            SCAN (columns[8: v4] predicate[null])
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or exists (select v6 from t1 where v5 = v1)) and not exists (select v5 from t1 where v4 = v2);
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 12: countRows IS NOT NULL])
    LEFT ANTI JOIN (join-predicate [2: v2 = 8: v4] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[8: v4] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[5: v5]] having [null]
            EXCHANGE SHUFFLE[5]
                AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[5: v5]] having [null]
                    SCAN (columns[5: v5] predicate[null])
[end]

[sql]
select * from t0 where exists (select * from (values(1,2),(3,4)) t)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        VALUES (1),(3)
[end]

[sql]
select * from t0 where not exists (select * from (values(2),(3)) t)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{6: count=count(1)}] group by [[]] having [6: count = 0]
            VALUES (2),(3)
[end]

[sql]
select * from t0 where (v3 > 3) = (exists (select v7 from t2 where t0.v2 = t2.v8))
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 5: v8] post-join-predicate [3: v3 > 3 = 8: countRows IS NOT NULL])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    AGGREGATE ([GLOBAL] aggregate [{8: countRows=count(8: countRows)}] group by [[5: v8]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{8: countRows=count(1)}] group by [[5: v8]] having [null]
                SCAN (columns[5: v8] predicate[null])
[end]

[sql]
select case (exists (select v7 from t2 where t0.v2 = t2.v8)) when TRUE then 1 when FALSE then 2 end from t0;
[result]
RIGHT OUTER JOIN (join-predicate [5: v8 = 2: v2] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{9: countRows=count(9: countRows)}] group by [[5: v8]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{9: countRows=count(1)}] group by [[5: v8]] having [null]
                SCAN (columns[5: v8] predicate[null])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[2: v2] predicate[null])
[end]

[sql]
select not (exists (select v7 from t2 where t0.v2 = t2.v8)) from t0;
[result]
RIGHT OUTER JOIN (join-predicate [5: v8 = 2: v2] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{9: countRows=count(9: countRows)}] group by [[5: v8]] having [null]
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([LOCAL] aggregate [{9: countRows=count(1)}] group by [[5: v8]] having [null]
                SCAN (columns[5: v8] predicate[null])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[2: v2] predicate[null])
[end]

/* test ExistentialApply2OuterJoinRule */

[sql]
select v1, exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6) from t0;
[result]
RIGHT OUTER JOIN (join-predicate [4: v4 = 1: v1 AND 1: v1 = 1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{9: countRows=count(1)}] group by [[4: v4, 5: v5, 6: v6]] having [null]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1, exists (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6) from t0;
[result]
RIGHT OUTER JOIN (join-predicate [abs(add(1: v1, 4: v4)) = cast(add(1: v1, 5: v5) as largeint(40)) AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    EXCHANGE GATHER
        AGGREGATE ([GLOBAL] aggregate [{9: countRows=count(1)}] group by [[4: v4, 5: v5, 6: v6]] having [null]
            SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE GATHER
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1, exists (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [add(18: cast, cast(1: v1 as double)) = cast(add(4: v4, 19: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4, 5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{20: countRows=count(20: countRows)}] group by [[18: cast, 19: cast]] having [null]
            EXCHANGE SHUFFLE[18, 19]
                AGGREGATE ([LOCAL] aggregate [{20: countRows=count(1)}] group by [[18: cast, 19: cast]] having [null]
                    SCAN (columns[7: t1a, 9: t1c] predicate[null])
[end]

[sql]
select t0.v1, exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5)
from t0 left join t1 on true;
[result]
LEFT OUTER JOIN (join-predicate [21: add = 18: cast AND add(19: add, 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{20: countRows=count(20: countRows)}] group by [[18: cast, 19: add]] having [null]
            EXCHANGE SHUFFLE[18, 19]
                AGGREGATE ([LOCAL] aggregate [{20: countRows=count(1)}] group by [[18: cast, 19: add]] having [null]
                    SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select t0.v1, exists (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6) from  t0 left join t1 on t0.v3 = t1.
v6;
[result]
LEFT OUTER JOIN (join-predicate [add(19: cast, 3: v3) = subtract(10: t1d, 6: v6)] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
        SCAN (columns[1: v1, 3: v3] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[6: v6] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{20: countRows=count(20: countRows)}] group by [[19: cast, 10: t1d]] having [null]
            EXCHANGE SHUFFLE[19, 10]
                AGGREGATE ([LOCAL] aggregate [{20: countRows=count(1)}] group by [[19: cast, 10: t1d]] having [null]
                    SCAN (columns[9: t1c, 10: t1d] predicate[null])
[end]

[sql]
select v1, not exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6) from t0;
[result]
RIGHT OUTER JOIN (join-predicate [4: v4 = 1: v1 AND 1: v1 = 1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{10: countRows=count(1)}] group by [[4: v4, 5: v5, 6: v6]] having [null]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1, not exists (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6) from t0;
[result]
RIGHT OUTER JOIN (join-predicate [abs(add(1: v1, 4: v4)) = cast(add(1: v1, 5: v5) as largeint(40)) AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    EXCHANGE GATHER
        AGGREGATE ([GLOBAL] aggregate [{10: countRows=count(1)}] group by [[4: v4, 5: v5, 6: v6]] having [null]
            SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE GATHER
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1, not exists (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [add(19: cast, cast(1: v1 as double)) = cast(add(4: v4, 20: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4, 5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{21: countRows=count(21: countRows)}] group by [[19: cast, 20: cast]] having [null]
            EXCHANGE SHUFFLE[19, 20]
                AGGREGATE ([LOCAL] aggregate [{21: countRows=count(1)}] group by [[19: cast, 20: cast]] having [null]
                    SCAN (columns[7: t1a, 9: t1c] predicate[null])
[end]

[sql]
select t0.v1, not exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5) from t0 left join t1 on true;
[result]
LEFT OUTER JOIN (join-predicate [22: add = 19: cast AND add(20: add, 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{21: countRows=count(21: countRows)}] group by [[19: cast, 20: add]] having [null]
            EXCHANGE SHUFFLE[19, 20]
                AGGREGATE ([LOCAL] aggregate [{21: countRows=count(1)}] group by [[19: cast, 20: add]] having [null]
                    SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select t0.v1, not exists (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6) from  t0 left join t1 on t0.v3 = t1.v6;
[result]
LEFT OUTER JOIN (join-predicate [add(20: cast, 3: v3) = subtract(10: t1d, 6: v6)] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
        SCAN (columns[1: v1, 3: v3] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[6: v6] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{21: countRows=count(21: countRows)}] group by [[20: cast, 10: t1d]] having [null]
            EXCHANGE SHUFFLE[20, 10]
                AGGREGATE ([LOCAL] aggregate [{21: countRows=count(1)}] group by [[20: cast, 10: t1d]] having [null]
                    SCAN (columns[9: t1c, 10: t1d] predicate[null])
[end]

[sql]
select v1 from t0 where exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
RIGHT SEMI JOIN (join-predicate [4: v4 = 1: v1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 1])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
LEFT SEMI JOIN (join-predicate [19: add = 18: cast AND add(add(18: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select v1 from t0 where not exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
RIGHT ANTI JOIN (join-predicate [1: v1 = 1 AND 4: v4 = 1: v1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where not exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
LEFT ANTI JOIN (join-predicate [19: add = 18: cast AND add(add(18: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

/* test ExistentialApply2JoinRule */

[sql]
select v1 from t0 where exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
RIGHT SEMI JOIN (join-predicate [4: v4 = 1: v1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 1])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
LEFT SEMI JOIN (join-predicate [19: add = 18: cast AND add(add(18: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select v1 from t0 where not exists (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
RIGHT ANTI JOIN (join-predicate [1: v1 = 1 AND 4: v4 = 1: v1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where not exists (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
LEFT ANTI JOIN (join-predicate [19: add = 18: cast AND add(add(18: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select t0.v1 from t0, t1 where exists (select v7 from t2 where t0.v1 = 1 and t1.v4 = 2) = true;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[1: v1 = 1])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[4: v4 = 2])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [11: countRows IS NOT NULL = true]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1 from t0, t1 where exists (select v7 from t2 where t0.v1 = t1.v4) = true;
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[1: v1 IS NOT NULL])
        EXCHANGE SHUFFLE[4]
            SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [11: countRows IS NOT NULL = true]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1 from t0, t1 where not exists (select v7 from t2 where t0.v1 = 1 and t1.v4 = 2) = true;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 1 AND 4: v4 = 2] post-join-predicate [11: countRows IS NOT NULL != true])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1 from t0, t1 where not exists (select v7 from t2 where t0.v1 = t1.v4) = true;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [11: countRows IS NOT NULL != true])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1, exists (select v7 from t2 where t0.v1 = 1 and t1.v4 = 2) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 1 AND 4: v4 = 2] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1, exists (select v7 from t2 where t0.v1 = t1.v4) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{11: countRows=count(11: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{11: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1, not exists (select v7 from t2 where t0.v1 = 1 and t1.v4 = 2) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 1 AND 4: v4 = 2] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]

[sql]
select t0.v1, not exists (select v7 from t2 where t0.v1 = t1.v4) from t0, t1;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[]] having [null]
                    SCAN (columns[7: v7] predicate[null])
[end]