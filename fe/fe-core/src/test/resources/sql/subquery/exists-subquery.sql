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
    AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count)}] group by [[]] having [8: count = 0]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{8: count=count(1)}] group by [[]] having [null]
                SCAN (columns[4: v10] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[1: v1] predicate[null])
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
    AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count)}] group by [[]] having [8: count = 0]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{8: count=count(1)}] group by [[]] having [null]
                SCAN (columns[4: v10] predicate[4: v10 = 123])
    EXCHANGE BROADCAST
        SCAN (columns[1: v1] predicate[null])
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
    AGGREGATE ([GLOBAL] aggregate [{9: count=count(9: count)}] group by [[]] having [9: count = 0]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{9: count=count(1)}] group by [[]] having [null]
                SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 7: v6])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
            EXCHANGE SHUFFLE[3]
                AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                    SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having not exists (select max(v4) from t1 where v5 = 5 group by v6);
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{10: count=count(10: count)}] group by [[]] having [10: count = 0]
        EXCHANGE GATHER
            AGGREGATE ([LOCAL] aggregate [{10: count=count(1)}] group by [[]] having [null]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[7: v6]] having [null]
                    EXCHANGE SHUFFLE[7]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[7: v6]] having [null]
                            SCAN (columns[6: v5, 7: v6] predicate[6: v5 = 5])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
            EXCHANGE SHUFFLE[3]
                AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                    SCAN (columns[1: v1, 3: v3] predicate[null])
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
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[8: v4]] having [null]
            AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[8: v4]] having [null]
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
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[8: v4]] having [null]
            AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[8: v4]] having [null]
                SCAN (columns[8: v4] predicate[null])
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or exists (select v6 from t1 where v5 = v1)) and not exists (select v5 from t1 where v4 = v2);
[result]
LEFT ANTI JOIN (join-predicate [2: v2 = 8: v4] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [3: v3 = 3 OR 12: countRows IS NOT NULL])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows)}] group by [[5: v5]] having [null]
                EXCHANGE SHUFFLE[5]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1)}] group by [[5: v5]] having [null]
                        SCAN (columns[5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[8: v4] predicate[null])
[end]

[sql]
select * from t0 where exists (select * from (values(2),(3)) t)
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        VALUES (2),(3)

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
    AGGREGATE ([GLOBAL] aggregate [{6: count=count(6: count)}] group by [[]] having [6: count = 0]
        AGGREGATE ([LOCAL] aggregate [{6: count=count(1)}] group by [[]] having [null]
            VALUES (2),(3)
    EXCHANGE BROADCAST
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
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
