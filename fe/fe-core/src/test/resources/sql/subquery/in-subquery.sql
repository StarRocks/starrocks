[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3 where t0.v3 = t3.v3)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 = 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3 where t0.v3 > t3.v3)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 > 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 = t3.v3)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 = 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 < t3.v3)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 < 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 = t3.v3) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 = 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3 where t0.v3 > t3.v3) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 > 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[5: v2 = 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 > t3.v3) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 > 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 > t3.v3) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 > 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v2, 6: v3] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where t0.v3 = t3.v3 and t3.v1 > 2) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 = 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v1, 5: v2, 6: v3] predicate[4: v1 > 2])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3 where t0.v3 = t3.v3 and t3.v1 > 2) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2 AND 3: v3 = 6: v3] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v1, 5: v2, 6: v3] predicate[4: v1 > 2 AND 5: v2 = 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v2 from t3 where  t3.v1 > 2) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v2] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v1, 5: v2] predicate[4: v1 > 2])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v2 from t3 where t3.v1 > 2) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v2] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v1, 5: v2] predicate[5: v2 = 3 AND 4: v1 > 2])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) in (select v4 from t1 where v5 = v2);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 4: min(1: v1) AND 6: v5 = 2: v2] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) in (select v4 from t1 where v5 = v2 and v2 < v6);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 4: min(1: v1) AND 6: v5 = 2: v2 AND 2: v2 < 7: v6] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having v2 in (select v4 from t1 where v5 = v2);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 2: v2 AND 6: v5 = 2: v2] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5] predicate[null])
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 in (select v4 from t1 where v5 = v6);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 3: v3] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 7: v6])
    EXCHANGE SHUFFLE[3]
        AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[3: v3]] having [null]
            EXCHANGE SHUFFLE[3]
                AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[3: v3]] having [null]
                    SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 in (select max(v4) from t1 where v5 = 5 group by v6);
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 8: max(5: v4)] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: max(5: v4)=max(8: max(5: v4))}] group by [[7: v6]] having [null]
            EXCHANGE SHUFFLE[7]
                AGGREGATE ([LOCAL] aggregate [{8: max(5: v4)=max(5: v4)}] group by [[7: v6]] having [null]
                    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) not in (select v4 from t1 where v5 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [4: min(1: v1) = 5: v4 AND 2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having v2 not in (select v4 from t1 where v5 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v4 AND 2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 not in (select v4 from t1 where v5 = v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = 5: v4] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 7: v6])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 not in (select max(v4) from t1 where v5 = 5 group by v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = 8: max(5: v4)] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min(1: v1)=min(4: min(1: v1))}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min(1: v1)=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: max(5: v4)=max(8: max(5: v4))}] group by [[7: v6]] having [null]
            EXCHANGE SHUFFLE[7]
                AGGREGATE ([LOCAL] aggregate [{8: max(5: v4)=max(5: v4)}] group by [[7: v6]] having [null]
                    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1);
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [3: v3 = 3 OR 5: v5 IS NOT NULL AND 6: v6 IS NOT NULL])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v1 = 4;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [3: v3 = 3 OR 5: v5 IS NOT NULL AND 6: v6 IS NOT NULL OR 1: v1 = 4])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or (t0.v2 in (select v6 from t1 where v5 = v1) and t0.v1 = 4);
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [3: v3 = 3 OR 5: v5 IS NOT NULL AND 6: v6 IS NOT NULL AND 1: v1 = 4])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[null])
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 not in (select v6 from t1 where v5 = v1)) and t0.v1 = 4;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [3: v3 = 3 OR 5: v5 IS NULL AND 6: v6 IS NULL])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 4])
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
            EXCHANGE SHUFFLE[5, 6]
                AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                    SCAN (columns[5: v5, 6: v6] predicate[5: v5 = 4])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 8: v4 AND 2: v2 = 9: v5] post-join-predicate [3: v3 = 3 OR 7: expr OR 8: v4 IS NULL AND 9: v5 IS NULL])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                EXCHANGE SHUFFLE[5, 6]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                        SCAN (columns[5: v5, 6: v6] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v4, 9: v5]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v4, 9: v5]] having [null]
                SCAN (columns[8: v4, 9: v5] predicate[null])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 8: v4 AND 2: v2 = 9: v5] post-join-predicate [3: v3 = 3 OR 7: expr AND 8: v4 IS NULL AND 9: v5 IS NULL])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                EXCHANGE SHUFFLE[5, 6]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                        SCAN (columns[5: v5, 6: v6] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v4, 9: v5]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v4, 9: v5]] having [null]
                SCAN (columns[8: v4, 9: v5] predicate[null])
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1)) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 9: v5 AND 2: v2 = 8: v4] post-join-predicate [null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [3: v3 = 3 OR 5: v5 IS NOT NULL AND 6: v6 IS NOT NULL])
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
        EXCHANGE SHUFFLE[5]
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                EXCHANGE SHUFFLE[5, 6]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v5, 6: v6]] having [null]
                        SCAN (columns[5: v5, 6: v6] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[8: v4, 9: v5] predicate[null])
[end]

[sql]
select v1 from t0 group by v1, v1 in (1) having v1 in (1)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 4: expr]] having [null]
    AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 4: expr]] having [null]
        SCAN (columns[1: v1] predicate[1: v1 = 1])
[end]

[sql]
select * from t0 where v3 in (select * from (values(2),(3)) t);
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = cast(4: expr as bigint(20))] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    VALUES (2),(3)
[end]

[sql]
select * from t0 where v3 not in (select * from (values(2),(3)) t);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = cast(4: expr as bigint(20))] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    VALUES (2),(3)
[end]

[sql]
select * from t0 where v3 in (select 2);
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = cast(4: expr as bigint(20))] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    VALUES (2)
[end]

[sql]
select * from t0 where v3 not in (select 2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = cast(4: expr as bigint(20))] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    VALUES (2)
[end]

[sql]
select * from t0 where (v3 > 3) = (v3 in (select v7 from t2 where t0.v2 = t2.v8))
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 5: v8 AND 3: v3 = 4: v7] post-join-predicate [3: v3 > 3 = 4: v7 IS NOT NULL AND 5: v8 IS NOT NULL])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
                SCAN (columns[4: v7, 5: v8] predicate[null])
[end]

[sql]
select case (v3 in (select v7 from t2 where t0.v2 = t2.v8)) when TRUE then 1 when FALSE then 2 end from t0;
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 5: v8 AND 3: v3 = 4: v7] post-join-predicate [null])
    SCAN (columns[2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
                SCAN (columns[4: v7, 5: v8] predicate[null])
[end]

[sql]
select not (v3 in (select v7 from t2 where t0.v2 = t2.v8)) from t0;
[result]
LEFT OUTER JOIN (join-predicate [2: v2 = 5: v8 AND 3: v3 = 4: v7] post-join-predicate [null])
    SCAN (columns[2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v7, 5: v8]] having [null]
                SCAN (columns[4: v7, 5: v8] predicate[null])
[end]