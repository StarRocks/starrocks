[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11] predicate[null])
[end]

[sql]
select t0.v1 from t0 where (t0.v2 in (select t3.v11 from t3)) is null
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[5: v11] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [CASE WHEN 10: countRows IS NULL OR 10: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v11 IS NOT NULL THEN true WHEN 11: countNulls < 10: countRows THEN null ELSE false END IS NULL])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v11] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v11]] having [null]
                    EXCHANGE SHUFFLE[8]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v11]] having [null]
                            CTEConsumer(cteid=3)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{10: countRows=count(10: countRows), 11: countNulls=count(11: countNulls)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{10: countRows=count(1), 11: countNulls=count(9: v11)}] group by [[]] having [null]
                        CTEConsumer(cteid=3)
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 = t3.v12)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 > t3.v12)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 > 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 = t3.v12)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 < t3.v12)
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 < 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 = t3.v12) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 > t3.v12) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 > 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[5: v11 = 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 > t3.v12) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 > 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 > t3.v12) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 > 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[null])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where t0.v3 = t3.v12 and t3.v10 > 2) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 5: v11, 6: v12] predicate[4: v10 > 2])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 = t3.v12 and t3.v10 > 2) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 5: v11, 6: v12] predicate[4: v10 > 2 AND 5: v11 = 3])
[end]

[sql]
select t0.v1 from t0 where t0.v2 not in (select t3.v11 from t3 where  t3.v10 > 2) and t0.v2 = 3
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 5: v11] predicate[4: v10 > 2])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t3.v10 > 2) and t0.v2 = 3
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 3])
    EXCHANGE BROADCAST
        SCAN (columns[4: v10, 5: v11] predicate[5: v11 = 3 AND 4: v10 > 2])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) in (select v4 from t1 where v5 = v2);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 4: min AND 6: v5 = 2: v2] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) in (select v4 from t1 where v5 = v2 and v2 < v6);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 4: min AND 6: v5 = 2: v2 AND 2: v2 < 7: v6] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having v2 in (select v4 from t1 where v5 = v2);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 2: v2 AND 6: v5 = 2: v2] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5] predicate[null])
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 in (select v4 from t1 where v5 = v6);
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 5: v4] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 7: v6])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 in (select max(v4) from t1 where v5 = 5 group by v6);
[result]
LEFT SEMI JOIN (join-predicate [3: v3 = 8: max] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[8]
        AGGREGATE ([GLOBAL] aggregate [{8: max=max(8: max)}] group by [[7: v6]] having [null]
            EXCHANGE SHUFFLE[7]
                AGGREGATE ([LOCAL] aggregate [{8: max=max(5: v4)}] group by [[7: v6]] having [null]
                    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) not in (select v4 from t1 where v5 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [4: min = 5: v4 AND 2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5] predicate[null])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having v2 not in (select v4 from t1 where v5 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 5: v4 AND 2: v2 = 6: v5] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[2: v2]] having [null]
                SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5] predicate[null])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 not in (select v4 from t1 where v5 = v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = 5: v4] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 7: v6])
[end]

[sql]
select v3, min(v1) from t0 group by v3 having v3 not in (select max(v4) from t1 where v5 = 5 group by v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = 8: max] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{4: min=min(4: min)}] group by [[3: v3]] having [null]
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([LOCAL] aggregate [{4: min=min(1: v1)}] group by [[3: v3]] having [null]
                SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        AGGREGATE ([GLOBAL] aggregate [{8: max=max(8: max)}] group by [[7: v6]] having [null]
            EXCHANGE SHUFFLE[7]
                AGGREGATE ([LOCAL] aggregate [{8: max=max(5: v4)}] group by [[7: v6]] having [null]
                    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[6: v5 = 5])
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1);
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNulls < 12: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=3)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNulls=count(13: countNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=3)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v1 = 4;
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNulls < 12: countRows THEN null ELSE false END OR 1: v1 = 4])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=3)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNulls=count(13: countNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=3)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or (t0.v2 in (select v6 from t1 where v5 = v1) and t0.v1 = 4);
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNulls < 12: countRows THEN null ELSE false END AND 1: v1 = 4])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=3)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNulls=count(13: countNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=3)
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 not in (select v6 from t1 where v5 = v1)) and t0.v1 = 4;
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR NOT CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNulls < 12: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 4])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            PREDICATE 9: v5 = 4
                                CTEConsumer(cteid=3)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNulls=count(13: countNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        PREDICATE 11: v5 = 4
                            CTEConsumer(cteid=3)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
CTEAnchor(cteid=4)
    CTEProducer(cteid=4)
        SCAN (columns[8: v4, 9: v5] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 15: v4] post-join-predicate [3: v3 = 3 OR 7: expr OR NOT CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v5 IS NOT NULL THEN true WHEN 17: countNulls < 16: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 12: v5 AND 2: v2 = 13: v4] post-join-predicate [null])
            CTEAnchor(cteid=5)
                CTEProducer(cteid=5)
                    SCAN (columns[5: v5, 6: v6] predicate[null])
                LEFT OUTER JOIN (join-predicate [1: v1 = 21: v5] post-join-predicate [null])
                    LEFT OUTER JOIN (join-predicate [2: v2 = 18: v6 AND 1: v1 = 19: v5] post-join-predicate [null])
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                        EXCHANGE SHUFFLE[19]
                            AGGREGATE ([GLOBAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                EXCHANGE SHUFFLE[18, 19]
                                    AGGREGATE ([LOCAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                        CTEConsumer(cteid=5)
                    EXCHANGE SHUFFLE[21]
                        AGGREGATE ([GLOBAL] aggregate [{22: countRows=count(22: countRows), 23: countNulls=count(23: countNulls)}] group by [[21: v5]] having [null]
                            EXCHANGE SHUFFLE[21]
                                AGGREGATE ([LOCAL] aggregate [{22: countRows=count(1), 23: countNulls=count(20: v6)}] group by [[21: v5]] having [null]
                                    CTEConsumer(cteid=5)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                    EXCHANGE SHUFFLE[12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                            CTEConsumer(cteid=4)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{17: countNulls=count(17: countNulls), 16: countRows=count(16: countRows)}] group by [[15: v4]] having [null]
                EXCHANGE SHUFFLE[15]
                    AGGREGATE ([LOCAL] aggregate [{17: countNulls=count(14: v5), 16: countRows=count(1)}] group by [[15: v4]] having [null]
                        CTEConsumer(cteid=4)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
CTEAnchor(cteid=4)
    CTEProducer(cteid=4)
        SCAN (columns[8: v4, 9: v5] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 15: v4] post-join-predicate [3: v3 = 3 OR 7: expr AND NOT CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v5 IS NOT NULL THEN true WHEN 17: countNulls < 16: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 12: v5 AND 2: v2 = 13: v4] post-join-predicate [null])
            CTEAnchor(cteid=5)
                CTEProducer(cteid=5)
                    SCAN (columns[5: v5, 6: v6] predicate[null])
                LEFT OUTER JOIN (join-predicate [1: v1 = 21: v5] post-join-predicate [null])
                    LEFT OUTER JOIN (join-predicate [2: v2 = 18: v6 AND 1: v1 = 19: v5] post-join-predicate [null])
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                        EXCHANGE SHUFFLE[19]
                            AGGREGATE ([GLOBAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                EXCHANGE SHUFFLE[18, 19]
                                    AGGREGATE ([LOCAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                        CTEConsumer(cteid=5)
                    EXCHANGE SHUFFLE[21]
                        AGGREGATE ([GLOBAL] aggregate [{22: countRows=count(22: countRows), 23: countNulls=count(23: countNulls)}] group by [[21: v5]] having [null]
                            EXCHANGE SHUFFLE[21]
                                AGGREGATE ([LOCAL] aggregate [{22: countRows=count(1), 23: countNulls=count(20: v6)}] group by [[21: v5]] having [null]
                                    CTEConsumer(cteid=5)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                    EXCHANGE SHUFFLE[12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                            CTEConsumer(cteid=4)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{17: countNulls=count(17: countNulls), 16: countRows=count(16: countRows)}] group by [[15: v4]] having [null]
                EXCHANGE SHUFFLE[15]
                    AGGREGATE ([LOCAL] aggregate [{17: countNulls=count(14: v5), 16: countRows=count(1)}] group by [[15: v4]] having [null]
                        CTEConsumer(cteid=4)
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1)) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 9: v5 AND 2: v2 = 8: v4] post-join-predicate [null])
    CTEAnchor(cteid=4)
        CTEProducer(cteid=4)
            SCAN (columns[5: v5, 6: v6] predicate[null])
        LEFT OUTER JOIN (join-predicate [1: v1 = 15: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v6 IS NOT NULL THEN true WHEN 17: countNulls < 16: countRows THEN null ELSE false END])
            LEFT OUTER JOIN (join-predicate [2: v2 = 12: v6 AND 1: v1 = 13: v5] post-join-predicate [null])
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                EXCHANGE SHUFFLE[13]
                    AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v6, 13: v5]] having [null]
                        EXCHANGE SHUFFLE[12, 13]
                            AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v6, 13: v5]] having [null]
                                CTEConsumer(cteid=4)
            EXCHANGE SHUFFLE[15]
                AGGREGATE ([GLOBAL] aggregate [{17: countNulls=count(17: countNulls), 16: countRows=count(16: countRows)}] group by [[15: v5]] having [null]
                    EXCHANGE SHUFFLE[15]
                        AGGREGATE ([LOCAL] aggregate [{17: countNulls=count(14: v6), 16: countRows=count(1)}] group by [[15: v5]] having [null]
                            CTEConsumer(cteid=4)
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
LEFT SEMI JOIN (join-predicate [3: v3 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        VALUES (2),(3)
[end]

[sql]
select * from t0 where v3 not in (select * from (values(2),(3)) t);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [3: v3 = 6: cast] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        VALUES (2),(3)
[end]

[sql]
select * from t0 where (v3 > 3) = (v3 in (select v7 from t2 where t0.v2 = t2.v8))
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 11: v8] post-join-predicate [3: v3 > 3 = CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 3: v3 IS NULL THEN null WHEN 8: v7 IS NOT NULL THEN true WHEN 13: countNulls < 12: countRows THEN null ELSE false END])
        EXCHANGE SHUFFLE[2]
            LEFT OUTER JOIN (join-predicate [3: v3 = 8: v7 AND 2: v2 = 9: v8] post-join-predicate [null])
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v7, 9: v8]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v7, 9: v8]] having [null]
                            CTEConsumer(cteid=3)
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNulls=count(13: countNulls)}] group by [[11: v8]] having [null]
            EXCHANGE SHUFFLE[11]
                AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNulls=count(10: v7)}] group by [[11: v8]] having [null]
                    CTEConsumer(cteid=3)
[end]

[sql]
select case (v3 in (select v7 from t2 where t0.v2 = t2.v8)) when TRUE then 1 when FALSE then 2 end from t0;
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 12: v8] post-join-predicate [null])
        EXCHANGE SHUFFLE[2]
            RIGHT OUTER JOIN (join-predicate [9: v7 = 3: v3 AND 10: v8 = 2: v2] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                    EXCHANGE SHUFFLE[9, 10]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                            CTEConsumer(cteid=3)
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
        AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows), 14: countNulls=count(14: countNulls)}] group by [[12: v8]] having [null]
            EXCHANGE SHUFFLE[12]
                AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1), 14: countNulls=count(11: v7)}] group by [[12: v8]] having [null]
                    CTEConsumer(cteid=3)
[end]

[sql]
select not (v3 in (select v7 from t2 where t0.v2 = t2.v8)) from t0;
[result]
CTEAnchor(cteid=3)
    CTEProducer(cteid=3)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 12: v8] post-join-predicate [null])
        EXCHANGE SHUFFLE[2]
            RIGHT OUTER JOIN (join-predicate [9: v7 = 3: v3 AND 10: v8 = 2: v2] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                    EXCHANGE SHUFFLE[9, 10]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                            CTEConsumer(cteid=3)
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
        AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows), 14: countNulls=count(14: countNulls)}] group by [[12: v8]] having [null]
            EXCHANGE SHUFFLE[12]
                AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1), 14: countNulls=count(11: v7)}] group by [[12: v8]] having [null]
                    CTEConsumer(cteid=3)
[end]
