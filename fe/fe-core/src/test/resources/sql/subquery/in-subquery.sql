[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11] predicate[5: v11 IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where (t0.v2 in (select t3.v11 from t3)) is null
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[5: v11] predicate[null])
    INNER JOIN (join-predicate [CASE WHEN 10: countRows IS NULL OR 10: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v11 IS NOT NULL THEN true WHEN 11: countNotNulls < 10: countRows THEN null ELSE false END IS NULL] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v11] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v11]] having [null]
                    EXCHANGE SHUFFLE[8]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v11]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{10: countRows=count(10: countRows), 11: countNotNulls=count(11: countNotNulls)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{10: countRows=count(1), 11: countNotNulls=count(9: v11)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 = t3.v12)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 = 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[5: v11 IS NOT NULL AND 6: v12 IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 where t0.v2 in (select t3.v11 from t3 where t0.v3 > t3.v12)
[result]
LEFT SEMI JOIN (join-predicate [2: v2 = 5: v11 AND 3: v3 > 6: v12] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[5: v11, 6: v12] predicate[5: v11 IS NOT NULL])
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
        SCAN (columns[5: v11, 6: v12] predicate[5: v11 IS NOT NULL AND 5: v11 = 3])
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
        SCAN (columns[4: v10, 5: v11] predicate[5: v11 IS NOT NULL AND 5: v11 = 3 AND 4: v10 > 2])
[end]

[sql]
select v2, min(v1) from t0 group by v2 having min(v1) in (select v4 from t1 where v5 = v2);
[result]
RIGHT SEMI JOIN (join-predicate [5: v4 = 4: min AND 6: v5 = 2: v2] post-join-predicate [null])
    SCAN (columns[5: v4, 6: v5] predicate[5: v4 IS NOT NULL AND 6: v5 IS NOT NULL])
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
    SCAN (columns[5: v4, 6: v5, 7: v6] predicate[5: v4 IS NOT NULL AND 6: v5 IS NOT NULL])
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
    SCAN (columns[5: v4, 6: v5] predicate[5: v4 IS NOT NULL AND 6: v5 IS NOT NULL])
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
        SCAN (columns[5: v4, 6: v5, 7: v6] predicate[5: v4 IS NOT NULL AND 6: v5 = 7: v6])
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
        AGGREGATE ([GLOBAL] aggregate [{8: max=max(8: max)}] group by [[7: v6]] having [8: max IS NOT NULL]
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
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNotNulls < 12: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNotNulls=count(13: countNotNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNotNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v1 = 4;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNotNulls < 12: countRows THEN null ELSE false END OR 1: v1 = 4])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNotNulls=count(13: countNotNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNotNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or (t0.v2 in (select v6 from t1 where v5 = v1) and t0.v1 = 4);
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNotNulls < 12: countRows THEN null ELSE false END AND 1: v1 = 4])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNotNulls=count(13: countNotNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNotNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 not in (select v6 from t1 where v5 = v1)) and t0.v1 = 4;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 11: v5] post-join-predicate [3: v3 = 3 OR NOT CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 8: v6 IS NOT NULL THEN true WHEN 13: countNotNulls < 12: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 8: v6 AND 1: v1 = 9: v5] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 4])
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v6, 9: v5]] having [null]
                            PREDICATE 9: v5 = 4
                                CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[11]
            AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNotNulls=count(13: countNotNulls)}] group by [[11: v5]] having [null]
                EXCHANGE SHUFFLE[11]
                    AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNotNulls=count(10: v6)}] group by [[11: v5]] having [null]
                        PREDICATE 11: v5 = 4
                            CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) or t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[8: v4, 9: v5] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 15: v4] post-join-predicate [3: v3 = 3 OR 7: expr OR NOT CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v5 IS NOT NULL THEN true WHEN 17: countNotNulls < 16: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 12: v5 AND 2: v2 = 13: v4] post-join-predicate [null])
            CTEAnchor(cteid=2)
                CTEProducer(cteid=2)
                    SCAN (columns[5: v5, 6: v6] predicate[null])
                LEFT OUTER JOIN (join-predicate [1: v1 = 21: v5] post-join-predicate [null])
                    LEFT OUTER JOIN (join-predicate [2: v2 = 18: v6 AND 1: v1 = 19: v5] post-join-predicate [null])
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                        EXCHANGE SHUFFLE[19]
                            AGGREGATE ([GLOBAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                EXCHANGE SHUFFLE[18, 19]
                                    AGGREGATE ([LOCAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                        CTEConsumer(cteid=2)
                    EXCHANGE SHUFFLE[21]
                        AGGREGATE ([GLOBAL] aggregate [{22: countRows=count(22: countRows), 23: countNotNulls=count(23: countNotNulls)}] group by [[21: v5]] having [null]
                            EXCHANGE SHUFFLE[21]
                                AGGREGATE ([LOCAL] aggregate [{22: countRows=count(1), 23: countNotNulls=count(20: v6)}] group by [[21: v5]] having [null]
                                    CTEConsumer(cteid=2)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                    EXCHANGE SHUFFLE[12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{17: countNotNulls=count(17: countNotNulls), 16: countRows=count(16: countRows)}] group by [[15: v4]] having [null]
                EXCHANGE SHUFFLE[15]
                    AGGREGATE ([LOCAL] aggregate [{17: countNotNulls=count(14: v5), 16: countRows=count(1)}] group by [[15: v4]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[8: v4, 9: v5] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 15: v4] post-join-predicate [3: v3 = 3 OR 7: expr AND NOT CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v5 IS NOT NULL THEN true WHEN 17: countNotNulls < 16: countRows THEN null ELSE false END])
        LEFT OUTER JOIN (join-predicate [2: v2 = 12: v5 AND 2: v2 = 13: v4] post-join-predicate [null])
            CTEAnchor(cteid=2)
                CTEProducer(cteid=2)
                    SCAN (columns[5: v5, 6: v6] predicate[null])
                LEFT OUTER JOIN (join-predicate [1: v1 = 21: v5] post-join-predicate [null])
                    LEFT OUTER JOIN (join-predicate [2: v2 = 18: v6 AND 1: v1 = 19: v5] post-join-predicate [null])
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                        EXCHANGE SHUFFLE[19]
                            AGGREGATE ([GLOBAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                EXCHANGE SHUFFLE[18, 19]
                                    AGGREGATE ([LOCAL] aggregate [{}] group by [[18: v6, 19: v5]] having [null]
                                        CTEConsumer(cteid=2)
                    EXCHANGE SHUFFLE[21]
                        AGGREGATE ([GLOBAL] aggregate [{22: countRows=count(22: countRows), 23: countNotNulls=count(23: countNotNulls)}] group by [[21: v5]] having [null]
                            EXCHANGE SHUFFLE[21]
                                AGGREGATE ([LOCAL] aggregate [{22: countRows=count(1), 23: countNotNulls=count(20: v6)}] group by [[21: v5]] having [null]
                                    CTEConsumer(cteid=2)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                    EXCHANGE SHUFFLE[12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v5, 13: v4]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{17: countNotNulls=count(17: countNotNulls), 16: countRows=count(16: countRows)}] group by [[15: v4]] having [null]
                EXCHANGE SHUFFLE[15]
                    AGGREGATE ([LOCAL] aggregate [{17: countNotNulls=count(14: v5), 16: countRows=count(1)}] group by [[15: v4]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v3, v1 from t0 where (t0.v3 = 3 or t0.v2 in (select v6 from t1 where v5 = v1)) and t0.v2 not in (select v5 from t1 where v4 = v2);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 9: v5 AND 2: v2 = 8: v4] post-join-predicate [null])
    CTEAnchor(cteid=1)
        CTEProducer(cteid=1)
            SCAN (columns[5: v5, 6: v6] predicate[null])
        LEFT OUTER JOIN (join-predicate [1: v1 = 15: v5] post-join-predicate [3: v3 = 3 OR CASE WHEN 16: countRows IS NULL OR 16: countRows = 0 THEN false WHEN 2: v2 IS NULL THEN null WHEN 12: v6 IS NOT NULL THEN true WHEN 17: countNotNulls < 16: countRows THEN null ELSE false END])
            LEFT OUTER JOIN (join-predicate [2: v2 = 12: v6 AND 1: v1 = 13: v5] post-join-predicate [null])
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                EXCHANGE SHUFFLE[13]
                    AGGREGATE ([GLOBAL] aggregate [{}] group by [[12: v6, 13: v5]] having [null]
                        EXCHANGE SHUFFLE[12, 13]
                            AGGREGATE ([LOCAL] aggregate [{}] group by [[12: v6, 13: v5]] having [null]
                                CTEConsumer(cteid=1)
            EXCHANGE SHUFFLE[15]
                AGGREGATE ([GLOBAL] aggregate [{17: countNotNulls=count(17: countNotNulls), 16: countRows=count(16: countRows)}] group by [[15: v5]] having [null]
                    EXCHANGE SHUFFLE[15]
                        AGGREGATE ([LOCAL] aggregate [{17: countNotNulls=count(14: v6), 16: countRows=count(1)}] group by [[15: v5]] having [null]
                            CTEConsumer(cteid=1)
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
        PREDICATE cast(4: column_0 as bigint(20)) IS NOT NULL
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
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 11: v8] post-join-predicate [3: v3 > 3 = CASE WHEN 12: countRows IS NULL OR 12: countRows = 0 THEN false WHEN 3: v3 IS NULL THEN null WHEN 8: v7 IS NOT NULL THEN true WHEN 13: countNotNulls < 12: countRows THEN null ELSE false END])
        EXCHANGE SHUFFLE[2]
            LEFT OUTER JOIN (join-predicate [3: v3 = 8: v7 AND 2: v2 = 9: v8] post-join-predicate [null])
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v7, 9: v8]] having [null]
                    EXCHANGE SHUFFLE[8, 9]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v7, 9: v8]] having [null]
                            CTEConsumer(cteid=1)
        AGGREGATE ([GLOBAL] aggregate [{12: countRows=count(12: countRows), 13: countNotNulls=count(13: countNotNulls)}] group by [[11: v8]] having [null]
            EXCHANGE SHUFFLE[11]
                AGGREGATE ([LOCAL] aggregate [{12: countRows=count(1), 13: countNotNulls=count(10: v7)}] group by [[11: v8]] having [null]
                    CTEConsumer(cteid=1)
[end]

[sql]
select case (v3 in (select v7 from t2 where t0.v2 = t2.v8)) when TRUE then 1 when FALSE then 2 end from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 12: v8] post-join-predicate [null])
        EXCHANGE SHUFFLE[2]
            LEFT OUTER JOIN (join-predicate [3: v3 = 9: v7 AND 2: v2 = 10: v8] post-join-predicate [null])
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                    EXCHANGE SHUFFLE[9, 10]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                            CTEConsumer(cteid=1)
        AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows), 14: countNotNulls=count(14: countNotNulls)}] group by [[12: v8]] having [null]
            EXCHANGE SHUFFLE[12]
                AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1), 14: countNotNulls=count(11: v7)}] group by [[12: v8]] having [null]
                    CTEConsumer(cteid=1)
[end]

[sql]
select not (v3 in (select v7 from t2 where t0.v2 = t2.v8)) from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v7, 5: v8] predicate[null])
    LEFT OUTER JOIN (join-predicate [2: v2 = 12: v8] post-join-predicate [null])
        EXCHANGE SHUFFLE[2]
            LEFT OUTER JOIN (join-predicate [3: v3 = 9: v7 AND 2: v2 = 10: v8] post-join-predicate [null])
                EXCHANGE SHUFFLE[3, 2]
                    SCAN (columns[2: v2, 3: v3] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                    EXCHANGE SHUFFLE[9, 10]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[9: v7, 10: v8]] having [null]
                            CTEConsumer(cteid=1)
        AGGREGATE ([GLOBAL] aggregate [{13: countRows=count(13: countRows), 14: countNotNulls=count(14: countNotNulls)}] group by [[12: v8]] having [null]
            EXCHANGE SHUFFLE[12]
                AGGREGATE ([LOCAL] aggregate [{13: countRows=count(1), 14: countNotNulls=count(11: v7)}] group by [[12: v8]] having [null]
                    CTEConsumer(cteid=1)
[end]

/* test QuantifiedApply2OuterJoinRule */

[sql]
select v1, v2 in (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6) from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 15: v4 AND 1: v1 = 1 AND add(2: v2, 16: v5) = 17: v6] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [2: v2 = 10: add AND 1: v1 = 11: v4 AND 1: v1 = 1 AND add(2: v2, 12: v5) = 13: v6] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE SHUFFLE[11]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                    EXCHANGE SHUFFLE[10, 11, 12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                            PREDICATE 11: v4 = 1
                                CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[15]
            AGGREGATE ([GLOBAL] aggregate [{18: countRows=count(18: countRows), 19: countNotNulls=count(19: countNotNulls)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                EXCHANGE SHUFFLE[15, 16, 17]
                    AGGREGATE ([LOCAL] aggregate [{18: countRows=count(1), 19: countNotNulls=count(14: add)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                        PREDICATE 15: v4 = 1
                            CTEConsumer(cteid=1)
[end]

[sql]
select v1, v2 in (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6) from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    LEFT OUTER JOIN (join-predicate [abs(add(1: v1, 15: v4)) = cast(add(1: v1, 16: v5) as largeint(40)) AND add(2: v2, 16: v5) = 17: v6] post-join-predicate [null])
        RIGHT OUTER JOIN (join-predicate [10: add = 2: v2 AND abs(add(1: v1, 11: v4)) = cast(add(1: v1, 12: v5) as largeint(40)) AND add(2: v2, 12: v5) = 13: v6] post-join-predicate [null])
            EXCHANGE SHUFFLE[10]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                    EXCHANGE SHUFFLE[10, 11, 12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                            CTEConsumer(cteid=1)
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{18: countRows=count(18: countRows), 19: countNotNulls=count(19: countNotNulls)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                EXCHANGE SHUFFLE[15, 16, 17]
                    AGGREGATE ([LOCAL] aggregate [{18: countRows=count(1), 19: countNotNulls=count(14: add)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v1, v4 in (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1) from t0, t1;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c] predicate[null])
    LEFT OUTER JOIN (join-predicate [add(25: cast, cast(1: v1 as double)) = cast(add(4: v4, 26: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [29: cast = 21: cast AND add(22: cast, cast(1: v1 as double)) = cast(add(4: v4, 23: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[1: v1, 2: v2] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[4: v4, 5: v5] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: cast, 22: cast, 23: cast]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: cast, 22: cast, 23: cast]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: cast, 26: cast]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: cast)}] group by [[25: cast, 26: cast]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select t0.v1, t0.v1 + t1.v4 in (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5) from t0 left join t1 on true;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
    LEFT OUTER JOIN (join-predicate [29: add = 25: cast AND add(26: add, 1: v1) = 5: v5] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [30: cast = 21: cast AND 31: add = 22: cast AND add(23: add, 1: v1) = 5: v5] post-join-predicate [null])
            RIGHT OUTER JOIN (join-predicate [null] post-join-predicate [null])
                EXCHANGE GATHER
                    SCAN (columns[4: v4, 5: v5] predicate[null])
                EXCHANGE GATHER
                    SCAN (columns[1: v1] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: cast, 22: cast, 23: add]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: cast, 22: cast, 23: add]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: cast, 26: add]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: cast)}] group by [[25: cast, 26: add]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select t0.v1, case when t0.v1 > 1 then v1 when t1.v4 > 1 then v4 else t1.v5 end in (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6) from  t0 left join t1 on t0.v3 = t1.v6;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[null])
    LEFT OUTER JOIN (join-predicate [add(26: cast, 3: v3) = subtract(25: t1d, 6: v6)] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [29: cast = 21: abs AND add(23: cast, 3: v3) = subtract(22: t1d, 6: v6)] post-join-predicate [null])
            RIGHT OUTER JOIN (join-predicate [6: v6 = 3: v3] post-join-predicate [null])
                EXCHANGE SHUFFLE[6]
                    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
                EXCHANGE SHUFFLE[3]
                    SCAN (columns[1: v1, 3: v3] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: abs, 22: t1d, 23: cast]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: abs, 22: t1d, 23: cast]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: t1d, 26: cast]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: abs)}] group by [[25: t1d, 26: cast]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v1, v2 not in (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6) from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
    LEFT OUTER JOIN (join-predicate [1: v1 = 15: v4 AND 1: v1 = 1 AND add(2: v2, 16: v5) = 17: v6] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [2: v2 = 10: add AND 1: v1 = 11: v4 AND 1: v1 = 1 AND add(2: v2, 12: v5) = 13: v6] post-join-predicate [null])
            SCAN (columns[1: v1, 2: v2] predicate[null])
            EXCHANGE SHUFFLE[11]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                    EXCHANGE SHUFFLE[10, 11, 12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                            PREDICATE 11: v4 = 1
                                CTEConsumer(cteid=1)
        EXCHANGE SHUFFLE[15]
            AGGREGATE ([GLOBAL] aggregate [{18: countRows=count(18: countRows), 19: countNotNulls=count(19: countNotNulls)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                EXCHANGE SHUFFLE[15, 16, 17]
                    AGGREGATE ([LOCAL] aggregate [{18: countRows=count(1), 19: countNotNulls=count(14: add)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                        PREDICATE 15: v4 = 1
                            CTEConsumer(cteid=1)
[end]

[sql]
select v1, v2 not in (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6) from t0;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    LEFT OUTER JOIN (join-predicate [abs(add(1: v1, 15: v4)) = cast(add(1: v1, 16: v5) as largeint(40)) AND add(2: v2, 16: v5) = 17: v6] post-join-predicate [null])
        RIGHT OUTER JOIN (join-predicate [10: add = 2: v2 AND abs(add(1: v1, 11: v4)) = cast(add(1: v1, 12: v5) as largeint(40)) AND add(2: v2, 12: v5) = 13: v6] post-join-predicate [null])
            EXCHANGE SHUFFLE[10]
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                    EXCHANGE SHUFFLE[10, 11, 12, 13]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[10: add, 11: v4, 12: v5, 13: v6]] having [null]
                            CTEConsumer(cteid=1)
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{18: countRows=count(18: countRows), 19: countNotNulls=count(19: countNotNulls)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                EXCHANGE SHUFFLE[15, 16, 17]
                    AGGREGATE ([LOCAL] aggregate [{18: countRows=count(1), 19: countNotNulls=count(14: add)}] group by [[15: v4, 16: v5, 17: v6]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v1, v4 not in (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1) from t0, t1;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c] predicate[null])
    LEFT OUTER JOIN (join-predicate [add(25: cast, cast(1: v1 as double)) = cast(add(4: v4, 26: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [29: cast = 21: cast AND add(22: cast, cast(1: v1 as double)) = cast(add(4: v4, 23: cast) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                SCAN (columns[1: v1, 2: v2] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[4: v4, 5: v5] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: cast, 22: cast, 23: cast]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: cast, 22: cast, 23: cast]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: cast, 26: cast]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: cast)}] group by [[25: cast, 26: cast]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select t0.v1, t0.v1 + t1.v4 not in (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5) from t0 left join t1 on true;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
    LEFT OUTER JOIN (join-predicate [29: add = 25: cast AND add(26: add, 1: v1) = 5: v5] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [30: cast = 21: cast AND 31: add = 22: cast AND add(23: add, 1: v1) = 5: v5] post-join-predicate [null])
            RIGHT OUTER JOIN (join-predicate [null] post-join-predicate [null])
                EXCHANGE GATHER
                    SCAN (columns[4: v4, 5: v5] predicate[null])
                EXCHANGE GATHER
                    SCAN (columns[1: v1] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: cast, 22: cast, 23: add]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: cast, 22: cast, 23: add]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: cast, 26: add]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: cast)}] group by [[25: cast, 26: add]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select t0.v1, case when t0.v1 > 1 then v1 when t1.v4 > 1 then v4 else t1.v5 end not in (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6) from  t0 left join t1 on t0.v3 = t1.v6;
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[null])
    LEFT OUTER JOIN (join-predicate [add(26: cast, 3: v3) = subtract(25: t1d, 6: v6)] post-join-predicate [null])
        LEFT OUTER JOIN (join-predicate [29: cast = 21: abs AND add(23: cast, 3: v3) = subtract(22: t1d, 6: v6)] post-join-predicate [null])
            RIGHT OUTER JOIN (join-predicate [6: v6 = 3: v3] post-join-predicate [null])
                EXCHANGE SHUFFLE[6]
                    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
                EXCHANGE SHUFFLE[3]
                    SCAN (columns[1: v1, 3: v3] predicate[null])
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{}] group by [[21: abs, 22: t1d, 23: cast]] having [null]
                    EXCHANGE SHUFFLE[21, 22, 23]
                        AGGREGATE ([LOCAL] aggregate [{}] group by [[21: abs, 22: t1d, 23: cast]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{27: countRows=count(27: countRows), 28: countNotNulls=count(28: countNotNulls)}] group by [[25: t1d, 26: cast]] having [null]
                EXCHANGE SHUFFLE[25, 26]
                    AGGREGATE ([LOCAL] aggregate [{27: countRows=count(1), 28: countNotNulls=count(24: abs)}] group by [[25: t1d, 26: cast]] having [null]
                        CTEConsumer(cteid=1)
[end]

/* test QuantifiedApply2JoinRule */

[sql]
select v1 from t0 where v2 in (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
RIGHT SEMI JOIN (join-predicate [9: add = 2: v2 AND 4: v4 = 1: v1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 1])
[end]

[sql]
select v1 from t0 where v2 in (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6);
[result]
RIGHT SEMI JOIN (join-predicate [9: add = 2: v2 AND abs(add(1: v1, 4: v4)) = cast(add(1: v1, 5: v5) as largeint(40)) AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    EXCHANGE SHUFFLE[9]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1 from t0, t1 where v4 in (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1);
[result]
LEFT SEMI JOIN (join-predicate [18: cast = 19: cast AND add(19: cast, cast(1: v1 as double)) = cast(add(4: v4, cast(9: t1c as bigint(20))) as double)] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 1])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4, 5: v5] predicate[5: v5 = 1])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c] predicate[cast(7: t1a as double) IS NOT NULL])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where t0.v1 + t1.v4 in (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
LEFT SEMI JOIN (join-predicate [18: cast = 19: cast AND 21: add = 20: cast AND add(add(20: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    RIGHT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        EXCHANGE GATHER
            SCAN (columns[4: v4, 5: v5] predicate[null])
        EXCHANGE GATHER
            SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select t0.v1 from  t0 left join t1 on t0.v3 = t1.v6 where case when t0.v1 > 1 then v1 when t1.v4 > 1 then v4 else t1.v5 end in (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6);
[result]
LEFT SEMI JOIN (join-predicate [19: cast = 20: abs AND add(cast(9: t1c as bigint(20)), 3: v3) = subtract(10: t1d, 6: v6)] post-join-predicate [null])
    RIGHT OUTER JOIN (join-predicate [6: v6 = 3: v3] post-join-predicate [null])
        EXCHANGE SHUFFLE[6]
            SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
        EXCHANGE SHUFFLE[3]
            SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[abs(cast(7: t1a as double)) IS NOT NULL])
[end]

[sql]
select v1 from t0 where v2 not in (select v5 + v4 from t1 where v1 = 1 and v1 = v4 and v2 + v5 = v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 9: add AND 1: v1 = 4: v4 AND 1: v1 = 1 AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
[end]

[sql]
select v1 from t0 where v2 not in (select v5 + v4 from t1 where v4 = 1 and abs(v1 + v4) = v1 + v5 and v2 + v5 = v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [2: v2 = 9: add AND abs(add(1: v1, 4: v4)) = cast(add(1: v1, 5: v5) as largeint(40)) AND add(2: v2, 5: v5) = 6: v6] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[4: v4 = 1])
[end]

[sql]
select v1 from t0, t1 where v4 not in (select t1a from test_all_type where t1a + v1 = v4 + t1c and v2 = 1 and v5 = 1);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [18: cast = 19: cast AND add(19: cast, cast(1: v1 as double)) = cast(add(4: v4, cast(9: t1c as bigint(20))) as double) AND 2: v2 = 1 AND 5: v5 = 1] post-join-predicate [null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        SCAN (columns[1: v1, 2: v2] predicate[null])
        EXCHANGE BROADCAST
            SCAN (columns[4: v4, 5: v5] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c] predicate[null])
[end]

[sql]
select t0.v1 from t0 left join t1 on true where t0.v1 + t1.v4 not in (select t1a from test_all_type where t1c = t0.v1 + t1.v5 and t1a = 'a' and t1c + t1d + t0.v1 = t1.v5);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [18: cast = 19: cast AND 21: add = 20: cast AND add(add(20: cast, 10: t1d), 1: v1) = 5: v5] post-join-predicate [null])
    RIGHT OUTER JOIN (join-predicate [null] post-join-predicate [null])
        EXCHANGE GATHER
            SCAN (columns[4: v4, 5: v5] predicate[null])
        EXCHANGE GATHER
            SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[7: t1a = a])
[end]

[sql]
select t0.v1 from  t0 left join t1 on t0.v3 = t1.v6 where case when t0.v1 > 1 then v1 when t1.v4 > 1 then v4 else t1.v5 end not in (select abs(t1a) from test_all_type where t1c + t0.v3 = t1d - t1.v6);
[result]
NULL AWARE LEFT ANTI JOIN (join-predicate [19: cast = 20: abs AND add(cast(9: t1c as bigint(20)), 3: v3) = subtract(10: t1d, 6: v6)] post-join-predicate [null])
    RIGHT OUTER JOIN (join-predicate [6: v6 = 3: v3] post-join-predicate [null])
        EXCHANGE SHUFFLE[6]
            SCAN (columns[4: v4, 5: v5, 6: v6] predicate[null])
        EXCHANGE SHUFFLE[3]
            SCAN (columns[1: v1, 3: v3] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[7: t1a, 9: t1c, 10: t1d] predicate[null])
[end]