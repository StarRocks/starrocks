[sql]
select grouping(v1), grouping(v2), grouping_id(v1,v2), v1,v2 from t0 group by grouping sets((v1,v2),(v1),(v2));
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
    EXCHANGE SHUFFLE[1, 2, 4, 5, 6, 7]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
            REPEAT [[1: v1, 2: v2], [1: v1], [2: v2]]
                SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select grouping(v1), grouping(v2), grouping_id(v1,v2), v1,v2 from t0 group by rollup(v1, v2);
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
    EXCHANGE SHUFFLE[1, 2, 4, 5, 6, 7]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
            REPEAT [[], [1: v1], [1: v1, 2: v2]]
                SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select grouping(v1), grouping(v2), grouping_id(v1,v2), v1,v2 from t0 group by cube(v1,v2)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
    EXCHANGE SHUFFLE[1, 2, 4, 5, 6, 7]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1, 2: v2, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
            REPEAT [[], [1: v1], [2: v2], [1: v1, 2: v2]]
                SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select sum(v1) from t0 having null
[result]
VALUES
[end]

[sql]
select sum(v1) from t0 having sum(v1) between 1 and 2
[result]
AGGREGATE ([GLOBAL] aggregate [{4: sum=sum(4: sum)}] group by [[]] having [4: sum >= 1 AND 4: sum <= 2]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: sum=sum(1: v1)}] group by [[]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v2 from t0 group by true,v2 having true = v2
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[2: v2]] having [null]
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[2: v2]] having [null]
            SCAN (columns[2: v2] predicate[2: v2 = 1])
[end]

[sql]
select v1 from t0 group by not (false), v1 having not (false) != v1
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1]] having [null]
    AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1] predicate[1: v1 = 0])
[end]

[sql]
select count(distinct v1), count(distinct v2) from t0
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1, 2: v2] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(6: v1)}] group by [[]] having [null]
                    CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{5: count=multi_distinct_count(5: count)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{5: count=multi_distinct_count(7: v2)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select count(distinct v1), avg(distinct v2), sum(distinct v3) from t0
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        CROSS JOIN (join-predicate [null] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[]] having [null]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(7: v1)}] group by [[]] having [null]
                            CTEConsumer(cteid=1)
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{6: sum=multi_distinct_sum(6: sum)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{6: sum=multi_distinct_sum(8: v3)}] group by [[]] having [null]
                                CTEConsumer(cteid=1)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{9: sum=multi_distinct_sum(9: sum)}] group by [[]] having [null]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{9: sum=multi_distinct_sum(10: v2)}] group by [[]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{11: count=multi_distinct_count(11: count)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{11: count=multi_distinct_count(12: v2)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select count(distinct v1), avg(v2) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count), 5: avg=avg(5: avg)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(1: v1), 5: avg=avg(2: v2)}] group by [[]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select count(distinct v1)/count(distinct v2), count(distinct v1)+avg(distinct v2), sum(distinct v3)-count(distinct v1) from t0
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        CROSS JOIN (join-predicate [null] post-join-predicate [null])
            CROSS JOIN (join-predicate [null] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[]] having [null]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(11: v1)}] group by [[]] having [null]
                            CTEConsumer(cteid=1)
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{5: count=multi_distinct_count(5: count)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{5: count=multi_distinct_count(12: v2)}] group by [[]] having [null]
                                CTEConsumer(cteid=1)
            EXCHANGE BROADCAST
                AGGREGATE ([GLOBAL] aggregate [{7: sum=multi_distinct_sum(7: sum)}] group by [[]] having [null]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{7: sum=multi_distinct_sum(13: v3)}] group by [[]] having [null]
                            CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{14: sum=multi_distinct_sum(14: sum)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{14: sum=multi_distinct_sum(15: v2)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select count(distinct v1), count(distinct v2) from t0 group by v3
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    INNER JOIN (join-predicate [7: v3 <=> 9: v3] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[7: v3]] having [null]
            EXCHANGE SHUFFLE[7]
                AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(6: v1)}] group by [[7: v3]] having [null]
                    CTEConsumer(cteid=1)
        AGGREGATE ([GLOBAL] aggregate [{5: count=multi_distinct_count(5: count)}] group by [[9: v3]] having [null]
            EXCHANGE SHUFFLE[9]
                AGGREGATE ([LOCAL] aggregate [{5: count=multi_distinct_count(8: v2)}] group by [[9: v3]] having [null]
                    CTEConsumer(cteid=1)
[end]

[sql]
select abs(v3 + 1) from (select v3, count(distinct v1), count(distinct v2) from t0 group by v3) t
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[3: v3]] having [null]
    EXCHANGE SHUFFLE[3]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[3: v3]] having [null]
            SCAN (columns[3: v3] predicate[null])
[end]

[sql]
select count(distinct v1) from t0 having avg(distinct v3) > 0 order by sum(distinct v2)
[result]
TOP-N (order by [[6: sum ASC NULLS FIRST]])
    TOP-N (order by [[6: sum ASC NULLS FIRST]])
        CTEAnchor(cteid=1)
            CTEProducer(cteid=1)
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
            INNER JOIN (join-predicate [divide(cast(9: sum as double), cast(11: count as double)) > 0] post-join-predicate [null])
                CROSS JOIN (join-predicate [null] post-join-predicate [null])
                    CROSS JOIN (join-predicate [null] post-join-predicate [null])
                        AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[]] having [null]
                            EXCHANGE GATHER
                                AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(7: v1)}] group by [[]] having [null]
                                    CTEConsumer(cteid=1)
                        EXCHANGE BROADCAST
                            AGGREGATE ([GLOBAL] aggregate [{6: sum=multi_distinct_sum(6: sum)}] group by [[]] having [null]
                                EXCHANGE GATHER
                                    AGGREGATE ([LOCAL] aggregate [{6: sum=multi_distinct_sum(8: v2)}] group by [[]] having [null]
                                        CTEConsumer(cteid=1)
                    EXCHANGE BROADCAST
                        AGGREGATE ([GLOBAL] aggregate [{9: sum=multi_distinct_sum(9: sum)}] group by [[]] having [null]
                            EXCHANGE GATHER
                                AGGREGATE ([LOCAL] aggregate [{9: sum=multi_distinct_sum(10: v3)}] group by [[]] having [null]
                                    CTEConsumer(cteid=1)
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{11: count=multi_distinct_count(11: count)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{11: count=multi_distinct_count(12: v3)}] group by [[]] having [null]
                                CTEConsumer(cteid=1)
[end]

[sql]
select count(distinct b1) from test_object having count(distinct b2) > 1 order by count(distinct b3)
[result]
TOP-N (order by [[15: count ASC NULLS FIRST]])
    TOP-N (order by [[15: count ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{13: count=bitmap_union_count(13: count), 14: count=bitmap_union_count(14: count), 15: count=bitmap_union_count(15: count)}] group by [[]] having [14: count > 1]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{13: count=bitmap_union_count(5: b1), 14: count=bitmap_union_count(6: b2), 15: count=bitmap_union_count(7: b3)}] group by [[]] having [null]
                    SCAN (columns[5: b1, 6: b2, 7: b3] predicate[null])
[end]

[sql]
select count(distinct h1) from test_object having count(distinct h2) > 1 order by count(distinct h3)
[result]
TOP-N (order by [[15: count ASC NULLS FIRST]])
    TOP-N (order by [[15: count ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{13: count=hll_union_agg(13: count), 14: count=hll_union_agg(14: count), 15: count=hll_union_agg(15: count)}] group by [[]] having [14: count > 1]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{13: count=hll_union_agg(9: h1), 14: count=hll_union_agg(10: h2), 15: count=hll_union_agg(11: h3)}] group by [[]] having [null]
                    SCAN (columns[9: h1, 10: h2, 11: h3] predicate[null])
[end]

[sql]
select v1,count(distinct b1) from test_object group by v1 having count(distinct h1) > 1
[result]
AGGREGATE ([GLOBAL] aggregate [{13: count=bitmap_union_count(13: count), 14: count=hll_union_agg(14: count)}] group by [[1: v1]] having [14: count > 1]
    AGGREGATE ([LOCAL] aggregate [{13: count=bitmap_union_count(5: b1), 14: count=hll_union_agg(9: h1)}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1, 5: b1, 9: h1] predicate[null])
[end]

[sql]
select v1,count(distinct b1)/ count(distinct h2) from test_object group by v1 having count(distinct h1) > 1
[result]
AGGREGATE ([GLOBAL] aggregate [{13: count=bitmap_union_count(13: count), 14: count=hll_union_agg(14: count), 15: count=hll_union_agg(15: count)}] group by [[1: v1]] having [15: count > 1]
    AGGREGATE ([LOCAL] aggregate [{13: count=bitmap_union_count(5: b1), 14: count=hll_union_agg(10: h2), 15: count=hll_union_agg(9: h1)}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1, 5: b1, 9: h1, 10: h2] predicate[null])
[end]

[sql]
select avg(distinct v1) from t0
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{5: sum=multi_distinct_sum(5: sum)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{5: sum=multi_distinct_sum(6: v1)}] group by [[]] having [null]
                    CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{7: count=multi_distinct_count(7: count)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{7: count=multi_distinct_count(8: v1)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select count(distinct v1), avg(distinct v1), sum(distinct v1) from t0
[result]
CTEAnchor(cteid=1)
    CTEProducer(cteid=1)
        SCAN (columns[1: v1] predicate[null])
    CROSS JOIN (join-predicate [null] post-join-predicate [null])
        AGGREGATE ([GLOBAL] aggregate [{4: count=multi_distinct_count(4: count)}] group by [[]] having [null]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{4: count=multi_distinct_count(7: v1)}] group by [[]] having [null]
                    CTEConsumer(cteid=1)
        EXCHANGE BROADCAST
            AGGREGATE ([GLOBAL] aggregate [{6: sum=multi_distinct_sum(6: sum)}] group by [[]] having [null]
                EXCHANGE GATHER
                    AGGREGATE ([LOCAL] aggregate [{6: sum=multi_distinct_sum(8: v1)}] group by [[]] having [null]
                        CTEConsumer(cteid=1)
[end]

[sql]
select v2,sum(v1) from t0 group by rollup(v2, 1)
[result]
AGGREGATE ([GLOBAL] aggregate [{5: sum=sum(5: sum)}] group by [[2: v2, 4: expr, 6: GROUPING_ID]] having [null]
    EXCHANGE SHUFFLE[2, 4, 6]
        AGGREGATE ([LOCAL] aggregate [{5: sum=sum(1: v1)}] group by [[2: v2, 4: expr, 6: GROUPING_ID]] having [null]
            REPEAT [[], [2: v2], [2: v2, 4: expr]]
                SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v7 from t2 group by rollup(v7,v7)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v7, 4: GROUPING_ID]] having [null]
    EXCHANGE SHUFFLE[1, 4]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v7, 4: GROUPING_ID]] having [null]
            REPEAT [[], [1: v7], [1: v7, 1: v7]]
                SCAN (columns[1: v7] predicate[null])
[end]

[sql]
select v7,v8 from t2 group by cube(v7,v7,v8)
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v7, 2: v8, 4: GROUPING_ID]] having [null]
    EXCHANGE SHUFFLE[1, 2, 4]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[1: v7, 2: v8, 4: GROUPING_ID]] having [null]
            REPEAT [[], [1: v7], [1: v7], [1: v7, 1: v7], [2: v8], [1: v7, 2: v8], [1: v7, 2: v8], [1: v7, 1: v7, 2: v8]]
                SCAN (columns[1: v7, 2: v8] predicate[null])
[end]