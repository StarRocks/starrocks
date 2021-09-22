[sql]
select grouping(v1), grouping(v2), grouping_id(v1,v2), v1,v2 from t0 group by grouping sets((v1,v2),(v1),(v2));
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[2: v2, 1: v1, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
    EXCHANGE SHUFFLE[2, 1, 4, 5, 6, 7]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[2: v2, 1: v1, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING, 7: GROUPING]] having [null]
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
            REPEAT [[], [2: v2], [1: v1], [1: v1, 2: v2]]
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
AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1))}] group by [[]] having [4: sum(1: v1) >= 1 AND 4: sum(1: v1) <= 2]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1)}] group by [[]] having [null]
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
        SCAN (columns[1: v1] predicate[1: v1 != 1])
[end]

[sql]
select count(distinct v1), count(distinct v2) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 5: count(distinct 2: v2)=multi_distinct_count(5: count(distinct 2: v2))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 5: count(distinct 2: v2)=multi_distinct_count(2: v2)}] group by [[]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select count(distinct v1), avg(distinct v2), sum(distinct v3) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 6: sum(distinct 3: v3)=multi_distinct_sum(6: sum(distinct 3: v3)), 7: multi_distinct_count=multi_distinct_count(7: multi_distinct_count), 8: multi_distinct_sum=multi_distinct_sum(8: multi_distinct_sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 6: sum(distinct 3: v3)=multi_distinct_sum(3: v3), 7: multi_distinct_count=multi_distinct_count(2: v2), 8: multi_distinct_sum=multi_distinct_sum(2: v2)}] group by [[]] having [null]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select count(distinct v1), avg(v2) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=MULTI_DISTINCT_COUNT(4: count(distinct 1: v1)), 5: avg(2: v2)=avg(5: avg(2: v2))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=MULTI_DISTINCT_COUNT(1: v1), 5: avg(2: v2)=avg(2: v2)}] group by [[]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select count(distinct v1)/count(distinct v2), count(distinct v1)+avg(distinct v2), sum(distinct v3)-count(distinct v1) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 5: count(distinct 2: v2)=multi_distinct_count(5: count(distinct 2: v2)), 7: sum(distinct 3: v3)=multi_distinct_sum(7: sum(distinct 3: v3)), 11: multi_distinct_sum=multi_distinct_sum(11: multi_distinct_sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 5: count(distinct 2: v2)=multi_distinct_count(2: v2), 7: sum(distinct 3: v3)=multi_distinct_sum(3: v3), 11: multi_distinct_sum=multi_distinct_sum(2: v2)}] group by [[]] having [null]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select count(distinct v1), count(distinct v2) from t0 group by v3
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 5: count(distinct 2: v2)=multi_distinct_count(5: count(distinct 2: v2))}] group by [[3: v3]] having [null]
    EXCHANGE SHUFFLE[3]
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 5: count(distinct 2: v2)=multi_distinct_count(2: v2)}] group by [[3: v3]] having [null]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select v3, count(distinct v1), count(distinct v2) from t0 group by v3
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 5: count(distinct 2: v2)=multi_distinct_count(5: count(distinct 2: v2))}] group by [[3: v3]] having [null]
    EXCHANGE SHUFFLE[3]
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 5: count(distinct 2: v2)=multi_distinct_count(2: v2)}] group by [[3: v3]] having [null]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
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
TOP-N (order by [[6: sum(distinct 2: v2) ASC NULLS FIRST]])
    TOP-N (order by [[6: sum(distinct 2: v2) ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 6: sum(distinct 2: v2)=multi_distinct_sum(6: sum(distinct 2: v2)), 7: multi_distinct_count=multi_distinct_count(7: multi_distinct_count), 8: multi_distinct_sum=multi_distinct_sum(8: multi_distinct_sum)}] group by [[]] having [divide(cast(8: multi_distinct_sum as double), cast(7: multi_distinct_count as double)) > 0.0]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 6: sum(distinct 2: v2)=multi_distinct_sum(2: v2), 7: multi_distinct_count=multi_distinct_count(3: v3), 8: multi_distinct_sum=multi_distinct_sum(3: v3)}] group by [[]] having [null]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select count(distinct b1) from test_object having count(distinct b2) > 1 order by count(distinct b3)
[result]
TOP-N (order by [[15: count(distinct 7: b3) ASC NULLS FIRST]])
    TOP-N (order by [[15: count(distinct 7: b3) ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(13: count(distinct 5: b1)), 14: count(distinct 6: b2)=bitmap_union_count(14: count(distinct 6: b2)), 15: count(distinct 7: b3)=bitmap_union_count(15: count(distinct 7: b3))}] group by [[]] having [14: count(distinct 6: b2) > 1]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(5: b1), 14: count(distinct 6: b2)=bitmap_union_count(6: b2), 15: count(distinct 7: b3)=bitmap_union_count(7: b3)}] group by [[]] having [null]
                    SCAN (columns[5: b1, 6: b2, 7: b3] predicate[null])
[end]

[sql]
select count(distinct h1) from test_object having count(distinct h2) > 1 order by count(distinct h3)
[result]
TOP-N (order by [[15: count(distinct 11: h3) ASC NULLS FIRST]])
    TOP-N (order by [[15: count(distinct 11: h3) ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{13: count(distinct 9: h1)=hll_union_agg(13: count(distinct 9: h1)), 14: count(distinct 10: h2)=hll_union_agg(14: count(distinct 10: h2)), 15: count(distinct 11: h3)=hll_union_agg(15: count(distinct 11: h3))}] group by [[]] having [14: count(distinct 10: h2) > 1]
            EXCHANGE GATHER
                AGGREGATE ([LOCAL] aggregate [{13: count(distinct 9: h1)=hll_union_agg(9: h1), 14: count(distinct 10: h2)=hll_union_agg(10: h2), 15: count(distinct 11: h3)=hll_union_agg(11: h3)}] group by [[]] having [null]
                    SCAN (columns[9: h1, 10: h2, 11: h3] predicate[null])
[end]

[sql]
select v1,count(distinct b1) from test_object group by v1 having count(distinct h1) > 1
[result]
AGGREGATE ([GLOBAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(13: count(distinct 5: b1)), 14: count(distinct 9: h1)=hll_union_agg(14: count(distinct 9: h1))}] group by [[1: v1]] having [14: count(distinct 9: h1) > 1]
    AGGREGATE ([LOCAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(5: b1), 14: count(distinct 9: h1)=hll_union_agg(9: h1)}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1, 5: b1, 9: h1] predicate[null])
[end]

[sql]
select v1,count(distinct b1)/ count(distinct h2) from test_object group by v1 having count(distinct h1) > 1
[result]
AGGREGATE ([GLOBAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(13: count(distinct 5: b1)), 14: count(distinct 10: h2)=hll_union_agg(14: count(distinct 10: h2)), 15: count(distinct 9: h1)=hll_union_agg(15: count(distinct 9: h1))}] group by [[1: v1]] having [15: count(distinct 9: h1) > 1]
    AGGREGATE ([LOCAL] aggregate [{13: count(distinct 5: b1)=bitmap_union_count(5: b1), 14: count(distinct 10: h2)=hll_union_agg(10: h2), 15: count(distinct 9: h1)=hll_union_agg(9: h1)}] group by [[1: v1]] having [null]
        SCAN (columns[1: v1, 5: b1, 9: h1, 10: h2] predicate[null])
[end]

[sql]
select avg(distinct v1) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{5: multi_distinct_count=multi_distinct_count(5: multi_distinct_count), 6: multi_distinct_sum=multi_distinct_sum(6: multi_distinct_sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{5: multi_distinct_count=multi_distinct_count(1: v1), 6: multi_distinct_sum=multi_distinct_sum(1: v1)}] group by [[]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select count(distinct v1), avg(distinct v1), sum(distinct v1) from t0
[result]
AGGREGATE ([GLOBAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(4: count(distinct 1: v1)), 6: sum(distinct 1: v1)=multi_distinct_sum(6: sum(distinct 1: v1))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: count(distinct 1: v1)=multi_distinct_count(1: v1), 6: sum(distinct 1: v1)=multi_distinct_sum(1: v1)}] group by [[]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]
