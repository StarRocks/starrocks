[sql]
-- pruned 4-stage: MergedLocal(GB v1,v2; PB v1,v2) -> Local(v2) -> Global(v2)
SELECT count(distinct v1), sum(v3) from t0 join t1 on v3=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([LOCAL] aggregate [{7: count=count(1: v1), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
            AGGREGATE ([LOCAL] aggregate [{8: sum=sum(3: v3)}] group by [[1: v1, 2: v2]] having [null]
                INNER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

[sql]
SELECT count(distinct v1), sum(v3) from t0 join [shuffle] t1 on v1=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([LOCAL] aggregate [{7: count=count(1: v1), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
            AGGREGATE ([LOCAL] aggregate [{8: sum=sum(3: v3)}] group by [[1: v1, 2: v2]] having [null]
                INNER JOIN (join-predicate [1: v1 = 6: v6] post-join-predicate [null])
                    EXCHANGE SHUFFLE[1]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL])
                    EXCHANGE SHUFFLE[6]
                        SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

[sql]
SELECT /*+SET_VAR(enable_cost_based_multi_stage_agg=false)*/ count(distinct v1), sum(v3) from t0 join t1 on v3=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: count=count(1: v1), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
    AGGREGATE ([DISTINCT_GLOBAL] aggregate [{8: sum=sum(8: sum)}] group by [[1: v1, 2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{8: sum=sum(3: v3)}] group by [[1: v1, 2: v2]] having [null]
                INNER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

[sql]
-- 3-stage: Local(GB v1*2,v2)->DistinctGlobal(GB v1*2,v2; PB v2) -> Global(v2)
SELECT count(distinct v1*2), sum(v3) from t0 join t1 on v3=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{8: count=count(7: expr), 9: sum=sum(9: sum)}] group by [[2: v2]] having [null]
    AGGREGATE ([DISTINCT_GLOBAL] aggregate [{9: sum=sum(9: sum)}] group by [[2: v2, 7: expr]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{9: sum=sum(3: v3)}] group by [[2: v2, 7: expr]] having [null]
                INNER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

[sql]
-- Force 3-stage: Local(GB v1,v2)->DistinctGlobal(GB v1,v2; PB v2) -> Global(v2)
SELECT /*+SET_VAR(new_planner_agg_stage=3)*/ count(distinct v1), sum(v3) from t0 join t1 on v3=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{7: count=count(1: v1), 8: sum=sum(8: sum)}] group by [[2: v2]] having [null]
    AGGREGATE ([DISTINCT_GLOBAL] aggregate [{8: sum=sum(8: sum)}] group by [[1: v1, 2: v2]] having [null]
        EXCHANGE SHUFFLE[2]
            AGGREGATE ([LOCAL] aggregate [{8: sum=sum(3: v3)}] group by [[1: v1, 2: v2]] having [null]
                INNER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
                    EXCHANGE BROADCAST
                        SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

[sql]
-- Force 4-stage: Local(GB v1*2,v2)->DistinctGlobal(GB v1*2,v2; PB v1,v2) -> Local(v2) -> Global(v2)
SELECT /*+SET_VAR(new_planner_agg_stage=4)*/ count(distinct v1*2), sum(v3) from t0 join t1 on v3=v6 group by v2;
[result]
AGGREGATE ([GLOBAL] aggregate [{8: count=count(8: count), 9: sum=sum(9: sum)}] group by [[2: v2]] having [null]
    EXCHANGE SHUFFLE[2]
        AGGREGATE ([LOCAL] aggregate [{8: count=count(7: expr), 9: sum=sum(9: sum)}] group by [[2: v2]] having [null]
            AGGREGATE ([DISTINCT_GLOBAL] aggregate [{9: sum=sum(9: sum)}] group by [[2: v2, 7: expr]] having [null]
                EXCHANGE SHUFFLE[2, 7]
                    AGGREGATE ([LOCAL] aggregate [{9: sum=sum(3: v3)}] group by [[2: v2, 7: expr]] having [null]
                        INNER JOIN (join-predicate [3: v3 = 6: v6] post-join-predicate [null])
                            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 IS NOT NULL])
                            EXCHANGE BROADCAST
                                SCAN (columns[6: v6] predicate[6: v6 IS NOT NULL])
[end]

