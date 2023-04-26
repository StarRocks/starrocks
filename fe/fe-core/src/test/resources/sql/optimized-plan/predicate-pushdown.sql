[sql]
select v1, v2, ta, td from t0 inner join tall on t0.v1 = tall.td
[result]
INNER JOIN (join-predicate [1: v1 = 7: td] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
    EXCHANGE SHUFFLE[7]
        SCAN (columns[4: ta, 7: td] predicate[7: td IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v1>v4
[result]
INNER JOIN (join-predicate [1: v1 > 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1 from t0 inner join t1 on v1>v4 and v2 = v5
[result]
INNER JOIN (join-predicate [2: v2 = 5: v5 AND 1: v1 > 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 IS NOT NULL])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5] predicate[5: v5 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v1 = v2
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 2: v2])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1 from t0 inner join t1 where v1 = v2 and v2 = 5
[result]
CROSS JOIN (join-predicate [null] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 2: v2 AND 2: v2 = 5 AND 1: v1 = 5])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1 from t0 inner join t1 where t0.v1 = t1.v4 and t0.v2 = 2 and t1.v5 = 5
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 2])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5] predicate[5: v5 = 5])
[end]

[sql]
select v1 from t0 inner join t1 on t0.v1 = t1.v4 and t0.v2 = 2 and t1.v5 = 5
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 2])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5] predicate[5: v5 = 5])
[end]

[sql]
select v1 from t0 inner join t1 on t0.v1 = t1.v4 and t0.v1 = 1
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[1: v1 = 1])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[4: v4 = 1])
[end]

[sql]
select v1 from t0 inner join t1 where t0.v1 = t1.v4 or t0.v2 = t1.v5
[result]
    INNER JOIN (join-predicate [1: v1 = 4: v4 OR 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select v1,v5 from t0,t1 where t0.v1 = t1.v4 and t0.v2 > t1.v5
[result]
    INNER JOIN (join-predicate [1: v1 = 4: v4 AND 2: v2 > 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 left join t1 on v1 = v4 where v4 = 1;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[1: v1 = 1])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[4: v4 = 1])
[end]

[sql]
select v1 from t0 left join t1 on v1 = v4 and t1.v5 = 1
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5] predicate[5: v5 = 1])
[end]

[sql]
select v1 from t0 left join t1 on v1 = v4 where v2 = 1
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 1])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1 from t0 left join t1 on v1 = v4 where v2 = v5 and v3 = 3
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4 AND 2: v2 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 = 3])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL AND 5: v5 IS NOT NULL])
[end]

[sql]
select v1 from t0 left join t1 on v1 = v4 and v1 = 1
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4 AND 1: v1 = 1] post-join-predicate [null])
    SCAN (columns[1: v1] predicate[null])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[4: v4 = 1])
[end]

[sql]
select * from t0 left semi join t1 on v1=v4 and v4=1 and v3 = 5 where v2 = 3;
[result]
LEFT SEMI JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 = 5 AND 1: v1 = 1 AND 2: v2 = 3])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[4: v4 = 1])
[end]

[sql]
select * from t0 left anti join t1 on v1=v4 and v4=1 and v3 = 5 where v2 = 3;
[result]
LEFT ANTI JOIN (join-predicate [1: v1 = 4: v4 AND 3: v3 = 5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[4: v4 = 1])
[end]

[sql]
select v1,v4 from t0 full outer join t1 on v1 = v4 where v3 = 3 and v6 = 6;
[result]
INNER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[3: v3 = 3])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4, 6: v6] predicate[6: v6 = 6])
[end]

[sql]
select v1,v4 from t0 full outer join t1 on v1 = v4 where v3 = 3;
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 3: v3] predicate[3: v3 = 3])
    EXCHANGE SHUFFLE[4]
        SCAN (columns[4: v4] predicate[null])
[end]

[sql]
select v1,v4 from t0 full outer join t1 on v2 = v5 where v6 = 6;
[result]
RIGHT OUTER JOIN (join-predicate [2: v2 = 5: v5] post-join-predicate [null])
    EXCHANGE SHUFFLE[2]
        SCAN (columns[1: v1, 2: v2] predicate[null])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[4: v4, 5: v5, 6: v6] predicate[6: v6 = 6])
[end]

[sql]
select v1 from t0 inner join t1 on t0.v1 = t1.v5 and t0.v2 = 2 and t1.v5 > 5
[result]
INNER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 2 AND 1: v1 > 5])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v5] predicate[5: v5 > 5])
[end]

[sql]
select v1 from t0 inner join t1 on t0.v1 = t1.v5 where t0.v2 = 2 and t1.v5 > 5
[result]
INNER JOIN (join-predicate [1: v1 = 5: v5] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[2: v2 = 2 AND 1: v1 > 5])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v5] predicate[5: v5 > 5])
[end]

[sql]
select v1 from t0 left outer join t1 on t0.v1 = t1.v5 where t0.v2 = t1.v6
[result]
    INNER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL AND 2: v2 IS NOT NULL])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v5, 6: v6] predicate[5: v5 IS NOT NULL AND 6: v6 IS NOT NULL])
[end]

[sql]
select v1 from t0 right outer join t1 on t0.v1 = t1.v5 where t0.v2 = t1.v6
[result]
    INNER JOIN (join-predicate [1: v1 = 5: v5 AND 2: v2 = 6: v6] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2] predicate[1: v1 IS NOT NULL AND 2: v2 IS NOT NULL])
    EXCHANGE SHUFFLE[5]
        SCAN (columns[5: v5, 6: v6] predicate[5: v5 IS NOT NULL AND 6: v6 IS NOT NULL])
[end]

[sql]
select * from t0 left outer join (select v4,max(v5) as m from t1 group by v4) t on v1 = v4 where v2 = case (t.m is null) when true then NULL when false then m end
[result]
INNER JOIN (join-predicate [4: v4 = 1: v1 AND 8: case = 2: v2] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[4: v4]] having [CASE 7: max IS NULL WHEN true THEN null WHEN false THEN 7: max END IS NOT NULL]
        AGGREGATE ([LOCAL] aggregate [{7: max=max(5: v5)}] group by [[4: v4]] having [null]
            SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL AND 2: v2 IS NOT NULL])
[end]

[sql]
select * from t0 left outer join (select v4,max(v5) as m from t1 group by v4) t on v1 = v4 where v2 = case (t.m is null) when true then m when false then NULL end
[result]
INNER JOIN (join-predicate [4: v4 = 1: v1 AND 8: case = 2: v2] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{7: max=max(7: max)}] group by [[4: v4]] having [CASE 7: max IS NULL WHEN true THEN 7: max WHEN false THEN null END IS NOT NULL]
        AGGREGATE ([LOCAL] aggregate [{7: max=max(5: v5)}] group by [[4: v4]] having [null]
            SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL AND 2: v2 IS NOT NULL])
[end]

[sql]
select * from t0 left outer join (select v4,count(v5) as m from t1 group by v4) t on v1 = v4 where v2 = case (t.m is null) when true then 0 when false then m end
[result]
LEFT OUTER JOIN (join-predicate [1: v1 = 4: v4] post-join-predicate [2: v2 = CASE 7: count IS NULL WHEN true THEN 0 WHEN false THEN 7: count END])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count)}] group by [[4: v4]] having [null]
            AGGREGATE ([LOCAL] aggregate [{7: count=count(5: v5)}] group by [[4: v4]] having [null]
                SCAN (columns[4: v4, 5: v5] predicate[null])
[end]

[sql]
select * from t0 left outer join (select v4,count(v5) as m from t1 group by v4) t on v1 = v4 where v2 = case (t.m is null) when true then m when false then 0 end
[result]
INNER JOIN (join-predicate [4: v4 = 1: v1 AND 8: case = 2: v2] post-join-predicate [null])
    AGGREGATE ([GLOBAL] aggregate [{7: count=count(7: count)}] group by [[4: v4]] having [CASE 7: count IS NULL WHEN true THEN 7: count WHEN false THEN 0 END IS NOT NULL]
        AGGREGATE ([LOCAL] aggregate [{7: count=count(5: v5)}] group by [[4: v4]] having [null]
            SCAN (columns[4: v4, 5: v5] predicate[4: v4 IS NOT NULL])
    EXCHANGE SHUFFLE[1]
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IS NOT NULL AND 2: v2 IS NOT NULL])
[end]

[sql]
select * from (select v1 + unnest as u from (select * from t0,unnest([1,2,3])) t ) x where u = 1
[result]
PREDICATE add(1: v1, cast(4: unnest as bigint(20))) = 1
    TABLE FUNCTION (unnest)
        SCAN (columns[1: v1] predicate[null])
[end]