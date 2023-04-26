[sql]
select * from tarray cross join lateral unnest(v3)
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select v1 from tarray, lateral unnest(v3)
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v1,unnest from tarray, unnest(v3)
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v1,unnest from tarray, unnest(v3) where v2 = 2
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2])
[end]

[sql]
select v3,unnest from tarray, unnest(array_append(v3,1))
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[3: v3] predicate[null])
[end]

[sql]
select v1 from tarray, unnest(array_append(v3,1)) where v2 = 2
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2])
[end]

[sql]
select v1 from tarray, unnest(v3) where unnest = 1
[result]
PREDICATE 4: unnest = 1
    TABLE FUNCTION (unnest)
        SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v1 from tarray, unnest(v3) limit 5
[result]
EXCHANGE GATHER
    TABLE FUNCTION (unnest) LIMIT 5
        SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select v1,v2,unnest + 1 from t0,unnest([1,2,3]);
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select *,u from (select v1,v2,unnest + 1 as u from t0,unnest([1,2,3])) t;
[result]
TABLE FUNCTION (unnest)
    SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1,v2,unnest + 1 from t0,unnest([1,2,3]) where unnest = 1;
[result]
PREDICATE 4: unnest = 1
    TABLE FUNCTION (unnest)
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]
