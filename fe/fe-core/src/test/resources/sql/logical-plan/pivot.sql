[sql]
select v3,
       sum(case v2 when 1 then v1 else null end) as `1`,
       sum(case v2 when 2 then v1 else null end) as `2`,
       sum(case v2 when 3 then v1 else null end) as `3`
    from t0
    group by v3;
[result]
logical project (col,col,col,col)
    logical aggregate (col) (sum(col),sum(col),sum(col))
        logical project (col,if(col = 1, col, null),if(col = 2, col, null),if(col = 3, col, null))
            logical project (col,col,col)
                logical scan
[end]

[sql]
select * from t0 pivot (sum(v1) for v2 in (1, 2, 3))
[result]
logical project (col,col,col,col)
    logical aggregate (col) (sum(col),sum(col),sum(col))
        logical project (col,if(col = 1, col, null),if(col = 2, col, null),if(col = 3, col, null))
            logical project (col,col,col)
                logical scan
[end]

[sql]
select * from t0 pivot (sum(v1) for v2 in (1, 2, 3)) order by v3
[result]
logical sort (col)
    logical project (col,col,col,col)
        logical project (col,col,col,col)
            logical aggregate (col) (sum(col),sum(col),sum(col))
                logical project (col,if(col = 1, col, null),if(col = 2, col, null),if(col = 3, col, null))
                    logical project (col,col,col)
                        logical scan
[end]

[sql]
select * from t0 pivot (sum(v1), count(*) for v2 in (1 as a ,2 as b,3 as c))
[result]
logical project (col,col,col,col,col,col,col)
    logical aggregate (col) (sum(col),count(col),sum(col),count(col),sum(col),count(col))
        logical project (col,if(col = 1, col, null),if(col = 1, 1, null),if(col = 2, col, null),if(col = 2, 1, null),if(col = 3, col, null),if(col = 3, 1, null))
            logical project (col,col,col)
                logical scan
[end]

[sql]
select * from t0 join t1 on v1 = v4 pivot (sum(v2) for v4 in (1.5, 2.5, 3.5))
[result]
logical project (col,col,col,col,col,col,col)
    logical aggregate (col,col,col,col) (sum(col),sum(col),sum(col))
        logical project (col,col,col,col,if(cast(col as decimal(20, 1)) = 1.5, col, null),if(cast(col as decimal(20, 1)) = 2.5, col, null),if(cast(col as decimal(20, 1)) = 3.5, col, null))
            logical project (col,col,col,col,col,col)
                logical inner join (col = col)
                    logical project (col,col,col)
                        logical scan
                    logical project (col,col,col)
                        logical scan
[end]

[sql]
select * from t0 join t1 on v1 = v4 pivot (sum(v3), count(*) for (v4, v1) in ((1,2), (3,4), (5,6)))
[result]
logical project (col,col,col,col,col,col,col,col,col)
    logical aggregate (col,col,col) (sum(col),count(col),sum(col),count(col),sum(col),count(col))
        logical project (col,col,col,if(col = 1 AND col = 2, col, null),if(col = 1 AND col = 2, 1, null),if(col = 3 AND col = 4, col, null),if(col = 3 AND col = 4, 1, null),if(col = 5 AND col = 6, col, null),if(col = 5 AND col = 6, 1, null))
            logical project (col,col,col,col,col,col)
                logical inner join (col = col)
                    logical project (col,col,col)
                        logical scan
                    logical project (col,col,col)
                        logical scan
[end]