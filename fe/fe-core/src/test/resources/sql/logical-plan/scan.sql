[sql]
select v1 * v1 / v1 % v1 + v1 - v1 DIV v1 from t0
[result]
logical project (mod(cast(col * col as double) / cast(col as double), cast(col as double)) + cast(col as double) - cast(int_divide(col, col) as double))
    logical project (col,col,col)
        logical scan
[end]

[sql]
select v2&~v1|v3^1 from t0
[result]
logical project (bitxor(bitor(bitand(col, bitnot(col)), col), 1))
    logical project (col,col,col)
        logical scan
[end]

[sql]
select * from t0 where v1 in (1,2,3)
[result]
logical project (col,col,col)
    logical filter (col IN (1, 2, 3))
        logical project (col,col,col)
            logical scan
[end]

[sql]
select * from tall where ta like "%a";
[result]
logical project (col,col,col,col,col,col,col,col,col)
    logical filter (col LIKE %a)
        logical project (col,col,col,col,col,col,col,col,col)
            logical scan
[end]

[sql]
select cast(v1 as int) from t0
[result]
logical project (cast(col as int(11)))
    logical project (col,col,col)
        logical scan
[end]

[sql]
select v1+20, case v2 when v3 then 1 else 0 end from t0;
[result]
logical project (col + 20,if(col = col, 1, 0))
    logical project (col,col,col)
        logical scan
[end]

[sql]
select 1, 1.1, '2020-01-01', '2020-01-01 00:00:00', 'a' from t0;
[result]
logical project (1,1.1,2020-01-01,2020-01-01 00:00:00,a)
    logical project (col,col,col)
        logical scan
[end]

[sql]
select v1, v2 from t0 where v3 is null
[result]
logical project (col,col)
    logical filter (3: v3 IS NULL)
        logical project (col,col,col)
            logical scan
[end]