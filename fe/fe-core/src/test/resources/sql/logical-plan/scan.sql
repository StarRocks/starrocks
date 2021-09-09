[sql]
select v1 from t0
[result]
logical project (col)
    logical scan
[end]

[sql]
select * from tall
[result]
logical project (col,col,col,col,col,col,col,col,col)
    logical scan
[end]

[sql]
select abs(v1) from t0
[result]
logical project (abs(col))
    logical scan
[end]

[sql]
select v1 from t0 where v1 = 1
[result]
logical project (col)
    logical filter (col = 1)
        logical scan
[end]

[sql]
select v1 from t0 where v1 = 1 and v2 > 2
[result]
logical project (col)
    logical filter (col = 1 AND col > 2)
        logical scan
[end]

[sql]
select v1 from t0 where v1 = 1 limit 10
[result]
logical limit (10)
    logical project (col)
        logical filter (col = 1)
            logical scan
[end]

[sql]
select v1 from t0 limit 10
[result]
logical limit (10)
    logical project (col)
        logical scan
[end]

[sql]
select * from t0 where !(v1 = 1 and v2 = 2 or v3 =3)
[result]
logical project (col,col,col)
    logical filter (col != 1 OR col != 2 AND col != 3)
        logical scan
[end]

[sql]
select * from t0 where v1 between 1 and 2
[result]
logical project (col,col,col)
    logical filter (col >= 1 AND col <= 2)
        logical scan
[end]

[sql]
select * from t0 where v1 >= 1 and v1 <=10 and v2 > 1 and v2 < 10 and v3 != 10 and v3 <=> 10;
[result]
logical project (col,col,col)
    logical filter (col >= 1 AND col <= 10 AND col > 1 AND col < 10 AND col != 10 AND col <=> 10)
        logical scan
[end]

[sql]
select v1 * v1 / v1 % v1 + v1 - v1 DIV v1 from t0
[result]
logical project (mod(cast(col * col as double) / cast(col as double), cast(col as double)) + cast(col as double) - cast(int_divide(col, col) as double))
    logical scan
[end]

[sql]
select v2&~v1|v3^1 from t0
[result]
logical project (bitxor(bitor(bitand(col, bitnot(col)), col), 1))
    logical scan
[end]

[sql]
select * from t0 where v1 in (1,2,3)
[result]
logical project (col,col,col)
    logical filter (col IN (1, 2, 3))
        logical scan
[end]

[sql]
select * from tall where ta like "%a";
[result]
logical project (col,col,col,col,col,col,col,col,col)
    logical filter (col LIKE %a)
        logical scan
[end]

[sql]
select cast(v1 as int) from t0
[result]
logical project (cast(col as int(11)))
    logical scan
[end]

[sql]
select v1+20, case v2 when v3 then 1 else 0 end from t0;
[result]
logical project (col + 20,CaseWhen(col, col, 1, 0))
    logical scan
[end]

[sql]
select 1, 1.1, '2020-01-01', '2020-01-01 00:00:00', 'a' from t0;
[result]
logical project (1,1.1,2020-01-01,2020-01-01 00:00:00,a)
    logical scan
[end]

[sql]
select v1, v2 from t0 where v3 is null
[result]
logical project (col,col)
    logical filter (3: v3 IS NULL)
        logical scan
[end]