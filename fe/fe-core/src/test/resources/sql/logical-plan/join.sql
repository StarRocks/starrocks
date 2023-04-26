[sql]
select v1, v2, ta, td from t0 inner join tall on t0.v1 = tall.td
[result]
logical project (col,col,col,col)
    logical project (col,col,col,col,col,col,col,col,col,col,col,col)
        logical inner join (col = col)
            logical project (col,col,col)
                logical scan
            logical project (col,col,col,col,col,col,col,col,col)
                logical scan
[end]

[sql]
select * from (select sum(v1) as v, sum(v2) from t0) a inner join (select v1,v2 from t0 order by v3) b on a.v = b.v2;
[result]
logical project (col,col,col,col)
    logical project (col,col,col,col)
        logical inner join (col = col)
            logical project (col,col)
                logical aggregate () (sum(col),sum(col))
                    logical project (col,col)
                        logical project (col,col,col)
                            logical scan
            logical project (col,col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select * from (select sum(v1) as v, sum(v2) from t0) a left semi join (select v1,v2 from t0 order by v3) b on a.v = b.v2;
[result]
logical project (col,col)
    logical project (col,col)
        logical left semi join (col = col)
            logical project (col,col)
                logical aggregate () (sum(col),sum(col))
                    logical project (col,col)
                        logical project (col,col,col)
                            logical scan
            logical project (col,col)
                logical project (col,col,col)
                    logical scan
[end]