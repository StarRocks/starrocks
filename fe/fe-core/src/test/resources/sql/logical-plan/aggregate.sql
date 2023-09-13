[sql]
select v1 as v2 from t0 group by v1, t0.v2;
[result]
logical project (col)
    logical aggregate (col,col) ()
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1 as v2, v2 from t0 group by v1, t0.v2;
[result]
logical project (col,col)
    logical aggregate (col,col) ()
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1 as v2 from t0 group by v1, v2;
[result]
logical project (col)
    logical aggregate (col,col) ()
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select sum(v1), sum(v2) from t0
[result]
logical project (col,col)
    logical aggregate () (sum(col),sum(col))
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1, sum(v2), sum(v3) from t0 group by v1
[result]
logical project (col,col,col)
    logical aggregate (col) (sum(col),sum(col))
        logical project (col,col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select sum(v1) + 1 from t0
[result]
logical project (col + 1)
    logical aggregate () (sum(col))
        logical project (col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select sum(v1) + sum(v2) from t0
[result]
logical project (col + col)
    logical aggregate () (sum(col),sum(col))
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1+1, sum(v2) from t0 group by v1+1
[result]
logical project (col,col)
    logical aggregate (col) (sum(col))
        logical project (col,col + 1)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select sum(v1) from t0 having sum(v1) > 0
[result]
logical project (col)
    logical filter (col > 0)
        logical aggregate () (sum(col))
            logical project (col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select sum(v1) from t0 having avg(v1) - avg(v2) > 10
[result]
logical project (col)
    logical filter (col - col > 10)
        logical aggregate () (sum(col),avg(col),avg(col))
            logical project (col,col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select v2,sum(v1) from t0 group by v2 having v2 > 0
[result]
logical project (col,col)
    logical filter (col > 0)
        logical aggregate (col) (sum(col))
            logical project (col,col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select sum(v1) from t0 where v2 > 2 having sum(v1) > 0;
[result]
logical project (col)
    logical filter (col > 0)
        logical aggregate () (sum(col))
            logical project (col)
                logical filter (col > 2)
                    logical project (col,col,col)
                        logical scan
[end]

[sql]
select v1,v3,max(v3) from t0 group by v1,v3;
[result]
logical project (col,col,col)
    logical aggregate (col,col) (max(col))
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1+1 as v from t0 group by v1 having v1+1= 1;
[result]
logical project (col + 1)
    logical filter (col = 0)
        logical aggregate (col) ()
            logical project (col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select count(*), sum(v1), v2, v1 from t0 group by v1, v2 having count(*) > 1 and sum(v1) > 1;
[result]
logical project (col,col,col,col)
    logical filter (col > 1 AND col > 1)
        logical aggregate (col,col) (count(),sum(col))
            logical project (col,col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
SELECT - v2 AS v1 FROM t0 GROUP BY v2, v1, v3 HAVING NOT + + v3 + - v1 >= + v3
[result]
logical project (-1 * col)
    logical filter (col + -1 * col < col)
        logical aggregate (col,col,col) ()
            logical project (col,col,col)
                logical project (col,col,col)
                    logical scan
[end]