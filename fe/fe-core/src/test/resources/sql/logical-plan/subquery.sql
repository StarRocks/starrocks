[sql]
select k from (select v1 + 1 as k from t0) a
[result]
logical project (col)
    logical project (col + 1)
        logical project (col,col,col)
            logical scan
[end]

[sql]
select * from (select k2 from (select v1 as k1, v2 as k2 from t0) a) b
[result]
logical project (col)
    logical project (col)
        logical project (col,col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1 from t0 where v2 in (select td from tall)
[result]
logical project (col)
    logical apply (col IN (col))
        logical project (col,col,col)
            logical scan
        logical project (col)
            logical project (col,col,col,col,col,col,col,col,col)
                logical scan
[end]

[sql]
select v1 from t0 where exists (select td from tall where tc = v3)
[result]
logical project (col)
    logical apply (EXISTS col)
        logical project (col,col,col)
            logical scan
        logical project (col)
            logical filter (cast(col as bigint(20)) = col)
                logical project (col,col,col,col,col,col,col,col,col)
                    logical scan
[end]

[sql]
select v1 from t0 where v2 in (select td from tall where tc = v3) and v1 > 10
[result]
logical project (col)
    logical filter (col > 10)
        logical apply (col IN (col))
            logical project (col,col,col)
                logical scan
            logical project (col)
                logical filter (cast(col as bigint(20)) = col)
                    logical project (col,col,col,col,col,col,col,col,col)
                        logical scan
[end]

[sql]
select t2.v7 from t0,t2 where t0.v1 in (select v4 from t1 where v5 = t0.v2)
[result]
logical project (col)
    logical apply (col IN (col))
        logical project (col,col,col,col,col,col)
            logical cross join
                logical project (col,col,col)
                    logical scan
                logical project (col,col,col)
                    logical scan
        logical project (col)
            logical filter (col = col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select v1 from t0 where v2 in (select v4 from t1 where v5 = v3 and v6 in (select v7 from t2));
[result]
logical project (col)
    logical apply (col IN (col))
        logical project (col,col,col)
            logical scan
        logical project (col)
            logical filter (col = col)
                logical apply (col IN (col))
                    logical project (col,col,col)
                        logical scan
                    logical project (col)
                        logical project (col,col,col)
                            logical scan
[end]

[sql]
select v1 from t0 where v2 in (select v4 from t1) and v3 in (select v7 from t2);
[result]
logical project (col)
    logical apply (col IN (col))
        logical apply (col IN (col))
            logical project (col,col,col)
                logical scan
            logical project (col)
                logical project (col,col,col)
                    logical scan
        logical project (col)
            logical project (col,col,col)
                logical scan
[end]

[sql]
select v1 from t0 where v2 in (select v4 from t1) and v3 in (1, 2, 3);
[result]
logical project (col)
    logical filter (col IN (1, 2, 3))
        logical apply (col IN (col))
            logical project (col,col,col)
                logical scan
            logical project (col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select v1 from t0 where v2 in (select v4 from t1) and exists (select v7 from t2 where v3 = v8);
[result]
logical project (col)
    logical apply (EXISTS col)
        logical apply (col IN (col))
            logical project (col,col,col)
                logical scan
            logical project (col)
                logical project (col,col,col)
                    logical scan
        logical project (col)
            logical filter (col = col)
                logical project (col,col,col)
                    logical scan
[end]

[sql]
select v1 from t0 where v2 = (select v4 from t1) and (select sum(v7) from t2) = v3;
[result]
logical project (col)
    logical filter (col = col AND col = col)
        logical apply (col)
            logical apply (col)
                logical project (col,col,col)
                    logical scan
                logical project (col)
                    logical project (col,col,col)
                        logical scan
            logical project (col)
                logical aggregate () (sum(col))
                    logical project (col)
                        logical project (col,col,col)
                            logical scan
[end]

[sql]
select v1 from t0 where v1 = (select v4 from t1 where v2 = v5);
[result]
logical project (col)
    logical filter (col = col)
        logical apply (col)
            logical project (col,col,col)
                logical scan
            logical project (col)
                logical filter (col = col)
                    logical project (col,col,col)
                        logical scan
[end]

[sql]
select * from t0 where v1 in (select sum(v4) from t1 where v2= v4);
[except]
Getting analyzing error. Detail message: Unsupported correlated in predicate subquery with grouping or aggregation.
[end]

[sql]
select * from t0 where exists (select sum(v4) from t1 where v2= v4);
[result]
logical project (col,col,col)
    logical apply (EXISTS col)
        logical project (col,col,col)
            logical scan
        logical project (col)
            logical aggregate () (sum(col))
                logical project (col)
                    logical filter (col = col)
                        logical project (col,col,col)
                            logical scan
[end]

[sql]
select t0.v1 from t0 where exists (select t0.v1, t1.v4 from t1 where true)
[except]
Only support use correlated columns in the where clause of subqueries
[end]