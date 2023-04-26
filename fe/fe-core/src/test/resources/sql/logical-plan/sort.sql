/*
 * Simple sort
 */
[sql]
select v1 from t0 order by v1
[result]
logical sort (col)
    logical project (col)
        logical project (col,col,col)
            logical project (col,col,col)
                logical scan
[end]
[sql]
select v1,v2 from t0 order by v3
[result]
logical project (col,col)
    logical sort (col)
        logical project (col,col,col)
            logical project (col,col,col)
                logical project (col,col,col)
                    logical scan
[end]

/*
 * order by with limit
 */
[sql]
select v1 from t0 order by v1 limit 10
[result]
logical limit (10)
    logical sort (col)
        logical project (col)
            logical project (col,col,col)
                logical project (col,col,col)
                    logical scan
[end]

/*
 * order by with output scope reference
 */
[sql]
select v1+2 as v, * from t0 order by v+1;
[result]
logical project (col,col,col,col)
    logical sort (col)
        logical project (col,col,col,col,col + 1)
            logical project (col,col,col,col + 2)
                logical project (col,col,col)
                    logical scan
[end]

/*
 * order by aggregate function
 */
[sql]
select v1,sum(v2) from t0 group by v1 order by v1
[result]
logical sort (col)
    logical project (col,col)
        logical project (col,col)
            logical aggregate (col) (sum(col))
                logical project (col,col)
                    logical project (col,col,col)
                        logical scan
[end]
[sql]
select v1, sum(v2) from t0 group by v1 order by sum(v2)
[result]
logical sort (col)
    logical project (col,col)
        logical project (col,col)
            logical aggregate (col) (sum(col))
                logical project (col,col)
                    logical project (col,col,col)
                        logical scan
[end]
[sql]
select v1,sum(v2) from t0 group by v1 order by max(v3)
[result]
logical project (col,col)
    logical sort (col)
        logical project (col,col,col)
            logical project (col,col,col)
                logical aggregate (col) (sum(col),max(col))
                    logical project (col,col,col)
                        logical project (col,col,col)
                            logical scan
[end]
[sql]
select v1+1 as v from t0 group by v1+1 order by v
[result]
logical sort (col)
    logical project (col)
        logical project (col)
            logical aggregate (col) ()
                logical project (col + 1)
                    logical project (col,col,col)
                        logical scan
[end]
[sql]
select v1, sum(v2) as v from t0 group by v1 order by v+1;
[result]
logical project (col,col)
    logical sort (col)
        logical project (col,col,col + 1)
            logical project (col,col)
                logical aggregate (col) (sum(col))
                    logical project (col,col)
                        logical project (col,col,col)
                            logical scan
[end]

[sql]
select v1 as v, sum(v2) + 1 as k from t0 group by v1 order by k;
[result]
logical sort (col)
    logical project (col,col)
        logical project (col,col + 1)
            logical aggregate (col) (sum(col))
                logical project (col,col)
                    logical project (col,col,col)
                        logical scan
[end]
