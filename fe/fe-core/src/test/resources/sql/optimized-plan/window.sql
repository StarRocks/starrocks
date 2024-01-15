[sql]
select v1,v2,v3,row_number() over() from t0;
[result]
ANALYTIC ({4: row_number()=row_number()} [] [] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    EXCHANGE GATHER
        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 order by v3 range unbounded preceding) from t0;
[result]
ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 order by v3), avg(v1) over(partition by v2 order by v3) from t0;
[result]
ANALYTIC ({4: sum(1: v1)=sum(1: v1), 5: avg(1: v1)=avg(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 order by v3 rows between  1 preceding and 1 following), avg(v1) over(partition by v2 order by v3 rows between  1 preceding and 1 following) from t0;
[result]
ANALYTIC ({4: sum(1: v1)=sum(1: v1), 5: avg(1: v1)=avg(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2), avg(v1) over(partition by v2 order by v3) from t0;
[result]
ANALYTIC ({5: avg(1: v1)=avg(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [2: v2] [] )
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 order by v3), avg(v1) over(partition by v2) from t0;
[result]
ANALYTIC ({5: avg(1: v1)=avg(1: v1)} [2: v2] [] )
    ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(), sum(v1) over(partition by v2 order by v3) from t0;
[result]
ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [] [] )
    EXCHANGE GATHER
        ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
                EXCHANGE SHUFFLE[2]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(), sum(v1) over(order by v3) from t0;
[result]
ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [] [] )
        TOP-N (order by [[3: v3 ASC NULLS FIRST]])
            TOP-N (order by [[3: v3 ASC NULLS FIRST]])
                SCAN (columns[1: v1, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 order by v3), avg(v1) over(partition by v2 order by v3 rows current row) from t0;
[result]
ANALYTIC ({5: avg(1: v1)=avg(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] ROWS BETWEEN CURRENT ROW AND CURRENT ROW)
    ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v2) over(partition by v1) from t0
[result]
ANALYTIC ({4: sum(2: v2)=sum(2: v2)} [1: v1] [] )
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        SCAN (columns[1: v1, 2: v2] predicate[null])
[end]

[sql]
select v1, lead(v1) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lead(1: v1, 1, null)=lead(1: v1, 1, null)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1, lead(v1,3) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lead(1: v1, 3, null)=lead(1: v1, 3, null)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1, lead(v1, 3, 1) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lead(1: v1, 3, 1)=lead(1: v1, 3, 1)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1, lag(v1) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lag(1: v1, 1, null)=lag(1: v1, 1, null)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1, lag(v1,3) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lag(1: v1, 3, null)=lag(1: v1, 3, null)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 3 PRECEDING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select v1, lag(v1, 3, 1) over(order by v1) as s from t0;
[result]
ANALYTIC ({4: lag(1: v1, 3, 1)=lag(1: v1, 3, 1)} [] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND 3 PRECEDING)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select sum(v2) over (partition by v3 order by v1) from t0 group by v1,v2,v3;
[result]
ANALYTIC ({4: sum(2: v2)=sum(2: v2)} [3: v3] [1: v1 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[3: v3 ASC NULLS FIRST, 1: v1 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[3]
            AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: v1, 2: v2, 3: v3]] having [null]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2 + 1), avg(v1) over(partition by v2 order by v3) from t0;
[result]
ANALYTIC ({9: avg(4: v1)=avg(4: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[2]
            ANALYTIC ({8: sum(4: v1)=sum(4: v1)} [7: expr] [] )
                TOP-N (order by [[7: expr ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[7]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2), avg(v1) over(partition by v2+1 order by v3) from t0;
[result]
ANALYTIC ({9: avg(4: v1)=avg(4: v1)} [7: expr] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[7: expr ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[7]
            ANALYTIC ({8: sum(4: v1)=sum(4: v1)} [2: v2] [] )
                TOP-N (order by [[2: v2 ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[2]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select v1,sum(v2) over(partition by v2+1 order by v3+1) from t0;
[result]
ANALYTIC ({9: sum(5: v2)=sum(5: v2)} [7: expr] [8: expr ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[7: expr ASC NULLS FIRST, 8: expr ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[7]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v2+1 order by v3) from (select v1,v2,v3 from t0) a
[result]
ANALYTIC ({8: sum(4: v1)=sum(4: v1)} [7: expr] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[7: expr ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[7]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from (select v2,v3,row_number() over(partition by v3 order by v3) as s from t0) t where v2 =1;
[result]
PREDICATE 2: v2 = 1
    ANALYTIC ({4: row_number()=row_number()} [3: v3] [3: v3 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[3]
                SCAN (columns[2: v2, 3: v3] predicate[null])
[end]

[sql]
select * from (select v2,v3,row_number() over(partition by v3 order by v3) as s from t0) t where v3 =1;
[result]
ANALYTIC ({4: row_number()=row_number()} [3: v3] [3: v3 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[3]
            SCAN (columns[2: v2, 3: v3] predicate[3: v3 = 1])
[end]

[sql]
select * from (select v2,v3,row_number() over(partition by v3,v2 order by v3) as s from t0) t where v2 =v3;
[result]
ANALYTIC ({4: row_number()=row_number()} [3: v3, 2: v2] [3: v3 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[3: v3 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[3, 2]
            SCAN (columns[2: v2, 3: v3] predicate[2: v2 = 3: v3])
[end]

[sql]
select * from (select v2,v3,row_number() over(partition by v3,v2 order by v1) as s from t0) t where v2 =v3;
[result]
ANALYTIC ({4: row_number()=row_number()} [3: v3, 2: v2] [1: v1 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[3: v3 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST, 1: v1 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[3, 2]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 3: v3])
[end]

[sql]
select * from (select v2,v3,row_number() over(partition by v3,v1 order by v2) as s from t0) t where v2 =v3;
[result]
PREDICATE 2: v2 = 3: v3
    ANALYTIC ({4: row_number()=row_number()} [3: v3, 1: v1] [2: v2 ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[3: v3 ASC NULLS FIRST, 1: v1 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST]])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(order by v2), avg(v1) over(partition by v2, v3 order by v2) from t0
[result]
ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST]])
        TOP-N (order by [[2: v2 ASC NULLS FIRST]])
            ANALYTIC ({5: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[2, 3]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2, v3 order by v2), sum(v1) over(order by v2) from t0
[result]
ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST]])
        TOP-N (order by [[2: v2 ASC NULLS FIRST]])
            ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[2, 3]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2, v3 order by v2), sum(v1) over(partition by v2 order by v2) from t0
[result]
ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [2: v2] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2, v3 order by v2), sum(v1) over(partition by v2 order by v3) from t0
[result]
ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v3 order by v2), sum(v1) over(partition by v2 order by v3) from t0
[result]
ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        EXCHANGE SHUFFLE[2]
            ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                TOP-N (order by [[3: v3 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[3]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2,v3 order by v2), sum(v1) over(partition by v3 order by v2) from t0
[result]
ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            TOP-N (order by [[3: v3 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST]])
                EXCHANGE SHUFFLE[3]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select sum(v1) over(partition by v3 order by v2), avg(v1) over(partition by v2,v3 order by v2) from t0
[result]
ANALYTIC ({5: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
        ANALYTIC ({4: sum(1: v1)=sum(1: v1)} [3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            TOP-N (order by [[3: v3 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST]])
                EXCHANGE SHUFFLE[3]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2 order by v3), sum(v1) over(partition by v2 order by v2), max(v1) over(partition by v2 order by v3) from t0
[result]
ANALYTIC ({6: sum(1: v1)=sum(1: v1)} [2: v2] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({4: avg(1: v1)=avg(1: v1), 5: max(1: v1)=max(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            EXCHANGE SHUFFLE[2]
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2 order by v3), sum(v1) over(partition by v2 order by v2), sum(v1) over(partition by v2 order by v3 rows between  1 preceding and 1 following) from t0
[result]
ANALYTIC ({6: sum(1: v1)=sum(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
    ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [2: v2] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
                EXCHANGE SHUFFLE[2]
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v2,v3 order by v3), sum(v1) over(partition by v3 order by v3), max(v1) over(partition by v2, v3 order by v2) from t0
[result]
ANALYTIC ({6: max(1: v1)=max(1: v1)} [2: v2, 3: v3] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [2: v2, 3: v3] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [3: v3] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                TOP-N (order by [[3: v3 ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[3]
                        SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(partition by v1,v2 order by v3), sum(v1) over(partition by v1 order by v2) from t0
[result]
ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [1: v1, 2: v2] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [1: v1] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[1: v1 ASC NULLS FIRST, 2: v2 ASC NULLS FIRST, 3: v3 ASC NULLS FIRST]])
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select avg(v1) over(order by v3), sum(v1) over(order by v2) from t0
[result]
ANALYTIC ({5: sum(1: v1)=sum(1: v1)} [] [2: v2 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST]])
        ANALYTIC ({4: avg(1: v1)=avg(1: v1)} [] [3: v3 ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            TOP-N (order by [[3: v3 ASC NULLS FIRST]])
                TOP-N (order by [[3: v3 ASC NULLS FIRST]])
                    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select row_number() over(partition by v1) col from (select v1 from t0 order by v1) t;
[result]
ANALYTIC ({4: row_number()=row_number()} [1: v1] [] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select row_number() over(partition by v1) col from (select v1 from t0 order by v1 limit 1) t;
[result]
ANALYTIC ({4: row_number()=row_number()} [1: v1] [] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[1: v1 ASC NULLS FIRST]])
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select * from (select * from (select v1, row_number() over(partition by v1) as `rank` from t0) a where a.`rank` = 1) b where b.v1 = 1;
[result]
PREDICATE 4: row_number() = 1
    ANALYTIC ({4: row_number()=row_number()} [1: v1] [] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        TOP-N (order by [[1: v1 ASC NULLS FIRST]])
            TOP-N (order by [[1: v1 ASC NULLS FIRST]])
                SCAN (columns[1: v1] predicate[1: v1 = 1])
[end]

[sql]
select min(v1) over(partition by v2 order by v3 range between current row and unbounded following) from t0
[result]
ANALYTIC ({4: min(1: v1)=min(1: v1)} [2: v2] [3: v3 DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    TOP-N (order by [[2: v2 ASC NULLS FIRST, 3: v3 DESC NULLS LAST]])
        EXCHANGE SHUFFLE[2]
            SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
[end]

[sql]
select v1 from (select v1, row_number() over() from t0) temp limit 1;
[result]
EXCHANGE GATHER
    SCAN (columns[1: v1] predicate[null]) Limit 1
[end]

[sql]
select count(*) from (select v1, row_number() over (order by v2, v3) from t0) t
[result]
AGGREGATE ([GLOBAL] aggregate [{5: count=count(5: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{5: count=count()}] group by [[]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]

[sql]
select count(*) from (select v1, row_number() over (order by v2, v3) from t0 where v2 = 2) t
[result]
AGGREGATE ([GLOBAL] aggregate [{5: count=count(5: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{5: count=count()}] group by [[]] having [null]
            SCAN (columns[2: v2] predicate[2: v2 = 2])
[end]