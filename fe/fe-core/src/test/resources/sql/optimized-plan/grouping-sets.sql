[sql]
select sum(v1) from t0 having sum(v1) between 1 and 2
[result]
AGGREGATE ([GLOBAL] aggregate [{4: sum=sum(4: sum)}] group by [[]] having [4: sum <= 2 AND 4: sum >= 1]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{4: sum=sum(1: v1)}] group by [[]] having [null]
            SCAN (columns[1: v1] predicate[null])
[end]