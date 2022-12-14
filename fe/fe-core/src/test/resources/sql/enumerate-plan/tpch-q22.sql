[sql]
select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone , 1  ,2) as cntrycode,
            c_acctbal
        from
            customer
        where
                substring(c_phone , 1  ,2)  in
                ('21', '28', '24', '32', '35', '34', '37')
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                    c_acctbal > 0.00
              and substring(c_phone , 1  ,2)  in
                  ('21', '28', '24', '32', '35', '34', '37')
        )
          and not exists (
                select
                    *
                from
                    orders
                where
                        o_custkey = c_custkey
            )
    ) as custsale
group by
    cntrycode
order by
    cntrycode ;
[planCount]
4
[plan-1]
TOP-N (order by [[32: substring ASC NULLS FIRST]])
    TOP-N (order by [[32: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{33: count=count(), 34: sum=sum(6: C_ACCTBAL)}] group by [[32: substring]] having [null]
            EXCHANGE SHUFFLE[32]
                LEFT ANTI JOIN (join-predicate [1: C_CUSTKEY = 22: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [6: C_ACCTBAL > 19: avg] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 5: C_PHONE, 6: C_ACCTBAL] predicate[substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                        EXCHANGE BROADCAST
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{19: avg=avg(15: C_ACCTBAL)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        SCAN (columns[14: C_PHONE, 15: C_ACCTBAL] predicate[15: C_ACCTBAL > 0.0 AND substring(14: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                    EXCHANGE SHUFFLE[22]
                        EXCHANGE SHUFFLE[22]
                            SCAN (columns[22: O_CUSTKEY] predicate[null])
[end]
[plan-2]
TOP-N (order by [[32: substring ASC NULLS FIRST]])
    TOP-N (order by [[32: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{33: count=count(), 34: sum=sum(6: C_ACCTBAL)}] group by [[32: substring]] having [null]
            EXCHANGE SHUFFLE[32]
                LEFT ANTI JOIN (join-predicate [1: C_CUSTKEY = 22: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [6: C_ACCTBAL > 19: avg] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 5: C_PHONE, 6: C_ACCTBAL] predicate[substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                        EXCHANGE BROADCAST
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{19: avg=avg(19: avg)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{19: avg=avg(15: C_ACCTBAL)}] group by [[]] having [null]
                                            SCAN (columns[14: C_PHONE, 15: C_ACCTBAL] predicate[15: C_ACCTBAL > 0.0 AND substring(14: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                    EXCHANGE SHUFFLE[22]
                        EXCHANGE SHUFFLE[22]
                            SCAN (columns[22: O_CUSTKEY] predicate[null])
[end]
[plan-3]
TOP-N (order by [[32: substring ASC NULLS FIRST]])
    TOP-N (order by [[32: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{33: count=count(33: count), 34: sum=sum(34: sum)}] group by [[32: substring]] having [null]
            EXCHANGE SHUFFLE[32]
                AGGREGATE ([LOCAL] aggregate [{33: count=count(), 34: sum=sum(6: C_ACCTBAL)}] group by [[32: substring]] having [null]
                    RIGHT ANTI JOIN (join-predicate [22: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[22]
                            SCAN (columns[22: O_CUSTKEY] predicate[null])
                        EXCHANGE SHUFFLE[1]
                            INNER JOIN (join-predicate [6: C_ACCTBAL > 19: avg] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 5: C_PHONE, 6: C_ACCTBAL] predicate[substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                                EXCHANGE BROADCAST
                                    ASSERT LE 1
                                        AGGREGATE ([GLOBAL] aggregate [{19: avg=avg(15: C_ACCTBAL)}] group by [[]] having [null]
                                            EXCHANGE GATHER
                                                SCAN (columns[14: C_PHONE, 15: C_ACCTBAL] predicate[15: C_ACCTBAL > 0.0 AND substring(14: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
[end]
[plan-4]
TOP-N (order by [[32: substring ASC NULLS FIRST]])
    TOP-N (order by [[32: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{33: count=count(33: count), 34: sum=sum(34: sum)}] group by [[32: substring]] having [null]
            EXCHANGE SHUFFLE[32]
                AGGREGATE ([LOCAL] aggregate [{33: count=count(), 34: sum=sum(6: C_ACCTBAL)}] group by [[32: substring]] having [null]
                    RIGHT ANTI JOIN (join-predicate [22: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[22]
                            SCAN (columns[22: O_CUSTKEY] predicate[null])
                        EXCHANGE SHUFFLE[1]
                            INNER JOIN (join-predicate [6: C_ACCTBAL > 19: avg] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 5: C_PHONE, 6: C_ACCTBAL] predicate[substring(5: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                                EXCHANGE BROADCAST
                                    ASSERT LE 1
                                        AGGREGATE ([GLOBAL] aggregate [{19: avg=avg(19: avg)}] group by [[]] having [null]
                                            EXCHANGE GATHER
                                                AGGREGATE ([LOCAL] aggregate [{19: avg=avg(15: C_ACCTBAL)}] group by [[]] having [null]
                                                    SCAN (columns[14: C_PHONE, 15: C_ACCTBAL] predicate[15: C_ACCTBAL > 0.0 AND substring(14: C_PHONE, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
[end]