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
[result]
TOP-N (order by [[29: substring ASC NULLS FIRST]])
    TOP-N (order by [[29: substring ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{30: count=count(30: count), 31: sum=sum(31: sum)}] group by [[29: substring]] having [null]
            EXCHANGE SHUFFLE[29]
                AGGREGATE ([LOCAL] aggregate [{30: count=count(), 31: sum=sum(6: c_acctbal)}] group by [[29: substring]] having [null]
                    LEFT ANTI JOIN (join-predicate [1: c_custkey = 21: o_custkey] post-join-predicate [null])
                        INNER JOIN (join-predicate [cast(6: c_acctbal as decimal128(38, 8)) > 17: avg] post-join-predicate [null])
                            SCAN (table[customer] columns[1: c_custkey, 5: c_phone, 6: c_acctbal] predicate[substring(5: c_phone, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                            EXCHANGE BROADCAST
                                ASSERT LE 1
                                    AGGREGATE ([GLOBAL] aggregate [{103: sum=sum(103: sum), 104: count=sum(104: count)}] group by [[]] having [null]
                                        EXCHANGE GATHER
                                            AGGREGATE ([LOCAL] aggregate [{103: sum=sum(46: c_sum), 104: count=sum(45: c_count)}] group by [[]] having [null]
                                                SCAN (mv[customer_agg_mv1] columns[43: c_acctbal, 44: substring_phone, 45: c_count, 46: c_sum] predicate[43: c_acctbal > 0.00 AND 44: substring_phone IN (21, 28, 24, 32, 35, 34, 37)])
                        EXCHANGE SHUFFLE[21]
                            SCAN (table[orders] columns[21: o_custkey] predicate[null])
[end]

