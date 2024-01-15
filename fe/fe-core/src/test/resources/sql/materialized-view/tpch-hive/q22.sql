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
            hive0.tpch.customer
        where
                substring(c_phone , 1  ,2)  in
                ('21', '28', '24', '32', '35', '34', '37')
          and c_acctbal > (
            select
                avg(c_acctbal)
            from
                hive0.tpch.customer
            where
                    c_acctbal > 0.00
              and substring(c_phone , 1  ,2)  in
                  ('21', '28', '24', '32', '35', '34', '37')
        )
          and not exists (
                select
                    *
                from
                    hive0.tpch.orders
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
                    RIGHT ANTI JOIN (join-predicate [20: o_custkey = 1: c_custkey] post-join-predicate [null])
                        EXCHANGE SHUFFLE[20]
                            HIVE SCAN (columns{20} predicate[null])
                        EXCHANGE SHUFFLE[1]
                            INNER JOIN (join-predicate [cast(6: c_acctbal as DECIMAL128(38,8)) > 17: avg] post-join-predicate [null])
                                HIVE SCAN (columns{1,5,6} predicate[substring(5: c_phone, 1, 2) IN (21, 28, 24, 32, 35, 34, 37)])
                                EXCHANGE BROADCAST
                                    ASSERT LE 1
                                        AGGREGATE ([GLOBAL] aggregate [{95: sum=sum(95: sum), 96: count=sum(96: count)}] group by [[]] having [null]
                                            EXCHANGE GATHER
                                                AGGREGATE ([LOCAL] aggregate [{95: sum=sum(40: c_sum), 96: count=sum(39: c_count)}] group by [[]] having [null]
                                                    SCAN (mv[customer_agg_mv1] columns[37: c_acctbal, 38: substring_phone, 39: c_count, 40: c_sum] predicate[37: c_acctbal > 0.00 AND 38: substring_phone IN (21, 24, 28, 32, 34, 35, 37)])
[end]

