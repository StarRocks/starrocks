[sql]
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem,
    hive0.tpch.nation
where
    c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-05-01'
  and o_orderdate < date '1994-08-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc limit 20;
[result]
TOP-N (order by [[39: sum DESC NULLS LAST]])
    TOP-N (order by [[39: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{142: sum=sum(142: sum)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 35, 3, 8]
                AGGREGATE ([LOCAL] aggregate [{142: sum=sum(140: sum)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
                    INNER JOIN (join-predicate [4: c_nationkey = 34: n_nationkey] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: c_custkey = 10: o_custkey] post-join-predicate [null])
                            HIVE SCAN (columns{1,2,3,4,5,6,8} predicate[1: c_custkey IS NOT NULL])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [9: o_orderkey = 18: l_orderkey] post-join-predicate [null])
                                    HIVE SCAN (columns{9,10,13} predicate[13: o_orderdate >= 1994-05-01 AND 13: o_orderdate < 1994-08-01])
                                    EXCHANGE BROADCAST
                                        AGGREGATE ([GLOBAL] aggregate [{141: sum=sum(141: sum)}] group by [[84: l_orderkey]] having [null]
                                            EXCHANGE SHUFFLE[84]
                                                AGGREGATE ([LOCAL] aggregate [{141: sum=sum(94: sum_disc_price)}] group by [[84: l_orderkey]] having [null]
                                                    SCAN (mv[lineitem_agg_mv1] columns[84: l_orderkey, 86: l_returnflag, 94: sum_disc_price] predicate[86: l_returnflag = R])
                        EXCHANGE BROADCAST
                            HIVE SCAN (columns{34,35} predicate[34: n_nationkey IS NOT NULL])
[end]

