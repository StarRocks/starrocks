[sql]
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem
where
    c_mktsegment = 'HOUSEHOLD'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-11'
  and l_shipdate > date '1995-03-11'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate limit 10;
[result]
TOP-N (order by [[35: sum DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[35: sum DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{123: sum=sum(123: sum)}] group by [[18: l_orderkey, 13: o_orderdate, 16: o_shippriority]] having [null]
            EXCHANGE SHUFFLE[18, 13, 16]
                AGGREGATE ([LOCAL] aggregate [{123: sum=sum(121: sum)}] group by [[18: l_orderkey, 13: o_orderdate, 16: o_shippriority]] having [null]
                    INNER JOIN (join-predicate [10: o_custkey = 1: c_custkey] post-join-predicate [null])
                        INNER JOIN (join-predicate [9: o_orderkey = 18: l_orderkey] post-join-predicate [null])
                            HIVE SCAN (columns{9,10,13,16} predicate[13: o_orderdate < 1995-03-11])
                            EXCHANGE BROADCAST
                                AGGREGATE ([GLOBAL] aggregate [{122: sum=sum(122: sum)}] group by [[40: l_orderkey]] having [null]
                                    EXCHANGE SHUFFLE[40]
                                        AGGREGATE ([LOCAL] aggregate [{122: sum=sum(50: sum_disc_price)}] group by [[40: l_orderkey]] having [null]
                                            SCAN (mv[lineitem_agg_mv1] columns[40: l_orderkey, 41: l_shipdate, 50: sum_disc_price] predicate[41: l_shipdate > 1995-03-11])
                        EXCHANGE BROADCAST
                            HIVE SCAN (columns{1,7} predicate[7: c_mktsegment = HOUSEHOLD])
[end]

