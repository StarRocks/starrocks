[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            hive0.tpch.lineitem
        group by
            l_orderkey having
            sum(l_quantity) > 315
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate limit 100;
[result]
TOP-N (order by [[12: o_totalprice DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[12: o_totalprice DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{143: sum=sum(143: sum)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 13: o_orderdate, 12: o_totalprice]] having [null]
            EXCHANGE SHUFFLE[2, 1, 9, 13, 12]
                TOP-N (order by [[12: o_totalprice DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
                    AGGREGATE ([LOCAL] aggregate [{143: sum=sum(138: sum)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 13: o_orderdate, 12: o_totalprice]] having [null]
                        INNER JOIN (join-predicate [1: c_custkey = 10: o_custkey] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                HIVE SCAN (columns{1,2} predicate[1: c_custkey IS NOT NULL])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [9: o_orderkey = 18: l_orderkey] post-join-predicate [null])
                                    LEFT SEMI JOIN (join-predicate [9: o_orderkey = 34: l_orderkey] post-join-predicate [null])
                                        HIVE SCAN (columns{9,10,12,13} predicate[10: o_custkey IS NOT NULL])
                                        EXCHANGE BROADCAST
                                            AGGREGATE ([GLOBAL] aggregate [{142: sum=sum(140: sum)}] group by [[34: l_orderkey]] having [142: sum > 315.00]
                                                AGGREGATE ([GLOBAL] aggregate [{141: sum=sum(141: sum)}] group by [[125: l_orderkey]] having [null]
                                                    EXCHANGE SHUFFLE[125]
                                                        AGGREGATE ([LOCAL] aggregate [{141: sum=sum(129: sum_qty)}] group by [[125: l_orderkey]] having [null]
                                                            SCAN (mv[lineitem_agg_mv1] columns[125: l_orderkey, 129: sum_qty] predicate[125: l_orderkey IS NOT NULL])
                                    EXCHANGE BROADCAST
                                        AGGREGATE ([GLOBAL] aggregate [{139: sum=sum(139: sum)}] group by [[125: l_orderkey]] having [null]
                                            EXCHANGE SHUFFLE[125]
                                                AGGREGATE ([LOCAL] aggregate [{139: sum=sum(129: sum_qty)}] group by [[125: l_orderkey]] having [null]
                                                    SCAN (mv[lineitem_agg_mv1] columns[125: l_orderkey, 129: sum_qty] predicate[null])
[end]

