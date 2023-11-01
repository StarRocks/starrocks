[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
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
TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
<<<<<<< HEAD
        AGGREGATE ([GLOBAL] aggregate [{52: sum=sum(52: sum)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
            EXCHANGE SHUFFLE[2, 1, 9, 10, 13]
                AGGREGATE ([LOCAL] aggregate [{52: sum=sum(23: l_quantity)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
                    LEFT SEMI JOIN (join-predicate [9: o_orderkey = 35: l_orderkey] post-join-predicate [null])
                        SCAN (mv[lineitem_mv] columns[67: c_name, 73: l_orderkey, 75: l_quantity, 82: o_custkey, 83: o_orderdate, 87: o_totalprice] predicate[null])
                        EXCHANGE BROADCAST
                            AGGREGATE ([GLOBAL] aggregate [{153: sum=sum(153: sum)}] group by [[110: l_orderkey]] having [153: sum > 315.00]
                                EXCHANGE SHUFFLE[110]
                                    AGGREGATE ([LOCAL] aggregate [{153: sum=sum(115: sum_qty)}] group by [[110: l_orderkey]] having [null]
                                        SCAN (mv[lineitem_agg_mv1] columns[110: l_orderkey, 115: sum_qty] predicate[null])
=======
        AGGREGATE ([GLOBAL] aggregate [{52: sum=sum(23: l_quantity)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
            LEFT SEMI JOIN (join-predicate [9: o_orderkey = 35: l_orderkey] post-join-predicate [null])
                EXCHANGE SHUFFLE[9]
                    SCAN (mv[lineitem_mv] columns[67: c_name, 73: l_orderkey, 75: l_quantity, 82: o_custkey, 83: o_orderdate, 87: o_totalprice] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{153: sum=sum(153: sum)}] group by [[110: l_orderkey]] having [153: sum > 315.00]
                    EXCHANGE SHUFFLE[110]
                        AGGREGATE ([LOCAL] aggregate [{153: sum=sum(115: sum_qty)}] group by [[110: l_orderkey]] having [null]
                            SCAN (mv[lineitem_agg_mv1] columns[110: l_orderkey, 115: sum_qty] predicate[null])
>>>>>>> ec13f480d6 ([Enhancement] update distribution info using projection (#33749))
[end]

