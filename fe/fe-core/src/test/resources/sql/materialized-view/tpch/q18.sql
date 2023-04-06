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
        AGGREGATE ([GLOBAL] aggregate [{52: sum=sum(23: l_quantity)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
            LEFT SEMI JOIN (join-predicate [9: o_orderkey = 35: l_orderkey] post-join-predicate [null])
                EXCHANGE SHUFFLE[9]
                    SCAN (mv[lineitem_mv] columns[111: c_name, 117: l_orderkey, 119: l_quantity, 126: o_custkey, 127: o_orderdate, 131: o_totalprice] predicate[null])
                EXCHANGE SHUFFLE[35]
                    AGGREGATE ([GLOBAL] aggregate [{163: sum=sum(163: sum)}] group by [[53: l_orderkey]] having [163: sum > 315]
                        EXCHANGE SHUFFLE[53]
                            AGGREGATE ([LOCAL] aggregate [{163: sum=sum(57: sum_qty)}] group by [[53: l_orderkey]] having [null]
                                SCAN (mv[lineitem_agg_mv1] columns[53: l_orderkey, 57: sum_qty] predicate[null])
[end]

