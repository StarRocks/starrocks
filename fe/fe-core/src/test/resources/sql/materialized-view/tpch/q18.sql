
[result]
TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{52: sum=sum(23: l_quantity)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
            RIGHT SEMI JOIN (join-predicate [35: l_orderkey = 9: o_orderkey] post-join-predicate [null])
                EXCHANGE SHUFFLE[35]
                    AGGREGATE ([GLOBAL] aggregate [{140: sum=sum(135: sum_qty)}] group by [[130: l_orderkey]] having [140: sum > 315.00]
                        SCAN (mv[lineitem_agg_mv1] columns[130: l_orderkey, 135: sum_qty] predicate[null])
                EXCHANGE SHUFFLE[9]
                    SCAN (mv[lineitem_mv] columns[61: c_name, 67: l_orderkey, 69: l_quantity, 76: o_custkey, 77: o_orderdate, 81: o_totalprice] predicate[null])
[end]

