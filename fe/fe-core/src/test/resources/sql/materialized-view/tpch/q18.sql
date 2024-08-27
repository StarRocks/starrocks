[result]
TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[13: o_totalprice DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{52: sum=sum(23: l_quantity)}] group by [[2: c_name, 1: c_custkey, 9: o_orderkey, 10: o_orderdate, 13: o_totalprice]] having [null]
            RIGHT SEMI JOIN (join-predicate [35: l_orderkey = 9: o_orderkey] post-join-predicate [null])
                AGGREGATE ([GLOBAL] aggregate [{153: sum=sum(153: sum)}] group by [[109: l_orderkey]] having [153: sum > 315.00]
                    EXCHANGE SHUFFLE[109]
                        AGGREGATE ([LOCAL] aggregate [{153: sum=sum(114: sum_qty)}] group by [[109: l_orderkey]] having [null]
                            SCAN (mv[lineitem_agg_mv1] columns[109: l_orderkey, 114: sum_qty] predicate[null])
                EXCHANGE SHUFFLE[9]
                    SCAN (mv[lineitem_mv] columns[60: c_name, 66: l_orderkey, 68: l_quantity, 75: o_custkey, 76: o_orderdate, 80: o_totalprice] predicate[null])
[end]

