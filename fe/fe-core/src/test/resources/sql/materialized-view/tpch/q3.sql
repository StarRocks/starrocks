[result]
TOP-N (order by [[35: sum DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[35: sum DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[19: l_orderkey, 10: o_orderdate, 16: o_shippriority]] having [null]
            EXCHANGE SHUFFLE[19, 10, 16]
                AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[19: l_orderkey, 10: o_orderdate, 16: o_shippriority]] having [null]
                    SCAN (mv[lineitem_mv] columns[49: c_mktsegment, 56: l_orderkey, 61: l_shipdate, 66: o_orderdate, 69: o_shippriority, 79: l_saleprice] predicate[66: o_orderdate < 1995-03-11 AND 61: l_shipdate > 1995-03-11 AND 49: c_mktsegment = HOUSEHOLD])
[end]

