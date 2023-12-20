[result]
TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
    TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{115: count=sum(115: count)}] group by [[58: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[58]
                AGGREGATE ([LOCAL] aggregate [{115: count=sum(59: order_count)}] group by [[58: o_orderpriority]] having [null]
                    SCAN (mv[query4_mv] columns[57: o_orderdate, 58: o_orderpriority, 59: order_count] predicate[57: o_orderdate >= 1994-09-01 AND 57: o_orderdate < 1994-12-01])
[end]

