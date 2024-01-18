[result]
TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
    TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{1286: sum=sum(1286: sum)}] group by [[99: n_name2, 98: o_orderyear]] having [null]
            EXCHANGE SHUFFLE[99, 98]
                AGGREGATE ([LOCAL] aggregate [{1286: sum=sum(100: sum_amount)}] group by [[99: n_name2, 98: o_orderyear]] having [null]
                    SCAN (mv[lineitem_mv_agg_mv1] columns[97: p_name, 98: o_orderyear, 99: n_name2, 100: sum_amount] predicate[97: p_name LIKE %peru%])
[end]

