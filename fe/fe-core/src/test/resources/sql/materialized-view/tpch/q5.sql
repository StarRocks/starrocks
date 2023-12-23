[result]
TOP-N (order by [[49: sum DESC NULLS LAST]])
    TOP-N (order by [[49: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum)}] group by [[42: n_name]] having [null]
            EXCHANGE SHUFFLE[42]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(48: expr)}] group by [[42: n_name]] having [null]
                    SCAN (mv[lineitem_mv] columns[81: c_nationkey, 96: o_orderdate, 107: s_nationkey, 109: l_saleprice, 115: n_name2, 118: r_name2] predicate[107: s_nationkey = 81: c_nationkey AND 96: o_orderdate >= 1995-01-01 AND 96: o_orderdate < 1996-01-01 AND 118: r_name2 = AFRICA])
[end]

