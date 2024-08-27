[result]
TOP-N (order by [[49: sum DESC NULLS LAST]])
    TOP-N (order by [[49: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum)}] group by [[42: n_name]] having [null]
            EXCHANGE SHUFFLE[42]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(48: expr)}] group by [[42: n_name]] having [null]
                    SCAN (mv[lineitem_mv] columns[76: c_nationkey, 91: o_orderdate, 102: s_nationkey, 104: l_saleprice, 110: n_name2, 113: r_name2] predicate[102: s_nationkey = 76: c_nationkey AND 91: o_orderdate >= 1995-01-01 AND 91: o_orderdate < 1996-01-01 AND 113: r_name2 = AFRICA])
[end]