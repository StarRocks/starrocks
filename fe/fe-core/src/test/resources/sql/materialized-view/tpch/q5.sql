[result]
TOP-N (order by [[49: sum DESC NULLS LAST]])
    TOP-N (order by [[49: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum)}] group by [[42: n_name]] having [null]
            EXCHANGE SHUFFLE[42]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(48: expr)}] group by [[42: n_name]] having [null]
                    SCAN (mv[lineitem_mv] columns[78: c_nationkey, 104: s_nationkey, 106: l_saleprice, 112: n_name2, 115: r_name2] predicate[115: r_name2 = AFRICA AND 78: c_nationkey = 104: s_nationkey])
[end]

