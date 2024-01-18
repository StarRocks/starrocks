[result]
TOP-N (order by [[42: n_name ASC NULLS FIRST, 46: n_name ASC NULLS FIRST, 49: year ASC NULLS FIRST]])
    TOP-N (order by [[42: n_name ASC NULLS FIRST, 46: n_name ASC NULLS FIRST, 49: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{467: sum=sum(467: sum)}] group by [[151: n_name1, 152: n_name2, 153: l_shipyear]] having [null]
            EXCHANGE SHUFFLE[151, 152, 153]
                AGGREGATE ([LOCAL] aggregate [{467: sum=sum(154: sum_saleprice)}] group by [[151: n_name1, 152: n_name2, 153: l_shipyear]] having [null]
                    SCAN (mv[lineitem_mv_agg_mv2] columns[150: l_shipdate, 151: n_name1, 152: n_name2, 153: l_shipyear, 154: sum_saleprice] predicate[150: l_shipdate <= 1996-12-31 AND 150: l_shipdate >= 1995-01-01 AND 151: n_name1 = CANADA AND 152: n_name2 = IRAN OR 151: n_name1 = IRAN AND 152: n_name2 = CANADA])
[end]

