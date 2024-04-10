[result]
TOP-N (order by [[61: year ASC NULLS FIRST]])
    TOP-N (order by [[61: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{65: sum=sum(65: sum), 64: sum=sum(64: sum)}] group by [[61: year]] having [null]
            EXCHANGE SHUFFLE[61]
                AGGREGATE ([LOCAL] aggregate [{65: sum=sum(62: expr), 64: sum=sum(63: case)}] group by [[61: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[87: o_orderdate, 96: p_type, 100: l_saleprice, 103: o_orderyear, 104: n_name1, 109: r_name2] predicate[109: r_name2 = MIDDLE EAST AND 87: o_orderdate <= 1996-12-31 AND 87: o_orderdate >= 1995-01-01 AND 96: p_type = ECONOMY ANODIZED STEEL])
[end]
