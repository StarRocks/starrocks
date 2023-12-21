[result]
TOP-N (order by [[24: l_shipmode ASC NULLS FIRST]])
    TOP-N (order by [[24: l_shipmode ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{28: sum=sum(28: sum), 29: sum=sum(29: sum)}] group by [[24: l_shipmode]] having [null]
            EXCHANGE SHUFFLE[24]
                AGGREGATE ([LOCAL] aggregate [{28: sum=sum(26: case), 29: sum=sum(27: case)}] group by [[24: l_shipmode]] having [null]
                    SCAN (mv[lineitem_mv] columns[41: l_commitdate, 47: l_receiptdate, 49: l_shipdate, 51: l_shipmode, 55: o_orderpriority] predicate[47: l_receiptdate >= 1997-01-01 AND 47: l_receiptdate < 1998-01-01 AND 51: l_shipmode IN (MAIL, REG AIR) AND 41: l_commitdate < 47: l_receiptdate AND 41: l_commitdate > 49: l_shipdate])
[end]

