[result]
TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
    TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[25: L_SHIPMODE]] having [null]
            EXCHANGE SHUFFLE[25]
                AGGREGATE ([LOCAL] aggregate [{30: sum=sum(28: case), 31: sum=sum(29: case)}] group by [[25: L_SHIPMODE]] having [null]
                    INNER JOIN (join-predicate [1: O_ORDERKEY = 11: L_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[1: O_ORDERKEY, 6: O_ORDERPRIORITY] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[21: L_SHIPDATE, 22: L_COMMITDATE, 23: L_RECEIPTDATE, 25: L_SHIPMODE, 11: L_ORDERKEY] predicate[25: L_SHIPMODE IN (REG AIR, MAIL) AND 22: L_COMMITDATE < 23: L_RECEIPTDATE AND 21: L_SHIPDATE < 22: L_COMMITDATE AND 23: L_RECEIPTDATE >= 1997-01-01 AND 23: L_RECEIPTDATE < 1998-01-01])
[end]

