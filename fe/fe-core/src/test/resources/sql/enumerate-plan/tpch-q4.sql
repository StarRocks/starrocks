[planCount]
2
[plan-1]
TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
    TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{29: count=count()}] group by [[6: O_ORDERPRIORITY]] having [null]
            EXCHANGE SHUFFLE[6]
                LEFT SEMI JOIN (join-predicate [1: O_ORDERKEY = 11: L_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[1: O_ORDERKEY, 5: O_ORDERDATE, 6: O_ORDERPRIORITY] predicate[5: O_ORDERDATE >= 1994-09-01 AND 5: O_ORDERDATE < 1994-12-01])
                    EXCHANGE SHUFFLE[11]
                        SCAN (columns[22: L_COMMITDATE, 23: L_RECEIPTDATE, 11: L_ORDERKEY] predicate[23: L_RECEIPTDATE > 22: L_COMMITDATE])
[end]
[plan-2]
TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
    TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{29: count=count(29: count)}] group by [[6: O_ORDERPRIORITY]] having [null]
            EXCHANGE SHUFFLE[6]
                AGGREGATE ([LOCAL] aggregate [{29: count=count()}] group by [[6: O_ORDERPRIORITY]] having [null]
                    RIGHT SEMI JOIN (join-predicate [11: L_ORDERKEY = 1: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[22: L_COMMITDATE, 23: L_RECEIPTDATE, 11: L_ORDERKEY] predicate[23: L_RECEIPTDATE > 22: L_COMMITDATE])
                        EXCHANGE SHUFFLE[1]
                            SCAN (columns[1: O_ORDERKEY, 5: O_ORDERDATE, 6: O_ORDERPRIORITY] predicate[5: O_ORDERDATE >= 1994-09-01 AND 5: O_ORDERDATE < 1994-12-01])
[end]